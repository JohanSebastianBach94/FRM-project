"""
Check Block Coverage - Validate series availability for DCC-GARCH blocks

Reads country_blocks_extended.yaml and merge_report.json to verify which series
are available for each country block before running DCC-GARCH pipeline.

Usage:
    python scripts/check_block_coverage.py
    
Output:
    - Console report showing coverage per block
    - analysis_outputs/block_coverage_report.json
"""

import yaml
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np

import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.coverage_calendar import (
    get_trading_calendar,
    calculate_window_coverage,
)

def _select_series_catalog_path() -> Path:
    """Pick the catalog that contains series-level coverage metadata.

    Note: `data_repository/catalog.csv` is a dataset registry (different schema)
    and should not be used for coverage_ratio filtering.
    """

    candidates = [
        ROOT / "catalog.csv",
        ROOT / "data_repository" / "catalog.csv",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            with candidate.open("r", encoding="utf-8", errors="ignore") as fh:
                header = (fh.readline() or "").strip()
        except OSError:
            continue
        if not header:
            continue
        cols = {c.strip() for c in header.split(",") if c.strip()}
        if {"series", "coverage_ratio"}.issubset(cols):
            return candidate
    return ROOT / "catalog.csv"


CATALOG_PATH = _select_series_catalog_path()

THRESHOLD_CONFIG_PATH = ROOT / "analysis_outputs" / "coverage_threshold_config.json"


def _load_threshold_config(
    default_series: float = 0.62,
    default_block: float = 0.7,
    default_window_years: int = 10,
) -> dict[str, float | int]:
    if not THRESHOLD_CONFIG_PATH.exists():
        return {
            "series_threshold": default_series,
            "block_threshold": default_block,
            "coverage_window_years": default_window_years,
        }
    try:
        payload = json.loads(THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {
            "series_threshold": default_series,
            "block_threshold": default_block,
            "coverage_window_years": default_window_years,
        }

    series_value = payload.get("series_threshold", payload.get("threshold", default_series))
    block_value = payload.get("block_threshold")
    try:
        series_threshold = float(series_value)
    except Exception:
        series_threshold = default_series

    if block_value is None:
        has_series_threshold = "series_threshold" in payload or "threshold" in payload
        block_value = series_threshold if has_series_threshold else default_block

    try:
        block_threshold = float(block_value)
    except Exception:
        block_threshold = default_block

    window_years_value = payload.get("coverage_window_years", default_window_years)
    try:
        window_years = int(window_years_value)
    except Exception:
        window_years = default_window_years
    if window_years <= 0:
        window_years = default_window_years

    return {
        "series_threshold": series_threshold,
        "block_threshold": block_threshold,
        "coverage_window_years": window_years,
    }


def _load_catalog_metadata(path: Path) -> tuple[set[str], dict[str, float]]:
    """Read catalog metadata we need for coverage filtering."""
    if not path.exists():
        return set(), {}
    try:
        df = pd.read_csv(path, dtype=str)
    except Exception:
        return set(), {}

    do_not_use = set()
    if "series" in df.columns and "do_not_use" in df.columns:
        mask = (
            df["do_not_use"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
            == "DO NOT USE"
        )
        do_not_use = set(df.loc[mask & df["series"].notna(), "series"].astype(str))

    coverage_map: dict[str, float] = {}
    if "series" in df.columns and "coverage_ratio" in df.columns:
        ratios = pd.to_numeric(df["coverage_ratio"], errors="coerce").fillna(0.0)
        for name, ratio in zip(df["series"].astype(str), ratios):
            series_name = name.strip()
            if series_name:
                coverage_map[series_name] = float(ratio)

    return do_not_use, coverage_map


def _filter_codes(codes: list, included: set[str], suppressed: set[str]) -> list[str]:
    return [
        code for code in codes
        if isinstance(code, str)
        and code
        and code not in suppressed
        and code in included
    ]

print("=" * 80)
print("BLOCK COVERAGE VALIDATION")
print("=" * 80)

THRESHOLD_CONFIG = _load_threshold_config()
SERIES_THRESHOLD = THRESHOLD_CONFIG["series_threshold"]
BLOCK_THRESHOLD = THRESHOLD_CONFIG["block_threshold"]
# Load country blocks configuration
print("\n[1/3] Loading country_blocks_extended.yaml...")
with open("config/country_blocks_extended.yaml", "r") as f:
    config = yaml.safe_load(f)

# Canonical view: full stress panel used for block design
print("[2/3] Loading canonical stress panel (full coverage view)...")
try:
    df_full = pd.read_csv("stress_indicators_expanded.csv", index_col=0, parse_dates=True)
    canonical_dataset = "stress_indicators_expanded.csv"
except FileNotFoundError:
    df_full = pd.read_csv("data/stress_indicators_expanded.csv", index_col=0, parse_dates=True)
    canonical_dataset = "data/stress_indicators_expanded.csv"

df_full.sort_index(inplace=True)

coverage_frame = df_full.copy()

trading_calendar_start = "1990-02-01"
if THRESHOLD_CONFIG_PATH.exists():
    try:
        payload = json.loads(THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
        trading_calendar_start = str(payload.get("trading_calendar_start") or trading_calendar_start)
    except Exception:
        trading_calendar_start = trading_calendar_start

TRADING_CALENDAR = get_trading_calendar(start=trading_calendar_start)
# Determine trailing-coverage health per series (last N years).
window_years = int(THRESHOLD_CONFIG.get("coverage_window_years", 10))
window_end = TRADING_CALENDAR[-1]
window_start = window_end - pd.DateOffset(years=window_years)

if window_end not in coverage_frame.index:
    coverage_frame = coverage_frame.reindex(coverage_frame.index.union([window_end]))

auto_extended_series = []
for col in coverage_frame.columns:
    col_series = coverage_frame[col]
    non_zero_values = col_series.dropna()
    if non_zero_values.empty:
        continue
    if np.isclose(non_zero_values.abs().sum(), 0.0):
        continue
    last_valid = col_series.last_valid_index()
    if last_valid is None or last_valid >= window_end:
        continue
    coverage_frame.at[window_end, col] = col_series.loc[last_valid]
    auto_extended_series.append(col)

coverage_frame.sort_index(inplace=True)
if auto_extended_series:
    print(
        f"Auto-extended trailing coverage for {len(auto_extended_series)} series "
        "(forward-filled final observation for coverage checks)"
    )

def evaluate_trailing_coverage(values: pd.Series, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Frequency-aware coverage check for the most recent window_years."""

    series = values.dropna()
    if series.empty:
        return {
            "has_window_coverage": False,
            "coverage_ratio": 0.0,
            "median_gap_days": None,
            "last_observation": None,
            "reason": "no_data",
        }

    window_series = series[(series.index >= start) & (series.index <= end)]
    if window_series.empty:
        return {
            "has_window_coverage": False,
            "coverage_ratio": 0.0,
            "median_gap_days": None,
            "last_observation": series.index.max().isoformat(),
            "reason": "no_recent_data",
        }

    diffs = window_series.index.to_series().diff().dropna().dt.total_seconds() / 86400.0
    if diffs.empty:
        median_gap_days = 365.0
    else:
        median_gap_days = max(float(diffs.median()), 1.0)

    if median_gap_days <= 3:
        freq_label = "daily"
        tolerance_days = 60
    elif median_gap_days <= 10:
        freq_label = "weekly"
        tolerance_days = 120
    elif median_gap_days <= 45:
        freq_label = "monthly"
        tolerance_days = 240
    elif median_gap_days <= 120:
        freq_label = "quarterly"
        tolerance_days = 420
    else:
        freq_label = "annual"
        tolerance_days = 540

    coverage_ratio = calculate_window_coverage(series, TRADING_CALENDAR, start, end)

    last_obs = window_series.index.max()
    first_obs = window_series.index.min()

    has_recent_obs = (end - last_obs).days <= tolerance_days
    covers_window_start = first_obs <= start + pd.Timedelta(days=tolerance_days)

    has_window_coverage = coverage_ratio >= SERIES_THRESHOLD and has_recent_obs and covers_window_start

    return {
        "has_window_coverage": bool(has_window_coverage),
        "coverage_ratio": float(coverage_ratio),
        "median_gap_days": float(median_gap_days),
        "frequency_label": freq_label,
        "last_observation": last_obs.isoformat(),
        "reason": None if has_window_coverage else "insufficient_trailing_coverage",
    }


do_not_use_series, _ = _load_catalog_metadata(CATALOG_PATH)

alias_map = {}
alias_path = Path("config/series_alias_map.yaml")
if alias_path.exists():
    with open(alias_path, "r") as alias_file:
        alias_payload = yaml.safe_load(alias_file) or {}
        alias_map = alias_payload.get("aliases", {}) or {}

candidate_columns = [
    col for col in coverage_frame.columns if col not in do_not_use_series
]

series_trailing_coverage = {
    col: evaluate_trailing_coverage(coverage_frame[col], window_start, window_end)
    for col in candidate_columns
}

if alias_map:
    for source_name, target_name in alias_map.items():
        if source_name in do_not_use_series or target_name in do_not_use_series:
            continue
        if target_name in series_trailing_coverage and source_name not in series_trailing_coverage:
            series_trailing_coverage[source_name] = series_trailing_coverage[target_name]

available_series = {
    col for col, meta in series_trailing_coverage.items()
    if meta.get("coverage_ratio", 0.0) >= SERIES_THRESHOLD
}

print(f"Found {len(available_series)} available series in {canonical_dataset} (canonical view)\n")
if do_not_use_series:
    print(f"Skipped {len(do_not_use_series)} catalog entries marked DO NOT USE\n")
print(f"Series inclusion threshold: {SERIES_THRESHOLD:.2f}")
print(f"Block coverage threshold: {BLOCK_THRESHOLD * 100:.0f}% of required series must be present")


def _describe_series(code: str) -> str:
    coverage_meta = series_trailing_coverage.get(code, {})
    extras = []
    ratio = coverage_meta.get("coverage_ratio")
    if ratio is not None:
        extras.append(f"coverage={ratio:.2f}")
    last_obs = coverage_meta.get("last_observation")
    if last_obs:
        extras.append(f"last={last_obs.split('T')[0]}")
    reason = coverage_meta.get("reason")
    if reason:
        extras.append(reason)
    if extras:
        return f"{code} ({', '.join(extras)})"
    return code

# Check coverage for each country and block
coverage_report = {
    "check_date": datetime.now().isoformat(),
    "total_available_series": len(available_series),
    "canonical_dataset": canonical_dataset,
    "coverage_config": {
        "series_threshold": SERIES_THRESHOLD,
        "block_threshold": BLOCK_THRESHOLD,
        "coverage_window_years": window_years,
        "window_start": window_start.date().isoformat(),
        "window_end": window_end.date().isoformat(),
        "trading_calendar_end": pd.Timestamp(TRADING_CALENDAR[-1]).date().isoformat() if len(TRADING_CALENDAR) else None,
        "catalog_path": str(CATALOG_PATH.relative_to(ROOT)) if CATALOG_PATH.exists() else str(CATALOG_PATH),
        "threshold_config_path": str(THRESHOLD_CONFIG_PATH.relative_to(ROOT)) if THRESHOLD_CONFIG_PATH.exists() else str(THRESHOLD_CONFIG_PATH),
    },
    "countries": []
}

print("=" * 80)
print("COVERAGE BY COUNTRY AND BLOCK")
print("=" * 80)

holes_feed_rows = []

for country in config["country_blocks"]:
    country_name = country["country"]
    iso_code = country["iso_code"]
    
    print(f"\n{country_name} ({iso_code})")
    print("-" * 60)
    
    country_report = {
        "country": country_name,
        "iso_code": iso_code,
        "blocks": []
    }
    
    for block in country["blocks"]:
        block_key = block["key"]
        raw_required = block.get("series_codes") or []
        raw_optional = block.get("optional_series_codes") or []
        required = _filter_codes(raw_required, available_series, do_not_use_series)
        optional = _filter_codes(raw_optional, available_series, do_not_use_series)
        
        # Check which series are present
        required_present = []
        required_missing = []
        required_insufficient = []
        optional_present = []
        optional_missing = []
        optional_insufficient = []

        for series_code in required:
            if series_code not in available_series:
                required_missing.append(series_code)
                continue
            coverage_meta = series_trailing_coverage.get(series_code, {"has_window_coverage": False})
            if coverage_meta.get("has_window_coverage"):
                required_present.append(series_code)
            else:
                required_insufficient.append(series_code)

        for series_code in optional:
            if series_code not in available_series:
                optional_missing.append(series_code)
                continue
            coverage_meta = series_trailing_coverage.get(series_code, {"has_window_coverage": False})
            if coverage_meta.get("has_window_coverage"):
                optional_present.append(series_code)
            else:
                optional_insufficient.append(series_code)
        
        # Calculate coverage percentage
        total_required = len(required)
        coverage_pct = (len(required_present) / total_required * 100) if total_required > 0 else 100
        coverage_threshold_pct = BLOCK_THRESHOLD * 100

        # Status indicator uses the configured block coverage threshold
        if coverage_pct >= coverage_threshold_pct and not required_missing and not required_insufficient:
            status = "READY"
        elif coverage_pct >= coverage_threshold_pct:
            status = "PARTIAL"
        else:
            status = "INSUFFICIENT"
        
        print(f"  {block_key:20} {status:15} {len(required_present)}/{total_required} required, {len(optional_present)}/{len(optional)} optional")
        
        if required_missing:
            print(f"    Missing required (no data): {', '.join(required_missing[:3])}")
            if len(required_missing) > 3:
                print(f"    ... and {len(required_missing) - 3} more")
        if required_insufficient:
            details = [_describe_series(code) for code in required_insufficient]
            print(f"    Insufficient coverage (required): {', '.join(details[:3])}")
            if len(details) > 3:
                print(f"    ... and {len(details) - 3} more")
        
        if optional_missing:
            if len(optional_missing) <= 5:
                print(f"    Missing optional (no data): {', '.join(optional_missing)}")
            else:
                print(f"    Missing {len(optional_missing)} optional series")
        if optional_insufficient:
            preview_opt = ", ".join([_describe_series(code) for code in optional_insufficient[:3]])
            more_opt = f" ...(+{len(optional_insufficient) - 3})" if len(optional_insufficient) > 3 else ""
            print(f"    Insufficient coverage (optional): {preview_opt}{more_opt}")
        
        block_report = {
            "key": block_key,
            "required_series": total_required,
            "required_present": len(required_present),
            "required_missing": len(required_missing),
            "optional_series": len(optional),
            "optional_present": len(optional_present),
            "optional_missing": len(optional_missing),
            "coverage_percentage": round(coverage_pct, 1),
            "status": status,
            "missing_required_list": required_missing,
            "missing_optional_list": optional_missing,
            "insufficient_required_list": required_insufficient,
            "insufficient_optional_list": optional_insufficient,
        }
        
        country_report["blocks"].append(block_report)

        if required_missing or required_insufficient:
            holes_feed_rows.append(
                {
                    "iso": iso_code,
                    "country": country_name,
                    "block": block_key,
                    "status": status,
                    "required_present": len(required_present),
                    "required_missing": len(required_missing),
                    "optional_present": len(optional_present),
                    "optional_missing": len(optional_missing),
                    "missing_required_list": "|".join(required_missing),
                    "missing_optional_list": "|".join(optional_missing),
                    "insufficient_required_list": "|".join(required_insufficient),
                    "insufficient_optional_list": "|".join(optional_insufficient),
                    "insufficient_required_details": "|".join(
                        [_describe_series(code) for code in required_insufficient]
                    ),
                    "insufficient_optional_details": "|".join(
                        [_describe_series(code) for code in optional_insufficient]
                    ),
                    "coverage_threshold": coverage_threshold_pct,
                    "series_threshold": SERIES_THRESHOLD,
                }
            )
    
    coverage_report["countries"].append(country_report)

# Save report
Path("analysis_outputs").mkdir(exist_ok=True)
output_file = "analysis_outputs/block_coverage_report.json"
with open(output_file, "w") as f:
    json.dump(coverage_report, f, indent=2)

print(f"\n{'=' * 80}")
print("SUMMARY")
print(f"{'=' * 80}")

total_blocks = sum(len(c["blocks"]) for c in coverage_report["countries"])
ready_blocks = sum(
    1 for c in coverage_report["countries"] 
    for b in c["blocks"] 
    if b["status"] == "READY"
)
partial_blocks = sum(
    1 for c in coverage_report["countries"] 
    for b in c["blocks"] 
    if b["status"] == "PARTIAL"
)
insufficient_blocks = sum(
    1 for c in coverage_report["countries"] 
    for b in c["blocks"] 
    if b["status"] == "INSUFFICIENT"
)

block_threshold_pct = BLOCK_THRESHOLD * 100
print(f"Block coverage requirement: {block_threshold_pct:.0f}% of required series must be available")
print(f"Series inclusion threshold (catalog coverage): {SERIES_THRESHOLD:.2f}")

print(f"Total blocks checked: {total_blocks}")
print(f"  Ready (all required series): {ready_blocks}")
print(f"  Partial (>={block_threshold_pct:.0f}% coverage): {partial_blocks}")
print(f"  Insufficient (<{block_threshold_pct:.0f}% coverage): {insufficient_blocks}")

print(f"\n[SAVED] {output_file}")

feed_path = Path("analysis_outputs") / "risk_factor_holes_feed.csv"
pd.DataFrame(holes_feed_rows).to_csv(feed_path, index=False)
print(f"[SAVED] {feed_path}")


def evaluate_health(series_name: str, values: pd.Series) -> dict:
    cleaned = values.dropna()
    coverage = float(
        calculate_window_coverage(values, TRADING_CALENDAR, window_start, window_end)
    )
    flagged = []
    if coverage < 0.62:
        flagged.append("low_coverage")
    if cleaned.empty:
        flagged.append("no_data")
        mean = std = min_val = max_val = np.nan
    else:
        mean = float(cleaned.mean())
        std = float(cleaned.std())
        min_val = float(cleaned.min())
        max_val = float(cleaned.max())
        if np.isclose(std, 0.0):
            flagged.append("flat_series")
        if not np.isfinite(min_val) or not np.isfinite(max_val):
            flagged.append("non_finite_value")
        if max(abs(min_val), abs(max_val)) > 1e6:
            flagged.append("extreme_magnitude")
        lower_name = series_name.lower()
        if "spread" in lower_name and min_val < -1e-6:
            flagged.append("spread_negative")
        if "gdp" in lower_name and min_val <= 0:
            flagged.append("gdp_nonpositive")
        if "rate" in lower_name and max_val > 100:
            flagged.append("rate_implausible")

    return {
        "series": series_name,
        "coverage": coverage,
        "mean": mean,
        "std": std,
        "min": min_val,
        "max": max_val,
        "flags": "|".join(flagged),
        "flagged": bool(flagged),
    }


health_rows = [evaluate_health(col, df_full[col]) for col in df_full.columns]
health_path = Path("analysis_outputs") / "risk_factor_health.csv"
pd.DataFrame(health_rows).to_csv(health_path, index=False)
print(f"[SAVED] {health_path}")

# Determine if pipeline can proceed
if insufficient_blocks > 0:
    print(f"\n[WARNING] {insufficient_blocks} blocks have insufficient coverage (<{block_threshold_pct:.0f}% )")
    print("Consider adding proxies or marking additional series as optional")
else:
    print("\n[SUCCESS] All blocks have sufficient coverage to proceed with DCC-GARCH")

print("\n" + "=" * 80)
