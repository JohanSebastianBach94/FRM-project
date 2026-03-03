
"""
Merge Industry Data with Existing Pipeline
Combines stress_indicators.csv (72 series) + industry_data_raw.csv (52 series)
Output: stress_indicators_expanded.csv (124 series)
"""

import pandas as pd
import numpy as np
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional
import yaml
import json
import os

BASE_DIR = Path(__file__).resolve().parent
SERIES_METADATA_PATH = BASE_DIR / "config" / "series_metadata.yaml"
DEFAULT_SERIES_METADATA = {"frequency": "daily", "measurement_type": "level"}
DIAGNOSTIC_OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "diagnostics"


def _normalize_datetime_index(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """Ensure the dataframe index is a clean DatetimeIndex."""
    idx_series = pd.Series(df.index.astype(str), index=df.index)
    converted = pd.to_datetime(idx_series, errors="coerce")

    if converted.isna().any():
        fallback = pd.to_datetime(idx_series.str.slice(0, 10), errors="coerce")
        fixed = converted.isna() & fallback.notna()
        if fixed.any():
            converted = converted.where(~fixed, fallback)
            print(f"  [INFO] Normalized {fixed.sum()} index values with date-only fallback in {label}")

    missing_mask = converted.isna()
    if missing_mask.any():
        dropped = int(missing_mask.sum())
        df = df.loc[~missing_mask].copy()
        converted = converted[~missing_mask]
        print(f"  [WARNING] Dropped {dropped} rows with non-date index values in {label}")

    converted_index = pd.Index(converted)
    dup_mask = converted_index.duplicated(keep="first")
    if dup_mask.any():
        duplicates = int(dup_mask.sum())
        df = df.loc[~dup_mask].copy()
        converted_index = converted_index[~dup_mask]
        print(f"  [INFO] Removed {duplicates} duplicate date rows in {label}")

    df.index = pd.DatetimeIndex(converted_index)
    return df


DATE_CANDIDATES = [
    "date",
    "Date",
    "DATE",
    "period",
    "Period",
    "PERIOD",
    "time",
    "Time",
    "TIME",
    "observation_date",
    "Month",
    "month",
    "year",
    "Year",
    "YEAR",
]

VALUE_CANDIDATES = [
    "value",
    "Value",
    "VALUE",
    "close",
    "Close",
    "CLOSE",
    "price",
    "Price",
    "spread",
    "Spread",
    "rate",
    "Rate",
    "yield",
    "Yield",
    "cds_proxy",
]


def _coerce_datetime(values: pd.Series) -> pd.DatetimeIndex:
    text = values.astype(str)
    # Special-case years (e.g., 1990, 1991) stored as numbers/strings.
    try:
        numeric = pd.to_numeric(text, errors="coerce")
        if numeric.notna().all() and numeric.between(1800, 2200).all():
            years = numeric.astype(int).astype(str)
            return pd.to_datetime(years + "-12-31", errors="coerce")
    except Exception:
        pass

    if text.str.contains("Q").any():
        try:
            period_index = pd.PeriodIndex(text, freq="Q", name="date")
            # Normalize to midnight so timestamps match typical daily indices.
            return period_index.to_timestamp(how="end").normalize()
        except Exception:
            pass
    return pd.to_datetime(text, errors="coerce")


def _detect_date_column(df: pd.DataFrame) -> str:
    for candidate in DATE_CANDIDATES:
        if candidate in df.columns:
            return candidate
    return df.columns[0]


def _detect_value_column(df: pd.DataFrame, exclude: str) -> str:
    for candidate in VALUE_CANDIDATES:
        if candidate in df.columns and candidate != exclude:
            return candidate
    for column in df.columns:
        if column != exclude:
            return column
    raise ValueError("No value column found")


def _load_series_from_csv(path: Path, series_name: str) -> Optional[pd.Series]:
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        print(f"  [WARN] Failed to read {path}: {exc}")
        return None
    if df.empty:
        return None
    date_col = _detect_date_column(df)
    value_col = _detect_value_column(df, exclude=date_col)

    idx = _coerce_datetime(df[date_col])
    series = pd.to_numeric(df[value_col], errors="coerce")

    data = pd.Series(series.values, index=idx, name=series_name)
    data = data.dropna()
    data = data[~data.index.duplicated(keep="last")].sort_index()
    return data


def _load_series_metadata_config() -> Dict[str, Dict[str, str]]:
    if not SERIES_METADATA_PATH.exists():
        return {}
    try:
        with SERIES_METADATA_PATH.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        return raw.get("series_metadata", {})
    except Exception as exc:
        print(f"  [WARN] Failed to load series metadata: {exc}")
        return {}


_SERIES_METADATA = _load_series_metadata_config()


def _describe_series_metadata(series_name: str) -> Dict[str, str]:
    entry = _SERIES_METADATA.get(series_name, {})
    use_default = not bool(entry)
    return {
        "series": series_name,
        "frequency": entry.get("frequency", DEFAULT_SERIES_METADATA["frequency"]),
        "measurement_type": entry.get("measurement_type", DEFAULT_SERIES_METADATA["measurement_type"]),
        "metadata_source": "config" if not use_default else "default",
    }


def _write_merged_metadata_review(rows: list[dict]) -> None:
    DIAGNOSTIC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    freq_counts = Counter(row["frequency"] for row in rows)
    measurement_counts = Counter(row["measurement_type"] for row in rows)
    defaults = [row["series"] for row in rows if row["metadata_source"] == "default"]
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_series": len(rows),
        "defaulted_count": len(defaults),
        "defaulted_sample": defaults[:20],
        "frequency_distribution": dict(freq_counts),
        "measurement_distribution": dict(measurement_counts),
        "rows": rows,
    }
    summary_path = DIAGNOSTIC_OUTPUT_DIR / "merged_series_metadata_review.json"
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(
        f"  [INFO] Wrote merged metadata review ({len(rows)} series, {len(defaults)} defaulted) to {summary_path}"
    )


_SERIES_METADATA = _load_series_metadata_config()


def _is_flat_zero(series: pd.Series) -> bool:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return False
    if int(s.nunique()) != 1:
        return False
    try:
        return float(s.iloc[0]) == 0.0
    except Exception:
        return False


def _find_latest_zcb_strip_file(iso: str) -> Optional[Path]:
    zcb_dir = BASE_DIR / "data" / "ZCB STRIPS"
    if not zcb_dir.exists():
        return None

    iso_lower = iso.lower()
    candidates = sorted(zcb_dir.glob(f"{iso_lower}_historical_strips_*.csv"))
    if not candidates:
        candidates = sorted(zcb_dir.glob(f"{iso}_historical_strips_*.csv"))
    if not candidates:
        return None

    # Filenames include a timestamp; lexicographic sort works.
    return candidates[-1]


def _load_zcb_yield_at_maturity(iso: str, maturity_years: float) -> Optional[pd.Series]:
    """Load a daily yield series (in percent) for a specific maturity from ZCB strips."""

    path = _find_latest_zcb_strip_file(iso)
    if path is None or not path.exists():
        return None

    try:
        df = pd.read_csv(path, usecols=["date", "maturity_years", "yield_percent"])
    except Exception as exc:
        print(f"  [WARN] Failed to read ZCB strips for {iso} at {path}: {exc}")
        return None

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["maturity_years"] = pd.to_numeric(df["maturity_years"], errors="coerce")
    df["yield_percent"] = pd.to_numeric(df["yield_percent"], errors="coerce")
    df = df.dropna(subset=["date", "maturity_years", "yield_percent"]).copy()
    if df.empty:
        return None

    mask = np.isclose(df["maturity_years"].values.astype(float), float(maturity_years), atol=1e-9)
    df = df.loc[mask]
    if df.empty:
        return None

    series = df.groupby("date")["yield_percent"].mean().sort_index()
    series.name = f"zcb_yield_{iso}_{maturity_years}Y"
    return series


def _compute_debt_pct_from_levels(iso: str, series_name: str) -> Optional[pd.Series]:
    """Compute debt (% GDP) from local debt level CSV + WB nominal GDP JSON."""

    level_path = BASE_DIR / "data_repository" / "raw" / "macro" / f"general_government_gross_debt_level_{iso}.csv"
    gdp_json = BASE_DIR / "data_repository" / "raw" / "macro" / f"wb_NY.GDP.MKTP.CD_{iso}.json"

    if not level_path.exists() or not gdp_json.exists():
        return None

    try:
        df_level = pd.read_csv(level_path)
        if df_level.empty:
            return None
        # expected columns: year,debt_level (but be tolerant)
        df_level = df_level.rename(columns={df_level.columns[0]: "year", df_level.columns[1]: "debt_level"})
        df_level["year"] = pd.to_numeric(df_level["year"], errors="coerce")
        df_level["debt_level"] = pd.to_numeric(df_level["debt_level"], errors="coerce")
        df_level = df_level.dropna(subset=["year", "debt_level"]).copy()
        if df_level.empty:
            return None
        df_level["year"] = df_level["year"].astype(int)
    except Exception:
        return None

    try:
        raw = json.loads(gdp_json.read_text(encoding="utf-8"))
        obs = raw[1] if isinstance(raw, list) and len(raw) >= 2 and isinstance(raw[1], list) else []
        gdp_pairs = {}
        for item in obs:
            if not isinstance(item, dict):
                continue
            year = item.get("date")
            value = item.get("value")
            if year is None or value is None:
                continue
            try:
                gdp_pairs[int(year)] = float(value)
            except (TypeError, ValueError):
                continue
        if not gdp_pairs:
            return None
        gdp = pd.Series(gdp_pairs).sort_index()
    except Exception:
        return None

    common_years = sorted(set(df_level["year"].tolist()) & set(gdp.index.tolist()))
    if not common_years:
        return None

    level = pd.Series(df_level.set_index("year")["debt_level"]).reindex(common_years)
    denom = gdp.reindex(common_years)
    valid = (denom > 0) & level.notna() & denom.notna()
    if not bool(valid.any()):
        return None

    pct = (level[valid] / denom[valid]) * 100.0
    idx = pd.to_datetime(pd.Index(pct.index.astype(str)) + "-12-31", errors="coerce")
    out = pd.Series(pct.values, index=idx, name=series_name).dropna().sort_index()
    return out


def _load_eurostat_gov_debt_pct(iso: str, series_name: str) -> Optional[pd.Series]:
    """Load Eurostat government consolidated gross debt as % GDP (PC_GDP).

    Uses per-country payloads written as `data_repository/raw/macro/euro_gov_10dd_edpt1_<ISO>.json`.
    """

    path = BASE_DIR / "data_repository" / "raw" / "macro" / f"euro_gov_10dd_edpt1_{iso}.json"
    if not path.exists():
        return None

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

    values = raw.get("value")
    if not isinstance(values, dict) or not values:
        return None

    dims = raw.get("dimension") or {}
    id_order = raw.get("id") or []
    sizes = raw.get("size") or []
    if not (isinstance(id_order, list) and isinstance(sizes, list) and len(id_order) == len(sizes)):
        return None

    def _dim_pos(dim_name: str, key: str) -> Optional[int]:
        try:
            return int(dims[dim_name]["category"]["index"][key])
        except Exception:
            return None

    # Per-file geo should contain exactly one member; fall back to first.
    geo_pos = None
    try:
        geo_index = dims.get("geo", {}).get("category", {}).get("index", {})
        if isinstance(geo_index, dict) and geo_index:
            geo_pos = int(next(iter(geo_index.values())))
    except Exception:
        geo_pos = None

    freq_pos = _dim_pos("freq", "A")
    unit_pos = _dim_pos("unit", "PC_GDP")
    sector_pos = _dim_pos("sector", "S13")
    na_item_pos = _dim_pos("na_item", "GD")

    time_index = None
    try:
        time_index = dims["time"]["category"]["index"]
    except Exception:
        time_index = None

    if any(p is None for p in [geo_pos, freq_pos, unit_pos, sector_pos, na_item_pos]) or not isinstance(time_index, dict):
        return None

    # Pre-compute multipliers for flattened index.
    multipliers = []
    for i in range(len(sizes)):
        m = 1
        for s in sizes[i + 1 :]:
            m *= int(s)
        multipliers.append(m)

    fixed = {
        "freq": freq_pos,
        "unit": unit_pos,
        "sector": sector_pos,
        "na_item": na_item_pos,
        "geo": geo_pos,
    }

    out_pairs = {}
    for year_str, time_pos in time_index.items():
        try:
            year = int(year_str)
            time_pos = int(time_pos)
        except Exception:
            continue

        coords = {"time": time_pos, **fixed}
        flat = 0
        ok = True
        for dim_name, mul in zip(id_order, multipliers):
            if dim_name not in coords:
                ok = False
                break
            flat += int(coords[dim_name]) * int(mul)
        if not ok:
            continue

        v = values.get(str(flat))
        if v is None:
            continue
        try:
            out_pairs[year] = float(v)
        except Exception:
            continue

    if not out_pairs:
        return None

    idx = pd.to_datetime(pd.Index([str(y) for y in sorted(out_pairs.keys())]) + "-12-31", errors="coerce")
    ser = pd.Series([out_pairs[y] for y in sorted(out_pairs.keys())], index=idx, name=series_name)
    ser = ser.dropna().sort_index()
    return ser

print("=" * 80)
print("MERGING INDUSTRY DATA WITH EXISTING PIPELINE")
print("=" * 80)

# Load optional series configuration
optional_series = set()
required_series = set()
config = {}
alias_map = {}
try:
    with open("config/country_blocks_extended.yaml", "r") as f:
        config = yaml.safe_load(f)
        for country in config.get("country_blocks", []):
            for block in country.get("blocks", []):
                required_series.update(block.get("series_codes", []))
                optional_series.update(block.get("optional_series_codes", []))
    print(f"\nLoaded config: {len(required_series)} required, {len(optional_series)} optional series")
except Exception as e:
    print(f"\n[WARNING] Could not load country_blocks_extended.yaml: {e}")

alias_path = "config/series_alias_map.yaml"
if os.path.exists(alias_path):
    try:
        with open(alias_path, "r") as f:
            alias_config = yaml.safe_load(f) or {}
            alias_map = alias_config.get("aliases", {})
        print(f"Loaded {len(alias_map)} alias mappings from {alias_path}")
    except Exception as exc:
        print(f"[WARNING] Could not parse {alias_path}: {exc}")
else:
    print(f"[INFO] No alias map found at {alias_path}; skipping canonical rename step.")

# Load existing stress indicators
print("\n[1/3] Loading existing stress_indicators.csv...")
try:
    df_existing = pd.read_csv("data/stress_indicators.csv", index_col=0, parse_dates=True)
    print(f"  Loaded: {df_existing.shape[0]} dates x {df_existing.shape[1]} series")
    print(f"  Date range: {df_existing.index.min()} to {df_existing.index.max()}")
except Exception as e:
    print(f"  [ERROR] {e}")
    print("  [FALLBACK] Trying alternative path...")
    df_existing = pd.read_csv("stress_indicators.csv", index_col=0, parse_dates=True)
    print(f"  Loaded: {df_existing.shape[0]} dates x {df_existing.shape[1]} series")

if "Unnamed: 0" in df_existing.columns:
    df_existing = df_existing.drop(columns=["Unnamed: 0"])
    print("  Removed stray 'Unnamed: 0' column from existing dataset")
df_existing = _normalize_datetime_index(df_existing, "existing dataset")

# Load new industry data
print("\n[2/3] Loading industry_data_raw.csv...")
df_industry = pd.read_csv("industry_data_raw.csv", index_col=0, parse_dates=True)
print(f"  Loaded: {df_industry.shape[0]} dates x {df_industry.shape[1]} series")

if "Unnamed: 0" in df_industry.columns:
    df_industry = df_industry.drop(columns=["Unnamed: 0"])
    print("  Removed stray 'Unnamed: 0' column from industry dataset")
df_industry = _normalize_datetime_index(df_industry, "industry dataset")
print(f"  Date range: {df_industry.index.min()} to {df_industry.index.max()}")

# Check for overlapping column names
overlapping = set(df_existing.columns) & set(df_industry.columns)
if overlapping:
    print(f"  [WARNING] {len(overlapping)} overlapping series names:")
    for col in list(overlapping)[:5]:
        print(f"    - {col}")
    print("  These will be taken from existing data only")
    # Drop overlapping from industry data
    df_industry = df_industry.drop(columns=list(overlapping))

# Merge datasets (outer join to preserve all dates)
print("\n[3/3] Merging datasets...")
df_merged = pd.concat([df_existing, df_industry], axis=1, join="outer")
df_merged = _normalize_datetime_index(df_merged, "merged dataset")
df_merged = df_merged.sort_index()
df_merged.index.name = "Date"

alias_applied = []
alias_missing_sources = []
alias_collisions = []
if alias_map:
    for source_name, target_name in alias_map.items():
        if source_name not in df_merged.columns:
            alias_missing_sources.append(source_name)
            continue
        if target_name in df_merged.columns and target_name != source_name:
            alias_collisions.append({
                "source": source_name,
                "target": target_name,
                "action": "skipped_existing_target"
            })
            continue
        df_merged = df_merged.rename(columns={source_name: target_name})
        alias_applied.append({"source": source_name, "target": target_name})

    print(f"\nAlias mapping summary: {len(alias_applied)} renamed, {len(alias_missing_sources)} missing sources, {len(alias_collisions)} collisions")
else:
    print("\nAlias mapping skipped (no entries loaded).")


print("\n[post-merge] Repairing flat/missing public-finance proxies...")

local_series_files: Dict[str, str] = {}
try:
    for country in config.get("country_blocks", []) if isinstance(config, dict) else []:
        for block in country.get("blocks", []):
            local_map = block.get("local_series_files", {}) or {}
            if not isinstance(local_map, dict):
                continue
            for series_name, rel_path in local_map.items():
                if isinstance(series_name, str) and isinstance(rel_path, str) and series_name not in local_series_files:
                    local_series_files[series_name] = rel_path
except Exception as exc:
    print(f"  [WARN] Could not extract local_series_files from config: {exc}")

repair_attempts = 0
repair_applied = 0

# 1) Sovereign spreads: build from daily ZCB strips (10Y) to avoid sparse monthly coverage.
spread_cols = [c for c in df_merged.columns if isinstance(c, str) and c.startswith("Sovereign_spread_vs_Germany_")]
if spread_cols:
    benchmark_iso = "DEU"
    benchmark_y10 = _load_zcb_yield_at_maturity(benchmark_iso, maturity_years=10.0)
    if benchmark_y10 is None or benchmark_y10.empty:
        print("  [WARN] ZCB benchmark (DEU 10Y) not available; skipping ZCB-based spread rebuild")
    else:
        for col in sorted(spread_cols):
            iso = col.split("_")[-1]
            if iso == benchmark_iso:
                continue
            iso_y10 = _load_zcb_yield_at_maturity(iso, maturity_years=10.0)
            if iso_y10 is None or iso_y10.empty:
                continue

            spread = (iso_y10 - benchmark_y10).rename(col)
            new_series = spread.reindex(df_merged.index)
            new_nonnull = int(new_series.notna().sum())
            existing_nonnull = int(df_merged[col].notna().sum()) if col in df_merged.columns else 0

            if new_nonnull > existing_nonnull:
                df_merged[col] = new_series
                repair_applied += 1
                print(f"  [FIX] Rebuilt {col} from ZCB strips (10Y): {existing_nonnull} -> {new_nonnull} obs")

# 2) Local-file repairs for flat CDS/spreads.
for series_name, rel_path in sorted(local_series_files.items()):
    if not (series_name.startswith("Sovereign_spread_vs_Germany_") or series_name.startswith("CDS_5y_")):
        continue
    if series_name not in df_merged.columns:
        # Only materialize if the config explicitly defines the file.
        needs_repair = True
    else:
        needs_repair = _is_flat_zero(df_merged[series_name])
    if not needs_repair:
        continue

    path = (BASE_DIR / rel_path).resolve()
    if not path.exists():
        continue
    repair_attempts += 1
    loaded = _load_series_from_csv(path, series_name)
    if loaded is None or loaded.empty:
        continue
    if int(loaded.nunique()) == 1:
        try:
            if float(loaded.iloc[0]) == 0.0:
                continue
        except Exception:
            pass
    df_merged[series_name] = loaded.reindex(df_merged.index)
    repair_applied += 1
    print(f"  [FIX] Overrode {series_name} from {rel_path}")

# 2a) Optional CDS override from TradingEconomics (when available).
# These files are only created if the user has a TE API key with permission.
te_cds_dir = BASE_DIR / "data_repository" / "raw" / "providers" / "tradingeconomics"
if te_cds_dir.exists():
    for iso in ["USA", "DEU", "FRA", "ITA", "ESP"]:
        series_name = f"CDS_5y_{iso}"
        te_path = te_cds_dir / f"cds_5y_{iso}.csv"
        if not te_path.exists():
            continue
        loaded = _load_series_from_csv(te_path, series_name)
        if loaded is None or loaded.empty:
            continue
        candidate = loaded.reindex(df_merged.index)
        new_obs = int(candidate.notna().sum())
        if new_obs <= 0:
            continue
        old_obs = int(df_merged[series_name].notna().sum()) if series_name in df_merged.columns else 0
        if new_obs <= old_obs:
            continue
        df_merged[series_name] = candidate
        repair_applied += 1
        print(f"  [FIX] Overrode {series_name} from TradingEconomics: {old_obs} -> {new_obs} obs")

# 2b) Materialize missing/all-NaN series from configured local files.
# This is intentionally conservative: it only fills data when the panel column is missing or entirely empty.
for series_name, rel_path in sorted(local_series_files.items()):
    if not isinstance(series_name, str) or not isinstance(rel_path, str):
        continue
    # Let the dedicated debt backfill handle GC.DOD series (it can choose the best source).
    if series_name.startswith("GC.DOD.TOTL.GD.ZS_"):
        continue
    # Avoid circular/self references.
    if rel_path.endswith("stress_indicators_expanded.csv") or rel_path.endswith("industry_data_raw.csv"):
        continue

    if series_name in df_merged.columns and df_merged[series_name].notna().any():
        continue

    path = (BASE_DIR / rel_path).resolve()
    if not path.exists() or not path.is_file():
        continue

    loaded = _load_series_from_csv(path, series_name)
    if loaded is None or loaded.empty:
        continue
    df_merged[series_name] = loaded.reindex(df_merged.index)
    repair_applied += 1
    print(f"  [FIX] Filled {series_name} from {rel_path}")


def _series_median(series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    return float(s.median())


def _assign_if_better(df: pd.DataFrame, name: str, candidate: pd.Series, label: str) -> bool:
    if candidate is None or candidate.empty:
        return False
    candidate = pd.to_numeric(candidate, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if getattr(candidate.index, "has_duplicates", False):
        candidate = candidate[~candidate.index.duplicated(keep="last")].sort_index()
    new_obs = int(candidate.notna().sum())
    if new_obs <= 0:
        return False

    if name in df.columns:
        old_obs = int(pd.to_numeric(df[name], errors="coerce").notna().sum())
        if new_obs <= old_obs:
            return False
        df[name] = candidate.reindex(df.index)
        print(f"  [FX] Rebuilt {name} via {label}: {old_obs} -> {new_obs} obs")
        return True

    df[name] = candidate.reindex(df.index)
    print(f"  [FX] Created {name} via {label}: {new_obs} obs")
    return True


def _assign_if_more_complete(df: pd.DataFrame, name: str, candidate: pd.Series, label: str, prefix: str) -> bool:
    """Assign a derived series when it increases non-null coverage.

    If the target already exists, we keep existing values where present and fill
    missing with the candidate.
    """

    if candidate is None or candidate.empty:
        return False

    candidate = pd.to_numeric(candidate, errors="coerce").replace([np.inf, -np.inf], np.nan)
    if getattr(candidate.index, "has_duplicates", False):
        candidate = candidate[~candidate.index.duplicated(keep="last")].sort_index()
    candidate = candidate.reindex(df.index)

    existing = None
    if name in df.columns:
        existing = pd.to_numeric(df[name], errors="coerce").replace([np.inf, -np.inf], np.nan)
        existing = existing.reindex(df.index)

    if existing is not None:
        combined = existing.combine_first(candidate)
        old_obs = int(existing.notna().sum())
    else:
        combined = candidate
        old_obs = 0

    new_obs = int(combined.notna().sum())
    if new_obs <= old_obs:
        return False

    df[name] = combined
    if old_obs > 0:
        print(f"  {prefix} Rebuilt {name} via {label}: {old_obs} -> {new_obs} obs")
    else:
        print(f"  {prefix} Created {name} via {label}: {new_obs} obs")
    return True


def _load_and_fill_from_csv(df: pd.DataFrame, series_name: str, rel_path: str, label: str, prefix: str) -> bool:
    path = (BASE_DIR / rel_path).resolve()
    if not path.exists() or not path.is_file():
        return False
    loaded = _load_series_from_csv(path, series_name)
    if loaded is None or loaded.empty:
        return False
    return _assign_if_more_complete(df, series_name, loaded, label=label, prefix=prefix)


print("\n[post-merge] Ensuring core coverage for spreads/vol/liquidity/FX legs...")

# OECD macro (quarterly) series: add the missing USA NAEXKP code so macro blocks
# can reference it directly.
_load_and_fill_from_csv(
    df_merged,
    "NAEXKP01USQ661S",
    "data_repository/raw/fred/NAEXKP01USQ661S.csv",
    label="FRED raw NAEXKP01USQ661S (OECD NAEXKP series)",
    prefix="[MACRO]",
)

# 1) Named sovereign spreads: compute from daily ZCB strips (10Y) for coverage.
benchmark_iso = "DEU"
benchmark_y10 = _load_zcb_yield_at_maturity(benchmark_iso, maturity_years=10.0)

def _zcb_y10_or_none(iso: str) -> Optional[pd.Series]:
    return _load_zcb_yield_at_maturity(iso, maturity_years=10.0)


def _yield_or_none(series_name: str) -> Optional[pd.Series]:
    if series_name in df_merged.columns:
        s = pd.to_numeric(df_merged[series_name], errors="coerce").dropna()
        if not s.empty:
            return pd.to_numeric(df_merged[series_name], errors="coerce")
    return None


def _build_spread(name: str, iso: str, fallback_fred: Optional[str]) -> None:
    if benchmark_y10 is None or benchmark_y10.empty:
        return
    iso_y10 = _zcb_y10_or_none(iso)
    label = None
    spread = None
    if iso_y10 is not None and not iso_y10.empty:
        spread = iso_y10 - benchmark_y10
        label = f"ZCB strips 10Y ({iso} - {benchmark_iso})"
    elif fallback_fred:
        y = _yield_or_none(fallback_fred)
        y_b = _yield_or_none("IRLTLT01DEM156N")
        if y is not None and y_b is not None:
            spread = y - y_b
            label = f"FRED long-term yields ({fallback_fred} - IRLTLT01DEM156N)"
    if spread is None or label is None:
        return
    _assign_if_more_complete(df_merged, name, spread, label=label, prefix="[SPREAD]")


_build_spread("BTP_Bund_Spread", "ITA", fallback_fred="IRLTLT01ITM156N")
_build_spread("Bonos_Bund_Spread", "ESP", fallback_fred="IRLTLT01ESM156N")
_build_spread("OAT_Bund_Spread", "FRA", fallback_fred="IRLTLT01FRM156N")
_build_spread("Treasury_Bund_Spread", "USA", fallback_fred="IRLTLT01USM156N")

# DEU composite spread from euro-area periphery spreads.
periphery_components = [c for c in ["BTP_Bund_Spread", "Bonos_Bund_Spread", "OAT_Bund_Spread"] if c in df_merged.columns]
if len(periphery_components) >= 2:
    composite = pd.concat([pd.to_numeric(df_merged[c], errors="coerce") for c in periphery_components], axis=1).mean(axis=1, skipna=True)
    composite.name = "DEU_Periphery_Spread_Composite"
    _assign_if_more_complete(
        df_merged,
        "DEU_Periphery_Spread_Composite",
        composite,
        label=f"mean({', '.join(periphery_components)})",
        prefix="[SPREAD]",
    )

# 2) Volatility proxy: fill V2X from realized-vol proxy if it increases coverage.
_load_and_fill_from_csv(
    df_merged,
    "V2X",
    "data_repository/raw/providers/derived_risk_drivers/Euro_STOXX_RealizedVol20.csv",
    label="Euro STOXX realized-vol proxy",
    prefix="[VOL]",
)

# Optional backcast: extend V2X earlier history using VIXCLS where V2X is missing.
# This is intentionally simple (log-log OLS) and only fills gaps; it does not
# overwrite existing V2X observations.
if "V2X" in df_merged.columns and "VIXCLS" in df_merged.columns:
    v2x = pd.to_numeric(df_merged["V2X"], errors="coerce")
    vix = pd.to_numeric(df_merged["VIXCLS"], errors="coerce")
    overlap = v2x.notna() & vix.notna() & (v2x > 0) & (vix > 0)
    if int(overlap.sum()) >= 250:
        x = np.log(vix.loc[overlap].astype(float))
        y = np.log(v2x.loc[overlap].astype(float))
        # y = a + b*x
        b, a = np.polyfit(x.values, y.values, deg=1)
        vix_pos = vix.where(vix > 0)
        v2x_hat = np.exp(a + b * np.log(vix_pos.astype(float)))
        v2x_hat.name = "V2X"
        _assign_if_more_complete(
            df_merged,
            "V2X",
            v2x_hat,
            label="log-log OLS backcast from VIXCLS (fill-only)",
            prefix="[VOL]",
        )

# 3) Italy equity index: splice proxy (1990-) with official FTSEMIB (1998-).
# The country-block config expects `^FTSEMIB` (Yahoo-style naming), so materialize that.
proxy_path = BASE_DIR / "analysis_outputs" / "ftsemib_proxy" / "eq_weight_proxy_1990_2025.csv"
official_path = BASE_DIR / "data_repository" / "raw" / "market_data" / "EQUITY_Italy__FTSEMIB.csv"

proxy_series = _load_series_from_csv(proxy_path, "ftsemib_proxy") if proxy_path.exists() else None
official_series = _load_series_from_csv(official_path, "^FTSEMIB") if official_path.exists() else None
if proxy_series is not None and not proxy_series.empty and official_series is not None and not official_series.empty:
    stitched = None
    anchor_date = official_series.first_valid_index()
    if anchor_date is not None:
        try:
            proxy_at_anchor = proxy_series.loc[:anchor_date].iloc[-1] if (proxy_series.index <= anchor_date).any() else None
            official_at_anchor = official_series.loc[anchor_date]
            if proxy_at_anchor is not None and float(proxy_at_anchor) != 0.0:
                scale = float(official_at_anchor) / float(proxy_at_anchor)
                proxy_scaled = proxy_series * scale
                stitched = pd.concat([proxy_scaled.loc[:anchor_date], official_series.loc[anchor_date:]]).sort_index()
                stitched.name = "^FTSEMIB"
        except Exception:
            stitched = None

    if stitched is not None and not stitched.empty:
        _assign_if_more_complete(df_merged, "^FTSEMIB", stitched, label="proxy splice (1990-) + official (1998-)", prefix="[EQ]")
        _assign_if_more_complete(df_merged, "FTSEMIB", stitched.rename("FTSEMIB"), label="alias of ^FTSEMIB", prefix="[EQ]")
    else:
        _assign_if_more_complete(df_merged, "^FTSEMIB", official_series, label="official FTSEMIB local CSV", prefix="[EQ]")
        _assign_if_more_complete(df_merged, "FTSEMIB", official_series.rename("FTSEMIB"), label="alias of ^FTSEMIB", prefix="[EQ]")
elif official_series is not None and not official_series.empty:
    _assign_if_more_complete(df_merged, "^FTSEMIB", official_series, label="official FTSEMIB local CSV", prefix="[EQ]")
    _assign_if_more_complete(df_merged, "FTSEMIB", official_series.rename("FTSEMIB"), label="alias of ^FTSEMIB", prefix="[EQ]")
elif proxy_series is not None and not proxy_series.empty:
    proxy_only = proxy_series.rename("^FTSEMIB")
    _assign_if_more_complete(df_merged, "^FTSEMIB", proxy_only, label="FTSEMIB proxy only", prefix="[EQ]")
    _assign_if_more_complete(df_merged, "FTSEMIB", proxy_only.rename("FTSEMIB"), label="alias of ^FTSEMIB", prefix="[EQ]")

# 4) Liquidity balance sheets: ensure ECBASSETS/WALCL are present.
# ECBASSETS is discontinued on FRED; ECBASSETSW is the weekly replacement.
_load_and_fill_from_csv(
    df_merged,
    "ECBASSETS",
    "data_repository/raw/fred/ECBASSETS.csv",
    label="FRED raw ECBASSETS",
    prefix="[LIQ]",
)
_load_and_fill_from_csv(
    df_merged,
    "ECBASSETSW",
    "data_repository/raw/fred/ECBASSETSW.csv",
    label="FRED raw ECBASSETSW (weekly replacement)",
    prefix="[LIQ]",
)
if "ECBASSETSW" in df_merged.columns:
    _assign_if_more_complete(
        df_merged,
        "ECBASSETS",
        df_merged["ECBASSETSW"],
        label="ECBASSETSW -> ECBASSETS (weekly replacement)",
        prefix="[LIQ]",
    )
_load_and_fill_from_csv(
    df_merged,
    "WALCL",
    "data_repository/raw/fred/WALCL.csv",
    label="FRED raw WALCL",
    prefix="[LIQ]",
)

# 4b) Funding stress input: ensure EURIBOR_3m is up-to-date.
# Use FRED's EUR3MTD156N where available (older EURIBOR_3m CSV was truncated).
if "EUR3MTD156N" in df_merged.columns:
    _assign_if_more_complete(
        df_merged,
        "EURIBOR_3m",
        df_merged["EUR3MTD156N"],
        label="alias of EUR3MTD156N (FRED)",
        prefix="[FIX]",
    )

# 5) FX plan: fill from up-to-date local sources even when columns already exist.
_load_and_fill_from_csv(df_merged, "DEXUSEU", "data_repository/raw/fred/DEXUSEU.csv", label="FRED raw DEXUSEU", prefix="[FX]")
_load_and_fill_from_csv(df_merged, "DEXUSUK", "data_repository/raw/fred/DEXUSUK.csv", label="FRED raw DEXUSUK", prefix="[FX]")
_load_and_fill_from_csv(df_merged, "DEXJPUS", "data_repository/raw/fred/DEXJPUS.csv", label="FRED raw DEXJPUS", prefix="[FX]")
_load_and_fill_from_csv(df_merged, "DEXCHUS", "data_repository/raw/fred/DEXCHUS.csv", label="FRED raw DEXCHUS", prefix="[FX]")
_load_and_fill_from_csv(df_merged, "DEXINUS", "data_repository/raw/fred/DEXINUS.csv", label="FRED raw DEXINUS", prefix="[FX]")
_load_and_fill_from_csv(df_merged, "EUR_USD", "data_repository/raw/market_data/FX_EURUSD_EURUSD_X.csv", label="market_data EURUSD", prefix="[FX]")

# Backfill EUR_USD (USD per EUR) from FRED's DEXUSEU (also USD per EUR) when the
# market_data source starts later (often ~2001). This helps EUR cross rates clear
# the coverage threshold in 1990–2025 windows.
if "EUR_USD" in df_merged.columns and "DEXUSEU" in df_merged.columns:
    eur_usd_market = pd.to_numeric(df_merged["EUR_USD"], errors="coerce")
    eur_usd_fred = pd.to_numeric(df_merged["DEXUSEU"], errors="coerce")
    eur_usd_combined = eur_usd_market.fillna(eur_usd_fred)
    _assign_if_more_complete(
        df_merged,
        "EUR_USD",
        eur_usd_combined,
        label="EUR_USD backfilled via DEXUSEU",
        prefix="[FX]",
    )

# Ensure USA legs exist as named columns with good coverage.
if "DEXUSUK" in df_merged.columns:
    _assign_if_more_complete(df_merged, "GBP_USD", df_merged["DEXUSUK"], label="from DEXUSUK", prefix="[FX]")
if "DEXCHUS" in df_merged.columns:
    # DEXCHUS is CNY per USD; keep as USD_CNY for downstream direction detection.
    _assign_if_more_complete(df_merged, "USD_CNY", df_merged["DEXCHUS"], label="from DEXCHUS", prefix="[FX]")
if "DEXJPUS" in df_merged.columns:
    _assign_if_more_complete(df_merged, "USD_JPY", df_merged["DEXJPUS"], label="from DEXJPUS", prefix="[FX]")
if "DEXINUS" in df_merged.columns:
    # DEXINUS is INR per USD.
    _assign_if_more_complete(df_merged, "USD_INR", df_merged["DEXINUS"], label="from DEXINUS", prefix="[FX]")

# Keep common EUR/* names around if only *_XR variants exist.
if "EUR_GBP" not in df_merged.columns and "EUR_GBP_XR" in df_merged.columns:
    _assign_if_more_complete(df_merged, "EUR_GBP", df_merged["EUR_GBP_XR"], label="alias EUR_GBP_XR", prefix="[FX]")
if "EUR_CHF" not in df_merged.columns and "EUR_CHF_XR" in df_merged.columns:
    _assign_if_more_complete(df_merged, "EUR_CHF", df_merged["EUR_CHF_XR"], label="alias EUR_CHF_XR", prefix="[FX]")
if "EUR_JPY" not in df_merged.columns and "EUR_JPY_XR" in df_merged.columns:
    _assign_if_more_complete(df_merged, "EUR_JPY", df_merged["EUR_JPY_XR"], label="alias EUR_JPY_XR", prefix="[FX]")
if "EUR_INR" not in df_merged.columns and "EUR_INR_XR" in df_merged.columns:
    _assign_if_more_complete(df_merged, "EUR_INR", df_merged["EUR_INR_XR"], label="alias EUR_INR_XR", prefix="[FX]")


print("\n[post-merge] Deriving EUR-based FX cross rates (EUR/*) from USD legs...")

# Goal: For euro-area country blocks, literature typically uses EUR vs key partner currencies.
# We derive these consistently from EUR_USD (USD per EUR) and USD-legs (quoted either USD/CCY or CCY/USD).

eur_usd = df_merged["EUR_USD"] if "EUR_USD" in df_merged.columns else None
if eur_usd is None or eur_usd.dropna().empty:
    print("  [WARN] EUR_USD not available; skipping EUR cross-rate derivations")
else:
    # USD per GBP
    usd_per_gbp = None
    if "DEXUSUK" in df_merged.columns:
        usd_per_gbp = df_merged["DEXUSUK"]
    elif "GBP_USD" in df_merged.columns:
        gbp_usd = df_merged["GBP_USD"]
        med = _series_median(gbp_usd)
        # Heuristic: GBP per USD ~ 0.6–0.9; USD per GBP ~ 1.1–1.8
        if np.isfinite(med) and med < 1.0:
            usd_per_gbp = 1.0 / pd.to_numeric(gbp_usd, errors="coerce")
        else:
            usd_per_gbp = gbp_usd

    # CHF per USD
    chf_per_usd = df_merged["DEXSZUS"] if "DEXSZUS" in df_merged.columns else None
    if chf_per_usd is None and "USD_CHF" in df_merged.columns:
        usd_chf = df_merged["USD_CHF"]
        med = _series_median(usd_chf)
        if np.isfinite(med) and med > 1.0:
            chf_per_usd = 1.0 / pd.to_numeric(usd_chf, errors="coerce")
        else:
            chf_per_usd = usd_chf

    # JPY per USD
    jpy_per_usd = None
    if "DEXJPUS" in df_merged.columns:
        jpy_per_usd = df_merged["DEXJPUS"]
    elif "USD_JPY" in df_merged.columns:
        jpy_per_usd = df_merged["USD_JPY"]

    # CNY per USD
    cny_per_usd = None
    if "DEXCHUS" in df_merged.columns:
        cny_per_usd = df_merged["DEXCHUS"]
    elif "USD_CNY" in df_merged.columns:
        usd_cny = df_merged["USD_CNY"]
        med = _series_median(usd_cny)
        # Heuristic: CNY per USD ~ 5–9; USD per CNY ~ 0.1–0.2
        if np.isfinite(med) and med < 1.0:
            cny_per_usd = 1.0 / pd.to_numeric(usd_cny, errors="coerce")
        else:
            cny_per_usd = usd_cny

    # EUR/GBP (GBP per EUR) = (USD per EUR) / (USD per GBP)
    if usd_per_gbp is not None:
        eur_gbp = pd.to_numeric(eur_usd, errors="coerce") / pd.to_numeric(usd_per_gbp, errors="coerce")
        _assign_if_better(df_merged, "EUR_GBP_XR", eur_gbp, "EUR_USD / USD_per_GBP")
    else:
        print("  [INFO] Missing USD-per-GBP leg; cannot derive EUR_GBP_XR")

    # EUR/CHF (CHF per EUR) = (USD per EUR) * (CHF per USD)
    if chf_per_usd is not None:
        eur_chf = pd.to_numeric(eur_usd, errors="coerce") * pd.to_numeric(chf_per_usd, errors="coerce")
        _assign_if_better(df_merged, "EUR_CHF_XR", eur_chf, "EUR_USD * CHF_per_USD")
    else:
        print("  [INFO] Missing CHF-per-USD leg; cannot derive EUR_CHF_XR")

    # EUR/JPY (JPY per EUR) = (USD per EUR) * (JPY per USD)
    if jpy_per_usd is not None:
        eur_jpy = pd.to_numeric(eur_usd, errors="coerce") * pd.to_numeric(jpy_per_usd, errors="coerce")
        _assign_if_better(df_merged, "EUR_JPY_XR", eur_jpy, "EUR_USD * JPY_per_USD")
    else:
        print("  [INFO] Missing JPY-per-USD leg; cannot derive EUR_JPY_XR")

    # EUR/CNY (CNY per EUR) = (USD per EUR) * (CNY per USD)
    if cny_per_usd is not None:
        eur_cny = pd.to_numeric(eur_usd, errors="coerce") * pd.to_numeric(cny_per_usd, errors="coerce")
        _assign_if_better(df_merged, "EUR_CNY_XR", eur_cny, "EUR_USD * CNY_per_USD")
    else:
        print("  [INFO] Missing CNY-per-USD leg; cannot derive EUR_CNY_XR")

    # INR per USD
    inr_per_usd = None
    if "DEXINUS" in df_merged.columns:
        inr_per_usd = df_merged["DEXINUS"]
    elif "USD_INR" in df_merged.columns:
        inr_per_usd = df_merged["USD_INR"]

    # EUR/INR (INR per EUR) = (USD per EUR) * (INR per USD)
    if inr_per_usd is not None:
        eur_inr = pd.to_numeric(eur_usd, errors="coerce") * pd.to_numeric(inr_per_usd, errors="coerce")
        _assign_if_better(df_merged, "EUR_INR_XR", eur_inr, "EUR_USD * INR_per_USD")
        _assign_if_better(df_merged, "EUR_INR", eur_inr, "alias of EUR_INR_XR")
    else:
        print("  [INFO] Missing INR-per-USD leg; cannot derive EUR_INR_XR")


print("\n[post-merge] Deriving EUR funding stress proxy (Euribor minus €STR)...")

euribor_3m = df_merged["EUR3MTD156N"] if "EUR3MTD156N" in df_merged.columns else None
estr = df_merged["ECBESTRVOLWGTTRMDMNRT"] if "ECBESTRVOLWGTTRMDMNRT" in df_merged.columns else None
dfr = df_merged["ECBDFR"] if "ECBDFR" in df_merged.columns else None

funding_spread = None
label = None
if euribor_3m is not None and estr is not None:
    funding_spread = pd.to_numeric(euribor_3m, errors="coerce") - pd.to_numeric(estr, errors="coerce")
    label = "EUR3MTD156N - ECB €STR"
elif euribor_3m is not None and dfr is not None:
    # Fallback if €STR is unavailable: use ECB deposit facility rate as policy-rate proxy.
    funding_spread = pd.to_numeric(euribor_3m, errors="coerce") - pd.to_numeric(dfr, errors="coerce")
    label = "EUR3MTD156N - ECBDFR (fallback)"

if funding_spread is None:
    print("  [WARN] Missing inputs for EURIBOR_ESTR_SPREAD; skipping")
else:
    df_merged["EURIBOR_ESTR_SPREAD"] = funding_spread.reindex(df_merged.index)
    obs = int(pd.to_numeric(df_merged["EURIBOR_ESTR_SPREAD"], errors="coerce").notna().sum())
    print(f"  [FIX] Created EURIBOR_ESTR_SPREAD via {label}: {obs} obs")

# 3) Debt-to-GDP: backfill empty GC.DOD series from macro CSVs.
# Use both required-series list and any existing empty GC.DOD.* columns (defensive against config drift).
debt_targets = set(
    [s for s in required_series if isinstance(s, str) and s.startswith("GC.DOD.TOTL.GD.ZS_")]
)
debt_targets |= set(
    [c for c in df_merged.columns if isinstance(c, str) and c.startswith("GC.DOD.TOTL.GD.ZS_")]
)

for series_name in sorted(debt_targets):
    existing_series = df_merged[series_name] if series_name in df_merged.columns else None
    existing_obs = int(existing_series.notna().sum()) if existing_series is not None else 0

    iso = series_name.split("_")[-1]
    macro_path = BASE_DIR / "data_repository" / "raw" / "macro" / f"general_government_gross_debt_pct_gdp_{iso}.csv"
    wb_loaded = None
    if macro_path.exists():
        wb_loaded = _load_series_from_csv(macro_path, series_name)

    # If WB %GDP is sparse/missing, prefer computing from provider debt-levels + provider nominal GDP.
    # This avoids prefilled/interpolated extensions when possible.
    loaded = None
    if wb_loaded is not None and int(wb_loaded.notna().sum()) >= 5:
        loaded = wb_loaded
    else:
        eurostat_pct = _load_eurostat_gov_debt_pct(iso, series_name)
        if eurostat_pct is not None and not eurostat_pct.empty:
            # Prefer official Eurostat %GDP when available; keep WB observations for any extra years.
            loaded = eurostat_pct
            if wb_loaded is not None and not wb_loaded.empty:
                loaded = wb_loaded.combine_first(loaded)

        if loaded is None:
            computed = _compute_debt_pct_from_levels(iso, series_name)
            if computed is not None and not computed.empty:
                # Keep any WB observations that do exist, but fill missing years with computed values.
                if wb_loaded is not None and not wb_loaded.empty:
                    loaded = wb_loaded.combine_first(computed)
                else:
                    loaded = computed

        # Last resort: use extended (prefilled) version only if we couldn't compute anything.
        if loaded is None:
            extended_path = BASE_DIR / "data_repository" / "raw" / "macro" / f"general_government_gross_debt_pct_gdp_{iso}_extended.csv"
            if extended_path.exists():
                ext_loaded = _load_series_from_csv(extended_path, series_name)
                if ext_loaded is not None and not ext_loaded.empty:
                    loaded = ext_loaded

    if loaded is None or loaded.empty:
        continue

    loaded = loaded.reindex(df_merged.index)

    # Only apply if this adds coverage (new non-null points) or if the existing series is effectively missing.
    if existing_series is not None:
        adds = int((loaded.notna() & existing_series.isna()).sum())
        if existing_obs >= 5 and adds <= 0:
            continue
        df_merged[series_name] = existing_series.combine_first(loaded)
        label = "Extended" if adds > 0 else "Replaced"
        print(f"  [FIX] {label} {series_name} (+{adds} new obs, loaded {int(loaded.notna().sum())})")
    else:
        df_merged[series_name] = loaded
        print(f"  [FIX] Backfilled {series_name} ({int(loaded.notna().sum())} obs)")

    repair_applied += 1

print(f"  Repair summary: {repair_applied} applied (attempted {repair_attempts} local-file loads)")

print(f"  Result: {df_merged.shape[0]} dates x {df_merged.shape[1]} series")
print(f"  Date range: {df_merged.index.min()} to {df_merged.index.max()}")

metadata_rows = [_describe_series_metadata(col) for col in df_merged.columns]
_write_merged_metadata_review(metadata_rows)

# Check data quality
print("\n" + "=" * 80)
print("DATA QUALITY CHECK")
print("=" * 80)

missing_pct = (df_merged.isna().sum() / len(df_merged) * 100).sort_values(ascending=False)
print(f"\nSeries with >50% missing data:")
high_missing = missing_pct[missing_pct > 50]
if len(high_missing) > 0:
    for series, pct in high_missing.head(10).items():
        print(f"  {series:30} {pct:5.1f}% missing")
    print(f"  ... and {len(high_missing) - 10} more" if len(high_missing) > 10 else "")
else:
    print("  None! All series have <50% missing data")

# Series breakdown
print(f"\n{'=' * 80}")
print("SERIES BREAKDOWN")
print(f"{'=' * 80}")
print(f"Existing series:     {len(df_existing.columns)}")
print(f"New industry series: {len(df_industry.columns)}")
print(f"Total series:        {len(df_merged.columns)}")
print(f"Correlation pairs:   {len(df_merged.columns) * (len(df_merged.columns) - 1) // 2:,}")

# Save merged dataset
output_file = "stress_indicators_expanded.csv"
df_merged.to_csv(output_file)
print(f"\n[SAVED] {output_file}")

# Also save to data/ directory if it exists
import os
if os.path.exists("data"):
    df_merged.to_csv("data/stress_indicators_expanded.csv")
    print("[SAVED] data/stress_indicators_expanded.csv")

# Track which optional series are present
merged_series = set(df_merged.columns)
optional_present = list(optional_series & merged_series)
optional_missing = list(optional_series - merged_series)
required_present = list(required_series & merged_series)
required_missing = list(required_series - merged_series)

# Generate merge report
merge_report = {
    "merge_date": datetime.now().isoformat(),
    "input_files": {
        "existing": "stress_indicators.csv",
        "industry": "industry_data_raw.csv"
    },
    "output_file": output_file,
    "series_counts": {
        "existing": len(df_existing.columns),
        "industry_new": len(df_industry.columns),
        "total": len(df_merged.columns),
        "overlapping_removed": len(overlapping)
    },
    "date_range": {
        "start": str(df_merged.index.min()),
        "end": str(df_merged.index.max()),
        "n_dates": len(df_merged)
    },
    "correlation_pairs": len(df_merged.columns) * (len(df_merged.columns) - 1) // 2,
    "data_quality": {
        "series_with_high_missing": len(high_missing),
        "threshold": "50%"
    },
    "optional_series_status": {
        "optional_present": len(optional_present),
        "optional_missing": len(optional_missing),
        "required_present": len(required_present),
        "required_missing": len(required_missing),
        "missing_optional_list": optional_missing,
        "missing_required_list": required_missing
    },
    "alias_summary": {
        "applied_count": len(alias_applied),
        "applied_pairs": alias_applied,
        "missing_sources": alias_missing_sources,
        "collisions": alias_collisions
    }
}

print(f"\n{'=' * 80}")
print("OPTIONAL SERIES COVERAGE")
print(f"{'=' * 80}")
print(f"Required series present: {len(required_present)}/{len(required_series)}")
print(f"Optional series present: {len(optional_present)}/{len(optional_series)}")
if required_missing:
    print(f"\n[WARNING] Missing {len(required_missing)} REQUIRED series:")
    for s in required_missing[:10]:
        print(f"  - {s}")
if optional_missing:
    print(f"\n[INFO] Missing {len(optional_missing)} optional series (continuing without them):")
    for s in optional_missing[:10]:
        print(f"  - {s}")

# Country/block snapshot so users can spot gaps without opening the JSON report
if config.get("country_blocks"):
    print(f"\n{'=' * 80}")
    print("COUNTRY/BLOCK COVERAGE SNAPSHOT")
    print(f"{'=' * 80}")
    for country_cfg in config.get("country_blocks", []):
        country_name = country_cfg.get("country", "UNKNOWN")
        iso_code = country_cfg.get("iso_code", "-")
        print(f"\n{country_name} ({iso_code})")
        print("-" * 60)
        for block in country_cfg.get("blocks", []):
            block_key = block.get("key", "n/a")
            required_codes = block.get("series_codes", [])
            optional_codes = block.get("optional_series_codes", [])

            required_present_block = [s for s in required_codes if s in merged_series]
            required_missing_block = [s for s in required_codes if s not in merged_series]
            optional_present_block = [s for s in optional_codes if s in merged_series]
            optional_missing_block = [s for s in optional_codes if s not in merged_series]

            total_required = len(required_codes) or 1  # avoid div-zero; empty block treated as full
            coverage_pct = len(required_present_block) / total_required * 100

            if coverage_pct >= 70 and not required_missing_block:
                status = "READY"
            elif coverage_pct >= 70:
                status = "PARTIAL"
            else:
                status = "INSUFFICIENT"

            print(
                f"  {block_key:20} {status:11} "
                f"{len(required_present_block)}/{len(required_codes)} required, "
                f"{len(optional_present_block)}/{len(optional_codes)} optional"
            )
            if required_missing_block:
                preview = ", ".join(required_missing_block[:3])
                more = f" ...(+{len(required_missing_block) - 3})" if len(required_missing_block) > 3 else ""
                print(f"    Missing required: {preview}{more}")
            if optional_missing_block:
                preview_opt = ", ".join(optional_missing_block[:3])
                more_opt = f" ...(+{len(optional_missing_block) - 3})" if len(optional_missing_block) > 3 else ""
                print(f"    Missing optional: {preview_opt}{more_opt}")
else:
    print("\n[INFO] Skipping block snapshot because country_blocks_extended.yaml was not loaded.")

with open("merge_report.json", "w") as f:
    json.dump(merge_report, f, indent=2)

print("[SAVED] merge_report.json")

print("\n" + "=" * 80)
print("MERGE COMPLETE!")
print("=" * 80)
print(f"Next step: Run DCC-GARCH on {len(df_merged.columns)} series")
print(f"Expected runtime: 10-15 minutes")
print(f"Expected output: {len(df_merged.columns) * (len(df_merged.columns) - 1) // 2:,} correlation pairs")
