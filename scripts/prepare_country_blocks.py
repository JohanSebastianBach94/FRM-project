#!/usr/bin/env python3
"""Validate country block metadata and emit canonical definitions."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import yaml

BLOCKS_PATH = Path("config") / "country_blocks_extended.yaml"
METADATA_PATH = Path("config") / "series_metadata.yaml"
OUTPUT_DIR = Path("outputs")
DIAG_DIR = Path("analysis_outputs") / "diagnostics"
BLOCK_COVERAGE_REPORT = Path("analysis_outputs") / "block_coverage_report.json"
THRESHOLD_CONFIG_PATH = Path("analysis_outputs") / "coverage_threshold_config.json"
CATALOG_PATH = Path("catalog.csv")

# Economically essential drivers that must survive coverage thresholding and
# weakest-ISO harmonization cutoffs. These are treated as "always-keep" when
# present in a block's configured series_codes ordering.
REQUIRED_SERIES = {
    "BTP_Bund_Spread",
    "Bonos_Bund_Spread",
    "OAT_Bund_Spread",
    "Treasury_Bund_Spread",
    "DEU_Periphery_Spread_Composite",
    "^FTSEMIB",
    "FTSEMIB",
    "V2X",
    "VIXCLS",
    "^SWAPTION_VOL_",
    "ECBASSETS",
    "WALCL",
    "COMM_PAPER_SPREAD_EUR",
    "COMM_PAPER_SPREAD_USA",
    "TEDRATE",
    "EURIBOR_3m",
    "MYAGM2EZM196N",
    "MORTGAGE30US",
    "Mortgage_rate_DEU",
    "Mortgage_rate_ESP",
    "Mortgage_rate_FRA",
    "Mortgage_rate_ITA",
    "^GSPC",
    "^GDAXI",
    "^FCHI",
    "^IBEX",
    "GC.DOD.TOTL.GD.ZS_DEU",
    "GC.DOD.TOTL.GD.ZS_ESP",
    "GC.DOD.TOTL.GD.ZS_FRA",
    "GC.DOD.TOTL.GD.ZS_ITA",
    "GC.DOD.TOTL.GD.ZS_USA",
    "Sovereign_spread_vs_Germany_USA",
    "GBP_USD",
    "USD_JPY",
    "USD_CNY",
    "USD_INR",
    "DEXINUS",
    "EUR_GBP",
    "EUR_GBP_XR",
    "EUR_JPY",
    "EUR_JPY_XR",
    "EUR_CNY_XR",
    "EUR_INR",
    "EUR_INR_XR",
}

_REQUIRED_EXACT = {s for s in REQUIRED_SERIES if not str(s).startswith("^")}
_REQUIRED_PATTERNS = [re.compile(s) for s in REQUIRED_SERIES if str(s).startswith("^")]


def _is_required_series(series_code: str) -> bool:
    if series_code in _REQUIRED_EXACT:
        return True
    return any(pat.match(series_code) for pat in _REQUIRED_PATTERNS)

# Series that should survive the weakest-ISO harmonization cutoff *when they meet*
# the coverage threshold. This keeps economically important drivers (e.g., country
# credit spreads) from being removed simply because not every ISO has an available
# proxy yet.
ALWAYS_KEEP_IF_ABOVE_THRESHOLD = {
    "credit_spread_DEU",
    "credit_spread_ESP",
    "credit_spread_FRA",
    "credit_spread_ITA",
    "credit_spread_USA",
    "Bank_equity_index_DEU",
    "Bank_equity_index_ESP",
    "Bank_equity_index_FRA",
    "Bank_equity_index_ITA",
    "Bank_equity_index_USA",
}


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    DIAG_DIR.mkdir(parents=True, exist_ok=True)


def load_yaml(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def gather_series(blocks: List[Dict]) -> List[str]:
    series = []
    for block in blocks:
        series.extend(block.get("series_codes", []))
    return sorted(set(series))


def _load_block_coverage_report(path: Path) -> Dict[tuple[str, str], Dict]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    result: Dict[tuple[str, str], Dict] = {}
    for country in payload.get("countries", []) or []:
        iso = country.get("iso_code")
        for block in country.get("blocks", []) or []:
            key = block.get("key")
            if iso and key:
                result[(str(iso), str(key))] = block
    return result


def _normalize_series_metadata(payload: Dict) -> Dict[str, Dict]:
    """Support both flat and nested metadata YAML shapes."""
    if not isinstance(payload, dict):
        return {}
    nested = payload.get("series_metadata") or payload.get("series")
    if isinstance(nested, dict):
        return nested
    return payload


def validate_metadata(series: str, metadata: Dict[str, Dict]) -> Tuple[bool, List[str]]:
    entry = metadata.get(series)
    if not entry:
        return False, ["missing metadata"]

    # The project metadata schema is not guaranteed to include 'source'/'transform'
    # for every series; requiring them would create noisy false negatives.
    missing = [field for field in ("frequency",) if not entry.get(field)]
    return len(missing) == 0, missing


def _load_series_threshold(path: Path) -> float:
    if not path.exists():
        return 0.62
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0.62
    try:
        return float(payload.get("series_threshold", 0.62))
    except Exception:
        return 0.62


def _load_catalog_coverage(path: Path) -> Dict[str, float]:
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "series" not in df.columns or "coverage_ratio" not in df.columns:
        return {}
    series_to_cov: Dict[str, float] = {}
    for _, row in df.iterrows():
        series = str(row.get("series", "")).strip()
        if not series:
            continue
        try:
            cov = float(row.get("coverage_ratio"))
        except Exception:
            continue
        series_to_cov[series] = cov
    return series_to_cov


def _apply_threshold_and_harmonize(
    payload: Dict,
    series_to_cov: Dict[str, float],
    series_threshold: float,
) -> Tuple[
    Dict,
    Dict[str, Dict[str, List[Dict[str, float]]]],
    Dict[str, Dict[str, List[Dict[str, float]]]],
]:
    """Mutates payload in-place.

        Returns two nested reports:
            - dropped_above_threshold[iso][block_key] = [{"series": ..., "coverage_ratio": ...}, ...]
                for series dropped due to harmonization even though they were >= threshold.
            - would_survive_but_below_threshold[iso][block_key] = [{"series": ..., "coverage_ratio": ...}, ...]
                for series that *would* survive harmonization if the coverage threshold were not applied,
                but currently fail the threshold.
    """
    # Collect eligible lists per (block_key, iso) preserving existing ordering.
    # We keep REQUIRED_SERIES regardless of threshold and exclude them from the
    # weakest-ISO cutoff K so they always survive harmonization.
    eligible_by_block_iso: Dict[tuple[str, str], List[str]] = {}
    required_by_block_iso: Dict[tuple[str, str], List[str]] = {}

    # Counterfactual: harmonization ignoring coverage threshold.
    eligible_all_by_block_iso: Dict[tuple[str, str], List[str]] = {}
    required_all_by_block_iso: Dict[tuple[str, str], List[str]] = {}
    iso_list: List[str] = []
    for country in payload.get("country_blocks", []) or []:
        iso = country.get("iso_code")
        if not iso:
            continue
        iso = str(iso)
        iso_list.append(iso)
        for block in country.get("blocks", []) or []:
            key = str(block.get("key") or "")
            series_codes = [str(x) for x in (block.get("series_codes") or [])]

            required_in_order: List[str] = []
            eligible_non_required: List[str] = []

            required_all_in_order: List[str] = []
            eligible_all_non_required: List[str] = []
            for series in series_codes:
                # Build harmonization-only lists (ignore coverage threshold).
                if _is_required_series(series):
                    required_all_in_order.append(series)
                elif series in ALWAYS_KEEP_IF_ABOVE_THRESHOLD:
                    # Counterfactual assumes the series would be retained if it were above threshold.
                    required_all_in_order.append(series)
                else:
                    eligible_all_non_required.append(series)

                if _is_required_series(series):
                    required_in_order.append(series)
                    continue

                cov = series_to_cov.get(series, float("nan"))
                if pd.isna(cov) or cov < series_threshold:
                    continue

                if series in ALWAYS_KEEP_IF_ABOVE_THRESHOLD:
                    required_in_order.append(series)
                else:
                    eligible_non_required.append(series)

            required_by_block_iso[(key, iso)] = required_in_order
            eligible_by_block_iso[(key, iso)] = eligible_non_required

            required_all_by_block_iso[(key, iso)] = required_all_in_order
            eligible_all_by_block_iso[(key, iso)] = eligible_all_non_required

    iso_list = sorted(set(iso_list))
    # Determine weakest-ISO K per block after applying threshold.
    # This cutoff applies only to non-required series.
    block_keys = sorted({key for (key, _) in eligible_by_block_iso.keys() if key})
    k_by_block: Dict[str, int] = {}
    for key in block_keys:
        counts = [len(eligible_by_block_iso.get((key, iso), [])) for iso in iso_list]
        k_by_block[key] = min(counts) if counts else 0

    # Determine weakest-ISO K per block ignoring the threshold (harmonization-only counterfactual).
    block_keys_all = sorted({key for (key, _) in eligible_all_by_block_iso.keys() if key})
    k_all_by_block: Dict[str, int] = {}
    for key in block_keys_all:
        counts = [len(eligible_all_by_block_iso.get((key, iso), [])) for iso in iso_list]
        k_all_by_block[key] = min(counts) if counts else 0

    dropped_above_threshold: Dict[str, Dict[str, List[Dict[str, float]]]] = {}
    would_survive_but_below_threshold: Dict[str, Dict[str, List[Dict[str, float]]]] = {}
    for country in payload.get("country_blocks", []) or []:
        iso = country.get("iso_code")
        if not iso:
            continue
        iso = str(iso)
        for block in country.get("blocks", []) or []:
            key = str(block.get("key") or "")
            _ = [str(x) for x in (block.get("series_codes") or [])]

            required_in_order = required_by_block_iso.get((key, iso), [])
            eligible = eligible_by_block_iso.get((key, iso), [])
            k = k_by_block.get(key, 0)

            kept = list(required_in_order) + eligible[:k]
            dropped = eligible[k:]

            # Harmonization-only (ignoring threshold) selection, to identify series that fail only
            # because of coverage thresholding.
            required_all = required_all_by_block_iso.get((key, iso), [])
            eligible_all = eligible_all_by_block_iso.get((key, iso), [])
            k_all = k_all_by_block.get(key, 0)
            kept_all = list(required_all) + eligible_all[:k_all]

            below_kept_all = []
            for series in kept_all:
                if _is_required_series(series):
                    # These survive thresholding by design; don't flag as “below threshold”.
                    continue
                cov = series_to_cov.get(series, float("nan"))
                if pd.isna(cov) or float(cov) < series_threshold:
                    below_kept_all.append(series)
            if below_kept_all:
                iso_entry = would_survive_but_below_threshold.setdefault(iso, {})
                block_entry = iso_entry.setdefault(key, [])
                for series in below_kept_all:
                    cov = float(series_to_cov.get(series, float("nan")))
                    block_entry.append({"series": series, "coverage_ratio": cov})

            # Record dropped series that were >= threshold (by construction).
            if dropped:
                iso_entry = dropped_above_threshold.setdefault(iso, {})
                block_entry = iso_entry.setdefault(key, [])
                for series in dropped:
                    cov = float(series_to_cov.get(series, float("nan")))
                    block_entry.append({"series": series, "coverage_ratio": cov})

            # Mutate the block series_codes to the harmonized + cutoff set.
            # If k == 0, keep only REQUIRED_SERIES (hard cutoff for others).
            block["series_codes"] = kept

                # Preserve local_series_files etc; governance is via series_codes.

    return payload, dropped_above_threshold, would_survive_but_below_threshold


def _write_harmonization_report(
    report: Dict[str, Dict[str, List[Dict[str, float]]]],
    would_survive_but_below_threshold: Dict[str, Dict[str, List[Dict[str, float]]]],
    series_threshold: float,
    path: Path,
) -> None:
    lines: List[str] = []
    lines.append("# Harmonization Drops Above Coverage Threshold\n")
    lines.append(f"Generated: {datetime.utcnow().isoformat()}Z\n")
    lines.append(f"Coverage threshold (series_threshold): {series_threshold}\n")

    if not report:
        lines.append("No series were dropped due to harmonization after applying the coverage threshold.\n")
    else:
        for iso in sorted(report.keys()):
            lines.append(f"## {iso}\n")
            by_block = report[iso]
            for block_key in sorted(by_block.keys()):
                entries = by_block[block_key]
                if not entries:
                    continue
                entries_sorted = sorted(entries, key=lambda x: (-(x.get("coverage_ratio") or 0.0), str(x.get("series") or "")))
                formatted = ", ".join(
                    f"{e['series']} ({e['coverage_ratio']:.3f})" if isinstance(e.get("coverage_ratio"), float) else str(e.get("series"))
                    for e in entries_sorted
                )
                lines.append(f"- {block_key}: {formatted}")
            lines.append("")

    lines.append("\n# Below-Threshold Series That Would Survive Harmonization (Counterfactual)\n")
    lines.append("These are series that would be kept by the weakest-ISO harmonization cutoff *if we ignored* the coverage threshold,\n")
    lines.append("but which currently fail the threshold and therefore do not appear in post-threshold outputs.\n")

    if not would_survive_but_below_threshold:
        lines.append("No below-threshold series would have survived harmonization.\n")
        path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
        return

    for iso in sorted(would_survive_but_below_threshold.keys()):
        lines.append(f"## {iso}\n")
        by_block = would_survive_but_below_threshold[iso]
        for block_key in sorted(by_block.keys()):
            entries = by_block[block_key]
            if not entries:
                continue
            entries_sorted = sorted(entries, key=lambda x: (x.get("coverage_ratio") or 0.0, str(x.get("series") or "")))
            formatted = ", ".join(
                f"{e['series']} ({e['coverage_ratio']:.3f})" if isinstance(e.get("coverage_ratio"), float) else str(e.get("series"))
                for e in entries_sorted
            )
            lines.append(f"- {block_key}: {formatted}")
        lines.append("")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    ensure_dirs()
    payload = load_yaml(BLOCKS_PATH)
    meta = _normalize_series_metadata(load_yaml(METADATA_PATH))
    coverage_map = _load_block_coverage_report(BLOCK_COVERAGE_REPORT)

    series_threshold = _load_series_threshold(THRESHOLD_CONFIG_PATH)
    series_to_cov = _load_catalog_coverage(CATALOG_PATH)

    payload, dropped_above_threshold, would_survive_but_below_threshold = _apply_threshold_and_harmonize(
        payload=payload,
        series_to_cov=series_to_cov,
        series_threshold=series_threshold,
    )
    _write_harmonization_report(
        report=dropped_above_threshold,
        would_survive_but_below_threshold=would_survive_but_below_threshold,
        series_threshold=series_threshold,
        path=DIAG_DIR / "harmonization_drops_above_threshold.md",
    )

    shared_series_allowlist = {
        "SWAPTION_VOL_DEU",
    }

    iso_codes = [entry.get("iso_code") for entry in payload.get("country_blocks", []) if entry.get("iso_code")]
    iso_set = {str(code) for code in iso_codes if isinstance(code, str)}

    block_def = {}
    membership = []
    warnings: List[str] = []
    for entry in payload.get("country_blocks", []):
        iso = entry.get("iso_code")
        if not iso:
            continue
        blocks = entry.get("blocks", [])
        block_def[iso] = {
            "country": entry.get("country"),
            "region": entry.get("region"),
            "coverage_window": entry.get("coverage_window"),
            "blocks": [],
        }
        for block in blocks:
            key = block.get("key")
            series_codes = block.get("series_codes", [])

            coverage_payload = coverage_map.get((str(iso), str(key)), {})

            # Flag obvious mis-assignments where a series name ends with another ISO.
            foreign_iso_series: List[str] = []
            for series in series_codes:
                if not isinstance(series, str):
                    continue
                if series in shared_series_allowlist:
                    continue
                suffix = series.split("_")[-1]
                if suffix in iso_set and suffix != iso:
                    foreign_iso_series.append(series)

            if foreign_iso_series:
                warnings.append(
                    f"{iso}:{key} has series ending with a different ISO suffix: {', '.join(sorted(foreign_iso_series))}"
                )

            block_def[iso]["blocks"].append(
                {
                    "key": key,
                    "coverage": block.get("coverage"),
                    "status_notes": block.get("status_notes"),
                    "series_codes": series_codes,
                    "coverage_status": coverage_payload.get("status"),
                    "required_series": coverage_payload.get("required_series"),
                    "required_present": coverage_payload.get("required_present"),
                    "optional_series": coverage_payload.get("optional_series"),
                    "optional_present": coverage_payload.get("optional_present"),
                    "coverage_percentage": coverage_payload.get("coverage_percentage"),
                }
            )
            for series in series_codes:
                is_valid, missing = validate_metadata(series, meta)
                if not is_valid:
                    warnings.append(f"{iso}:{series} missing {', '.join(missing)}")
                membership.append({
                    "iso": iso,
                    "country": entry.get("country"),
                    "region": entry.get("region"),
                    "block": key,
                    "series": series,
                    "metadata_present": is_valid,
                    "missing_fields": ",".join(missing) if missing else "",
                    "coverage_status": coverage_payload.get("status"),
                    "coverage_percentage": coverage_payload.get("coverage_percentage"),
                })
    block_path = OUTPUT_DIR / "country_block_definition.json"
    with block_path.open("w", encoding="utf-8") as fp:
        json.dump(block_def, fp, indent=2)
    membership_df = pd.DataFrame(membership)
    membership_path = DIAG_DIR / "block_membership_matrix.csv"
    membership_df.to_csv(membership_path, index=False)
    warnings_path = DIAG_DIR / "country_block_metadata_warnings.md"
    with warnings_path.open("w", encoding="utf-8") as fp:
        fp.write("# Country Block Metadata Warnings\n\n")
        if warnings:
            fp.write("Warnings:\n")
            for w in warnings:
                fp.write(f"- {w}\n")
        else:
            fp.write("All series metadata present.\n")
    print(f"Country block definition written to {block_path}")
    print(f"Membership matrix: {membership_path}")
    print(f"Metadata warnings: {warnings_path}")
    print(f"Harmonization report: {DIAG_DIR / 'harmonization_drops_above_threshold.md'}")


if __name__ == "__main__":
    main()