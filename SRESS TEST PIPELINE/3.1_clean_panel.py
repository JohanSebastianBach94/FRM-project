"""Phase 3.1: Build the cleaned monthly panel with QC reporting."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

REGRESSION_LONG = PROJECT_ROOT / "data_final" / "regression_long.csv"
METADATA_FILE = PROJECT_ROOT / "config" / "series_metadata.yaml"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
MISSING_LOG = OUTPUT_DIR / "missing_series.log"
QC_SUMMARY = OUTPUT_DIR / "panel_qc_summary.json"
CLEANED_PANEL = PROJECT_ROOT / "data" / "cleaned_monthly_panel.parquet"
FULL_CLEANED_PANEL = PROJECT_ROOT / "data" / "cleaned_monthly_panel_full.parquet"
MIN_COVERAGE = 0.62
FREQ_RULE = "BME"


def load_metadata(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Metadata file missing: {path}")
    with path.open() as fh:
        payload = yaml.safe_load(fh) or {}
    if not isinstance(payload, dict):
        return {}
    # Support both historical and current config shapes.
    nested = payload.get("series_metadata") or payload.get("series")
    if isinstance(nested, dict):
        return nested
    return payload


def build_panel(df: pd.DataFrame, metadata: dict[str, dict[str, str]]) -> pd.DataFrame:
    df = df[df["series_code"].isin(metadata)].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    panel = df.pivot(index="date", columns="series_code", values="value")
    panel = panel.resample(FREQ_RULE).last()
    return panel


def apply_transform(panel: pd.DataFrame, metadata: dict[str, dict[str, str]]) -> pd.DataFrame:
    transformed = panel.copy()
    for col in panel.columns:
        transform = metadata.get(col, {}).get("transform", "identity").lower()
        col_series = panel[col]
        if transform == "log_pct_change":
            positive = col_series > 0
            logged = np.log(col_series.where(positive))
            transformed[col] = logged.diff()
        else:
            transformed[col] = col_series
    return transformed


def enforce_coverage(panel: pd.DataFrame, metadata: dict[str, dict[str, str]], min_coverage: float) -> tuple[pd.DataFrame, list[dict]]:
    if panel.empty:
        return panel, []

    # This script builds a *monthly* panel via resampling, so coverage must be
    # evaluated on the resulting monthly index (not the raw series' source frequency).
    start, end = panel.index.min(), panel.index.max()
    expected_index = pd.date_range(start=start, end=end, freq=FREQ_RULE)
    expected_count = int(len(expected_index))
    coverage_info: list[dict] = []
    keep_cols: list[str] = []
    for col in panel.columns:
        ser = panel[col]
        observed = int(ser.notna().sum())
        coverage = float(observed / expected_count) if expected_count else 0.0
        missing_slots = int(max(expected_count - observed, 0))
        info = {
            "series": col,
            "coverage": coverage,
            "missing_months": missing_slots,
        }
        if coverage >= min_coverage:
            keep_cols.append(col)
        else:
            coverage_info.append(info)
    kept_panel = panel[keep_cols]
    return kept_panel, coverage_info


def log_missing(issues: list[dict[str, float]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"# Missing coverage log {datetime.utcnow().isoformat()}Z\n"]
    lines.append("series_code,coverage,missing_months\n")
    for info in issues:
        lines.append(f"{info['series']},{info['coverage']:.2%},{info['missing_months']}\n")
    path.write_text("".join(lines))


def write_qc_summary(panel: pd.DataFrame, dropped: list[dict], path: Path) -> None:
    summary = {
        "run": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "start_date": panel.index.min().strftime("%Y-%m-%d") if not panel.empty else None,
        "end_date": panel.index.max().strftime("%Y-%m-%d") if not panel.empty else None,
        "series_count": panel.shape[1],
        "dropped_series": dropped,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2))


def main() -> None:
    metadata = load_metadata(METADATA_FILE)
    df = pd.read_csv(REGRESSION_LONG)
    panel = build_panel(df, metadata)
    transformed = apply_transform(panel, metadata)
    FULL_CLEANED_PANEL.parent.mkdir(parents=True, exist_ok=True)
    transformed.to_parquet(FULL_CLEANED_PANEL)
    cleaned, dropped = enforce_coverage(transformed, metadata, MIN_COVERAGE)
    log_missing(dropped, MISSING_LOG)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CLEANED_PANEL.parent.mkdir(parents=True, exist_ok=True)
    cleaned.to_parquet(CLEANED_PANEL)
    write_qc_summary(cleaned, dropped, QC_SUMMARY)
    print(
        f"Cleaned panel saved to {CLEANED_PANEL.relative_to(PROJECT_ROOT)} ({cleaned.shape[0]} rows, {cleaned.shape[1]} series)."
    )
    print(
        f"Full monthly panel (pre-coverage filter) saved to {FULL_CLEANED_PANEL.relative_to(PROJECT_ROOT)} ({transformed.shape[1]} series)."
    )


if __name__ == "__main__":
    main()