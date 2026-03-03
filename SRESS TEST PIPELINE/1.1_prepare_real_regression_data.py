"""Convert wide-format macro dataset into long format for regression diagnostics."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.calendar_utils import (
    expected_dates_between,
    load_series_metadata,
    normalize_frequency_label,
)

METADATA_FILE = PROJECT_ROOT / "config" / "series_metadata.yaml"
DIAGNOSTICS_DIR = PROJECT_ROOT / "analysis_outputs" / "diagnostics"
CALENDAR_FILTER_REPORT = DIAGNOSTICS_DIR / "series_calendar_filter_report.csv"


def convert(input_path: Path, output_path: Path) -> None:
    df = pd.read_csv(input_path, index_col=0, parse_dates=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.index.name = "date"
    long_df = df.reset_index().melt(id_vars="date", var_name="series_code", value_name="value")
    long_df.dropna(subset=["value"], inplace=True)

    metadata = load_series_metadata(PROJECT_ROOT, METADATA_FILE)
    keep_mask = pd.Series(False, index=long_df.index)
    report_rows: list[dict[str, object]] = []
    for series, group in long_df.groupby("series_code"):
        freq_label = normalize_frequency_label(
            metadata.get(series, {}).get("frequency", "daily")
        )
        start = group["date"].min()
        end = group["date"].max()
        expected = expected_dates_between(start, end, freq_label)
        if not expected.empty:
            mask = (long_df["series_code"] == series) & long_df["date"].isin(expected)
        else:
            mask = long_df["series_code"] == series
        keep_mask |= mask

        # Lightweight diagnostics: how many rows were outside the expected calendar?
        total_rows = int(len(group))
        kept_rows = int(mask.sum())
        dropped_rows = int(max(total_rows - kept_rows, 0))
        if dropped_rows:
            report_rows.append(
                {
                    "series_code": series,
                    "frequency": freq_label,
                    "start": pd.Timestamp(start).date().isoformat() if pd.notna(start) else None,
                    "end": pd.Timestamp(end).date().isoformat() if pd.notna(end) else None,
                    "rows_total": total_rows,
                    "rows_kept": kept_rows,
                    "rows_dropped_outside_calendar": dropped_rows,
                }
            )

    if not keep_mask.any():
        keep_mask = pd.Series(True, index=long_df.index)

    filtered_before = len(long_df)
    long_df = long_df[keep_mask]
    filtered_after = len(long_df)
    if filtered_before != filtered_after:
        print(f"[INFO] Dropped {filtered_before - filtered_after} rows outside expected calendars.")

    if report_rows:
        DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(report_rows).sort_values(
            by=["rows_dropped_outside_calendar", "series_code"], ascending=[False, True]
        ).to_csv(CALENDAR_FILTER_REPORT, index=False)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(output_path, index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare regression input by melting a wide CSV to long format.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data_final/dcc_garch_input_final_34series.csv"),
        help="Wide-format CSV with a date index column",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data_final/regression_long.csv"),
        help="Destination long-format CSV (columns: date, series_code, value)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    convert(args.input, args.output)


if __name__ == "__main__":
    main()
