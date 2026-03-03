#!/usr/bin/env python3
"""Audit the risk factor holes feed and explain each flagged driver."""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Iterable

import numpy as np
import pandas as pd
from pathlib import Path

FEED_COLUMNS = (
    "missing_required_list",
    "missing_optional_list",
    "insufficient_required_list",
    "insufficient_optional_list",
    "insufficient_required_details",
    "insufficient_optional_details",
)

SHORTLIST_DIR = Path("analysis_outputs") / "factors_daily_shortlist"
OUTPUT_CSV = Path("analysis_outputs") / "diagnostics" / "hole_audit.csv"


def parse_items(value: str) -> Iterable[str]:
    if not isinstance(value, str):
        return []
    for entry in value.split("|"):
        entry = entry.strip()
        if not entry:
            continue
        yield entry.split(" (", 1)[0].strip()


def longest_run(flag_array: Iterable[bool]) -> int:
    max_run = run = 0
    for flag in flag_array:
        if flag:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return max_run


def longest_flat(series: pd.Series) -> int:
    arr = series.dropna().to_numpy()
    if arr.size == 0:
        return 0
    max_run = run = 1
    for prev, curr in zip(arr, arr[1:]):
        if curr == prev:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 1
    return max_run


def collect_flagged_factors(feed_path: Path) -> pd.DataFrame:
    df = pd.read_csv(feed_path, dtype=str).fillna("")
    records = []
    for _, row in df.iterrows():
        iso = row["iso"].upper()
        for column in FEED_COLUMNS:
            for factor in parse_items(row.get(column, "")):
                records.append({"iso": iso, "factor": factor, "reason": column})
    return pd.DataFrame(records)


def load_shortlists(directory: Path) -> dict[str, pd.DataFrame]:
    shortlists = {}
    for path in sorted(directory.glob("*_factors_daily_shortlist.csv")):
        iso = path.stem.split("_")[0].upper()
        shortlists[iso] = pd.read_csv(path, parse_dates=["date"]).set_index("date")
    return shortlists


def compute_stats(shortlists: dict[str, pd.DataFrame], flagged: pd.DataFrame) -> pd.DataFrame:
    rows = []
    flagged = flagged.drop_duplicates()
    grouped = flagged.groupby(["iso", "factor"])
    for (iso, factor), group in grouped:
        df = shortlists.get(iso)
        if df is None:
            rows.append(
                {
                    "iso": iso,
                    "factor": factor,
                    "coverage": np.nan,
                    "max_na_run": np.nan,
                    "flat_run": np.nan,
                    "reasons": ",".join(sorted(group["reason"].unique())),
                    "note": "shortlist missing",
                }
            )
            continue
        if factor not in df.columns:
            rows.append(
                {
                    "iso": iso,
                    "factor": factor,
                    "coverage": 0.0,
                    "max_na_run": 0,
                    "flat_run": 0,
                    "reasons": ",".join(sorted(group["reason"].unique())),
                    "note": "factor absent from shortlist",
                }
            )
            continue
        series = df[factor]
        coverage = float(series.notna().mean())
        max_na_run = longest_run(series.isna())
        flat_run = longest_flat(series)
        rows.append(
            {
                "iso": iso,
                "factor": factor,
                "coverage": coverage,
                "max_na_run": int(max_na_run),
                "flat_run": int(flat_run),
                "reasons": ",".join(sorted(group["reason"].unique())),
                "note": "",
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    feed_path = Path("analysis_outputs") / "risk_factor_holes_feed.csv"
    if not feed_path.exists():
        raise FileNotFoundError(f"Missing risk feed at {feed_path}")
    flagged = collect_flagged_factors(feed_path)
    shortlists = load_shortlists(SHORTLIST_DIR)
    stats = compute_stats(shortlists, flagged)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    stats.sort_values(["iso", "coverage", "factor"], ascending=[True, False, True]).to_csv(OUTPUT_CSV, index=False)
    summary = Counter(flagged["factor"])
    print("Hole audit completed. Factors flagged:")
    for factor, count in summary.most_common():
        print(f"  {factor}: {count} entries")
    print(f"Detailed diagnostics saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
