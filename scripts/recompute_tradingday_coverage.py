"""Recompute trading-day aware coverage diagnostics for the canonical stress panel."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

CANONICAL_PATH = Path("stress_indicators_expanded.csv")
COVERAGE_OUTPUT = Path("analysis_outputs") / "coverage_validation_tradingday.csv"
LOW_COVERAGE_OUTPUT = Path("analysis_outputs") / "low_coverage_prioritized_from_recompute.csv"
WINDOW_YEARS = 10
EXPECTED_PER_YEAR = {
    "daily": 252,
    "weekly": 52,
    "monthly": 12,
    "quarterly": 4,
    "annual": 1,
}

FREQ_LABELS = [
    (3.0, "daily"),
    (10.0, "weekly"),
    (45.0, "monthly"),
    (120.0, "quarterly"),
    (float("inf"), "annual"),
]


def infer_frequency_label(median_gap: float | None) -> str:
    if median_gap is None or np.isnan(median_gap):
        return "unknown"
    for threshold, label in FREQ_LABELS:
        if median_gap <= threshold:
            return label
    return "unknown"


def compute_median_gap(series: pd.Series) -> float | None:
    idx = series.index.drop_duplicates()
    if len(idx) < 2:
        return None
    gaps = idx.to_series().diff().dt.total_seconds().div(86400.0).dropna()
    if gaps.empty:
        return None
    return float(gaps.median())


def main() -> None:
    if not CANONICAL_PATH.exists():
        raise FileNotFoundError(f"Canonical panel missing at {CANONICAL_PATH}")

    df = pd.read_csv(CANONICAL_PATH, parse_dates=["Date"]).set_index("Date")
    df.sort_index(inplace=True)

    window_end = df.index.max()
    window_start = window_end - pd.DateOffset(years=WINDOW_YEARS)

    rows: list[dict[str, object]] = []
    for column in df.columns:
        series = df[column].dropna()
        observed_non_na = int(series.shape[0])
        if observed_non_na == 0:
            rows.append({
                "series": column,
                "observed_non_na": 0,
                "real_obs_count": 0,
                "median_gap_days_real": None,
                "expected_in_window_tradingday": 0,
                "recomputed_cov_tradingday_realobs": 0.0,
            })
            continue

        median_gap = compute_median_gap(series)
        freq_label = infer_frequency_label(median_gap)
        expected_per_year = EXPECTED_PER_YEAR.get(freq_label, 0)
        expected_window = expected_per_year * WINDOW_YEARS

        window_series = series[series.index >= window_start]
        real_obs = int(window_series.shape[0])
        coverage = float(real_obs / expected_window) if expected_window else 0.0

        rows.append({
            "series": column,
            "observed_non_na": observed_non_na,
            "real_obs_count": real_obs,
            "median_gap_days_real": float(median_gap) if median_gap is not None else None,
            "expected_in_window_tradingday": expected_window,
            "recomputed_cov_tradingday_realobs": min(1.0, max(0.0, coverage)),
        })

    coverage_df = pd.DataFrame(rows)
    coverage_df.sort_values("recomputed_cov_tradingday_realobs", ascending=False, inplace=True)
    COVERAGE_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    coverage_df.to_csv(COVERAGE_OUTPUT, index=False)

    low_cov_df = (
        coverage_df[coverage_df["recomputed_cov_tradingday_realobs"] < 0.62]
        .copy()
        .reset_index(drop=True)
    )
    low_cov_df.to_csv(LOW_COVERAGE_OUTPUT, index=False)

    print("Recomputed trading-day coverage for", len(coverage_df), "series;")
    print("Low-coverage series (<62%):", len(low_cov_df))
    if low_cov_df.empty:
        print("All series pass the 62% threshold for the trailing 10-year window.")
    else:
        print("Top low-coverage candidates:", ", ".join(low_cov_df.head(3)["series"]))


if __name__ == "__main__":
    main()
