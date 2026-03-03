from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

TRADING_CALENDAR_PATH = Path("analysis_outputs") / "trading_calendar.csv"


def build_trading_calendar(start: str = "1990-02-01", end: pd.Timestamp | None = None) -> pd.DatetimeIndex:
    end = end or pd.Timestamp.now().normalize()
    calendar = pd.date_range(start=start, end=end, freq="B")
    TRADING_CALENDAR_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"date": calendar}).to_csv(
        TRADING_CALENDAR_PATH, index=False, date_format="%Y-%m-%d"
    )
    return calendar


def get_trading_calendar(start: str = "1990-02-01") -> pd.DatetimeIndex:
    if TRADING_CALENDAR_PATH.exists():
        df = pd.read_csv(TRADING_CALENDAR_PATH, parse_dates=["date"])
        calendar = pd.DatetimeIndex(df["date"])
        desired_start = pd.Timestamp(start)
        if len(calendar) == 0:
            return build_trading_calendar(start=start)
        if calendar[0].normalize() != desired_start.normalize():
            return build_trading_calendar(start=start)
        latest = pd.Timestamp.now().normalize()
        if calendar[-1] >= latest:
            return calendar
    return build_trading_calendar(start=start)


def _categorize_frequency(index: pd.DatetimeIndex) -> Literal["daily", "monthly", "quarterly", "annual", "sparse"]:
    if len(index) < 2:
        return "daily"
    diffs = index.to_series().diff().dropna().dt.days
    if diffs.empty:
        return "daily"
    median_diff = diffs.median()
    if median_diff <= 3:
        return "daily"
    if median_diff <= 35:
        return "monthly"
    if median_diff <= 120:
        return "quarterly"
    if median_diff <= 370:
        return "annual"
    return "sparse"


def _expected_periods(
    freq_category: Literal["daily", "monthly", "quarterly", "annual", "sparse"],
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
    calendar: pd.DatetimeIndex,
) -> int:
    window_start = pd.Timestamp(window_start)
    window_end = pd.Timestamp(window_end)
    if freq_category == "daily":
        return int(((calendar >= window_start) & (calendar <= window_end)).sum())
    if freq_category == "monthly":
        start = window_start.to_period("M")
        end = window_end.to_period("M")
        return len(pd.period_range(start=start, end=end, freq="M"))
    if freq_category == "quarterly":
        start = window_start.to_period("Q")
        end = window_end.to_period("Q")
        return len(pd.period_range(start=start, end=end, freq="Q"))
    if freq_category == "annual":
        start = window_start.to_period("Y")
        end = window_end.to_period("Y")
        return len(pd.period_range(start=start, end=end, freq="Y"))
    return int(((calendar >= window_start) & (calendar <= window_end)).sum())


def _count_unique_periods(
    index: pd.DatetimeIndex,
    freq_category: Literal["daily", "monthly", "quarterly", "annual", "sparse"],
) -> int:
    if index.empty:
        return 0
    if freq_category == "daily":
        return index.normalize().nunique()
    if freq_category == "monthly":
        return index.to_period("M").nunique()
    if freq_category == "quarterly":
        return index.to_period("Q").nunique()
    if freq_category == "annual":
        return index.to_period("Y").nunique()
    return index.normalize().nunique()


def calculate_window_coverage(
    series: pd.Series,
    calendar: pd.DatetimeIndex,
    window_start: pd.Timestamp,
    window_end: pd.Timestamp,
) -> float:
    if series.empty:
        return 0.0
    clipped = series.dropna()
    clipped = clipped.loc[(clipped.index >= window_start) & (clipped.index <= window_end)]
    if clipped.empty:
        return 0.0
    freq_category = _categorize_frequency(clipped.index)
    unique_periods = _count_unique_periods(clipped.index, freq_category)
    expected = _expected_periods(freq_category, window_start, window_end, calendar)
    if expected == 0:
        return 0.0
    return min(unique_periods / expected, 1.0)
