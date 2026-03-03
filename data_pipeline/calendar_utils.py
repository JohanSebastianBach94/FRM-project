"""Calendar helpers to align coverage calculations with actual trading calendars."""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
import yaml

FREQ_MAP: Dict[str, str] = {
    "daily": "B",
    "trading": "B",
    "weekly": "W-FRI",
    "monthly": "M",
    "quarterly": "Q",
    "annual": "A",
    "yearly": "A",
}


def normalize_frequency_label(label: Optional[str]) -> str:
    if not label:
        return "daily"
    label = str(label).strip().lower()
    if label in {"business", "business_day"}:
        return "daily"
    if label in {"year", "yr"}:
        return "annual"
    return label


def expected_dates_between(start: pd.Timestamp, end: pd.Timestamp, freq_label: str) -> pd.DatetimeIndex:
    if start is None or end is None or pd.isna(start) or pd.isna(end) or start > end:
        return pd.DatetimeIndex([])

    freq_label = normalize_frequency_label(freq_label)
    pandas_freq = FREQ_MAP.get(freq_label, "D")

    if pandas_freq == "B":
        return pd.bdate_range(start=start, end=end)

    return pd.date_range(start=start, end=end, freq=pandas_freq)


def expected_count_between(start: pd.Timestamp, end: pd.Timestamp, freq_label: str) -> int:
    return len(expected_dates_between(start, end, freq_label))


def weekend_dates_between(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    if start is None or end is None or pd.isna(start) or pd.isna(end) or start > end:
        return pd.DatetimeIndex([])
    all_days = pd.date_range(start=start, end=end, freq="D")
    return all_days[all_days.weekday >= 5]


def count_weekend_gaps(start: pd.Timestamp, end: pd.Timestamp, observed: Iterable[pd.Timestamp]) -> int:
    weekend_days = weekend_dates_between(start, end)
    if weekend_days.empty:
        return 0
    observed_idx = pd.DatetimeIndex(observed).intersection(weekend_days)
    return len(weekend_days.difference(observed_idx))


def load_series_metadata(base_dir: Path, metadata_path: Optional[Path] = None) -> dict[str, dict]:
    path = metadata_path or (base_dir / "config" / "series_metadata.yaml")
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    if isinstance(data, dict) and "series_metadata" in data:
        raw = data["series_metadata"]
    else:
        raw = data
    if isinstance(raw, dict):
        return {k: (v if isinstance(v, dict) else {}) for k, v in raw.items()}
    return {}