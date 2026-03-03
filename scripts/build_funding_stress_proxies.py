#!/usr/bin/env python3
"""Build funding-stress proxy series used in country blocks.

Creates/updates the following series codes (CSV files) under `data_repository/raw/fred/`:
This script is retained for backwards-compatibility but no longer builds any active
block series. Funding-stress is now represented via the higher-coverage
`COMM_PAPER_SPREAD_USA/EUR` series (derived elsewhere).

Notes
-----
- This script is conservative: if a required input series cannot be fetched from FRED and is not already
  present in the local cache, the corresponding proxy will not be written.
- The proxy names are stable and match `outputs/country_block_definition.json`.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
from pandas_datareader import data as web


BASE_DIR = Path(__file__).resolve().parent.parent
FRED_DIR = BASE_DIR / "data_repository" / "raw" / "fred"
CATALOG_CSV = BASE_DIR / "data_repository" / "catalog.csv"

CATALOG_FIELDS = [
    "dataset_name",
    "category",
    "frequency",
    "coverage",
    "source",
    "source_url",
    "storage_path",
    "refresh_method",
    "last_updated",
    "notes",
]


@dataclass(frozen=True)
class FetchResult:
    code: str
    ok: bool
    path: Optional[Path]
    error: Optional[str]


def _read_fred_csv(path: Path, column_name: str) -> pd.Series:
    df = pd.read_csv(path)

    # Support both common local cache formats:
    # - DATE,<CODE>
    # - date,value  (legacy/ECB-style exports)
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        if column_name in df.columns:
            value_col = column_name
        else:
            raise ValueError(f"Expected column {column_name!r} in {path.name}")
    elif "date" in df.columns and "value" in df.columns:
        df["DATE"] = pd.to_datetime(df["date"], errors="coerce")
        value_col = "value"
    else:
        raise ValueError(f"Unexpected time-series CSV format: {path}")

    df = df[["DATE", value_col]].dropna()
    df = df.sort_values("DATE")
    s = df.set_index("DATE")[value_col]
    s.name = column_name
    return s


def ensure_fred_series_cached(code: str, start: str = "1950-01-01") -> FetchResult:
    """Ensure `data_repository/raw/fred/<code>.csv` exists. Fetches from FRED if missing."""
    FRED_DIR.mkdir(parents=True, exist_ok=True)
    out = FRED_DIR / f"{code}.csv"

    if out.exists():
        return FetchResult(code=code, ok=True, path=out, error=None)

    try:
        df = web.DataReader(code, "fred", start)
        # df index is date, single column with name `code`
        df = df.dropna()
        if df.empty:
            return FetchResult(code=code, ok=False, path=None, error="Fetched but empty")
        df.to_csv(out, index_label="DATE")
        return FetchResult(code=code, ok=True, path=out, error=None)
    except Exception as e:
        return FetchResult(code=code, ok=False, path=None, error=str(e))


def first_available_series(codes: Iterable[str]) -> Optional[str]:
    for c in codes:
        res = ensure_fred_series_cached(c)
        if res.ok:
            return c
    return None


def write_proxy_series(code: str, series: pd.Series) -> Path:
    out = FRED_DIR / f"{code}.csv"
    df = series.dropna().sort_index().to_frame(name=code)
    df.index.name = "DATE"
    df.to_csv(out)
    return out


def _coverage_str(series: pd.Series) -> str:
    series = series.dropna()
    if series.empty:
        return "empty"
    start = series.index.min().date().isoformat()
    end = series.index.max().date().isoformat()
    return f"{start}-{end}"


def _infer_frequency(series: pd.Series) -> str:
    s = series.dropna()
    if s.empty:
        return "unknown"
    if len(s.index) < 3:
        return "unknown"
    diffs = s.index.to_series().diff().dropna()
    if diffs.empty:
        return "unknown"
    median_days = float(diffs.dt.total_seconds().median() / 86400.0)
    if median_days <= 7:
        return "daily"
    if 20 <= median_days <= 40:
        return "monthly"
    if 70 <= median_days <= 110:
        return "quarterly"
    return "unknown"


def upsert_catalog_entry(entry: dict) -> None:
    rows: list[dict] = []
    if CATALOG_CSV.exists():
        with CATALOG_CSV.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows.extend(reader)

    names = [r.get("dataset_name") for r in rows]
    if entry["dataset_name"] in names:
        idx = names.index(entry["dataset_name"])
        rows[idx].update(entry)
    else:
        rows.append(entry)

    with CATALOG_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CATALOG_FIELDS})


def main() -> int:
    print("Nothing to do: retired proxy builder (GCF_REPO_FED/EONIA_DEPO_SPREAD removed).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
