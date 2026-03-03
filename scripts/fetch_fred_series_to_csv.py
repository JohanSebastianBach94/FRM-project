"""Fetch one or more FRED series and save as local CSVs.

Writes: data_repository/raw/fred/<SERIES>.csv with columns DATE,<SERIES>
This format is compatible with merge_industry_data.py loader.

Usage:
  python scripts/fetch_fred_series_to_csv.py SPACPIALLMINMEI BAMLH0A0HYBB
"""

from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

try:
    from pandas_datareader import data as web
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "pandas_datareader is required. Install with: pip install pandas_datareader"
    ) from exc


BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data_repository" / "raw" / "fred"


def fetch_one(series_code: str, start: str = "1950-01-01") -> pd.DataFrame:
    df = web.DataReader(series_code, "fred", start=start)
    if df is None or df.empty:
        raise ValueError(f"FRED returned empty data for {series_code}")

    df = df.reset_index().rename(columns={"DATE": "DATE"})
    if "DATE" not in df.columns:
        # pandas_datareader sometimes uses 'DATE' as the index name but 'index' column after reset
        if "index" in df.columns:
            df = df.rename(columns={"index": "DATE"})

    if "DATE" not in df.columns or series_code not in df.columns:
        raise ValueError(f"Unexpected columns for {series_code}: {list(df.columns)}")

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df[series_code] = pd.to_numeric(df[series_code], errors="coerce")
    df = df.dropna(subset=["DATE", series_code]).sort_values("DATE")
    return df


def main(argv: list[str]) -> int:
    series_list = [a.strip() for a in argv if a.strip()]
    if not series_list:
        print("Provide one or more FRED series codes.")
        return 2

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for code in series_list:
        try:
            df = fetch_one(code)
        except Exception as exc:
            print(f"[WARN] {code}: {exc}")
            continue

        out_path = OUT_DIR / f"{code}.csv"
        df.to_csv(out_path, index=False)
        print(f"[OK] {code} -> {out_path} ({len(df):,} rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
