"""Utility to backfill stress-indicator series that FRED no longer publishes.

The script synthesizes a small set of missing indicators using transparent proxy
formulas so the downstream stress pipeline can keep running:

- EUR3MTD156N (3M Euribor): approximated with the ECB deposit facility rate
  plus a constant 35 bps spread.
- USD3MTD156N (3M USD Libor): proxied with the 3M U.S. Treasury bill yield
  plus a 30 bps spread.
- ITALRPPPPLOPM (Italy Real Residential Property Prices): computed from the
  nominal BIS house-price index deflated by the Italian CPI.
- GOLDAMGBD228NLBM (London A.M. gold fix): substituted with COMEX gold futures
  settlements (GC=F) sourced from Yahoo Finance.

Running the script updates both `fred_stress_indicators.csv` and the matching
metadata file under `Output/trial data folder/stress_indicators/`.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Callable, Dict

import pandas as pd
from pandas_datareader import data as web
import yfinance as yf
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
OUTPUT_DIR = PROJECT_ROOT / "Output" / "trial data folder" / "stress_indicators"
FRED_PATH = OUTPUT_DIR / "fred_stress_indicators.csv"
METADATA_PATH = OUTPUT_DIR / "fred_metadata.csv"

# Proxy spreads (in percentage points) applied to policy/sovereign rates to
# approximate the removed Euribor and USD Libor series.
EUR_EURIBOR_SPREAD = 0.35
USD_LIBOR_SPREAD = 0.30


def _load_fred_frame() -> pd.DataFrame:
    if not FRED_PATH.exists():
        raise FileNotFoundError(f"Missing FRED export: {FRED_PATH}")

    df = pd.read_csv(FRED_PATH, index_col=0, parse_dates=True)
    df.index.name = "date"
    return df


def _align_to_index(series: pd.Series, target_index: pd.DatetimeIndex) -> pd.Series:
    """Align an arbitrary-frequency series to the target index via forward fill."""

    if series.empty:
        raise ValueError("Received an empty series for alignment")

    series = series.sort_index()
    combined_index = target_index.union(series.index)
    aligned = series.reindex(combined_index).sort_index().ffill()
    return aligned.reindex(target_index)


@dataclass
class SeriesBuilder:
    code: str
    builder: Callable[[pd.DatetimeIndex, pd.Timestamp, pd.Timestamp], pd.Series]
    description: str


def _fetch_fred(code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    data = web.DataReader(code, "fred", start, end)
    data.name = code
    return data.squeeze()


def _build_euribor(target_index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    base = _fetch_fred("ECBDFR", start, end)
    proxy = _align_to_index(base + EUR_EURIBOR_SPREAD, target_index)
    proxy.name = "EUR3MTD156N"
    return proxy


def _build_usd_libor(target_index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    base = _fetch_fred("DTB3", start, end)
    proxy = _align_to_index(base + USD_LIBOR_SPREAD, target_index)
    proxy.name = "USD3MTD156N"
    return proxy


def _build_italy_real_price(target_index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    nominal = _fetch_fred("QITN368BIS", start, end)
    cpi = _fetch_fred("ITACPIALLQINMEI", start, end)
    real_index = (nominal / cpi) * 100.0
    daily = real_index.resample("D").ffill()
    aligned = _align_to_index(daily, target_index)
    aligned.name = "ITALRPPPPLOPM"
    return aligned


def _build_gold(target_index: pd.DatetimeIndex, start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    data = yf.download(
        "GC=F",
        start=start.strftime("%Y-%m-%d"),
        end=(end + pd.Timedelta(days=2)).strftime("%Y-%m-%d"),
        progress=False,
    )

    if data.empty:
        raise RuntimeError("Unable to download GC=F quotes from Yahoo Finance")

    closes = data["Close"].tz_localize(None)
    aligned = _align_to_index(closes, target_index)
    aligned.name = "GOLDAMGBD228NLBM"
    return aligned


SERIES_BUILDERS: Dict[str, SeriesBuilder] = {
    "EUR3MTD156N": SeriesBuilder(
        code="EUR3MTD156N",
        builder=_build_euribor,
        description="ECB deposit facility plus 35 bps",
    ),
    "USD3MTD156N": SeriesBuilder(
        code="USD3MTD156N",
        builder=_build_usd_libor,
        description="3M Treasury bill plus 30 bps",
    ),
    "ITALRPPPPLOPM": SeriesBuilder(
        code="ITALRPPPPLOPM",
        builder=_build_italy_real_price,
        description="BIS nominal house price deflated by Italian CPI",
    ),
    "GOLDAMGBD228NLBM": SeriesBuilder(
        code="GOLDAMGBD228NLBM",
        builder=_build_gold,
        description="COMEX gold futures close (GC=F)",
    ),
}


def _rebuild_metadata(df: pd.DataFrame) -> pd.DataFrame:
    from config.stress_indicators_config import ALL_FRED_SERIES

    rows = []
    for code, meta in ALL_FRED_SERIES.items():
        if code not in df.columns:
            continue
        series = df[code]
        first_date = series.first_valid_index()
        last_date = series.last_valid_index()
        observations = int(series.notna().sum())
        missing_pct = float(series.isna().mean() * 100)
        rows.append(
            {
                "series_code": code,
                "name": meta.get("name", code),
                "frequency": meta.get("frequency", "unknown"),
                "category": meta.get("category", "unknown"),
                "country": meta.get("country", meta.get("region", "N/A")),
                "first_date": first_date.date() if first_date is not None else "",
                "last_date": last_date.date() if last_date is not None else "",
                "observations": observations,
                "missing_pct": missing_pct,
            }
        )

    metadata = pd.DataFrame(rows)
    return metadata.sort_values("series_code")


def backfill(force: bool = False) -> None:
    df = _load_fred_frame()
    target_index = df.index
    start = target_index.min()
    end = target_index.max()

    updates = {}
    for code, builder in SERIES_BUILDERS.items():
        if not force and code in df.columns and df[code].notna().any():
            print(f"Skipping {code}: already populated")
            continue
        print(f"Building proxy for {code}: {builder.description}")
        updates[code] = builder.builder(target_index, start, end)

    if not updates:
        print("No updates were required.")
        return

    for code, series in updates.items():
        df[code] = series

    df = df.sort_index(axis=1)
    df.to_csv(FRED_PATH)
    print(f"Updated FRED dataset with {len(updates)} proxy series -> {FRED_PATH}")

    metadata = _rebuild_metadata(df)
    metadata.to_csv(METADATA_PATH, index=False)
    print(f"Rebuilt metadata -> {METADATA_PATH}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill missing stress indicators with proxies.")
    parser.add_argument("--force", action="store_true", help="Recompute proxies even if the columns already exist.")
    args = parser.parse_args()
    backfill(force=args.force)


if __name__ == "__main__":
    main()
