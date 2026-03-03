"""Fallback sector ETF loader using Stooq (avoids Yahoo-history limits)."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import io
import logging

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DERIVED_DIR = ROOT / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
DERIVED_DIR.mkdir(parents=True, exist_ok=True)

LOGGER = logging.getLogger("fetch_stooq_sector_etfs")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)

STOOQ_BASE_URL = "https://stooq.com/q/d/l/"
SECTOR_ETFS = [
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
]
REQUEST_TIMEOUT = 30


def fetch_stooq_series(ticker: str) -> pd.DataFrame:
    params = {
        "s": f"{ticker.lower()}.us",
        "i": "d",
        "d1": "19900101",
    }
    try:
        response = requests.get(STOOQ_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Stooq download failed for %s: %s", ticker, exc)
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(response.text))
    if df.empty or "Close" not in df.columns:
        LOGGER.warning("Stooq returned no close data for %s", ticker)
        return pd.DataFrame()
    df = df[["Date", "Close"]].dropna()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"])
    df = df.rename(columns={"Date": "date", "Close": ticker})
    df = df.set_index("date")
    return df


def export_series(df: pd.DataFrame, label: str) -> None:
    if df.empty:
        LOGGER.warning("Skipping export for %s, no rows", label)
        return
    path = DERIVED_DIR / f"{label}.csv"
    df.to_csv(path)
    LOGGER.info("Saved %s (%d rows)", path.name, len(df))


def main() -> None:
    LOGGER.info("Fetching Stooq sector ETFs (fallback)")
    for ticker in SECTOR_ETFS:
        series = fetch_stooq_series(ticker)
        export_series(series, ticker)
    LOGGER.info("Stooq sector fetch complete (%s)" % datetime.utcnow().isoformat())


if __name__ == "__main__":
    main()