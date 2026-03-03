"""Utility to refresh EURIBOR_3m and COMM_PAPER_SPREAD_EUR derived series."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from fredapi import Fred

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_REPO = BASE_DIR / "data_repository"
RAW_DATA_DIR = DATA_REPO / "raw"
DERIVED_DIR = RAW_DATA_DIR / "providers" / "derived_risk_drivers"

START_DATE = "1990-01-01"


def ensure_dirs() -> None:
    (RAW_DATA_DIR / "fred").mkdir(parents=True, exist_ok=True)
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_fred_series(fred: Fred, series_id: str) -> pd.Series:
    series = fred.get_series(series_id, observation_start=START_DATE)
    s = pd.Series(series).dropna()
    s.index = pd.to_datetime(s.index)
    return s


def main() -> None:
    load_dotenv()
    fred = Fred(api_key=os.getenv("FRED_API_KEY"))
    ensure_dirs()

    # Euribor proxy (Euro area 3M interbank)
    euribor = fetch_fred_series(fred, "IR3TIB01EZM156N")
    euribor.to_frame("value").to_csv(
        RAW_DATA_DIR / "fred" / "IR3TIB01EZM156N.csv",
        index_label="date",
    )

    euribor_daily = euribor.resample("D").ffill()
    euribor_daily.name = "EURIBOR_3m"
    euribor_daily.to_frame("euribor_3m").to_csv(
        DERIVED_DIR / "EURIBOR_3m.csv",
        index_label="date",
    )

    # Euro HY minus Euribor spread
    hy = fetch_fred_series(fred, "BAMLHE00EHYIEY").rename("corp_yield")
    joined = pd.concat([hy, euribor_daily.rename("eur3m")], axis=1, join="inner").dropna()
    monthly = joined.resample("M").mean()
    comm_spread = (monthly["corp_yield"] - monthly["eur3m"]).dropna()
    comm_spread.name = "COMM_PAPER_SPREAD_EUR"
    comm_spread.to_frame("comm_paper_spread").to_csv(
        DERIVED_DIR / "comm_paper_spread_EUR.csv",
        index_label="date",
    )

    print("Generated EURIBOR_3m.csv and comm_paper_spread_EUR.csv")


if __name__ == "__main__":
    main()
