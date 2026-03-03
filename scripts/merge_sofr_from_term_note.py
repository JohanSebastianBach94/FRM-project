"""Produce a SOFR 3M series combining NY Fed term-note realized rates with the existing derived file."""
from __future__ import annotations

from pathlib import Path
import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
RAW_PROVIDERS = THIS_DIR.parent / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
STRUCTURAL = THIS_DIR.parent / "data_repository" / "raw" / "structural"

TERM_NOTE_PATH = STRUCTURAL / "FED_Note_Term_SOFR.csv"
SOFR_PATH = RAW_PROVIDERS / "SOFR_3m.csv"


def build_sofr_series() -> pd.DataFrame:
    term = pd.read_csv(TERM_NOTE_PATH, skiprows=10)
    term["DATE"] = pd.to_datetime(term["DATE"], dayfirst=True, errors="coerce")
    term["REALIZED_3M"] = pd.to_numeric(term["REALIZED_3M"], errors="coerce")
    realized = term[["DATE", "REALIZED_3M"]].dropna()
    realized = realized.rename(columns={"REALIZED_3M": "sofr_3m"})
    realized = realized.dropna(subset=["DATE"])

    legacy = pd.read_csv(SOFR_PATH)
    legacy["DATE"] = pd.to_datetime(legacy["date"], dayfirst=True, errors="coerce")
    legacy["sofr_3m"] = pd.to_numeric(legacy["sofr_3m"], errors="coerce")
    legacy = legacy.dropna(subset=["DATE", "sofr_3m"])

    last_realized = realized["DATE"].max()
    legacy = legacy.loc[legacy["DATE"] > last_realized]

    combined = pd.concat([realized, legacy[["DATE", "sofr_3m"]]], ignore_index=True)
    combined = combined.drop_duplicates(subset=["DATE"], keep="first")
    combined = combined.sort_values("DATE")
    combined["date"] = combined["DATE"].dt.strftime("%Y-%m-%d")

    return combined[["date", "sofr_3m"]]


def main() -> None:
    result = build_sofr_series()
    result.to_csv(SOFR_PATH, index=False)
    print("Updated", SOFR_PATH)
    print(result.tail())


if __name__ == "__main__":
    main()
