"""Derive rent-to-income and price-to-income ratios per country from raw series."""
import io
import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
DERIVED_DIR = ROOT / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
DERIVED_DIR.mkdir(parents=True, exist_ok=True)
EUROSTAT_CACHE_DIR = ROOT / "data_repository" / "raw" / "providers" / "eurostat_cache"
EUROSTAT_BASE = "https://api.ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"

DEFAULT_TIMEOUT = 30
LOGGER = logging.getLogger("derive_real_estate_ratios")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)

EURO_COUNTRIES = ["DEU", "FRA", "ITA", "ESP"]
FRED_COUNTRY = "USA"

EUROSTAT_PRICE_PARAMS = {"dataset": "prc_hpi", "unit": "I5", "freq": "Q"}
EUROSTAT_INCOME_PARAMS = {"dataset": "earn_gr", "freq": "Q"}
EUROSTAT_RENT_PARAMS = [
    {
        "dataset": "prc_hicp_midxr",
        "coicop": "CP0411",
        "unit": "I9",
        "freq": "Q",
        "cache_name": "prc_hicp_midxr_CP0411",
    },
    {
        "dataset": "prc_hicp_midxr",
        "coicop": "CP0412",
        "unit": "I9",
        "freq": "Q",
        "cache_name": "prc_hicp_midxr_CP0412",
    },
]

FRED_SERIES = {
    "USA": {
        "price": "CSUSHPISA",
        "rent": "CUSR0000SEHA",
        "income": "PI",
    },
}


def _parse_eurostat_csv(text: str) -> Optional[pd.DataFrame]:
    df = pd.read_csv(io.StringIO(text))
    time_col = next((c for c in df.columns if c.lower().startswith("time")), None)
    value_col = next((c for c in df.columns if c.lower().startswith("value")), None)
    geo_col = next((c for c in df.columns if c.lower() == "geo"), None)
    if not {time_col, value_col, geo_col}.issubset(df.columns):
        return None
    df = df[[time_col, geo_col, value_col]].dropna()
    df = df.rename(columns={time_col: "date", geo_col: "geo", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.set_index("date")
    return df


def _load_cached_series(cache_name: str, iso: str) -> Optional[pd.Series]:
    cache_path = EUROSTAT_CACHE_DIR / cache_name / f"{iso}.csv"
    if not cache_path.exists():
        return None
    df = pd.read_csv(cache_path)
    date_col = next((c for c in df.columns if c.lower() == "date"), None)
    value_col = next((c for c in df.columns if c.lower() == "value"), None)
    if date_col is None or value_col is None:
        return None
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
    df = df.dropna(subset=[date_col])
    series = pd.Series(df[value_col].values, index=df[date_col], name=value_col)
    return series.sort_index().dropna()


def fetch_eurostat_series(iso: str, params: Dict[str, str]) -> pd.Series:
    params = params.copy()
    dataset = params.pop("dataset")
    cache_name = params.pop("cache_name", dataset)
    cached = _load_cached_series(cache_name, iso)
    if cached is not None and not cached.empty:
        return cached
    query = {"format": "CSV", "precision": "1", "geo": iso}
    query.update(params)
    try:
        response = requests.get(f"{EUROSTAT_BASE}/{dataset}", params=query, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("Eurostat fetch failed for %s %s: %s", iso, dataset, exc)
        return pd.Series(dtype=float)
    df = _parse_eurostat_csv(response.text)
    if df is None:
        LOGGER.warning("Eurostat replied with unexpected schema for %s %s", iso, dataset)
        return pd.Series(dtype=float)
    pivot = df.pivot_table(index=df.index, columns="geo", values="value")
    return pivot.get(iso, pd.Series(dtype=float)).dropna()


def fetch_fred_series(series_id: str) -> pd.Series:
    try:
        response = requests.get(
            FRED_BASE,
            params={"id": series_id, "cosd": "1990-01-01", "freq": "m"},
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("FRED fetch failed for %s: %s", series_id, exc)
        return pd.Series(dtype=float)
    df = pd.read_csv(io.StringIO(response.text))
    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    value_col = next((c for c in df.columns if c != date_col), None)
    if date_col is None or value_col is None:
        LOGGER.warning("FRED %s lacked expected columns", series_id)
        return pd.Series(dtype=float)
    df = df[[date_col, value_col]].dropna()
    df = df.rename(columns={date_col: "date", value_col: series_id})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.set_index("date")
    return df[series_id].dropna()


def _resample_series(series: pd.Series, rule: str = "ME") -> pd.Series:
    if series.empty:
        return series
    return series.resample(rule).last().dropna()


def _export(series: pd.Series, label: str) -> None:
    if series.empty:
        LOGGER.warning("No data to export for %s", label)
        return
    path = DERIVED_DIR / f"{label}.csv"
    df = series.to_frame("value")
    df.index.name = "date"
    df.to_csv(path)
    LOGGER.info("Exported %s with %d rows", path.name, len(df))


def derive_euro_ratios() -> None:
    for iso in EURO_COUNTRIES:
        income = fetch_eurostat_series(iso, EUROSTAT_INCOME_PARAMS.copy())
        if income.empty:
            LOGGER.warning("Skipping ratios for %s because income data is missing", iso)
            continue
        price = fetch_eurostat_series(iso, EUROSTAT_PRICE_PARAMS.copy())
        if price.empty:
            LOGGER.warning("No price series for %s", iso)
        rent = pd.Series(dtype=float)
        for rent_conf in EUROSTAT_RENT_PARAMS:
            rent = fetch_eurostat_series(iso, rent_conf.copy())
            if not rent.empty:
                break
        if rent.empty:
            LOGGER.warning("No rent series for %s", iso)
        if not price.empty:
            common = income.index.intersection(price.index)
            if not common.empty:
                ratio = price.loc[common] / income.loc[common]
                ratio = ratio.rename(f"price_to_income_ratio_{iso}")
                _export(_resample_series(ratio, rule="Q"), f"Price_to_income_ratio_{iso}")
        if not rent.empty:
            common = income.index.intersection(rent.index)
            if not common.empty:
                ratio = rent.loc[common] / income.loc[common]
                ratio = ratio.rename(f"rent_to_income_ratio_{iso}")
                _export(_resample_series(ratio, rule="Q"), f"Rent_to_income_ratio_{iso}")


def derive_usa_ratios() -> None:
    cfg = FRED_SERIES[FRED_COUNTRY]
    income = fetch_fred_series(cfg["income"])
    if income.empty:
        LOGGER.warning("No income series for USA")
        return
    price = fetch_fred_series(cfg["price"])
    rent = fetch_fred_series(cfg["rent"])
    if not price.empty:
        ratio = (price / income).rename("price_to_income_ratio_USA")
        _export(_resample_series(ratio), "Price_to_income_ratio_USA")
    if not rent.empty:
        ratio = (rent / income).rename("rent_to_income_ratio_USA")
        _export(_resample_series(ratio), "Rent_to_income_ratio_USA")


def main() -> None:
    LOGGER.info("Deriving real-estate ratios")
    derive_euro_ratios()
    derive_usa_ratios()
    LOGGER.info("Real-estate ratio derivation complete")


if __name__ == "__main__":
    main()