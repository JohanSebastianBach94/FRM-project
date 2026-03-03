#!/usr/bin/env python3
"""Utility helpers to backfill zero-data series cited in catalog.csv.

This module encapsulates the concrete ingestion work needed to clear the remaining
`has_data=False` rows (BIS LBS, NPL proxies, real-estate ratios/HPI, and GC.DOD
coverage gaps). Each helper is idempotent and writes both a CSV file and a small
`.meta.json` file that captures provenance for downstream validation scripts.

Run from the repository root:

    python scripts/ingest_zero_data_series.py --all

You can also target specific groups, e.g. `--bis --npl --debt`.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

# Reuse Eurostat helpers from the existing ratio builder
from derive_real_estate_ratios import (
    DERIVED_DIR as RATIO_OUTPUT_DIR,
    EURO_COUNTRIES,
    EUROSTAT_PRICE_PARAMS,
    fetch_eurostat_series,
    derive_euro_ratios,
)

LOGGER = logging.getLogger("zero_data_ingestion")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)

BASE_DIR = Path(__file__).resolve().parents[1]
BIS_MONTHLY_DIR = BASE_DIR / "data_repository" / "raw" / "providers" / "bis_lbs" / "monthly"
BIS_MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
NPL_MONTHLY_DIR = BASE_DIR / "data_repository" / "raw" / "providers" / "npl_proxies" / "monthly"
NPL_MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
REAL_ESTATE_DIR = BASE_DIR / "data_repository" / "raw" / "providers" / "real_estate"
REAL_ESTATE_DIR.mkdir(parents=True, exist_ok=True)
MACRO_DIR = BASE_DIR / "data_repository" / "raw" / "macro"
MACRO_DIR.mkdir(parents=True, exist_ok=True)

WORLD_BANK_URL = (
    "https://api.worldbank.org/v2/country/{iso3}/indicator/GC.DOD.TOTL.GD.ZS"
    "?format=json&per_page=2000&date=1980:2025"
)
IMF_IFS_URL = (
    "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/A.{iso3}.GGXWDG_GDP"
    "?startPeriod=1980"
)
EUROSTAT_CPI_PARAMS = {
    "dataset": "prc_hicp_midx",
    "coicop": "CP00",
    "unit": "I15",
    "freq": "M",
}


@dataclass
class SeriesTask:
    series_name: str
    iso: str
    source: Path
    destination: Path


def _read_quarterly_csv(path: Path) -> pd.Series:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"Source file {path} is empty")
    date_col = next((c for c in df.columns if c.lower() in {"period", "date"}), df.columns[0])
    value_col = next((c for c in df.columns if c != date_col), df.columns[1])
    periods = pd.PeriodIndex(df[date_col].astype(str), freq="Q-DEC", name="period")
    values = pd.to_numeric(df[value_col], errors="coerce").to_numpy()
    ts = pd.Series(values, index=periods, name=value_col).dropna()
    return ts.to_timestamp(how="end")


def _resample_to_monthly(series: pd.Series) -> pd.Series:
    monthly = series.sort_index().resample("ME").ffill()
    monthly.index.name = "date"
    return monthly.dropna()


def _write_series(series: pd.Series, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = series.to_frame("value")
    df.to_csv(path, index_label="date")


def _write_meta(meta_path: Path, payload: Dict) -> None:
    meta_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def generate_bis_monthly_series() -> List[SeriesTask]:
    """Resample BIS LBS quarterly files to month-end frequency."""

    tasks: List[SeriesTask] = []
    for iso in ("USA", "DEU", "FRA", "ITA", "ESP"):
        src = BASE_DIR / "data_repository" / "processed" / f"BIS_LBS_Household_Loans_{iso}.csv"
        dst = BIS_MONTHLY_DIR / f"BIS_LBS_Household_Loans_{iso}.csv"
        tasks.append(SeriesTask("BIS_LBS_Household_Loans", iso, src, dst))
    for iso in ("USA", "DEU", "ITA"):
        src = BASE_DIR / "data_repository" / "processed" / f"BIS_LBS_Private_NFC_Total_{iso}.csv"
        dst = BIS_MONTHLY_DIR / f"BIS_LBS_Private_NFC_Total_{iso}.csv"
        tasks.append(SeriesTask("BIS_LBS_Private_NFC_Total", iso, src, dst))

    completed: List[SeriesTask] = []
    for task in tasks:
        if not task.source.exists():
            LOGGER.warning("Skipping %s (%s) – missing %s", task.series_name, task.iso, task.source)
            continue
        quarterly = _read_quarterly_csv(task.source)
        monthly = _resample_to_monthly(quarterly)
        if monthly.empty:
            LOGGER.warning("No data produced for %s_%s", task.series_name, task.iso)
            continue
        _write_series(monthly, task.destination)
        meta = {
            "series": f"{task.series_name}_{task.iso}",
            "source": task.source.as_posix(),
            "frequency": "monthly",
            "rows": int(len(monthly)),
            "first_observation": monthly.index.min().isoformat(),
            "last_observation": monthly.index.max().isoformat(),
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        _write_meta(task.destination.with_suffix(".meta.json"), meta)
        LOGGER.info("BIS monthly series ready: %s_%s (%d rows)", task.series_name, task.iso, len(monthly))
        completed.append(task)
    return completed


def generate_npl_monthly_series() -> List[SeriesTask]:
    """Forward-fill quarterly proxy CSVs to monthly frequency."""

    tasks: List[SeriesTask] = []
    for iso in ("USA", "DEU", "FRA", "ITA", "ESP"):
        src = BASE_DIR / "data_repository" / "raw" / "providers" / "npl_proxies" / f"npl_proxy_{iso}.csv"
        dst = NPL_MONTHLY_DIR / f"npl_proxy_{iso}.csv"
        tasks.append(SeriesTask("NPL_PROXY", iso, src, dst))

    completed: List[SeriesTask] = []
    for task in tasks:
        if not task.source.exists():
            LOGGER.warning("Skipping NPL proxy for %s – missing %s", task.iso, task.source)
            continue
        quarterly = _read_quarterly_csv(task.source)
        monthly = _resample_to_monthly(quarterly)
        if monthly.empty:
            LOGGER.warning("No proxy data produced for %s", task.iso)
            continue
        _write_series(monthly, task.destination)
        meta = {
            "series": f"NPL_PROXY_{task.iso}",
            "source": task.source.as_posix(),
            "frequency": "monthly",
            "rows": int(len(monthly)),
            "first_observation": monthly.index.min().isoformat(),
            "last_observation": monthly.index.max().isoformat(),
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        _write_meta(task.destination.with_suffix(".meta.json"), meta)
        LOGGER.info("NPL proxy monthly series ready: %s (%d rows)", task.iso, len(monthly))
        completed.append(task)
    return completed


def fetch_gc_dod_deu() -> Optional[Path]:
    """Fetch Germany's GC.DOD.TOTL.GD.ZS series via World Bank with IMF fallback."""

    def request_json(url: str) -> Optional[dict]:
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            LOGGER.warning("Fetch failed for %s: %s", url, exc)
            return None

    wb_payload = request_json(WORLD_BANK_URL.format(iso3="DEU"))
    records: Dict[int, Dict[str, float | str]] = {}
    if wb_payload and isinstance(wb_payload, list) and len(wb_payload) >= 2:
        for obs in wb_payload[1]:
            try:
                year = int(obs.get("date"))
                value = float(obs.get("value")) if obs.get("value") is not None else None
            except (TypeError, ValueError):
                continue
            if value is not None:
                records[year] = {"value": value, "source": "worldbank"}
        (MACRO_DIR / "wb_GC.DOD.TOTL.GD.ZS_DEU.json").write_text(
            json.dumps(wb_payload, indent=2), encoding="utf-8"
        )
        LOGGER.info("World Bank returned %d GC.DOD observations for DEU", len(records))

    if not records:
        imf_payload = request_json(IMF_IFS_URL.format(iso3="DEU"))
        if imf_payload:
            dataset = imf_payload.get("CompactData", {}).get("DataSet", {})
            series = dataset.get("Series")
            series_list = series if isinstance(series, list) else [series]
            for item in series_list:
                obs_list = item.get("Obs", [])
                obs_iter = obs_list if isinstance(obs_list, list) else [obs_list]
                for obs in obs_iter:
                    year = obs.get("@TIME_PERIOD") or obs.get("@TIME")
                    value = obs.get("@OBS_VALUE")
                    if year and value:
                        try:
                            records[int(year)] = {"value": float(value), "source": "imf"}
                        except ValueError:
                            continue
            LOGGER.info("IMF IFS returned %d GC.DOD observations for DEU", len(records))

    if not records:
        LOGGER.error("GC.DOD.TOTL.GD.ZS fetch failed for DEU")
        return None

    csv_path = MACRO_DIR / "general_government_gross_debt_pct_gdp_DEU.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        fh.write("year,value,indicator,source\n")
        for year in sorted(records):
            fh.write(f"{year},{records[year]['value']},GC.DOD.TOTL.GD.ZS,{records[year]['source']}\n")
    LOGGER.info(
        "Saved GC.DOD series for DEU (%d rows, %s-%s)",
        len(records),
        min(records),
        max(records),
    )
    return csv_path


def build_real_hpi_series(isos: Iterable[str] = EURO_COUNTRIES) -> List[SeriesTask]:
    """Fetch nominal Eurostat HPI + CPI and compute real HPI (2015=100)."""

    tasks: List[SeriesTask] = []
    for iso in isos:
        price = fetch_eurostat_series(iso, EUROSTAT_PRICE_PARAMS.copy())
        if price.empty:
            LOGGER.warning("Eurostat HPI missing for %s", iso)
            continue
        cpi = fetch_eurostat_series(iso, EUROSTAT_CPI_PARAMS.copy())
        if cpi.empty:
            LOGGER.warning("Eurostat CPI missing for %s", iso)
            continue
        cpi_q = cpi.resample("Q").mean()
        aligned = price.reindex(cpi_q.index.union(price.index)).sort_index().ffill()
        real = (aligned / (cpi_q / 100.0)).reindex(price.index).dropna()
        real.index = real.index.to_period("Q").to_timestamp(how="end")
        monthly = _resample_to_monthly(real)
        if monthly.empty:
            LOGGER.warning("No real HPI produced for %s", iso)
            continue
        dest = REAL_ESTATE_DIR / f"{iso}_HPI_REAL.csv"
        _write_series(monthly, dest)
        meta = {
            "series": f"{iso}_HPI_REAL",
            "source": "Eurostat prc_hpi + prc_hicp_midx",
            "frequency": "monthly",
            "rows": int(len(monthly)),
            "first_observation": monthly.index.min().isoformat(),
            "last_observation": monthly.index.max().isoformat(),
            "generated_at": datetime.utcnow().isoformat() + "Z",
        }
        _write_meta(dest.with_suffix(".meta.json"), meta)
        LOGGER.info("Real HPI ready for %s (%d rows)", iso, len(monthly))
        tasks.append(SeriesTask("HPI_REAL", iso, Path("prc_hpi"), dest))
    return tasks


def run_affordability_builder(include_usa: bool = False) -> None:
    """Delegate to the existing ratio builder to create price/rent-to-income files."""

    LOGGER.info("Deriving Eurostat affordability ratios...")
    derive_euro_ratios()
    if include_usa:
        from derive_real_estate_ratios import derive_usa_ratios

        derive_usa_ratios()
    LOGGER.info("Affordability ratios refreshed. Files live in %s", RATIO_OUTPUT_DIR)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill zero-data series")
    parser.add_argument("--bis", action="store_true", help="Build BIS LBS monthly files")
    parser.add_argument("--npl", action="store_true", help="Resample NPL proxies")
    parser.add_argument("--hpi", action="store_true", help="Compute real HPI series")
    parser.add_argument("--ratios", action="store_true", help="Refresh price/rent-to-income ratios")
    parser.add_argument("--debt", action="store_true", help="Fetch GC.DOD.TOTL.GD.ZS for DEU")
    parser.add_argument("--all", action="store_true", help="Run every ingestion step")
    args = parser.parse_args()

    if not any((args.bis, args.npl, args.hpi, args.ratios, args.debt, args.all)):
        parser.error("Select at least one step (e.g., --bis or --all)")

    if args.all or args.bis:
        generate_bis_monthly_series()
    if args.all or args.npl:
        generate_npl_monthly_series()
    if args.all or args.hpi:
        build_real_hpi_series()
    if args.all or args.ratios:
        run_affordability_builder(include_usa=False)
    if args.all or args.debt:
        fetch_gc_dod_deu()


if __name__ == "__main__":
    main()
