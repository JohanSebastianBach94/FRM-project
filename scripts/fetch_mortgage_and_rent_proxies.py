#!/usr/bin/env python3
"""Fetch ECB mortgage rates and Eurostat rent indices per country.
Writes CSV + metadata so `Mortgage_rate_{ISO}` and rent proxies can be wired into
`config/country_blocks_extended.yaml` once the files exist.
"""
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
ECB_DIR = ROOT / "data_repository" / "raw" / "ecb"
EUROSTAT_DIR = ROOT / "data_repository" / "raw" / "eurostat"
ECB_DIR.mkdir(parents=True, exist_ok=True)
EUROSTAT_DIR.mkdir(parents=True, exist_ok=True)

ECB_MORTGAGE_SERIES = {
    # MIR (MFI interest rate statistics)
    # Dimension order (ECB_MIR1):
    # FREQ.REF_AREA.BS_REP_SECTOR.BS_ITEM.MATURITY_NOT_IRATE.DATA_TYPE_MIR.AMOUNT_CAT.BS_COUNT_SECTOR.CURRENCY_TRANS.IR_BUS_COV
    # We use the standard mortgage proxy series:
    # - BS_ITEM=A2C: Lending for house purchase excluding revolving loans/overdrafts
    # - BS_REP_SECTOR=B: Deposit-taking corporations except the central bank
    # - BS_COUNT_SECTOR=2250: Households and NPISH
    # - IR_BUS_COV=N: New business
    "DEU": "MIR.M.DE.B.A2C.A.R.A.2250.EUR.N",
    "FRA": "MIR.M.FR.B.A2C.A.R.A.2250.EUR.N",
    "ITA": "MIR.M.IT.B.A2C.A.R.A.2250.EUR.N",
    "ESP": "MIR.M.ES.B.A2C.A.R.A.2250.EUR.N",
}

EUROSTAT_RENT_DATASETS = [
    {
        "dataset": "prc_hicp_midxr",
        "coicop": "CP0411",
        "unit": "I9",
    },
    {
        "dataset": "prc_hicp_midxr",
        "coicop": "CP0412",
        "unit": "I9",
    },
]

START_PERIOD = "1990-01"
END_PERIOD = "2025-12"


def _write_series(out_path: Path, rows: List[Tuple[str, float]], meta: Dict[str, str]) -> None:
    rows_sorted = sorted(rows, key=lambda x: x[0])
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "value"])
        for date, value in rows_sorted:
            writer.writerow([date, f"{value:.6f}"])
    meta_path = out_path.with_suffix(".meta.json")
    meta.update({"created_at": datetime.utcnow().isoformat() + "Z", "rows": len(rows_sorted)})
    with meta_path.open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    print(f"Wrote {out_path} ({len(rows_sorted)} rows)")


def _parse_sdw_csv(text: str) -> List[Tuple[str, float]]:
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return []
    delim = ";" if lines[0].count(";") >= lines[0].count(",") else ","
    reader = csv.reader(lines, delimiter=delim)
    header = None
    rows = []
    for row in reader:
        if not header:
            header = [col.upper() for col in row]
            if not any(col in header for col in ("OBS_VALUE", "VALUE")) or not any(col in header for col in ("TIME_PERIOD", "TIME", "DATE")):
                header = None
                continue
            continue
        if not header:
            continue
        col_idx = {col: idx for idx, col in enumerate(header)}
        date_col = col_idx.get("TIME_PERIOD") or col_idx.get("TIME") or col_idx.get("DATE") or col_idx.get("OBS_DATE")
        value_col = col_idx.get("OBS_VALUE") or col_idx.get("VALUE") or col_idx.get("OBS")
        if date_col is None or value_col is None:
            break
        date = row[date_col].strip()
        value = row[value_col].strip()
        if not date or value in ("", ".", "NaN"):
            continue
        try:
            rows.append((date, float(value)))
        except ValueError:
            continue
    return rows


def fetch_ecb_mortgage_series(series_id: str) -> List[Tuple[str, float]]:
    # Prefer ECB Data Portal API (sdw.ecb.europa.eu is sometimes blocked/unresolvable).
    # API guide: https://data.ecb.europa.eu/help/api/data.html
    flow = "MIR"
    key = series_id
    if key.startswith("MIR."):
        key = key[len("MIR.") :]

    url = f"https://data-api.ecb.europa.eu/service/data/{flow}/{key}"
    params = {
        "startPeriod": START_PERIOD,
        "endPeriod": END_PERIOD,
        "format": "csvdata",
        "detail": "dataonly",
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"ECB fetch failed for {series_id}: {exc}")
        return []
    return _parse_sdw_csv(resp.text)


def _jsonstat_index_map(dim: Dict) -> Dict[str, str]:
    index_map = dim.get("category", {}).get("index", {})
    return {str(value): key for key, value in index_map.items()}


def fetch_eurostat_series(dataset: str, iso: str, coicop: str, unit: str) -> List[Tuple[str, float]]:
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}"
    params = {"geo": iso, "coicop": coicop, "unit": unit}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"Eurostat fetch failed for {dataset} {iso}: {exc}")
        return []
    payload = resp.json()
    values = payload.get("value", {})
    if not values:
        return []
    dim_order = payload.get("id", [])
    dims = payload.get("dimension", {})
    index_maps = [_jsonstat_index_map(dims[dim]) for dim in dim_order]
    label_maps = [dims[dim].get("category", {}).get("label", {}) for dim in dim_order]
    rows = []
    for key, raw_value in values.items():
        if raw_value in (None, "", "u"):
            continue
        indices = key.split(":")
        coord = {}
        for idx, dim in enumerate(dim_order):
            label_key = index_maps[idx].get(indices[idx])
            label = label_maps[idx].get(label_key, label_key)
            coord[dim] = label
        date = coord.get("time") or coord.get("TIME_PERIOD")
        if not date:
            continue
        try:
            rows.append((date, float(raw_value)))
        except ValueError:
            continue
    return rows


def main() -> None:
    for iso, series in ECB_MORTGAGE_SERIES.items():
        print(f"Fetching mortgage rate for {iso} ({series})")
        data = fetch_ecb_mortgage_series(series)
        if not data:
            print(f"  -> no data fetched for {iso}")
            continue
        out_path = ECB_DIR / f"{series}.csv"
        meta = {
            "source": "ECB Data Portal SDMX",
            "series_id": series,
            "description": "MIR mortgage rate proxy: house purchase lending (A2C), new business",
        }
        _write_series(out_path, data, meta)

    for iso in ECB_MORTGAGE_SERIES:
        for conf in EUROSTAT_RENT_DATASETS:
            print(f"Attempting rent series for {iso} via {conf['dataset']}" )
            data = fetch_eurostat_series(conf["dataset"], iso, conf["coicop"], conf["unit"])
            if data:
                out_path = EUROSTAT_DIR / f"rent_index_{iso}.csv"
                meta = {
                    "source": "Eurostat",
                    "dataset": conf["dataset"],
                    "dimensions": {"coicop": conf["coicop"], "unit": conf["unit"]},
                }
                _write_series(out_path, data, meta)
                break
        else:
            print(f"  -> no Eurostat rent series found for {iso} (tried all configs)")

if __name__ == "__main__":
    main()
