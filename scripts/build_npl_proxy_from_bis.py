#!/usr/bin/env python3
"""Derive IMF NPL ratio proxies from BIS household credit aggregates.

This helper reads the pre-processed BIS LBS household outstanding loans files, computes
year-over-year changes, and writes quarterly proxy time series that can stand in when
IMF FSI NPLR data is unavailable.
"""

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
BIS_DIR = BASE / "data_repository" / "processed"
OUT_DIR = BASE / "data_repository" / "raw" / "providers" / "npl_proxies"
OUT_DIR.mkdir(parents=True, exist_ok=True)

ISO_TO_COUNTRY = {
    "DEU": "Germany",
    "FRA": "France",
    "ITA": "Italy",
    "ESP": "Spain",
    "USA": "United States",
}


def load_bis_series(country_name: str) -> pd.DataFrame:
    path = BIS_DIR / f"bis_lbs_household_{country_name}_agg.csv"
    if not path.exists():
        raise FileNotFoundError(f"BIS file missing: {path}")
    df = pd.read_csv(path, usecols=["period", "value"])
    df = df.dropna(subset=["period"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])
    df["period_index"] = pd.PeriodIndex(df["period"], freq="Q-DEC")
    df = df.sort_values("period_index").reset_index(drop=True)
    return df


def compute_proxy_values(df: pd.DataFrame) -> pd.DataFrame:
    df["proxy_value"] = df["value"].pct_change(periods=4) * 100.0
    return df.loc[df["proxy_value"].notna(), ["period", "proxy_value"]]


def write_csv(iso: str, records: List[Tuple[str, float]]) -> Path:
    csv_path = OUT_DIR / f"npl_proxy_{iso}.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        fh.write("period,value\n")
        for period, value in records:
            fh.write(f"{period},{value:.6f}\n")
    return csv_path


def write_meta(iso: str, csv_path: Path, source_path: Path, rows: int) -> Path:
    meta = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source": "BIS LBS household loans (processed totals)",
        "source_file": str(source_path),
        "frequency": "quarterly",
        "estimate": "year-over-year change to mimic NPL ratios",
        "rows": rows,
        "csv_path": str(csv_path),
    }
    meta_path = OUT_DIR / f"npl_proxy_{iso}.meta.json"
    with meta_path.open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    return meta_path


def main() -> int:
    failures = []
    for iso, country in ISO_TO_COUNTRY.items():
        try:
            df = load_bis_series(country)
        except FileNotFoundError as exc:
            print(f"Skipping {iso}: {exc}")
            failures.append(iso)
            continue
        proxy_df = compute_proxy_values(df)
        if proxy_df.empty:
            print(f"No proxy data produced for {iso} ({country}); need 4 quarters of overlap")
            failures.append(iso)
            continue
        records = list(proxy_df.itertuples(index=False, name=None))
        csv_path = write_csv(iso, records)
        meta_path = write_meta(iso, csv_path, BIS_DIR / f"bis_lbs_household_{country}_agg.csv", len(records))
        print(f"Wrote proxy for {iso}: {len(records)} rows -> {csv_path.name} (+{meta_path.name})")
    if failures:
        print("Proxy generation incomplete for:", ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())