#!/usr/bin/env python3
"""Fetch fallback European mortgage rates from FRED and store them locally.
"""
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "data_repository" / "raw" / "fred"
OUT_DIR.mkdir(parents=True, exist_ok=True)
ENV_PATH = ROOT / ".env"

SERIES_CONFIG: Dict[str, str] = {
    "DEU": "IRLTLT01DEM156N",
    "FRA": "IRLTLT01FRM156N",
    "ITA": "IRLTLT01ITM156N",
    "ESP": "IRLTLT01ESM156N",
}

START_DATE = "1990-01-01"


def load_env(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    result = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result

ENV_VARS = load_env(ENV_PATH)
FRED_KEY = ENV_VARS.get("FRED_API_KEY")
if not FRED_KEY:
    raise SystemExit("FRED_API_KEY not set in .env; cannot fetch fallback mortgage data.")


def write_series(out_path: Path, rows: List[Tuple[str, float]], meta: Dict[str, str]) -> None:
    sorted_rows = sorted(rows, key=lambda x: x[0])
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "value"])
        for date, value in sorted_rows:
            writer.writerow([date, f"{value:.6f}"])
    meta_path = out_path.with_suffix(".meta.json")
    meta.update({"created_at": datetime.utcnow().isoformat() + "Z", "rows": len(sorted_rows)})
    with meta_path.open("w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    print(f"Wrote {out_path.name} ({len(sorted_rows)} rows)")


def fetch_series(series_id: str) -> List[Tuple[str, float]]:
    params = {
        "series_id": series_id,
        "api_key": FRED_KEY,
        "file_type": "json",
        "observation_start": START_DATE,
    }
    resp = requests.get("https://api.stlouisfed.org/fred/series/observations", params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    rows = []
    for obs in payload.get("observations", []):
        value = obs.get("value")
        if value is None or value in ("", ".", "NaN"):
            continue
        try:
            rows.append((obs["date"], float(value)))
        except (KeyError, ValueError):
            continue
    return rows


def main() -> None:
    for iso, series_id in SERIES_CONFIG.items():
        print(f"Fetching {iso} -> {series_id}")
        rows = fetch_series(series_id)
        if not rows:
            print(f"  No rows returned for {series_id}")
            continue
        out_path = OUT_DIR / f"{series_id}.csv"
        meta = {
            "source": "FRED",
            "series_id": series_id,
            "description": "Mortgage interest rate series (loans to households for house purchase)",
        }
        write_series(out_path, rows, meta)


if __name__ == "__main__":
    main()
