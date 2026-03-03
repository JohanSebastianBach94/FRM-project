#!/usr/bin/env python3
"""Download IMF FSI non-performing loan ratios for selected countries.

The IMF publishes Financial Soundness Indicators (FSI) via an SDMX REST endpoint.
Each ISO is mapped to the IMF "NPLR" indicator key for the annual frequency.
The script saves one CSV and one meta JSON per country under
`data_repository/raw/providers/npl_ratios/` and prints status for each fetch.
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / "data_repository" / "raw" / "providers" / "npl_ratios"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FSI_BASE = "https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/FSI"
TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 2

COUNTRY_KEYS = {
    "USA": {"key": "USA.NPLR.A", "frequency": "annual"},
    "DEU": {"key": "DEU.NPLR.A", "frequency": "annual"},
    "FRA": {"key": "FRA.NPLR.A", "frequency": "annual"},
    "ITA": {"key": "ITA.NPLR.A", "frequency": "annual"},
    "ESP": {"key": "ESP.NPLR.A", "frequency": "annual"},
}

HEADERS = {"User-Agent": "FRM-fetch-npl-ratio/1.0"}


def fetch_json(url: str) -> dict:
    req = Request(url, headers=HEADERS)
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=TIMEOUT) as resp:
                return json.load(resp)
        except (HTTPError, URLError) as exc:
            last_exc = exc
            print(f"  attempt {attempt} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY * attempt)
    raise RuntimeError(f"Failed fetching {url} after {MAX_RETRIES} attempts: {last_exc}")


def parse_obs(data: dict) -> List[Tuple[str, float]]:
    compact = data.get("CompactData", {})
    dataset = compact.get("DataSet", {})
    if not dataset:
        return []
    series = dataset.get("Series")
    if not series:
        return []
    if isinstance(series, list):
        series = series[0]
    obs = series.get("Obs") or []
    if isinstance(obs, dict):
        obs = [obs]
    rows = []
    for entry in obs:
        period = entry.get("@TIME_PERIOD")
        value = entry.get("@OBS_VALUE")
        if period is None or value in (None, ""):
            continue
        try:
            val = float(value)
        except ValueError:
            continue
        rows.append((period, val))
    rows.sort(key=lambda x: x[0])
    return rows


def write_csv(iso: str, rows: List[Tuple[str, float]]) -> Path:
    csv_path = OUT_DIR / f"npl_ratio_{iso}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as fh:
        fh.write("period,value\n")
        for period, value in rows:
            fh.write(f"{period},{value}\n")
    return csv_path


def write_meta(iso: str, csv_path: Path, config: dict, rows: List[Tuple[str, float]]) -> Path:
    meta = {
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source": "IMF SDMX (FSI NPLR)",
        "imf_key": config["key"],
        "frequency": config["frequency"],
        "rows": len(rows),
        "csv_path": str(csv_path),
    }
    meta_path = OUT_DIR / f"npl_ratio_{iso}.meta.json"
    with open(meta_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)
    return meta_path


def main() -> int:
    failures = []
    for iso, cfg in COUNTRY_KEYS.items():
        url = f"{FSI_BASE}/{cfg['key']}"
        print(f"Fetching NPL ratio {iso} via IMF FSI ({cfg['key']})...")
        try:
            payload = fetch_json(url)
        except Exception as exc:
            print("  error:", exc)
            failures.append(iso)
            continue
        rows = parse_obs(payload)
        if not rows:
            print(f"  no data returned for {iso} ({cfg['key']})")
            failures.append(iso)
            continue
        csv_path = write_csv(iso, rows)
        meta_path = write_meta(iso, csv_path, cfg, rows)
        print(f"  wrote {len(rows)} rows -> {csv_path} (+{meta_path.name})")
    if failures:
        print("Some countries failed:", ", ".join(failures))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
