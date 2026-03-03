#!/usr/bin/env python3
"""Fetch sovereign CDS series from TradingEconomics (TE) and write CSVs.

This is a best-effort downloader:
- TE endpoints/entitlements for CDS can be plan-restricted.
- Guest credentials often return HTTP 403 for CDS.

Usage:
  python scripts/fetch_cds_tradingeconomics.py --key "YOUR_KEY" --countries USA DEU FRA ITA ESP

If --key is omitted, this script checks env vars:
  - TRADING_ECONOMICS_KEY
  - TRADINGECONOMICS_KEY

Outputs:
  data_repository/raw/providers/tradingeconomics/cds_5y_{ISO}.csv
  data_repository/raw/providers/tradingeconomics/cds_5y_{ISO}.meta.json

CSV format:
  date,value

Notes:
- The exact TE indicator name for sovereign CDS is not consistently exposed to guest keys.
  This script tries a small set of likely indicator strings; use --indicator to override.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
OUT_DIR = BASE / "data_repository" / "raw" / "providers" / "tradingeconomics"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_COUNTRIES = ["USA", "DEU", "FRA", "ITA", "ESP"]

# Candidate indicator strings (TE is case/space sensitive in some endpoints).
DEFAULT_INDICATORS = [
    "CDS",
    "CDS Spread",
    "CDS spread",
    "Sovereign CDS",
    "Sovereign CDS Spread",
]


def _env_key() -> str | None:
    return os.environ.get("TRADING_ECONOMICS_KEY") or os.environ.get("TRADINGECONOMICS_KEY")


def _fetch_url(url: str, timeout: int = 30) -> tuple[int, str]:
    req = urllib.request.Request(url, headers={"User-Agent": "FRM-fetch/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200))
            return status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return int(e.code), body
    except Exception as e:
        return 0, str(e)


def _try_parse_json(text: str):
    text = text.strip()
    if not text:
        return None
    if not (text.startswith("[") or text.startswith("{")):
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _iter_rows(payload) -> Iterable[dict]:
    if isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict):
                yield row
    elif isinstance(payload, dict):
        # Some TE endpoints wrap in an object.
        for k in ("data", "Data", "results", "Results"):
            v = payload.get(k)
            if isinstance(v, list):
                for row in v:
                    if isinstance(row, dict):
                        yield row


def _parse_timeseries(payload) -> pd.Series | None:
    rows = list(_iter_rows(payload))
    if not rows:
        return None

    dates = []
    values = []
    for r in rows:
        dt = r.get("DateTime") or r.get("Date") or r.get("date")
        val = r.get("Close") if "Close" in r else r.get("Value") or r.get("value")
        if dt is None or val is None:
            continue
        try:
            ts = pd.to_datetime(dt)
        except Exception:
            continue
        try:
            fv = float(val)
        except Exception:
            continue
        dates.append(ts)
        values.append(fv)

    if not dates:
        return None

    s = pd.Series(values, index=pd.DatetimeIndex(dates)).sort_index()
    s = s[~s.index.duplicated(keep="last")]
    return s


def fetch_country_indicator(country: str, indicator: str, key: str) -> tuple[pd.Series | None, str]:
    country_enc = urllib.parse.quote(country)
    indicator_enc = urllib.parse.quote(indicator)
    # TE historical-by-country+indicator endpoint.
    url = (
        f"https://api.tradingeconomics.com/historical/country/{country_enc}/indicator/{indicator_enc}"
        f"?c={urllib.parse.quote(key)}&format=json"
    )
    status, text = _fetch_url(url)
    if status != 200:
        return None, f"HTTP {status} for {url} ({text[:200].strip()})"

    payload = _try_parse_json(text)
    if payload is None:
        return None, "Unparseable JSON payload"

    series = _parse_timeseries(payload)
    if series is None or series.empty:
        return None, "No timeseries rows parsed"

    return series, f"OK ({len(series)} obs)"


def write_series(iso: str, series: pd.Series, source: dict) -> Path:
    out_csv = OUT_DIR / f"cds_5y_{iso}.csv"
    df = pd.DataFrame({"date": series.index.normalize(), "value": series.values})
    df = df.dropna().sort_values("date")
    df.to_csv(out_csv, index=False)

    meta = {
        "iso": iso,
        "source": source,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "rows": int(len(df)),
        "start": str(df["date"].min().date()) if not df.empty else None,
        "end": str(df["date"].max().date()) if not df.empty else None,
    }
    out_csv.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return out_csv


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--key", help="TradingEconomics API key (or use TRADING_ECONOMICS_KEY env var)")
    p.add_argument("--countries", nargs="*", default=DEFAULT_COUNTRIES)
    p.add_argument("--indicator", help="Override indicator string to try")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    key = args.key or _env_key()
    if not key:
        print("Missing TE API key. Provide --key or set TRADING_ECONOMICS_KEY.")
        return 2

    indicators = [args.indicator] if args.indicator else DEFAULT_INDICATORS

    ok = 0
    for iso in args.countries:
        iso = str(iso).upper()
        print(f"--- {iso} ---")
        best = None
        best_msg = None
        best_indicator = None
        for ind in indicators:
            series, msg = fetch_country_indicator(iso, ind, key)
            print(f"  try indicator={ind!r}: {msg}")
            if series is not None and (best is None or int(series.notna().sum()) > int(best.notna().sum())):
                best = series
                best_msg = msg
                best_indicator = ind

        if best is None:
            continue

        out = write_series(
            iso,
            best,
            source={
                "provider": "TradingEconomics",
                "endpoint": "historical/country/{country}/indicator/{indicator}",
                "indicator": best_indicator,
            },
        )
        print(f"  [SAVED] {out} ({best_msg})")
        ok += 1

    print(f"Done. Wrote {ok} CDS series to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
