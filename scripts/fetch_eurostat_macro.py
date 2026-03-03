#!/usr/bin/env python3
"""Fetch basic macro series from Eurostat SDMX-JSON API.

This is a small, dependency-light fetcher (requests + pandas only).
It writes a wide CSV where each column is one fetched series.

Defaults (can be overridden):
- Unemployment rate (monthly): dataset `une_rt_m`
- GDP (quarterly): dataset `namq_10_gdp`

Because Eurostat dimension codes can vary by dataset vintage, treat this as a
best-effort tool: if a query fails, it prints the URL and response so you can
adjust dimension codes.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

ISO_TO_EUROSTAT_GEO = {
    "DEU": "DE",
    "FRA": "FR",
    "ITA": "IT",
    "ESP": "ES",
}


def _parse_time_to_timestamp(label: str) -> pd.Timestamp:
    label = str(label)
    # Common encodings: 2024M01, 2024Q1, 2024-01
    if "M" in label and len(label) == 7 and label[:4].isdigit():
        year = int(label[:4])
        month = int(label[5:])
        return pd.Period(f"{year:04d}-{month:02d}", freq="M").to_timestamp(how="end")
    # Eurostat often uses YYYY-MM for monthly.
    if len(label) == 7 and label[:4].isdigit() and label[4] == "-" and label[5:7].isdigit():
        year = int(label[:4])
        month = int(label[5:7])
        return pd.Period(f"{year:04d}-{month:02d}", freq="M").to_timestamp(how="end")
    if "Q" in label and label[:4].isdigit():
        year = int(label[:4])
        quarter = int(label[-1])
        return pd.Period(f"{year:04d}Q{quarter}", freq="Q").to_timestamp(how="end")
    try:
        return pd.to_datetime(label)
    except Exception:
        return pd.NaT


def _extract_single_series(payload: dict) -> pd.Series:
    """Extract a time series from a Eurostat SDMX-JSON payload.

    If the payload is strictly 1D (time only), this behaves like the prior
    implementation. If the payload includes multiple dimensions (e.g., unit x time),
    this function requires that the caller requested a single series (i.e., all
    non-time dims have size 1). For general multi-dim extraction use
    `_extract_timeseries_slice`.
    """

    if not isinstance(payload, dict):
        raise ValueError("Eurostat payload is not a dict")

    ids = payload.get("id", [])
    sizes = payload.get("size", [])
    if not isinstance(ids, list) or not isinstance(sizes, list) or len(ids) != len(sizes):
        raise ValueError("Eurostat payload missing id/size")
    if "time" not in ids:
        raise ValueError("Eurostat payload missing time dimension")

    non_time_sizes = [s for dim, s in zip(ids, sizes) if dim != "time"]
    if any(int(s) > 1 for s in non_time_sizes):
        raise ValueError("Eurostat payload has multiple non-time dims; use slice extractor")

    return _extract_timeseries_slice(payload, selection={})


def _extract_timeseries_slice(payload: dict, selection: Dict[str, str]) -> pd.Series:
    """Extract a 1D time series from an N-dimensional Eurostat payload.

    `selection` maps non-time dimension name -> category label to fix.
    Any non-time dimension with size==1 is treated as fixed automatically.
    """

    if not isinstance(payload, dict):
        raise ValueError("Eurostat payload is not a dict")

    ids = payload.get("id", [])
    sizes = payload.get("size", [])
    if not isinstance(ids, list) or not isinstance(sizes, list) or len(ids) != len(sizes):
        raise ValueError("Eurostat payload missing id/size")
    if "time" not in ids:
        raise ValueError("Eurostat payload missing time dimension")

    dim = payload.get("dimension", {}) or {}
    time_index = (dim.get("time", {}) or {}).get("category", {}).get("index", {})
    if not isinstance(time_index, dict) or not time_index:
        raise ValueError("Eurostat payload missing time categories")

    # Build ordered time index.
    ordered_time = sorted(time_index.items(), key=lambda kv: kv[1])
    time_labels = [lbl for lbl, _ in ordered_time]
    times = [_parse_time_to_timestamp(lbl) for lbl in time_labels]

    # Compute strides for flat index decoding.
    strides: List[int] = []
    running = 1
    for size in reversed(sizes):
        strides.append(running)
        running *= int(size)
    strides = list(reversed(strides))

    # Resolve fixed index for each non-time dim.
    fixed_positions: Dict[str, int] = {}
    for dim_name, size in zip(ids, sizes):
        if dim_name == "time":
            continue
        size_i = int(size)
        if size_i == 1:
            fixed_positions[dim_name] = 0
            continue
        choice = selection.get(dim_name)
        if not choice:
            raise ValueError(f"Selection required for dimension '{dim_name}' (size={size_i})")
        idx_map = (dim.get(dim_name, {}) or {}).get("category", {}).get("index", {})
        if not isinstance(idx_map, dict) or choice not in idx_map:
            raise ValueError(f"Selection '{choice}' not found in dimension '{dim_name}'")
        fixed_positions[dim_name] = int(idx_map[choice])

    value = payload.get("value", {})
    if not isinstance(value, dict):
        value = {}

    vals: List[float] = []
    time_dim_pos = ids.index("time")
    for t_pos in range(len(times)):
        flat = 0
        for dim_name, dim_size, stride in zip(ids, sizes, strides):
            if dim_name == "time":
                dim_pos = t_pos
            else:
                dim_pos = fixed_positions[dim_name]
            flat += int(dim_pos) * int(stride)
        raw = value.get(str(flat))
        vals.append(float(raw) if raw is not None else float("nan"))

    series = pd.Series(vals, index=pd.DatetimeIndex(times), name="value")
    series = series[~series.index.isna()].sort_index()
    return series


def fetch_eurostat(dataset: str, params: Dict[str, str], *, timeout: int = 60) -> pd.Series:
    url = f"{EUROSTAT_BASE}/{dataset}"
    resp = requests.get(url, params=params, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Eurostat HTTP {resp.status_code}: {resp.text[:400]} | url={resp.url}")
    payload = resp.json()
    series = _extract_single_series(payload)
    return series


def fetch_eurostat_slice(
    dataset: str,
    params: Dict[str, str],
    *,
    selection: Dict[str, str],
    timeout: int = 60,
) -> pd.Series:
    """Fetch a Eurostat dataset and extract a 1D time slice.

    Use this when you intentionally request a payload with multiple dimensions
    (e.g., unit x time) and want to select one unit.
    """

    url = f"{EUROSTAT_BASE}/{dataset}"
    resp = requests.get(url, params=params, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Eurostat HTTP {resp.status_code}: {resp.text[:400]} | url={resp.url}")
    payload = resp.json()
    return _extract_timeseries_slice(payload, selection=selection)


@dataclass(frozen=True)
class EurostatQuery:
    name: str
    dataset: str
    params: Dict[str, str]


def build_default_queries(iso: str) -> List[EurostatQuery]:
    geo = ISO_TO_EUROSTAT_GEO.get(iso)
    if not geo:
        return []

    # Unemployment rate (monthly). Typical codes (may need adjustment):
    # unit=PC_ACT (percentage of active population), s_adj=SA, sex=T, age=TOTAL
    unrate = EurostatQuery(
        name=f"{iso}_UNRATE_EUROSTAT",
        dataset="une_rt_m",
        params={
            "unit": "PC_ACT",
            "s_adj": "SA",
            "sex": "T",
            "age": "TOTAL",
            "geo": geo,
        },
    )

    # Real GDP volume (quarterly).
    # Use NSA for consistent availability across geos; SA can be empty for some.
    gdp = EurostatQuery(
        name=f"{iso}_GDP_EUROSTAT",
        dataset="namq_10_gdp",
        params={
            "na_item": "B1GQ",
            "s_adj": "NSA",
            "unit": "CLV10_MEUR",
            "geo": geo,
        },
    )

    return [unrate, gdp]


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Eurostat macro series (unemployment, GDP)")
    parser.add_argument("--isos", nargs="*", default=["DEU", "FRA", "ITA", "ESP"], help="ISO3 codes")
    parser.add_argument(
        "--out",
        type=str,
        default=str(Path("data") / "eurostat_macro_raw.csv"),
        help="Output CSV path",
    )
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    series_map: Dict[str, pd.Series] = {}
    failures: List[Tuple[str, str]] = []

    for iso in [str(x).upper() for x in args.isos]:
        queries = build_default_queries(iso)
        if not queries:
            print(f"[SKIP] No Eurostat geo mapping for {iso}")
            continue
        for q in queries:
            try:
                s = fetch_eurostat(q.dataset, q.params, timeout=args.timeout)
            except Exception as exc:
                failures.append((q.name, str(exc)))
                print(f"[WARN] Failed {q.name}: {exc}")
                continue
            s.name = q.name
            series_map[q.name] = s
            print(f"[OK] {q.name}: {len(s)} obs ({s.index.min().date()} -> {s.index.max().date()})")

    if not series_map:
        raise SystemExit("No Eurostat series fetched (check warnings above)")

    df = pd.concat(series_map.values(), axis=1).sort_index()
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=True)
    print(f"[DONE] Wrote {out_path} with {df.shape[1]} series")

    if failures:
        fail_path = out_path.with_suffix(".failures.txt")
        fail_path.write_text("\n".join(f"{name}: {msg}" for name, msg in failures), encoding="utf-8")
        print(f"[INFO] Wrote failures to {fail_path}")


if __name__ == "__main__":
    main()
