#!/usr/bin/env python3
"""Build a simple daily Rt series per ISO from 10Y bond yields.

This is a pragmatic daily analogue to the existing monthly Rt
construction: it takes daily 10Y yields and computes log returns,
then aligns them to the daily factor preparation index.

Outputs:
- analysis_outputs/diag_corr_daily/{ISO}_Rt_daily.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
FACTOR_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation_daily"
RT_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"

ISO_BOND_MAP: Dict[str, str] = {
    "ITA": "BOND_Italy_10Y.csv",
    "FRA": "BOND_France_10Y.csv",
    "DEU": "BOND_United_10Y.csv",  # proxy for Germany if dedicated DEU not present
    "USA": "BOND_United_10Y.csv",
    "ESP": "BOND_Italy_10Y.csv",  # rough proxy; can be refined
}


def load_bond_series(path: Path) -> pd.Series:
    df = pd.read_csv(path)
    cols = [c.lower() for c in df.columns]
    colmap = dict(zip(df.columns, cols))
    df = df.rename(columns=colmap)
    date_col = "date" if "date" in df.columns else df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    # Choose yield column heuristically
    y_cols = [c for c in df.columns if "yield" in c.lower() or "rate" in c.lower()]
    if not y_cols:
        y_cols = [df.columns[0]]
    series = df[y_cols[0]].astype(float)
    return series


def compute_rt_from_yield(y: pd.Series) -> pd.Series:
    y = y.ffill().dropna()
    # Use log return of yield level as crude proxy
    rt = np.log(y / y.shift(1))
    rt = rt.replace([np.inf, -np.inf], np.nan).dropna()
    rt.name = "Rt_daily"
    return rt


def build_rt_for_iso(iso: str) -> Path | None:
    if iso not in ISO_BOND_MAP:
        print(f"[SKIP] No bond mapping for {iso}")
        return None

    bond_path = DATA_DIR / ISO_BOND_MAP[iso]
    factor_path = FACTOR_DAILY_DIR / f"{iso}_factors_daily.csv"
    if not bond_path.exists():
        print(f"[SKIP] Missing bond file for {iso}: {bond_path}")
        return None
    if not factor_path.exists():
        print(f"[SKIP] Missing daily factors for {iso}: {factor_path}")
        return None

    y = load_bond_series(bond_path)
    rt = compute_rt_from_yield(y)

    factors = pd.read_csv(factor_path, index_col=0, parse_dates=True)
    factors.index.name = "date"
    factors = factors.sort_index()

    aligned = rt.reindex(factors.index).dropna()
    if aligned.empty:
        print(f"[WARN] No overlapping dates between Rt and factors for {iso}")
    out_df = aligned.to_frame()

    RT_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RT_DAILY_DIR / f"{iso}_Rt_daily.csv"
    out_df.to_csv(out_path)
    print(f"[DONE] Wrote daily Rt for {iso} to {out_path}")
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build simple daily Rt from 10Y bond yields")
    parser.add_argument("--isos", nargs="*", default=["ITA", "FRA", "DEU", "USA", "ESP"], help="ISO codes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for iso in args.isos:
        build_rt_for_iso(iso)


if __name__ == "__main__":  # pragma: no cover
    main()
