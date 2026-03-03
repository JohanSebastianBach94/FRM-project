#!/usr/bin/env python3
"""Build DAILY Rt series from DNSS-implied 10Y yields.

This aligns the DAILY Rt definition with the monthly DNSS-based Rt.

Expected input per ISO (first matching file is used):
- ``Output/NSS_yield_curves_{ISO}.csv`` OR ``Output/curves/NSS_yield_curves_{ISO}.csv``

These files are assumed to contain at least:
- a date column (``date`` or the first column)
- a 10Y yield column (containing ``10Y`` in its name)

Outputs (per ISO):
- ``analysis_outputs/diag_corr_daily/{ISO}_Rt_daily.csv`` with columns:
  - ``Rt_daily``: log-return of the DNSS-implied 10Y yield
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"


def find_dnss_yield_file(iso: str) -> Optional[Path]:
    """Best-effort search for a DNSS yield-curve file for the given ISO."""

    candidates: List[Path] = []
    # Common locations / naming patterns used in this project
    patterns = [
        PROJECT_ROOT / "Output" / f"NSS_yield_curves_{iso}.csv",
        PROJECT_ROOT / "Output" / "curves" / f"NSS_yield_curves_{iso}.csv",
        PROJECT_ROOT / "outputs_final" / f"NSS_yield_curves_{iso}.csv",
    ]
    for p in patterns:
        if p.exists():
            candidates.append(p)

    return candidates[0] if candidates else None


def load_dnss_yield_series(path: Path) -> pd.Series:
    """Load DNSS-implied 10Y yield as a daily time series.

    Heuristics:
    - use ``date`` column if present, else first column as date
    - pick the first column whose name contains ``10Y`` (case-insensitive)
    """

    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
    else:
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
        df = df.set_index(df.columns[0])

    # Find 10Y column
    yield_cols = [c for c in df.columns if "10y" in c.lower()]
    if not yield_cols:
        # Fallback: try columns containing "10" and "yield"
        yield_cols = [
            c
            for c in df.columns
            if ("10" in c and "yield" in c.lower())
        ]
    if not yield_cols:
        raise ValueError(
            f"Could not identify a 10Y yield column in {path.name}; "
            f"available columns: {list(df.columns)}"
        )

    col = yield_cols[0]
    y = df[col].astype(float).replace([np.inf, -np.inf], np.nan).dropna()
    y = y.sort_index()
    y.name = "y_10y_dnss"
    return y


def compute_rt_from_yield(y: pd.Series) -> pd.Series:
    """Compute log-return of the yield series.

    Rt_t = log(y_t / y_{t-1})
    """

    rt = np.log(y / y.shift(1))
    rt = rt.replace([np.inf, -np.inf], np.nan).dropna()
    rt.name = "Rt_daily"
    return rt


def build_rt_for_iso(iso: str) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    src = find_dnss_yield_file(iso)
    if src is None:
        print(f"[SKIP] No DNSS yield-curve file found for {iso}")
        return

    print(f"[INFO] Using DNSS yield file for {iso}: {src}")
    y = load_dnss_yield_series(src)
    rt = compute_rt_from_yield(y)

    out_path = OUTPUT_DIR / f"{iso}_Rt_daily.csv"
    rt.to_frame().to_csv(out_path, index_label="date")
    print(f"[DONE] DNSS-based Rt_daily for {iso} → {out_path}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build DAILY Rt from DNSS 10Y yield")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO country codes")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    for iso in args.isos:
        build_rt_for_iso(iso)


if __name__ == "__main__":  # pragma: no cover
    main()
