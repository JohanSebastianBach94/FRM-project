#!/usr/bin/env python3
"""QA: Check whether realized V2X inputs are identical across ISOs.

Reads frozen realized inputs under:
  analysis_outputs/scenarios/<run_id>/inputs/<ISO>/covariance/

Compares the V2X columns in:
  - <ISO>_standardized_residuals_daily.csv  (z)
  - <ISO>_Dt_daily.csv                     (Dt)

Reports pairwise correlations and exact-equality flags (after rounding).
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


def _load_v2x(run_dir: Path, iso: str) -> pd.DataFrame:
    z_p = run_dir / "inputs" / iso / "covariance" / f"{iso}_standardized_residuals_daily.csv"
    dt_p = run_dir / "inputs" / iso / "covariance" / f"{iso}_Dt_daily.csv"

    z = pd.read_csv(z_p, usecols=["date", "V2X"])
    dt = pd.read_csv(dt_p, usecols=["date", "V2X"])
    z["date"] = pd.to_datetime(z["date"])
    dt["date"] = pd.to_datetime(dt["date"])

    m = z.merge(dt, on="date", how="inner", suffixes=("_z", "_dt")).sort_values("date")
    m = m.rename(columns={"V2X_z": "z", "V2X_dt": "dt"})
    m["shock"] = pd.to_numeric(m["z"], errors="coerce") * pd.to_numeric(m["dt"], errors="coerce")
    return m[["date", "z", "dt", "shock"]]


def _corr(x: pd.Series, y: pd.Series) -> float:
    a = pd.to_numeric(x, errors="coerce").to_numpy(dtype=float)
    b = pd.to_numeric(y, errors="coerce").to_numpy(dtype=float)
    ok = np.isfinite(a) & np.isfinite(b)
    if int(ok.sum()) < 3:
        return float("nan")
    return float(np.corrcoef(a[ok], b[ok])[0, 1])


def main() -> int:
    run_dir = Path("analysis_outputs/scenarios/latest").resolve()
    inputs_dir = run_dir / "inputs"
    isos: List[str] = []
    skipped: List[str] = []
    for p in sorted(inputs_dir.iterdir()) if inputs_dir.exists() else []:
        if not p.is_dir():
            continue
        iso = p.name
        cov = p / "covariance"
        if not cov.exists():
            continue
        z_p = cov / f"{iso}_standardized_residuals_daily.csv"
        dt_p = cov / f"{iso}_Dt_daily.csv"
        if not (z_p.exists() and dt_p.exists()):
            continue

        # Only include ISOs that actually have a V2X column.
        try:
            z_cols = set(pd.read_csv(z_p, nrows=0).columns)
            dt_cols = set(pd.read_csv(dt_p, nrows=0).columns)
        except Exception:
            skipped.append(iso)
            continue
        if "V2X" in z_cols and "V2X" in dt_cols:
            isos.append(iso)
        else:
            skipped.append(iso)

    if not isos:
        print("No ISOs with V2X realized inputs found.")
        return 1

    data: Dict[str, pd.DataFrame] = {iso: _load_v2x(run_dir, iso) for iso in isos}

    print(f"Run dir: {run_dir}")
    print(f"ISOs with V2X inputs: {', '.join(isos)}")
    if skipped:
        print(f"Skipped (missing V2X in realized inputs): {', '.join(sorted(set(skipped)))}")
    print("Pairwise V2X realized equality/corr (intersection by date):")

    any_exact: List[Tuple[str, str]] = []
    for a, b in itertools.combinations(isos, 2):
        ma = data[a].rename(columns={"z": f"z_{a}", "dt": f"dt_{a}", "shock": f"sh_{a}"})
        mb = data[b].rename(columns={"z": f"z_{b}", "dt": f"dt_{b}", "shock": f"sh_{b}"})
        m = ma.merge(mb, on="date", how="inner")
        if m.empty:
            print(f"{a}-{b} NO_OVERLAP")
            continue

        cz = _corr(m[f"z_{a}"], m[f"z_{b}"])
        cdt = _corr(m[f"dt_{a}"], m[f"dt_{b}"])
        csh = _corr(m[f"sh_{a}"], m[f"sh_{b}"])

        eqz = bool((m[f"z_{a}"].round(12) == m[f"z_{b}"].round(12)).all())
        eqdt = bool((m[f"dt_{a}"].round(12) == m[f"dt_{b}"].round(12)).all())
        eqsh = bool((m[f"sh_{a}"].round(12) == m[f"sh_{b}"].round(12)).all())
        if eqz and eqdt and eqsh:
            any_exact.append((a, b))

        d0 = m["date"].min().date()
        d1 = m["date"].max().date()
        print(
            f"{a}-{b} overlap={len(m)} {d0}..{d1} "
            f"corr(z)={cz:.6f} eq={eqz} "
            f"corr(Dt)={cdt:.6f} eq={eqdt} "
            f"corr(zDt)={csh:.6f} eq={eqsh}"
        )

    if any_exact:
        pairs = ", ".join([f"{a}-{b}" for a, b in any_exact])
        print(f"\nExact-identical (z, Dt, zDt) pairs: {pairs}")
    else:
        print("\nNo pairs were exactly identical across all three (z, Dt, zDt).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
