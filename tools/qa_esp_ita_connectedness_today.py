"""QA: Recompute realized 'today' connectedness and diagnose ESP–ITA correlation.

This script intentionally reuses the exact helper functions from:
  SRESS TEST PIPELINE/12.1_monte_carlo_scenario_plots.py

It writes:
  - analysis_outputs/qa/today_connectedness_matrix.csv
  - analysis_outputs/qa/esp_ita_today_connectedness_check.txt

Run (PowerShell):
  python tools/qa_esp_ita_connectedness_today.py
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STEP12_1_PATH = PROJECT_ROOT / "SRESS TEST PIPELINE" / "12.1_monte_carlo_scenario_plots.py"
RUN_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios" / "latest"
MONTE_DIR = RUN_DIR / "monte_carlo"
QA_DIR = PROJECT_ROOT / "analysis_outputs" / "qa"


@dataclass(frozen=True)
class IsoToday:
    iso: str
    end_date: Optional[pd.Timestamp]
    n_days: Optional[int]
    severity_today: Optional[float]
    stress: np.ndarray


def _load_step12_1_module():
    spec = importlib.util.spec_from_file_location("mcplots", STEP12_1_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Failed to import Step 12.1 module from {STEP12_1_PATH}")
    mod = importlib.util.module_from_spec(spec)
    # Ensure the module is visible in sys.modules while executing.
    # Some decorators (notably dataclasses + typing) expect this.
    sys.modules[str(spec.name)] = mod
    spec.loader.exec_module(mod)
    return mod


def _build_iso_block_to_factors(*, mc, run_dir: Path, iso: str, block_defs_path: Path) -> Dict[str, Set[str]]:
    block_defs = mc._load_block_definitions(block_defs_path)
    rev_map, _dups = mc._reverse_block_map(block_defs)

    mc_factors = mc._load_mc_factor_list(run_dir, iso=iso)
    iso_rev = rev_map.get(iso) or {}

    factor_to_blocks: Dict[str, Set[str]] = {}
    for f in mc_factors:
        base = mc._strip_lag_suffix(f)
        keys = iso_rev.get(f) or iso_rev.get(base)
        if keys:
            factor_to_blocks[f] = set(str(x) for x in keys)

    block_to_factors: Dict[str, Set[str]] = {}
    for f, blocks in factor_to_blocks.items():
        for b in blocks:
            if str(b) == "unmapped":
                continue
            block_to_factors.setdefault(str(b), set()).add(str(f))

    return block_to_factors


def _window_dates_for_iso(*, mc, run_dir: Path, iso: str, window_days: int) -> Optional[pd.DatetimeIndex]:
    df_z, df_dt = mc._load_realized_inputs_wide(run_dir=run_dir, iso=iso)
    if df_z.empty or df_dt.empty:
        return None
    if "date" not in df_z.columns or "date" not in df_dt.columns:
        return None

    try:
        df = df_z.merge(df_dt, on="date", how="inner", suffixes=("__z", "__dt"))
    except Exception:
        return None
    if df.empty:
        return None
    df = df.sort_values("date")
    df_win = df.tail(int(window_days))
    if df_win.empty:
        return None
    try:
        return pd.DatetimeIndex(pd.to_datetime(df_win["date"], errors="coerce").dropna())
    except Exception:
        return None


def _format_corr_matrix(isos: Sequence[str], C: np.ndarray) -> str:
    df = pd.DataFrame(C, index=list(isos), columns=list(isos))
    with pd.option_context("display.max_rows", None, "display.max_columns", None, "display.width", 200):
        return str(df.round(3))


def _diagnose_pair(*, mc, iso_a: IsoToday, iso_b: IsoToday, dates: Optional[pd.DatetimeIndex], top_k: int = 12) -> str:
    a = np.asarray(iso_a.stress, dtype=float)
    b = np.asarray(iso_b.stress, dtype=float)
    if a.size != b.size or a.size < 5:
        return "Not enough aligned data to diagnose."

    ad = np.diff(a)
    bd = np.diff(b)

    # Best-effort dates aligned to deltas (diff between t and t-1)
    if dates is not None and len(dates) == a.size:
        d = list(pd.DatetimeIndex(dates[1:]))
    else:
        d = [None] * int(ad.size)

    # Correlation is computed on deltas inside Step 12.1
    corr = float(mc._corr_over_time(ad, bd))

    # Rank by |delta| magnitude and show both legs and product
    idx = np.argsort(-np.abs(ad * bd))
    lines: List[str] = []
    lines.append(f"Pair {iso_a.iso}-{iso_b.iso}: corr(Δstress) = {corr:.4f}")

    # Sensitivity: how much is correlation driven by a few shared shock days?
    for drop_n in (1, 3, 5):
        if idx.size <= drop_n + 5:
            continue
        keep = np.ones(int(idx.size), dtype=bool)
        keep[idx[: int(drop_n)]] = False
        cd = float(mc._corr_over_time(ad[keep], bd[keep]))
        lines.append(f"  corr excluding top{drop_n} |ΔA·ΔB| days: {cd:.4f}")

    lines.append("Top days by |ΔA·ΔB| (shared-move importance):")

    for k in range(min(int(top_k), int(idx.size))):
        i = int(idx[k])
        dtxt = str(d[i].date()) if d[i] is not None else f"t={i}"
        lines.append(
            f"  {k+1:>2}. {dtxt}  Δ{iso_a.iso}={ad[i]: .4f}  Δ{iso_b.iso}={bd[i]: .4f}  prod={ad[i]*bd[i]: .6f}"
        )

    # Also show a rough concentration metric: share of sum |prod| from top 3
    ap = np.abs(ad * bd)
    denom = float(np.nansum(ap))
    if denom > 0:
        top3 = float(np.nansum(ap[idx[:3]]))
        lines.append(f"Concentration: top3 |prod| share ≈ {top3/denom:.1%}")

    return "\n".join(lines)


def _v2x_similarity(*, run_dir: Path, iso_a: str, iso_b: str) -> str:
    base_a = run_dir / "inputs" / str(iso_a) / "covariance"
    base_b = run_dir / "inputs" / str(iso_b) / "covariance"
    z_a = base_a / f"{iso_a}_standardized_residuals_daily.csv"
    dt_a = base_a / f"{iso_a}_Dt_daily.csv"
    z_b = base_b / f"{iso_b}_standardized_residuals_daily.csv"
    dt_b = base_b / f"{iso_b}_Dt_daily.csv"

    if not (z_a.exists() and dt_a.exists() and z_b.exists() and dt_b.exists()):
        return "V2X check: missing input CSV(s)."

    za = pd.read_csv(z_a, usecols=["date", "V2X"])
    da = pd.read_csv(dt_a, usecols=["date", "V2X"])
    zb = pd.read_csv(z_b, usecols=["date", "V2X"])
    db = pd.read_csv(dt_b, usecols=["date", "V2X"])

    for df in (za, da, zb, db):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.dropna(subset=["date"], inplace=True)

    a = za.merge(da, on="date", suffixes=("__z", "__dt")).dropna()
    b = zb.merge(db, on="date", suffixes=("__z", "__dt")).dropna()
    m = a.merge(b, on="date", suffixes=(f"__{iso_a}", f"__{iso_b}"))
    if m.empty:
        return "V2X check: no overlapping dates."

    z_corr = float(np.corrcoef(m[f"V2X__z__{iso_a}"], m[f"V2X__z__{iso_b}"])[0, 1])
    dt_corr = float(np.corrcoef(m[f"V2X__dt__{iso_a}"], m[f"V2X__dt__{iso_b}"])[0, 1])
    shock_a = pd.to_numeric(m[f"V2X__z__{iso_a}"], errors="coerce") * pd.to_numeric(m[f"V2X__dt__{iso_a}"], errors="coerce")
    shock_b = pd.to_numeric(m[f"V2X__z__{iso_b}"], errors="coerce") * pd.to_numeric(m[f"V2X__dt__{iso_b}"], errors="coerce")
    shock_corr = float(np.corrcoef(shock_a, shock_b)[0, 1])

    eq_z = bool((m[f"V2X__z__{iso_a}"].round(12) == m[f"V2X__z__{iso_b}"].round(12)).all())
    eq_dt = bool((m[f"V2X__dt__{iso_a}"].round(12) == m[f"V2X__dt__{iso_b}"].round(12)).all())
    eq_shock = bool((shock_a.round(12) == shock_b.round(12)).all())

    d0 = str(pd.to_datetime(m["date"].min()).date())
    d1 = str(pd.to_datetime(m["date"].max()).date())
    return (
        f"V2X check ({iso_a} vs {iso_b}) overlap={len(m)} span={d0}..{d1}\n"
        f"  corr(V2X z)={z_corr:.6f}  equal={eq_z}\n"
        f"  corr(V2X Dt)={dt_corr:.6f}  equal={eq_dt}\n"
        f"  corr(V2X shock=z*Dt)={shock_corr:.6f}  equal={eq_shock}"
    )


if __name__ == "__main__":
    mc = _load_step12_1_module()

    QA_DIR.mkdir(parents=True, exist_ok=True)
    out_txt = QA_DIR / "esp_ita_today_connectedness_check.txt"
    out_csv = QA_DIR / "today_connectedness_matrix.csv"

    # ISOs: default set used in the report bundle (adjust if needed)
    isos = ["DEU", "ESP", "FRA", "ITA", "USA"]
    H = 60

    # Use the same block-def selection logic as Step 12.1
    iso_to_block_to_factors: Dict[str, Dict[str, Set[str]]] = {}
    for iso in isos:
        block_def_path = mc._auto_select_block_def(RUN_DIR, iso=iso)
        iso_to_block_to_factors[iso] = _build_iso_block_to_factors(
            mc=mc, run_dir=RUN_DIR, iso=iso, block_defs_path=block_def_path
        )

    vol_t0_by_iso = mc._load_vol_t0_map(MONTE_DIR)

    today: Dict[str, IsoToday] = {}
    stress_today_by_iso: Dict[str, np.ndarray] = {}
    end_dates: Dict[str, Optional[pd.Timestamp]] = {}

    for iso in isos:
        block_to_factors = iso_to_block_to_factors.get(iso) or {}
        factor_to_vol = vol_t0_by_iso.get(iso) or {}
        end_date, n_days, sev_today, stress = mc._compute_realized_today_metrics(
            run_dir=RUN_DIR,
            iso=iso,
            block_to_factors=block_to_factors,
            factor_to_vol=factor_to_vol,
            window_days=H,
        )
        if stress is None:
            continue

        stress = np.asarray(stress, dtype=float)
        today[iso] = IsoToday(
            iso=str(iso),
            end_date=end_date,
            n_days=n_days,
            severity_today=sev_today,
            stress=stress,
        )
        stress_today_by_iso[str(iso)] = stress
        end_dates[str(iso)] = end_date

    isos_common, C_today = mc._realized_corr_matrix(stress_today_by_iso=stress_today_by_iso)
    df_C = pd.DataFrame(C_today, index=isos_common, columns=isos_common)
    df_C.to_csv(out_csv, index=True)

    # Pair diagnosis (ESP-ITA)
    esp = today.get("ESP")
    ita = today.get("ITA")
    dates_esp = _window_dates_for_iso(mc=mc, run_dir=RUN_DIR, iso="ESP", window_days=H)

    lines: List[str] = []
    lines.append("Realized today connectedness recomputation (corr of Δ stress index)\n")
    lines.append(f"Run dir: {RUN_DIR}")
    lines.append(f"Window days: {H}")
    lines.append(f"ISOs included: {', '.join(isos_common)}")
    lines.append("")

    lines.append("End dates / n_days / severity_today:")
    for iso in isos_common:
        it = today.get(iso)
        if it is None:
            continue
        ed = str(it.end_date.date()) if it.end_date is not None else "None"
        nd = str(it.n_days) if it.n_days is not None else "None"
        sv = f"{it.severity_today:.4f}" if it.severity_today is not None else "None"
        lines.append(f"  {iso}: end={ed}  n_days={nd}  sev_today={sv}")

    lines.append("\nC_today matrix (rounded):")
    lines.append(_format_corr_matrix(isos_common, C_today))

    if esp is not None and ita is not None:
        lines.append("\n" + _diagnose_pair(mc=mc, iso_a=esp, iso_b=ita, dates=dates_esp, top_k=15))
        lines.append("\n" + _v2x_similarity(run_dir=RUN_DIR, iso_a="ESP", iso_b="ITA"))

    out_txt.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    print(f"Wrote: {out_csv}")
    print(f"Wrote: {out_txt}")
