from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd


def _load_vol_t0(run_dir: Path, iso: str) -> dict[str, float]:
    p = run_dir / "monte_carlo" / "diagnostics" / "dims.csv"
    df = pd.read_csv(p)
    df = df[df["iso"].astype(str) == str(iso)]
    return {str(r["factor"]): float(r["vol_t0"]) for _, r in df.iterrows()}


def _aligned_realized_frames(run_dir: Path, iso: str) -> pd.DataFrame:
    base_cov = run_dir / "inputs" / iso / "covariance"
    base_short = run_dir / "inputs" / iso / "shortlist"

    df_z = pd.read_csv(base_cov / f"{iso}_standardized_residuals_daily.csv")
    df_dt = pd.read_csv(base_cov / f"{iso}_Dt_daily.csv")
    df_m = pd.read_csv(base_short / f"{iso}_block_factors_M_daily.csv")

    for df in (df_z, df_dt, df_m):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.dropna(subset=["date"], inplace=True)

    df = df_z.merge(df_dt, on="date", how="inner", suffixes=("__z", "__dt"))
    df = df.merge(df_m, on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _infer_lowfreq_gate(Dt_all: np.ndarray, lookback: int = 756, tol: float = 1e-9, frac_thr: float = 0.10) -> tuple[np.ndarray, np.ndarray]:
    """Return (gate_all, lowfreq_mask_per_factor)."""
    dt_diff = np.abs(np.diff(Dt_all, axis=0))
    dt_change = dt_diff > float(tol)
    # prepend True row so first obs counts as update
    dt_change = np.vstack([np.ones((1, dt_change.shape[1]), dtype=bool), dt_change])

    if dt_change.shape[0] > lookback:
        lb = dt_change[-lookback:, :]
    else:
        lb = dt_change

    frac_change = np.mean(lb, axis=0)
    lowfreq = frac_change < float(frac_thr)

    gate = np.ones_like(Dt_all, dtype=float)
    gate[:, lowfreq] = dt_change[:, lowfreq].astype(float)
    return gate, lowfreq


def compute_today(
    *,
    run_dir: Path,
    iso: str,
    window_days: int,
    demean_lookback_days: int = 756,
) -> dict[str, float | str]:
    df = _aligned_realized_frames(run_dir, iso)

    # Factors are those appearing in Dt file (excluding date/Rt_daily)
    dt_cols = [c for c in df.columns if c.endswith("__dt")]
    factors = [c[: -len("__dt")] for c in dt_cols if c[: -len("__dt")] not in {"date", "Rt_daily"}]

    vol_t0 = _load_vol_t0(run_dir, iso)
    factors = [f for f in factors if f in vol_t0 and f in df.columns and f + "__z" in df.columns]

    out: dict[str, float | str] = {
        "iso": iso,
        "n_aligned_days": float(len(df)),
        "end_date": str(df["date"].iloc[-1].date()) if len(df) else "(n/a)",
    }
    if len(df) < window_days + 5 or not factors:
        out["error"] = "insufficient data"
        return out

    # Build arrays
    Z_all = df[[f + "__z" for f in factors]].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    Dt_all = df[[f + "__dt" for f in factors]].apply(pd.to_numeric, errors="coerce").to_numpy(float)
    M_all = df[[f for f in factors]].apply(pd.to_numeric, errors="coerce").to_numpy(float)

    vols = np.array([vol_t0.get(f, 1.0) or 1.0 for f in factors], dtype=float)[None, :]

    # Sanity: M ≈ Z*Dt
    diff = (Z_all * Dt_all) - M_all
    diff = diff[np.isfinite(diff)]
    out["rms_m_minus_zdt"] = float(np.sqrt(np.mean(diff**2))) if diff.size else float("nan")

    # Stats on Z (should be ~0 mean, ~1 std)
    z0 = Z_all.reshape(-1)
    z0 = z0[np.isfinite(z0)]
    out["z_mean"] = float(np.mean(z0)) if z0.size else float("nan")
    out["z_std"] = float(np.std(z0)) if z0.size else float("nan")

    # Lowfreq gate (from Dt)
    gate_all, lowfreq_mask = _infer_lowfreq_gate(Dt_all, lookback=demean_lookback_days)

    # Current pipeline realized shock_z (no de-meaning)
    shock_z_cur = (Z_all * Dt_all) / vols
    shock_z_cur = shock_z_cur * gate_all

    # Alternative: use innovations M (same as Z*Dt) / vol, with de-meaning on gated series
    shock_z_m = M_all / vols
    shock_z_m = shock_z_m * gate_all

    # De-mean per factor using lookback window on the gated series (includes zeros for gated lowfreq)
    if shock_z_m.shape[0] > demean_lookback_days:
        lb = shock_z_m[-demean_lookback_days:, :]
    else:
        lb = shock_z_m
    mu = np.nanmean(lb, axis=0, keepdims=True)
    shock_z_dm = shock_z_m - mu

    # Window slice
    win_slice = slice(-int(window_days), None)
    cur_win = shock_z_cur[win_slice, :]
    dm_win = shock_z_dm[win_slice, :]

    # Aggregate blocks crudely as mean across factors (not using block map; this is just drift scale check)
    # Compute a toy severity as L2 of terminal cumulative factor means.
    def toy_sev(arr: np.ndarray) -> float:
        x = np.nanmean(arr, axis=1)
        x = np.where(np.isfinite(x), x, 0.0)
        cum = np.cumsum(x)
        return float(np.abs(cum[-1]))

    out["toy_terminal_cur"] = toy_sev(cur_win)
    out["toy_terminal_demeaned"] = toy_sev(dm_win)

    # Distribution stats on the windowed shocks
    for name, arr in [("cur", cur_win), ("demeaned", dm_win)]:
        a = arr.reshape(-1)
        a = a[np.isfinite(a)]
        out[f"{name}_mean"] = float(np.mean(a)) if a.size else float("nan")
        out[f"{name}_std"] = float(np.std(a)) if a.size else float("nan")

    out["n_factors"] = float(len(factors))
    out["n_lowfreq_factors"] = float(int(np.sum(lowfreq_mask)))

    return out


def main() -> None:
    run_dir = Path("analysis_outputs/scenarios/latest")
    H = 60
    rows = []
    for iso in ["USA", "FRA", "DEU", "ESP", "ITA"]:
        try:
            rows.append(compute_today(run_dir=run_dir, iso=iso, window_days=H))
        except Exception as e:
            rows.append({"iso": iso, "error": repr(e)})

    df = pd.DataFrame(rows)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 200)
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
