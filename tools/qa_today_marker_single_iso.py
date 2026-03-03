from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd


def _load_module(module_path: Path):
    spec = importlib.util.spec_from_file_location("mcplots", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    # Ensure the module is registered during execution (dataclasses expects this).
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iso", required=True)
    ap.add_argument(
        "--run-dir",
        default="analysis_outputs/scenarios/latest",
        help="Run directory containing inputs/ and monte_carlo/",
    )
    ap.add_argument("--window-days", type=int, default=60)
    ap.add_argument(
        "--mapping",
        choices=["blockdef", "mc"],
        default="mc",
        help="Which block→factor mapping to use. 'mc' reproduces the main script (lagged MC factors).",
    )
    ap.add_argument(
        "--module",
        default="SRESS TEST PIPELINE/12.1_monte_carlo_scenario_plots.py",
        help="Path to the plotting script to import helpers from",
    )
    args = ap.parse_args()

    iso = str(args.iso)
    run_dir = Path(args.run_dir)
    module_path = Path(args.module)

    mod = _load_module(module_path)

    block_def_path = mod._auto_select_block_def(run_dir, iso=iso)
    block_defs = mod._load_block_definitions(block_def_path)

    blocks_for_iso = (block_defs or {}).get(iso) or {}
    block_to_series_blockdef = {str(k): set(str(x) for x in (v or [])) for k, v in blocks_for_iso.items()}

    if args.mapping == "blockdef":
        block_to_factors = dict(block_to_series_blockdef)
    else:
        # Reproduce main-script mapping: assign *MC factors* (incl. lags) to blocks.
        rev_map, _dup_rows = mod._reverse_block_map(block_defs)
        iso_rev = (rev_map or {}).get(iso) or {}
        mc_factors = mod._load_mc_factor_list(run_dir, iso=iso)
        factor_to_blocks: dict[str, set[str]] = {}
        for f in mc_factors:
            base = mod._strip_lag_suffix(str(f))
            keys = iso_rev.get(str(f)) or iso_rev.get(str(base))
            if keys:
                factor_to_blocks[str(f)] = set(str(x) for x in keys)
            else:
                factor_to_blocks[str(f)] = {"unmapped"}

        block_to_factors_list: dict[str, list[str]] = {}
        for f, blocks in factor_to_blocks.items():
            for b in blocks:
                block_to_factors_list.setdefault(str(b), []).append(str(f))

        block_to_factors = {
            str(k): set(str(x) for x in v)
            for k, v in (block_to_factors_list or {}).items()
            if str(k) != "unmapped" and v
        }

    vol_t0_by_iso = mod._load_vol_t0_map(run_dir / "monte_carlo")
    factor_to_vol = (vol_t0_by_iso or {}).get(iso) or {}

    end_date, T, sev, stress = mod._compute_realized_today_metrics(
        run_dir=run_dir,
        iso=iso,
        block_to_factors=block_to_factors,
        factor_to_vol=factor_to_vol,
        window_days=int(args.window_days),
    )
    print("FUNC output:", {"end_date": str(end_date), "T": T, "sev": sev, "stress_last": float(stress[-1]) if stress is not None else None})

    # Recompute internals to compare gating / de-meaning
    df_z, df_dt = mod._load_realized_inputs_wide(run_dir=run_dir, iso=iso)
    df = (
        df_z.merge(df_dt, on="date", how="inner", suffixes=("__z", "__dt"))
        .sort_values("date")
        .reset_index(drop=True)
    )

    union_factors: set[str] = set()
    for fs in (block_to_factors or {}).values():
        union_factors |= set(str(x) for x in (fs or set()))

    usable = [f for f in sorted(union_factors) if f"{f}__z" in df.columns and f"{f}__dt" in df.columns]
    H = int(args.window_days)
    win = df.tail(H)

    Z_all = df[[f"{f}__z" for f in usable]].apply(pd.to_numeric, errors="coerce")
    Dt_all = df[[f"{f}__dt" for f in usable]].apply(pd.to_numeric, errors="coerce")
    vols = pd.Series({f: float(factor_to_vol.get(f, 1.0) or 1.0) for f in usable})
    vols = pd.to_numeric(vols, errors="coerce").fillna(1.0).replace(0.0, 1.0)

    dt_diff = Dt_all.diff().abs()
    dt_change = dt_diff > 1e-9
    if not dt_change.empty:
        dt_change.iloc[0, :] = True

    lookback = int(min(756, len(df)))
    dt_change_lb = dt_change.tail(lookback)

    lowfreq: set[str] = set()
    frac_change = dt_change_lb.mean(axis=0)
    for j, f in enumerate(usable):
        v = float(frac_change.iloc[j])
        if np.isfinite(v) and v < 0.10:
            lowfreq.add(f)

    gate = np.ones((len(df), len(usable)), dtype=float)
    for j, f in enumerate(usable):
        if f in lowfreq:
            gate[:, j] = dt_change.iloc[:, j].to_numpy(dtype=bool).astype(float)

    denom = vols.to_numpy(dtype=float)[None, :]
    shock = Z_all.to_numpy(dtype=float) * Dt_all.to_numpy(dtype=float)

    shock_z_g = (shock / denom) * gate
    shock_z_ng = (shock / denom)

    demean_lb = int(min(756, len(df)))
    mu_g = np.nanmean(shock_z_g[-demean_lb:, :], axis=0, keepdims=True)
    mu_ng = np.nanmean(shock_z_ng[-demean_lb:, :], axis=0, keepdims=True)

    shock_win_g = shock_z_g[-len(win) :, :]
    shock_win_ng = shock_z_ng[-len(win) :, :]

    shock_win_g_dm = shock_win_g - mu_g
    shock_win_ng_dm = shock_win_ng - mu_ng

    f_index = {f: i for i, f in enumerate(usable)}
    block_idxs: list[list[int]] = []
    for bk, fs in sorted((block_to_factors or {}).items()):
        idxs = [f_index.get(str(x)) for x in (fs or set())]
        idxs = [i for i in idxs if i is not None]
        if idxs:
            block_idxs.append(idxs)

    def sev_from(sh: np.ndarray) -> float:
        mats: list[np.ndarray] = []
        for idxs in block_idxs:
            m = np.nanmean(sh[:, idxs], axis=1)
            mats.append(m)
        B = np.vstack(mats)
        cum = np.cumsum(np.where(np.isfinite(B), B, 0.0), axis=1)
        term = cum[:, -1]
        return float(np.sqrt(np.sum(term**2)))

    print("Diagnostics:")
    print("  n_factors", len(usable), "n_lowfreq", len(lowfreq), "lowfreq_frac", (len(lowfreq) / max(1, len(usable))))
    print("  mean_abs_mu_g", float(np.nanmean(np.abs(mu_g))), "mean_abs_mu_ng", float(np.nanmean(np.abs(mu_ng))))
    print("  mean_shock_g_win", float(np.nanmean(shock_win_g)), "mean_shock_g_win_dm", float(np.nanmean(shock_win_g_dm)))

    print("  sev gated no-demean", sev_from(shock_win_g))
    print("  sev gated demean", sev_from(shock_win_g_dm))
    print("  sev NO-gate no-demean", sev_from(shock_win_ng))
    print("  sev NO-gate demean", sev_from(shock_win_ng_dm))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
