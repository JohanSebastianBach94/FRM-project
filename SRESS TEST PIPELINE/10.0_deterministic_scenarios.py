#!/usr/bin/env python3
"""Step 10.0 — Deterministic scenarios (literature-aligned).

Consumes the Step 9 run contract under:
  analysis_outputs/scenarios/<run_id>/manifest.json
and produces deterministic factor-shock paths in *factor shock space*:
  analysis_outputs/scenarios/<run_id>/deterministic/

Design rules (per IMPLEMENTATION_PLAN.md)
- Canonical shock space is factor innovations (return-like).
- Any decoding to levels is explicitly downstream reporting.

Outputs
- factor_shocks.csv: long format with per-ISO factor shocks by horizon.
- scenario_definition.json: scenario metadata + scaling choices.

This is intentionally minimal but auditable: it proves the downstream
scenario contract works end-to-end.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"
DIAG_CORR_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _infer_run_id(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    latest = SCENARIOS_DIR / "latest"
    if latest.exists() and latest.is_dir():
        return latest.name
    raise SystemExit("Missing --run-id (no scenarios/latest folder)")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _extract_block_key(block_id: str) -> str:
    # block ids look like "usa_real_estate"; key is suffix
    if "_" not in block_id:
        return block_id
    return block_id.split("_", 1)[1]


@dataclass(frozen=True)
class DeterministicScenarioSpec:
    scenario_id: str
    description: str
    horizon_days: int
    # shock units in z-space, later scaled by sigma_t0
    z_shocks: Dict[str, float]  # factor_name -> z
    path_shape: str = "step"  # step | ramp
    ramp_days: int = 5


def _build_default_scenarios() -> List[DeterministicScenarioSpec]:
    """Minimal, literature-friendly templates.

    These are intentionally simple “narrative” shocks in z-units.
    - Global risk-off proxy (if present): V2X (EU) or VIXCLS (USA) up
    - Returns shock proxy (if present): Rt_daily down (note: Rt_daily is not a factor; it may appear in pair paths)

    We keep it conservative: only shocks factors that exist for a given ISO.
    """
    return [
        DeterministicScenarioSpec(
            scenario_id="risk_off_global",
            description="Global risk-off: volatility proxy up",
            horizon_days=20,
            z_shocks={"V2X": +2.5, "VIXCLS": +2.5},
            path_shape="ramp",
            ramp_days=5,
        ),
        DeterministicScenarioSpec(
            scenario_id="mild_risk_off",
            description="Mild risk-off: volatility proxy up",
            horizon_days=20,
            z_shocks={"V2X": +1.5, "VIXCLS": +1.5},
            path_shape="ramp",
            ramp_days=5,
        ),
    ]


def _compute_sigma_t0_from_dt(dt_csv: Path, *, factors: List[str], t0: str) -> Dict[str, float]:
    """Compute per-factor sigma(t0) from a frozen Dt CSV (if available).

    Expected shape:
    - index: date
    - columns: subset/superset of factors

    Returns a dict factor -> sigma_t0.
    """
    df = pd.read_csv(dt_csv, index_col=0)
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        raise SystemExit(f"Dt file index is not parseable as dates: {dt_csv}")

    df = df.sort_index()
    t0_ts = pd.to_datetime(t0)
    df = df.loc[df.index <= t0_ts]
    if df.empty:
        raise SystemExit(f"No Dt observations <= t0={t0} in {dt_csv}")

    row = df.iloc[-1]
    out: Dict[str, float] = {}
    for f in factors:
        if f in row.index:
            val = _safe_float(row[f])
            if val is not None and np.isfinite(val) and val > 0:
                out[f] = float(val)
    return out


def _compute_sigma_t0_fallback(sigma_pairs_csv: Path, *, factors: List[str]) -> Dict[str, float]:
    """Fallback scaling when Dt is unavailable.

    Literature intent: scenarios in innovation space are scaled by current volatility.
    If Dt isn’t available, we fall back to a robust constant scale based on
    the recent distribution of pairwise covariances: sigma_i ≈ sqrt(median(Var_i)).

    Here we approximate Var_i using diagonal proxy from pair names "X_X" when present.
    If diagonals are absent (common), we return empty and downstream will keep z-units.
    """
    header = sigma_pairs_csv.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    cols = [c.strip() for c in header.split(",")]
    diag_cols = [c for c in cols if "_" in c and c.split("_", 1)[0] == c.split("_", 1)[1]]
    if not diag_cols:
        return {}

    df = pd.read_csv(sigma_pairs_csv, usecols=[0] + diag_cols)
    out: Dict[str, float] = {}
    for col in diag_cols:
        f = col.split("_", 1)[0]
        if f not in factors:
            continue
        series = pd.to_numeric(df[col], errors="coerce").dropna()
        if series.empty:
            continue
        var = float(series.tail(756).median())
        if np.isfinite(var) and var > 0:
            out[f] = float(np.sqrt(var))
    return out


def _load_residual_series(resid_csv: Path, *, factor: str) -> pd.Series:
    """Load a single standardized-residual series from a persisted daily residuals CSV."""
    if not resid_csv.exists():
        raise FileNotFoundError(str(resid_csv))

    # Files written by Step 6.2 -> daily_adcc_prep.py use an explicit 'date' index column.
    try:
        df = pd.read_csv(resid_csv, usecols=["date", factor], parse_dates=["date"])
        df = df.set_index("date")
    except ValueError:
        # Fall back in case the index column is unnamed.
        df = pd.read_csv(resid_csv, index_col=0)
        df.index = pd.to_datetime(df.index, errors="coerce")
        if factor not in df.columns:
            raise KeyError(f"Factor {factor!r} not found in {resid_csv}")

    s = pd.to_numeric(df[factor], errors="coerce")
    s = s.dropna()
    s.name = factor
    return s


def _calibrate_factor_z_from_residuals(
    *,
    iso_inputs: Dict[str, Any],
    factor: str,
    t0: str,
    window_days: int,
    quantile: float,
) -> tuple[Optional[float], Dict[str, Any]]:
    """Empirically calibrate a z-shock from standardized residuals.

    Returns (z, metadata). If calibration cannot be computed, returns (None, metadata).
    """
    t0_ts = pd.to_datetime(t0)
    # window_days is interpreted as trading days; we approximate by calendar slicing of dates.
    # The input residual series itself is business-day aligned.
    start_ts = t0_ts - pd.Timedelta(days=int(window_days) * 2)

    samples: list[np.ndarray] = []
    isos_used: list[str] = []
    files_used: list[str] = []

    for iso in sorted(iso_inputs.keys()):
        iso_meta = iso_inputs.get(iso) or {}
        frozen_files = iso_meta.get("frozen_files") or {}
        resid_frozen = (frozen_files.get("standardized_residuals") or {}).get("frozen")
        if isinstance(resid_frozen, str) and resid_frozen:
            resid_path = PROJECT_ROOT / resid_frozen
        else:
            resid_path = DIAG_CORR_DAILY_DIR / f"{iso}_standardized_residuals_daily.csv"

        if not resid_path.exists():
            continue
        try:
            s = _load_residual_series(resid_path, factor=factor)
        except Exception:
            continue

        s = s.sort_index()
        # Use a generous calendar pre-slice then take the last `window_days` business-day rows <= t0.
        s = s.loc[(s.index >= start_ts) & (s.index <= t0_ts)]
        if s.empty:
            continue
        s = s.tail(int(window_days))
        if s.empty:
            continue

        samples.append(s.to_numpy(dtype=float))
        isos_used.append(iso)
        try:
            files_used.append(str(resid_path.relative_to(PROJECT_ROOT)))
        except Exception:
            files_used.append(str(resid_path))

    meta: Dict[str, Any] = {
        "factor": factor,
        "quantile": float(quantile),
        "t0": str(t0_ts.date()),
        "window_days": int(window_days),
        "isos_used": isos_used,
        "files_used": files_used,
    }

    if not samples:
        meta["status"] = "missing_residuals"
        return None, meta

    vals = np.concatenate(samples)
    vals = vals[np.isfinite(vals)]
    meta["n_obs"] = int(vals.size)
    if vals.size < 50:
        meta["status"] = "too_few_samples"
        return None, meta

    z = float(np.quantile(vals, float(quantile)))
    meta["status"] = "ok"
    meta["z"] = z
    return z, meta


def _build_path(shape: str, horizon: int, *, level: float, ramp_days: int) -> np.ndarray:
    if horizon <= 0:
        return np.array([], dtype=float)
    if shape == "step":
        return np.full(horizon, level, dtype=float)
    if shape == "ramp":
        rd = max(1, min(int(ramp_days), horizon))
        ramp = np.linspace(0.0, level, rd, dtype=float)
        tail = np.full(horizon - rd, level, dtype=float)
        return np.concatenate([ramp, tail])
    raise SystemExit(f"Unknown path_shape: {shape}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 10.0 — Deterministic factor-shock scenarios")
    parser.add_argument("--run-id", default=None, help="Scenario run id created by Step 9")
    parser.add_argument("--scenario", default=None, help="Scenario id to run (default: all built-ins)")
    parser.add_argument("--horizon-days", type=int, default=None, help="Override horizon for all scenarios")
    parser.add_argument(
        "--disable-v2x-quantile-calibration",
        action="store_true",
        help="Disable empirical V2X quantile calibration and use the template z-shocks.",
    )
    parser.add_argument(
        "--v2x-quantile",
        type=float,
        default=0.99,
        help="Upper-tail quantile for V2X standardized residuals used by risk_off_global (default: 0.99)",
    )
    parser.add_argument(
        "--v2x-mild-quantile",
        type=float,
        default=0.95,
        help="Upper-tail quantile for V2X standardized residuals used by mild_risk_off (default: 0.95)",
    )
    parser.add_argument(
        "--v2x-calibration-window-days",
        type=int,
        default=None,
        help="Window length (trading days) for V2X quantile calibration (default: manifest window_days or 756)",
    )
    args = parser.parse_args()

    run_id = _infer_run_id(args.run_id)
    run_dir = SCENARIOS_DIR / run_id
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing Step 9 manifest: {manifest_path}")

    manifest = _read_json(manifest_path)
    iso_inputs: Dict[str, Any] = (manifest.get("iso_inputs") or {})
    if not iso_inputs:
        raise SystemExit("Manifest has no iso_inputs")

    config = manifest.get("config") or {}
    t0 = str(config.get("t0") or "")
    if not t0:
        # Fallback: use the first ISO's asof_t0.
        first_iso = sorted(iso_inputs.keys())[0]
        t0 = str((iso_inputs.get(first_iso) or {}).get("asof_t0") or (iso_inputs.get(first_iso) or {}).get("frozen_end"))
    if not t0:
        raise SystemExit("Unable to infer t0 from manifest")

    window_days = int(args.v2x_calibration_window_days or config.get("window_days") or 756)
    calibration: Dict[str, Any] = {}
    if not args.disable_v2x_quantile_calibration:
        v2x_z_severe, v2x_meta_severe = _calibrate_factor_z_from_residuals(
            iso_inputs=iso_inputs,
            factor="V2X",
            t0=t0,
            window_days=window_days,
            quantile=float(args.v2x_quantile),
        )
        calibration["V2X_risk_off_global"] = v2x_meta_severe

        v2x_z_mild, v2x_meta_mild = _calibrate_factor_z_from_residuals(
            iso_inputs=iso_inputs,
            factor="V2X",
            t0=t0,
            window_days=window_days,
            quantile=float(args.v2x_mild_quantile),
        )
        calibration["V2X_mild_risk_off"] = v2x_meta_mild

    scenarios = _build_default_scenarios()
    if args.scenario:
        scenarios = [s for s in scenarios if s.scenario_id == args.scenario]
        if not scenarios:
            raise SystemExit(f"Unknown scenario id: {args.scenario}")

    out_dir = _ensure_dir(run_dir / "deterministic")

    rows: List[Dict[str, Any]] = []
    scaling_rows: List[Dict[str, Any]] = []

    for iso, iso_meta in iso_inputs.items():
        factors: List[str] = list(iso_meta.get("factors") or [])
        asof_t0 = iso_meta.get("asof_t0") or iso_meta.get("frozen_end")
        if not asof_t0:
            raise SystemExit(f"Missing asof_t0/frozen_end for ISO {iso} in manifest")

        # Scaling source precedence: Dt (preferred) else fallback
        frozen_files = iso_meta.get("frozen_files") or {}
        dt_frozen = (frozen_files.get("dt") or {}).get("frozen")
        sigma_frozen = (frozen_files.get("sigma_pairs") or {}).get("frozen")

        sigma_t0: Dict[str, float] = {}
        scaling_method = "z_only"
        if isinstance(dt_frozen, str) and dt_frozen:
            dt_path = PROJECT_ROOT / dt_frozen
            if dt_path.exists():
                sigma_t0 = _compute_sigma_t0_from_dt(dt_path, factors=factors, t0=str(asof_t0))
                scaling_method = "Dt_t0"
        if not sigma_t0 and isinstance(sigma_frozen, str) and sigma_frozen:
            sigma_path = PROJECT_ROOT / sigma_frozen
            if sigma_path.exists():
                sigma_t0 = _compute_sigma_t0_fallback(sigma_path, factors=factors)
                if sigma_t0:
                    scaling_method = "Sigma_diag_proxy"

        for scen in scenarios:
            horizon = int(args.horizon_days or scen.horizon_days)

            for factor_name, z in scen.z_shocks.items():
                if factor_name not in factors:
                    continue
                z_effective = float(z)
                if (
                    scen.scenario_id == "risk_off_global"
                    and factor_name == "V2X"
                    and not args.disable_v2x_quantile_calibration
                ):
                    z_cal = (calibration.get("V2X_risk_off_global") or {}).get("z")
                    if z_cal is not None and np.isfinite(float(z_cal)):
                        z_effective = float(z_cal)
                if (
                    scen.scenario_id == "mild_risk_off"
                    and factor_name == "V2X"
                    and not args.disable_v2x_quantile_calibration
                ):
                    z_cal = (calibration.get("V2X_mild_risk_off") or {}).get("z")
                    if z_cal is not None and np.isfinite(float(z_cal)):
                        z_effective = float(z_cal)
                scale = float(sigma_t0.get(factor_name, 1.0))
                path = _build_path(scen.path_shape, horizon, level=float(z_effective) * scale, ramp_days=int(scen.ramp_days))

                scaling_rows.append(
                    {
                        "run_id": run_id,
                        "iso": iso,
                        "scenario_id": scen.scenario_id,
                        "factor": factor_name,
                        "asof_t0": str(asof_t0),
                        "scaling_method": scaling_method,
                        "z_template": float(z),
                        "z_effective": float(z_effective),
                        "scale_sigma_t0": float(scale),
                    }
                )

                for h in range(1, horizon + 1):
                    rows.append(
                        {
                            "run_id": run_id,
                            "scenario_id": scen.scenario_id,
                            "iso": iso,
                            "h": h,
                            "factor": factor_name,
                            "shock": float(path[h - 1]),
                            "shock_units": "innovation",
                        }
                    )

    shocks_df = pd.DataFrame(rows)
    shocks_path = out_dir / "factor_shocks.csv"
    shocks_df.to_csv(shocks_path, index=False)

    scaling_df = pd.DataFrame(scaling_rows)
    scaling_path = out_dir / "scaling_diagnostics.csv"
    scaling_df.to_csv(scaling_path, index=False)

    scenario_def = {
        "run_id": run_id,
        "created_at": _utc_now_iso(),
        "inputs_manifest": str(manifest_path),
        "canonical_shock_space": "factor_innovations",
        "scenarios": [asdict(s) for s in scenarios],
        "calibration": calibration,
        "notes": [
            "Shocks are produced in factor innovation space.",
            "Scaling uses Dt(t0) when available; otherwise falls back to z-units or a Sigma-diagonal proxy.",
            "For risk_off_global, V2X z-shock can be calibrated from empirical standardized-residual quantiles (see 'calibration').",
        ],
    }
    (out_dir / "scenario_definition.json").write_text(json.dumps(scenario_def, indent=2), encoding="utf-8")

    print(f"[OK] Wrote: {shocks_path}")
    print(f"[OK] Wrote: {scaling_path}")
    print(f"[OK] Wrote: {out_dir / 'scenario_definition.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
