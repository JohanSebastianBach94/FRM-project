#!/usr/bin/env python3
"""Step 10.2 — Factor-innovation shocks implied by an IMF/FSAP-style macro narrative.

Goal
- Read a macro narrative template YAML (Step 10.1 style) that defines quarterly macro deltas.
- Use Step 4 mappings (factors -> targets) to compute an *implied* factor shock vector
  that best matches the macro deltas, in a transparent and auditable way.
- Write deterministic/factor_shocks_from_macro.csv plus diagnostics.

Important semantics / disclaimers
- Step 4 models are trained as: target level ~ linear combo of (standardized) factors.
  This step treats macro narrative deltas as target deltas and inverts the linear map
  using ridge-regularized least squares.
- Outputs are in factor innovation units, scaled via Dt(t0) when available.
- This is an approximation intended for scenario prototyping and auditability; true
  macro-to-factor translation would ideally be calibrated/validated via a macro model.

Outputs (under analysis_outputs/scenarios/<run_id>/deterministic/)
- factor_shocks_from_macro.csv
- factor_shocks_from_macro_diagnostics.json
- factor_shocks_from_macro_mapping.csv (coefficient matrix used)
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"
FEATURE_CONTRIB_DIR = PROJECT_ROOT / "analysis_outputs"
DIAG_CORR_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"


# Macro inversion guardrails
# GDP is frequently weak/unstable OOS in the Step 4 linear mappings and is not recommended
# as a narrative inversion constraint.
_DISALLOWED_INVERSION_VARIABLES = {"gdp_growth_yoy"}

# Canonical shock space guardrails
# Step 10.2 must operate in *factor-innovation* space (return-like / stationary drivers).
# In particular, lag-features and structural ratios (e.g., debt-to-GDP levels) are not
# suitable as shockable factors for inversion.
_LAG_FEATURE_RE = re.compile(r"_lag\d+$", re.IGNORECASE)
_DEBT_GDP_RE = re.compile(r"(gc\.dod\.totl\.gd\.zs)|((debt|dod)[^a-z0-9]*.*gdp)", re.IGNORECASE)


_PC_RE = re.compile(r"^(?P<block>[A-Za-z0-9_]+)_pc(?P<k>\d+)$")


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


def _inversion_factor_exclusion_reason(factor: str) -> Optional[str]:
    """Return a reason string if a factor is disallowed for Step 10.2 inversion."""

    name = str(factor or "").strip()
    if not name:
        return "empty"
    if _LAG_FEATURE_RE.search(name):
        return "lag_feature"
    if _DEBT_GDP_RE.search(name):
        return "structural_debt_to_gdp"
    return None


def _is_target_lag_feature(feature: str, *, target: str) -> bool:
    """Return True if a feature looks like an autoregressive lag of the target itself."""

    f = str(feature or "")
    t = str(target or "")
    if not f or not t:
        return False
    # Common Step 4 convention: <TARGET>_lag1, <TARGET>_lag2, ...
    if f.startswith(f"{t}_") and _LAG_FEATURE_RE.search(f):
        return True
    return False


def _coef_l2_norm(coef_map: Mapping[str, float]) -> float:
    vals = [float(v) for v in coef_map.values() if v is not None and np.isfinite(float(v))]
    if not vals:
        return 0.0
    a = np.array(vals, dtype=float)
    return float(np.linalg.norm(a))


def _compute_nonlag_share(
    *,
    target: str,
    coef_map: Mapping[str, float],
    allowed_factor_set: Optional[set[str]] = None,
) -> Dict[str, Any]:
    """Compute how much of a target mapping lives on non-lag, allowed factors.

    Intended use: if a target is essentially AR-lags (or structural factors we exclude),
    it is not a good inversion constraint.
    """

    target = str(target)
    allowed_factor_set = allowed_factor_set or set()

    total_map: Dict[str, float] = {}
    allowed_map: Dict[str, float] = {}
    ar_lag_map: Dict[str, float] = {}
    non_ar_map: Dict[str, float] = {}

    for f, c in coef_map.items():
        try:
            cc = float(c)
        except Exception:
            continue
        if not np.isfinite(cc) or cc == 0.0:
            continue
        ff = str(f)
        total_map[ff] = cc
        is_ar = _is_target_lag_feature(ff, target=target)
        if is_ar:
            ar_lag_map[ff] = cc
        else:
            non_ar_map[ff] = cc
        if allowed_factor_set and ff in allowed_factor_set:
            allowed_map[ff] = cc

    total_norm = _coef_l2_norm(total_map)
    allowed_norm = _coef_l2_norm(allowed_map)
    ar_lag_norm = _coef_l2_norm(ar_lag_map)
    non_ar_norm = _coef_l2_norm(non_ar_map)

    allowed_share_total = 0.0 if total_norm <= 1e-12 else float(allowed_norm / total_norm)
    ar_lag_share_total = 0.0 if total_norm <= 1e-12 else float(ar_lag_norm / total_norm)
    allowed_share_of_non_ar = 0.0 if non_ar_norm <= 1e-12 else float(allowed_norm / non_ar_norm)

    return {
        "total_norm_l2": float(total_norm),
        "allowed_norm_l2": float(allowed_norm),
        "ar_lag_norm_l2": float(ar_lag_norm),
        "non_ar_norm_l2": float(non_ar_norm),
        "allowed_share_total": float(allowed_share_total),
        "ar_lag_share_total": float(ar_lag_share_total),
        "allowed_share_of_non_ar": float(allowed_share_of_non_ar),
        "n_total_features_nonzero": int(len(total_map)),
        "n_allowed_features_nonzero": int(len(allowed_map)),
        "n_ar_lag_features_nonzero": int(len(ar_lag_map)),
        "n_non_ar_features_nonzero": int(len(non_ar_map)),
        "allowed_features_sample": list(sorted(allowed_map.keys()))[:15],
        "ar_lag_features_sample": list(sorted(ar_lag_map.keys()))[:10],
    }


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_yaml_or_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Macro config not found: {path}")
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if path.suffix.lower() in {".yaml", ".yml"}:
        import yaml  # type: ignore

        return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise SystemExit(f"Unsupported macro config extension: {path.suffix}")


def _parse_coefficients_blob(blob: Any) -> Dict[str, float]:
    """Parse the Step 4 coefficients string: "feat:coef;feat2:coef2"."""

    if blob is None:
        return {}
    text = str(blob)
    out: Dict[str, float] = {}
    for part in text.split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        k, v = part.split(":", 1)
        k = k.strip()
        try:
            out[k] = float(v)
        except Exception:
            continue
    return out


def _load_pca_loadings(iso: str, block: str) -> Optional[pd.DataFrame]:
    path = PROJECT_ROOT / "analysis_outputs" / "factor_preparation" / f"{iso}_{block}_pca_loadings.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0)
    return df


def _expand_pc_coefficients(
    *,
    iso: str,
    coef_map: Dict[str, float],
    loadings_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]],
) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """Expand block PC features (e.g., macro_pc1) into underlying raw series via PCA loadings.

    If target = sum_j beta_j * PC_j and PC_j = sum_k loading_{k,j} * series_k,
    then target = sum_k (sum_j beta_j*loading_{k,j}) * series_k.

    This makes Step 4 mappings compatible with Step 9/10 factor lists (raw series/lagged series).
    """

    expanded: Dict[str, float] = {}
    diag: Dict[str, Any] = {
        "pc_features_seen": [],
        "pc_features_expanded": [],
        "pc_features_missing_loadings": [],
        "blocks_used": [],
    }

    for feat, beta in coef_map.items():
        m = _PC_RE.match(feat)
        if not m:
            expanded[feat] = expanded.get(feat, 0.0) + float(beta)
            continue

        diag["pc_features_seen"].append(feat)
        block = m.group("block")
        key = (iso, block)
        if key not in loadings_cache:
            loadings_cache[key] = _load_pca_loadings(iso, block)
        L = loadings_cache[key]
        if L is None or feat not in L.columns:
            diag["pc_features_missing_loadings"].append({"feature": feat, "block": block})
            continue

        diag["pc_features_expanded"].append(feat)
        if block not in diag["blocks_used"]:
            diag["blocks_used"].append(block)

        col = L[feat]
        for raw_name, loading in col.items():
            try:
                w = float(loading)
            except Exception:
                continue
            if not np.isfinite(w) or w == 0.0:
                continue
            expanded[str(raw_name)] = expanded.get(str(raw_name), 0.0) + float(beta) * w

    return expanded, diag


def _load_step4_summary(iso: str) -> pd.DataFrame:
    path = FEATURE_CONTRIB_DIR / f"feature_contributions_{iso}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing Step 4 summary: {path}")
    df = pd.read_csv(path)
    for col in ["iso", "target", "coefficients"]:
        if col not in df.columns:
            raise ValueError(f"Step 4 summary missing required column '{col}' in {path}")
    return df


def _read_step10_2_config(macro_cfg: Dict[str, Any]) -> Dict[str, Any]:
    block = macro_cfg.get("step10_2")
    if not isinstance(block, dict):
        raise SystemExit(
            "Macro config is missing required 'step10_2' section. "
            "Add step10_2.use_variables and step10_2.macro_to_target_by_iso."
        )
    use_vars = block.get("use_variables")
    if not isinstance(use_vars, list) or not use_vars:
        raise SystemExit("step10_2.use_variables must be a non-empty list")
    mapping = block.get("macro_to_target_by_iso")
    if not isinstance(mapping, dict) or not mapping:
        raise SystemExit("step10_2.macro_to_target_by_iso must be a non-empty mapping")
    min_test_r2 = float(block.get("min_test_r2", 0.0))
    use_by_iso = block.get("use_variables_by_iso")
    if use_by_iso is not None and not isinstance(use_by_iso, dict):
        raise SystemExit("step10_2.use_variables_by_iso must be a mapping if provided")

    use_vars_clean = [str(v) for v in use_vars]
    removed = [v for v in use_vars_clean if v in _DISALLOWED_INVERSION_VARIABLES]
    use_vars_clean = [v for v in use_vars_clean if v not in _DISALLOWED_INVERSION_VARIABLES]
    if not use_vars_clean:
        raise SystemExit(
            "step10_2.use_variables is empty after applying inversion guardrails. "
            f"Removed disallowed variables: {sorted(removed)}"
        )

    use_by_iso_clean: Dict[str, List[str]] = {}
    if isinstance(use_by_iso, dict):
        for iso, raw in use_by_iso.items():
            if not iso or not isinstance(raw, list):
                continue
            cleaned = [str(v) for v in raw if str(v) and str(v) not in _DISALLOWED_INVERSION_VARIABLES]
            if cleaned:
                use_by_iso_clean[str(iso).upper()] = cleaned
    return {
        "use_variables": use_vars_clean,
        "use_variables_by_iso": use_by_iso_clean,
        "macro_to_target_by_iso": mapping,
        "min_test_r2": min_test_r2,
        "removed_by_guardrails": removed,
    }


def _convert_macro_delta_to_target_units(delta: float, *, macro_units: str, target_transform: str) -> tuple[float, Optional[str]]:
    """Convert narrative delta into Step 4 target-space delta units.

    Macro template conventions:
      - pp: additive percentage points
      - bp: additive basis points
      - %: multiplicative shock on an index baseline

    Step 4 target transforms (from feature_contributions_*.csv):
      - level
      - yoy_log_pct (≈ YoY percent)
    """

    if not np.isfinite(delta):
        return float("nan"), "non_finite_delta"

    u = str(macro_units or "").strip().lower()
    tt = str(target_transform or "").strip().lower()

    if u in {"pp", "percentage_point", "percentage_points"}:
        return float(delta), None
    if u == "bp":
        # 1bp = 0.01 percentage points
        return float(delta) / 100.0, None
    if u == "%":
        # Cannot convert an index multiplicative shock into an additive delta without a baseline.
        return float(delta), f"units_percent_not_additive_for_target_transform={tt}"

    return float(delta), f"unknown_macro_units={macro_units!r}"


def _compute_sigma_t0_from_dt(dt_csv: Path, *, factors: List[str], t0: str) -> Dict[str, float]:
    df = pd.read_csv(dt_csv, index_col=0)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    t0_ts = pd.to_datetime(t0)
    df = df.loc[df.index <= t0_ts]
    if df.empty:
        return {}
    row = df.iloc[-1]
    out: Dict[str, float] = {}
    for f in factors:
        if f in row.index:
            try:
                val = float(row[f])
            except Exception:
                continue
            if np.isfinite(val) and val > 0:
                out[f] = val
    return out


def _load_residual_series(resid_csv: Path, *, factor: str) -> pd.Series:
    """Load one standardized residual column (daily) from disk."""

    if not resid_csv.exists():
        raise FileNotFoundError(str(resid_csv))

    try:
        df = pd.read_csv(resid_csv, usecols=["date", factor], parse_dates=["date"])
        df = df.set_index("date")
    except ValueError:
        df = pd.read_csv(resid_csv, index_col=0)
        df.index = pd.to_datetime(df.index, errors="coerce")
        if factor not in df.columns:
            raise KeyError(f"Factor {factor!r} not found in {resid_csv}")

    s = pd.to_numeric(df[factor], errors="coerce").dropna()
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

    Uses frozen residuals if present in the Step 9 manifest; otherwise falls back to
    analysis_outputs/diag_corr_daily/{ISO}_standardized_residuals_daily.csv.
    """

    t0_ts = pd.to_datetime(t0)
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
        s = s.loc[(s.index >= start_ts) & (s.index <= t0_ts)]
        if s.empty:
            continue
        s = s.tail(int(window_days))
        if s.empty:
            continue

        samples.append(s.to_numpy(dtype=float))
        isos_used.append(str(iso).upper())
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


def _load_residual_pool(
    *,
    iso_inputs: Dict[str, Any],
    factor: str,
    t0: str,
    window_days: int,
) -> tuple[np.ndarray, Dict[str, Any]]:
    """Load and pool standardized residuals across ISOs for a factor.

    Returns (vals, meta) where vals is a 1D array of finite residuals.
    """

    t0_ts = pd.to_datetime(t0)
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
        s = s.loc[(s.index >= start_ts) & (s.index <= t0_ts)]
        if s.empty:
            continue
        s = s.tail(int(window_days))
        if s.empty:
            continue

        samples.append(s.to_numpy(dtype=float))
        isos_used.append(str(iso).upper())
        try:
            files_used.append(str(resid_path.relative_to(PROJECT_ROOT)))
        except Exception:
            files_used.append(str(resid_path))

    meta: Dict[str, Any] = {
        "factor": factor,
        "t0": str(t0_ts.date()),
        "window_days": int(window_days),
        "isos_used": isos_used,
        "files_used": files_used,
    }

    if not samples:
        meta["status"] = "missing_residuals"
        return np.array([], dtype=float), meta

    vals = np.concatenate(samples)
    vals = vals[np.isfinite(vals)]
    meta["n_obs"] = int(vals.size)
    meta["status"] = "ok" if vals.size >= 50 else "too_few_samples"
    return vals, meta


def _empirical_percentile(vals: np.ndarray, z: float) -> Optional[float]:
    """Return empirical CDF percentile P(X <= z) in [0,1]."""

    if vals.size < 50 or not np.isfinite(z):
        return None
    return float(np.mean(vals <= float(z)))


def _scenario_ladder(macro_cfg: Dict[str, Any]) -> Dict[str, float]:
    defaults = macro_cfg.get("defaults")
    if not isinstance(defaults, dict):
        return {}
    ladder = defaults.get("scenario_ladder")
    if not isinstance(ladder, dict):
        return {}
    out: Dict[str, float] = {}
    for k, v in ladder.items():
        try:
            out[str(k)] = float(v)
        except Exception:
            continue
    return out


def _ridge_solve(B: np.ndarray, y: np.ndarray, lam: float) -> Tuple[np.ndarray, Dict[str, Any]]:
    """Solve min ||B x - y||^2 + lam ||x||^2."""

    BtB = B.T @ B
    n = BtB.shape[0]
    A = BtB + lam * np.eye(n)
    b = B.T @ y
    x = np.linalg.solve(A, b)

    resid = B @ x - y
    diag = {
        "lam": float(lam),
        "resid_l2": float(np.linalg.norm(resid)),
        "y_l2": float(np.linalg.norm(y)),
        "rel_resid": float(np.linalg.norm(resid) / (np.linalg.norm(y) + 1e-12)),
        "cond_B": float(np.linalg.cond(B)) if B.size else None,
        "cond_A": float(np.linalg.cond(A)) if A.size else None,
    }
    return x, diag


def _build_shaped_macro_path(*, peak: float, severity: float, shape: str, horizon_quarters: int) -> np.ndarray:
    """Generate a quarterly delta path using the same shape semantics as Step 10.1."""

    q = int(horizon_quarters)
    x = np.linspace(0.0, 1.0, q)
    if shape == "hump_trough_recover":
        trough = np.exp(-((x - 0.25) ** 2) / (2 * 0.12**2))
        trough = trough / trough.max()
        tail = np.linspace(1.0, 0.15, q)
        path = (peak * severity) * trough * tail
        return path.astype(float)
    if shape == "lagged_hump":
        hump = np.exp(-((x - 0.45) ** 2) / (2 * 0.16**2))
        hump = hump / hump.max()
        tail = np.linspace(1.0, 0.35, q)
        path = (peak * severity) * hump * tail
        return path.astype(float)
    if shape == "front_loaded_revert":
        k = max(1, min(3, q))
        head = np.linspace(0.6, 1.0, k)
        tail = np.exp(-np.linspace(0.0, 2.0, q - k))
        path = np.concatenate([(peak * severity) * head, (peak * severity) * tail])
        return path.astype(float)
    if shape == "front_loaded_partial_recover":
        k = max(1, min(2, q))
        head = np.linspace(0.8, 1.0, k)
        tail = 0.45 + 0.55 * np.exp(-np.linspace(0.0, 2.2, q - k))
        path = np.concatenate([(peak * severity) * head, (peak * severity) * tail])
        return path.astype(float)
    raise SystemExit(f"Unknown macro shape in YAML: {shape}")


def _auto_lam_grid(lam_min: float, lam_max: float, n: int) -> List[float]:
    n = max(3, int(n))
    a = float(lam_min)
    b = float(lam_max)
    if not np.isfinite(a) or not np.isfinite(b) or a <= 0 or b <= 0:
        return [1.0]
    lo = min(a, b)
    hi = max(a, b)
    vals = np.logspace(np.log10(lo), np.log10(hi), n)
    return [float(v) for v in vals]


def _aggregate(values: List[float], how: str) -> Optional[float]:
    v = np.array([float(x) for x in values if x is not None and np.isfinite(float(x))], dtype=float)
    if v.size == 0:
        return None
    how = str(how).strip().lower()
    if how == "max":
        return float(np.max(v))
    if how == "p95":
        return float(np.percentile(v, 95))
    if how == "median":
        return float(np.median(v))
    return float(np.percentile(v, 95))


def _evaluate_lambda_for_iso(
    *,
    lam: float,
    B: np.ndarray,
    used_targets: List[str],
    target_index: Dict[str, int],
    factor_index: Dict[str, int],
    macro_to_target: Dict[str, str],
    macro_paths_by_scen: Dict[str, Dict[str, np.ndarray]],
    horizon_quarters: int,
    target_transform_by_target: Dict[str, str],
    macro_units_by_var: Dict[str, str],
    v2x_anchor_status_by_scen: Dict[str, str],
    v2x_target_daily_z_by_scen: Dict[str, Optional[float]],
    v2x_anchor_factor: str,
    daily_scale: float,
    v2x_anchor_weight: float,
    target_weights_sqrt: Optional[np.ndarray] = None,
) -> Dict[str, Any]:
    """Return fit and shock diagnostics for a candidate lambda."""

    rel_resids: List[float] = []
    max_abs_zs: List[float] = []

    for scen_id, macro_paths in macro_paths_by_scen.items():
        v2x_anchor_status = str(v2x_anchor_status_by_scen.get(scen_id) or "disabled")
        v2x_target_daily_z = v2x_target_daily_z_by_scen.get(scen_id)
        for q in range(1, int(horizon_quarters) + 1):
            y = np.zeros((len(used_targets),), dtype=float)
            for mv, t in macro_to_target.items():
                i = target_index[t]
                if mv in macro_paths:
                    raw_delta = float(macro_paths[mv][q - 1])
                    units = str(macro_units_by_var.get(mv, "pp"))
                    tt = str(target_transform_by_target.get(t, ""))
                    delta, _ = _convert_macro_delta_to_target_units(raw_delta, macro_units=units, target_transform=tt)
                    y[i] = float(delta)

            B_base = B
            y_base = y
            if target_weights_sqrt is not None and target_weights_sqrt.size == len(used_targets):
                w = target_weights_sqrt.reshape(-1)
                B_base = B * w[:, None]
                y_base = y * w

            B_solve = B_base
            y_solve = y_base
            if v2x_anchor_status == "active":
                if v2x_target_daily_z is not None and np.isfinite(float(v2x_target_daily_z)) and v2x_anchor_factor in factor_index:
                    v2x_prior_quarter_z = float(v2x_target_daily_z) / float(daily_scale)
                    w = float(v2x_anchor_weight)
                    if np.isfinite(w) and w > 0:
                        jv = factor_index[v2x_anchor_factor]
                        extra_row = np.zeros((1, B.shape[1]), dtype=float)
                        extra_row[0, jv] = math.sqrt(w)
                        B_solve = np.vstack([B_base, extra_row])
                        y_solve = np.concatenate([y_base, np.array([math.sqrt(w) * v2x_prior_quarter_z], dtype=float)])

            x_z, _ = _ridge_solve(B_solve, y_solve, lam=float(lam))
            resid = B_base @ x_z - y_base
            rel_resids.append(float(np.linalg.norm(resid) / (np.linalg.norm(y) + 1e-12)))
            if x_z.size:
                max_abs_zs.append(float(np.max(np.abs(x_z))))

    return {
        "lam": float(lam),
        "fit_rel_resid_p95": _aggregate(rel_resids, "p95"),
        "fit_rel_resid_max": _aggregate(rel_resids, "max"),
        "fit_rel_resid_median": _aggregate(rel_resids, "median"),
        "shock_max_abs_z": _aggregate(max_abs_zs, "max"),
        "shock_p95_abs_z": _aggregate(max_abs_zs, "p95"),
    }


def _select_lambda(
    *,
    method: str,
    grid: List[float],
    fit_eps: float,
    fit_agg: str,
    z_cap: float,
    alpha: float,
    eval_fn,
) -> Tuple[float, Dict[str, Any], List[Dict[str, Any]]]:
    """Select lambda via constrained-fit first or scalar objective."""

    method = str(method).strip().lower()
    fit_agg = str(fit_agg).strip().lower()
    grid_sorted = sorted({float(x) for x in grid if np.isfinite(float(x)) and float(x) > 0}, reverse=True)
    if not grid_sorted:
        grid_sorted = [1.0]

    grid_rows: List[Dict[str, Any]] = []
    for lam in grid_sorted:
        m = dict(eval_fn(lam))
        fit_val = m.get("fit_rel_resid_p95")
        if fit_agg == "max":
            fit_val = m.get("fit_rel_resid_max")
        elif fit_agg == "median":
            fit_val = m.get("fit_rel_resid_median")
        m["fit_metric"] = fit_agg
        m["fit_value"] = fit_val

        shock = m.get("shock_max_abs_z")
        zcap_ok = True
        if z_cap is not None and np.isfinite(float(z_cap)) and float(z_cap) > 0:
            if shock is None or not np.isfinite(float(shock)):
                zcap_ok = False
            else:
                zcap_ok = float(shock) <= float(z_cap)
        m["zcap_ok"] = bool(zcap_ok)

        # scalar objective (always computed for audit)
        penalty = 0.0
        if shock is not None and np.isfinite(float(shock)) and z_cap and np.isfinite(float(z_cap)) and float(z_cap) > 0:
            penalty = max(0.0, float(shock) / float(z_cap) - 1.0)
        m["scalar_objective"] = (float(fit_val) if fit_val is not None and np.isfinite(float(fit_val)) else 1e9) + float(alpha) * float(penalty) ** 2
        m["penalty_over_zcap"] = float(penalty)
        grid_rows.append(m)

    # Constrained-fit: choose the LARGEST lambda that satisfies fit<=eps (shrinks shocks as much as possible).
    if method == "constrained":
        for m in grid_rows:
            fv = m.get("fit_value")
            if fv is None or not np.isfinite(float(fv)):
                continue
            if float(fv) <= float(fit_eps) and bool(m.get("zcap_ok", True)):
                chosen = float(m["lam"])
                meta = {"method": "constrained", "fit_eps": float(fit_eps), "fit_agg": fit_agg, "z_cap": float(z_cap), "alpha": float(alpha)}
                return chosen, {**meta, "chosen": m}, grid_rows

        # No feasible lambda under BOTH constraints.
        # If any lambda meets z-cap, pick the best-fit among those (even if fit>eps).
        zcap_rows = [r for r in grid_rows if bool(r.get("zcap_ok", False)) and r.get("fit_value") is not None and np.isfinite(float(r.get("fit_value")))]
        if zcap_rows:
            best_fit_within_zcap = min(zcap_rows, key=lambda r: float(r["fit_value"]))
            chosen = float(best_fit_within_zcap["lam"])
            meta = {
                "method": "constrained",
                "fit_eps": float(fit_eps),
                "fit_agg": fit_agg,
                "z_cap": float(z_cap),
                "alpha": float(alpha),
                "note": "no_lambda_met_fit_eps_within_zcap; chose_best_fit_within_zcap",
            }
            return chosen, {**meta, "chosen": best_fit_within_zcap}, grid_rows

        # Otherwise: no z-cap-feasible lambda exists; fall back to best fit overall.
        best_fit = min(
            (r for r in grid_rows if r.get("fit_value") is not None and np.isfinite(float(r.get("fit_value")))),
            key=lambda r: float(r["fit_value"]),
            default=grid_rows[-1],
        )
        chosen = float(best_fit["lam"])
        meta = {
            "method": "constrained",
            "fit_eps": float(fit_eps),
            "fit_agg": fit_agg,
            "z_cap": float(z_cap),
            "alpha": float(alpha),
            "note": "no_lambda_met_zcap_constraint; fell_back_to_best_fit",
        }
        return chosen, {**meta, "chosen": best_fit}, grid_rows

    # Scalar objective
    candidates = grid_rows
    if z_cap is not None and np.isfinite(float(z_cap)) and float(z_cap) > 0:
        zcap_ok = [r for r in grid_rows if bool(r.get("zcap_ok", False))]
        if zcap_ok:
            candidates = zcap_ok
    best = min(candidates, key=lambda r: float(r.get("scalar_objective") or 1e18))
    chosen = float(best["lam"])
    meta = {"method": "scalar", "fit_eps": float(fit_eps), "fit_agg": fit_agg, "z_cap": float(z_cap), "alpha": float(alpha)}
    return chosen, {**meta, "chosen": best}, grid_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 10.2 — Create factor shocks implied by macro narrative")
    parser.add_argument("--run-id", default=None, help="Scenario run id created by Step 9")
    parser.add_argument(
        "--macro-config",
        required=True,
        help="Macro template YAML/JSON (e.g., SRESS TEST PIPELINE/scenario_macro_templates.yaml)",
    )
    parser.add_argument("--scenario", default=None, help="Scenario id to run (default: all scenarios in YAML)")
    parser.add_argument(
        "--days-per-quarter",
        type=int,
        default=63,
        help="Trading-day approximation used to expand quarterly shocks to daily (default: 63)",
    )
    parser.add_argument(
        "--lam",
        type=float,
        default=1.0,
        help="Ridge regularization lambda for inverting Step 4 mapping (default: 1.0)",
    )
    parser.add_argument(
        "--auto-lam",
        choices=["off", "constrained", "scalar"],
        default="off",
        help=(
            "Automatically select ridge lambda by grid-search. "
            "'constrained' chooses the largest lambda with rel_resid <= --auto-lam-fit-eps; "
            "'scalar' minimizes fit + alpha*max(0, z/z_cap-1)^2. Default: off."
        ),
    )
    parser.add_argument("--auto-lam-lam-min", type=float, default=1e-8, help="Auto-lam grid min lambda (default: 1e-8)")
    parser.add_argument("--auto-lam-lam-max", type=float, default=1.0, help="Auto-lam grid max lambda (default: 1.0)")
    parser.add_argument("--auto-lam-lam-n", type=int, default=17, help="Auto-lam grid size (default: 17)")
    parser.add_argument(
        "--auto-lam-fit-eps",
        type=float,
        default=0.02,
        help="Constrained-fit tolerance on relative residual ||Bx-y||/||y|| (default: 0.02 = 2%)",
    )
    parser.add_argument(
        "--auto-lam-fit-agg",
        choices=["p95", "max", "median"],
        default="p95",
        help="How to aggregate relative residuals across quarters/scenarios when selecting lambda (default: p95)",
    )
    parser.add_argument(
        "--auto-lam-z-cap",
        type=float,
        default=6.0,
        help="Soft cap for max |factor z| used in scalar objective diagnostics (default: 6.0)",
    )
    parser.add_argument(
        "--auto-lam-alpha",
        type=float,
        default=5.0,
        help="Weight on shock-penalty term in scalar objective (default: 5.0)",
    )
    parser.add_argument(
        "--auto-lam-constrained-fallback",
        choices=["best_fit", "scalar"],
        default="scalar",
        help=(
            "If --auto-lam=constrained and no lambda meets the fit constraint, choose either "
            "the best-fit lambda or fall back to the scalar objective (default: scalar)."
        ),
    )
    parser.add_argument(
        "--no-auto-lam",
        action="store_true",
        help=(
            "Disable auto-reduction of --lam in --imf-mode=macro_first. "
            "By default macro_first caps lambda to improve macro narrative fit."
        ),
    )
    parser.add_argument(
        "--imf-mode",
        choices=["hybrid", "macro_first"],
        default="hybrid",
        help=(
            "Inversion mode. 'hybrid' (default) allows optional V2X ladder anchoring; "
            "'macro_first' fits macro targets as well as possible and reports implied V2X percentile as a diagnostic."
        ),
    )
    parser.add_argument(
        "--tag",
        default="",
        help=(
            "Optional output tag to avoid overwriting. When set, outputs become factor_shocks_from_macro_<tag>.csv, "
            "..._diagnostics_<tag>.json, ..._mapping_<tag>.csv, ..._status_<tag>.csv, and macro_narrative_paths_used_for_inversion_<tag>.csv."
        ),
    )
    parser.add_argument(
        "--min-test-r2",
        type=float,
        default=None,
        help="Override YAML step10_2.min_test_r2; refuse inversion below this (default: use YAML)",
    )
    parser.add_argument(
        "--dailyize",
        choices=["sqrt", "none"],
        default="sqrt",
        help="Convert quarter shock magnitude to daily innovation level: divide by sqrt(days) or keep constant",
    )
    parser.add_argument(
        "--anchor-v2x-to-ladder",
        action="store_true",
        help=(
            "If the macro config defines defaults.scenario_ladder and scenarios have ladder_level, "
            "rescale the inferred factor shocks so the V2X daily z-peak matches the ladder percentile. "
            "Ignored when --imf-mode=macro_first (severity is reported, not forced)."
        ),
    )
    parser.add_argument(
        "--v2x-anchor-factor",
        default="V2X",
        help="Factor used for ladder anchoring (default: V2X)",
    )
    parser.add_argument(
        "--v2x-anchor-weight",
        type=float,
        default=50.0,
        help=(
            "Strength of the soft constraint that nudges the inferred factor shocks to match the ladder-implied "
            "V2X daily z (higher enforces more strongly; default: 50.0)."
        ),
    )

    # Step 10.2 inversion target governance
    parser.add_argument(
        "--target-weighting",
        choices=["on", "off"],
        default="on",
        help=(
            "Weight macro inversion targets by Step4 OOS reliability and 'non-lag share'. "
            "When enabled, targets dominated by AR-lags are down-weighted or dropped (default: on)."
        ),
    )
    parser.add_argument(
        "--target-min-nonlag-share",
        type=float,
        default=0.01,
        help=(
            "Drop an inversion target if its mapping has too little overlap with allowed (non-lag) inversion factors. "
            "Uses allowed_share_total = ||coef_on_allowed_factors|| / ||all_coefs|| (default: 0.01)."
        ),
    )
    parser.add_argument(
        "--target-weight-floor",
        type=float,
        default=0.05,
        help="Minimum positive weight applied to retained targets (default: 0.05).",
    )
    parser.add_argument(
        "--target-weight-r2-power",
        type=float,
        default=1.0,
        help="Exponent applied to max(test_r2,0) when computing weights (default: 1.0).",
    )
    parser.add_argument(
        "--target-weight-nonlag-power",
        type=float,
        default=1.0,
        help="Exponent applied to nonlag_share when computing weights (default: 1.0).",
    )
    args = parser.parse_args()

    run_id = _infer_run_id(args.run_id)
    run_dir = SCENARIOS_DIR / run_id
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing Step 9 manifest: {manifest_path}")

    manifest = _read_json(manifest_path)
    iso_inputs: Dict[str, Any] = manifest.get("iso_inputs") or {}
    if not iso_inputs:
        raise SystemExit("Manifest has no iso_inputs")

    config = manifest.get("config") or {}
    t0 = str(config.get("t0") or "")
    if not t0:
        first_iso = sorted(iso_inputs.keys())[0]
        t0 = str((iso_inputs.get(first_iso) or {}).get("asof_t0") or (iso_inputs.get(first_iso) or {}).get("frozen_end"))
    if not t0:
        raise SystemExit("Unable to infer t0 from manifest")
    window_days = int(config.get("window_days") or 756)

    cfg_path = (PROJECT_ROOT / args.macro_config) if not Path(args.macro_config).is_absolute() else Path(args.macro_config)
    macro_cfg = _read_yaml_or_json(cfg_path)

    ladder = _scenario_ladder(macro_cfg)
    v2x_anchor_factor = str(args.v2x_anchor_factor)
    ladder_calibration: Dict[str, Any] = {"ladder": ladder, "factor": v2x_anchor_factor, "targets": {}}

    imf_mode = str(args.imf_mode)
    anchor_enabled = bool(args.anchor_v2x_to_ladder) and imf_mode != "macro_first"

    lam_input = float(args.lam)
    lam_default = float(lam_input)
    legacy_cap_applied = False
    if str(args.auto_lam) == "off" and imf_mode == "macro_first" and not bool(args.no_auto_lam):
        # Legacy behavior: cap lambda to avoid near-zero implied deltas.
        lam_cap = 1e-6
        if np.isfinite(lam_default) and lam_default > lam_cap:
            lam_default = float(lam_cap)
            legacy_cap_applied = True

    # Residual pool for implied severity diagnostics (used for anchoring targets and/or reporting percentiles).
    residual_pool_vals, residual_pool_meta = _load_residual_pool(
        iso_inputs=iso_inputs,
        factor=v2x_anchor_factor,
        t0=t0,
        window_days=window_days,
    )

    if anchor_enabled and ladder:
        for level, q in ladder.items():
            z, meta = _calibrate_factor_z_from_residuals(
                iso_inputs=iso_inputs,
                factor=v2x_anchor_factor,
                t0=t0,
                window_days=window_days,
                quantile=float(q),
            )
            ladder_calibration["targets"][level] = meta

    defaults = dict(macro_cfg.get("defaults") or {})
    horizon_quarters = int(defaults.get("horizon_quarters") or 12)

    scenario_defs = list(macro_cfg.get("scenarios") or [])
    if not scenario_defs:
        raise SystemExit("Macro config has no scenarios")

    if args.scenario:
        scenario_defs = [s for s in scenario_defs if isinstance(s, dict) and s.get("scenario_id") == args.scenario]
        if not scenario_defs:
            raise SystemExit(f"Scenario id not found in macro config: {args.scenario}")

    variables = list(macro_cfg.get("variables") or [])
    if not variables:
        raise SystemExit("Macro config has no variables")

    macro_units_by_var: Dict[str, str] = {}
    for v in variables:
        if not isinstance(v, dict) or not v.get("variable"):
            continue
        macro_units_by_var[str(v.get("variable"))] = str(v.get("units") or "pp")

    step10_2_cfg = _read_step10_2_config(macro_cfg)
    macro_vars_all = [str(v.get("variable")) for v in variables if isinstance(v, dict) and v.get("variable")]
    macro_vars_used = list(step10_2_cfg["use_variables"])
    min_test_r2 = float(step10_2_cfg["min_test_r2"]) if args.min_test_r2 is None else float(args.min_test_r2)

    out_dir = _ensure_dir(run_dir / "deterministic")

    status_rows: List[Dict[str, Any]] = []

    rows: List[Dict[str, Any]] = []
    mapping_rows: List[Dict[str, Any]] = []
    auto_lam_grid_rows_all: List[Dict[str, Any]] = []
    auto_lam_selection_rows: List[Dict[str, Any]] = []
    diagnostics: Dict[str, Any] = {
        "run_id": run_id,
        "macro_config": str(cfg_path),
        "lam_input": float(lam_input),
        "lam_default": float(lam_default),
        "legacy_cap_applied": bool(legacy_cap_applied),
        "auto_lam": {
            "mode": str(args.auto_lam),
            "lam_min": float(args.auto_lam_lam_min),
            "lam_max": float(args.auto_lam_lam_max),
            "lam_n": int(args.auto_lam_lam_n),
            "fit_eps": float(args.auto_lam_fit_eps),
            "fit_agg": str(args.auto_lam_fit_agg),
            "z_cap": float(args.auto_lam_z_cap),
            "alpha": float(args.auto_lam_alpha),
        },
        "imf_mode": imf_mode,
        "tag": str(args.tag or ""),
        "min_test_r2": float(min_test_r2),
        "days_per_quarter": int(args.days_per_quarter),
        "dailyize": str(args.dailyize),
        "macro_vars_all": macro_vars_all,
        "macro_vars_default_for_inversion": macro_vars_used,
        "macro_units_by_var": macro_units_by_var,
        "removed_by_guardrails": step10_2_cfg.get("removed_by_guardrails") or [],
        "manifest_t0": t0,
        "manifest_window_days": int(window_days),
        "residual_pool": {"status": residual_pool_meta.get("status"), **residual_pool_meta},
        "v2x_ladder_anchor": {
            "enabled": bool(anchor_enabled),
            "anchor_factor": v2x_anchor_factor,
            **ladder_calibration,
        },
        "iso": {},
    }

    for iso, iso_meta in iso_inputs.items():
        iso_u = str(iso).upper()
        factors: List[str] = list(iso_meta.get("factors") or [])
        if not factors:
            continue

        asof_t0 = iso_meta.get("asof_t0") or iso_meta.get("frozen_end")
        frozen_files = iso_meta.get("frozen_files") or {}
        dt_frozen = (frozen_files.get("dt") or {}).get("frozen")

        sigma_t0: Dict[str, float] = {}
        scaling_method = "z_only"
        if isinstance(dt_frozen, str) and dt_frozen:
            dt_path = PROJECT_ROOT / dt_frozen
            if dt_path.exists() and asof_t0:
                sigma_t0 = _compute_sigma_t0_from_dt(dt_path, factors=factors, t0=str(asof_t0))
                if sigma_t0:
                    scaling_method = "Dt_t0"

        # Load Step 4 summary and build coefficient matrix
        step4 = _load_step4_summary(iso_u)
        step4_targets = sorted(set(step4["target"].astype(str).tolist()))

        # Select variables for this ISO (default list overridden by YAML per-ISO config)
        use_by_iso = step10_2_cfg.get("use_variables_by_iso") or {}
        iso_use_vars = macro_vars_used
        if isinstance(use_by_iso, dict) and iso_u in use_by_iso:
            raw = use_by_iso.get(iso_u)
            if not isinstance(raw, list) or not raw:
                raise SystemExit(f"step10_2.use_variables_by_iso[{iso_u}] must be a non-empty list")
            iso_use_vars = [str(v) for v in raw]

        # Guardrail: never invert on disallowed variables even if present in per-ISO overrides.
        iso_use_vars = [v for v in iso_use_vars if v not in _DISALLOWED_INVERSION_VARIABLES]

        # Strict explicit mapping
        map_by_iso = step10_2_cfg["macro_to_target_by_iso"]
        iso_mapping = map_by_iso.get(iso_u) if isinstance(map_by_iso, dict) else None
        if not isinstance(iso_mapping, dict):
            iso_diag = {
                "asof_t0": asof_t0,
                "n_factors": len(factors),
                "scaling_method": scaling_method,
                "error": f"No step10_2.macro_to_target_by_iso mapping for ISO={iso_u}",
            }
            diagnostics["iso"][iso_u] = iso_diag
            # Mark all scenarios unsolved for this ISO
            for scen in scenario_defs:
                status_rows.append({"run_id": run_id, "iso": iso_u, "scenario_id": str(scen.get("scenario_id")), "status": "unsolved", "reason": "missing_iso_mapping"})
            continue

        missing_vars = [mv for mv in iso_use_vars if mv not in iso_mapping]
        if missing_vars:
            iso_diag = {
                "asof_t0": asof_t0,
                "n_factors": len(factors),
                "scaling_method": scaling_method,
                "error": f"Missing explicit macro->target mappings for variables: {missing_vars}",
                "macro_vars_used": iso_use_vars,
            }
            diagnostics["iso"][iso_u] = iso_diag
            for scen in scenario_defs:
                status_rows.append({"run_id": run_id, "iso": iso_u, "scenario_id": str(scen.get("scenario_id")), "status": "unsolved", "reason": "missing_variable_mapping"})
            continue

        macro_to_target = {mv: str(iso_mapping[mv]) for mv in iso_use_vars}
        match_diag = {
            "macro_vars_used": iso_use_vars,
            "matched": macro_to_target,
            "unmatched": [mv for mv in macro_vars_all if mv not in macro_to_target],
            "note": "Explicit mapping from YAML step10_2.macro_to_target_by_iso.",
        }

        iso_diag: Dict[str, Any] = {
            "asof_t0": asof_t0,
            "n_factors": len(factors),
            "scaling_method": scaling_method,
            "sigma_t0": sigma_t0,
            "matched_targets": match_diag,
            "scenarios": {},
        }

        # Build matrix B over matched targets only (rows = targets, cols = factors)
        used_targets = sorted(set(macro_to_target.values()))
        if not used_targets:
            iso_diag["error"] = "No macro variables could be matched to Step 4 targets"
            diagnostics["iso"][iso_u] = iso_diag
            for scen in scenario_defs:
                status_rows.append({"run_id": run_id, "iso": iso_u, "scenario_id": str(scen.get("scenario_id")), "status": "unsolved", "reason": "no_targets"})
            continue

        # Build per-target coefficient dict
        loadings_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}
        pc_expand_diag_by_target: Dict[str, Any] = {}
        target_quality: Dict[str, Any] = {}
        target_transform_by_target: Dict[str, str] = {}
        coef_by_target: Dict[str, Dict[str, float]] = {}
        for t in used_targets:
            sub = step4.loc[step4["target"].astype(str) == t]
            if sub.empty:
                continue
            # Pick the first row (there should be one per target)
            row0 = sub.iloc[0]
            target_transform_by_target[t] = str(row0.get("target_transform") or "")
            target_quality[t] = {
                "train_r2": None if "train_r2" not in sub.columns else float(row0.get("train_r2")) if pd.notna(row0.get("train_r2")) else None,
                "test_r2": None if "test_r2" not in sub.columns else float(row0.get("test_r2")) if pd.notna(row0.get("test_r2")) else None,
                "negative_test_r2": None
                if "negative_test_r2" not in sub.columns
                else bool(row0.get("negative_test_r2"))
                if pd.notna(row0.get("negative_test_r2"))
                else None,
                "alpha": None if "alpha" not in sub.columns else float(row0.get("alpha")) if pd.notna(row0.get("alpha")) else None,
                "model_path": None if "model_path" not in sub.columns else (str(row0.get("model_path")) if pd.notna(row0.get("model_path")) else None),
            }

            raw_coef = _parse_coefficients_blob(row0.get("coefficients"))
            expanded_coef, pc_diag = _expand_pc_coefficients(
                iso=iso_u,
                coef_map=raw_coef,
                loadings_cache=loadings_cache,
            )
            coef_by_target[t] = expanded_coef
            pc_expand_diag_by_target[t] = pc_diag

        iso_diag["pc_expansion"] = pc_expand_diag_by_target
        iso_diag["target_quality"] = target_quality
        iso_diag["target_transform"] = target_transform_by_target

        # Flag obviously-weak Step 4 mappings (macro targets often have poor OOS fit).
        warnings: List[str] = []
        for t, q in target_quality.items():
            tr2 = q.get("test_r2")
            if tr2 is not None and isinstance(tr2, (int, float)) and tr2 < 0:
                warnings.append(f"Step4 target {t} has negative test_r2={tr2:.4f}; macro inversion may be unstable")
        if warnings:
            iso_diag["warnings"] = warnings

        # Hard gate: refuse inversion if any required target has missing/low test_r2.
        bad_targets: List[Dict[str, Any]] = []
        for t in used_targets:
            q = target_quality.get(t) or {}
            tr2 = q.get("test_r2")
            if tr2 is None:
                bad_targets.append({"target": t, "reason": "missing_test_r2"})
            elif isinstance(tr2, (int, float)) and float(tr2) < float(min_test_r2):
                bad_targets.append({"target": t, "test_r2": float(tr2), "reason": "test_r2_below_threshold"})
        if bad_targets:
            iso_diag["unsolved_reason"] = {
                "type": "insufficient_step4_quality",
                "min_test_r2": float(min_test_r2),
                "bad_targets": bad_targets,
            }
            diagnostics["iso"][iso_u] = iso_diag
            for scen in scenario_defs:
                status_rows.append({
                    "run_id": run_id,
                    "iso": iso_u,
                    "scenario_id": str(scen.get("scenario_id")),
                    "status": "unsolved",
                    "reason": "step4_test_r2_below_threshold",
                })
            continue

        # Restrict factor universe to intersection between manifest factors and coefficients seen
        coef_features = sorted({f for d in coef_by_target.values() for f in d.keys()})
        coef_features_set = set(coef_features)
        factor_universe_raw = [f for f in factors if f in coef_features_set]

        dropped: List[Dict[str, Any]] = []
        factor_universe: List[str] = []
        drop_counts: Dict[str, int] = {}
        for f in factor_universe_raw:
            reason = _inversion_factor_exclusion_reason(f)
            if reason is None:
                factor_universe.append(f)
            else:
                dropped.append({"factor": f, "reason": reason})
                drop_counts[reason] = int(drop_counts.get(reason, 0)) + 1

        iso_diag["inversion_factor_filter"] = {
            "n_before": int(len(factor_universe_raw)),
            "n_after": int(len(factor_universe)),
            "drop_counts": drop_counts,
            "dropped_sample": dropped[:50],
        }

        if not factor_universe:
            iso_diag["error"] = "No allowed inversion factors after canonical shock-space filtering (lags/structural removed)"
            iso_diag["coef_features_sample"] = coef_features[:25]
            diagnostics["iso"][iso_u] = iso_diag
            for scen in scenario_defs:
                status_rows.append(
                    {
                        "run_id": run_id,
                        "iso": iso_u,
                        "scenario_id": str(scen.get("scenario_id")),
                        "status": "unsolved",
                        "reason": "no_allowed_inversion_factors",
                    }
                )
            continue

        # Build a stable set of allowed factors for computing nonlag_share.
        allowed_factor_set = set(factor_universe)

        # Target weighting / gating: down-weight or drop inversion targets dominated by AR lags.
        target_weighting_enabled = str(args.target_weighting).strip().lower() == "on"
        target_weights: Dict[str, Any] = {
            "enabled": bool(target_weighting_enabled),
            "min_nonlag_share": float(args.target_min_nonlag_share),
            "weight_floor": float(args.target_weight_floor),
            "r2_power": float(args.target_weight_r2_power),
            "nonlag_power": float(args.target_weight_nonlag_power),
            "targets": {},
        }

        weights_vec: List[float] = []
        kept_targets: List[str] = []
        dropped_targets: List[Dict[str, Any]] = []
        for t in used_targets:
            q = target_quality.get(t) or {}
            tr2 = q.get("test_r2")
            tr2_val = float(tr2) if tr2 is not None and np.isfinite(float(tr2)) else float("nan")
            tr2_pos = max(0.0, tr2_val) if np.isfinite(tr2_val) else 0.0

            coef_map = coef_by_target.get(t) or {}
            nl = _compute_nonlag_share(target=t, coef_map=coef_map, allowed_factor_set=allowed_factor_set)
            allowed_share_total = float(nl.get("allowed_share_total") or 0.0)
            ar_lag_share_total = float(nl.get("ar_lag_share_total") or 0.0)

            w = 1.0
            drop_reason = None
            if target_weighting_enabled:
                if allowed_share_total < float(args.target_min_nonlag_share):
                    w = 0.0
                    drop_reason = "allowed_share_total_below_threshold"
                else:
                    # If a mapping is mostly AR lags on the target itself, it is not a robust inversion constraint.
                    ar_strength = max(0.0, 1.0 - float(ar_lag_share_total))
                    w = (tr2_pos ** float(args.target_weight_r2_power)) * (ar_strength ** float(args.target_weight_nonlag_power))
                    if w > 0:
                        w = max(float(args.target_weight_floor), float(w))
                    w = float(min(1.0, max(0.0, w)))

            target_weights["targets"][t] = {
                "test_r2": None if not np.isfinite(tr2_val) else float(tr2_val),
                "test_r2_pos": float(tr2_pos),
                "allowed_share_total": float(allowed_share_total),
                "ar_lag_share_total": float(ar_lag_share_total),
                "weight": float(w),
                "drop_reason": drop_reason,
                **nl,
            }

            if w <= 0.0:
                dropped_targets.append({"target": t, "reason": drop_reason or "weight_zero", "test_r2": tr2, "allowed_share_total": allowed_share_total, "ar_lag_share_total": ar_lag_share_total})
            else:
                kept_targets.append(t)
                weights_vec.append(float(w))

        iso_diag["inversion_target_weights"] = target_weights
        if dropped_targets:
            iso_diag["inversion_targets_dropped"] = dropped_targets

        if target_weighting_enabled:
            used_targets = kept_targets
            if not used_targets:
                iso_diag["unsolved_reason"] = {
                    "type": "no_inversion_targets_after_weighting",
                    "dropped_targets": dropped_targets,
                }
                diagnostics["iso"][iso_u] = iso_diag
                for scen in scenario_defs:
                    status_rows.append({
                        "run_id": run_id,
                        "iso": iso_u,
                        "scenario_id": str(scen.get("scenario_id")),
                        "status": "unsolved",
                        "reason": "no_inversion_targets_after_weighting",
                    })
                continue
        if not factor_universe:
            iso_diag["error"] = "No overlap between manifest factors and Step 4 coefficient features"
            iso_diag["coef_features_sample"] = coef_features[:25]
            diagnostics["iso"][iso_u] = iso_diag
            for scen in scenario_defs:
                status_rows.append({
                    "run_id": run_id,
                    "iso": iso_u,
                    "scenario_id": str(scen.get("scenario_id")),
                    "status": "unsolved",
                    "reason": "no_factor_overlap_step4_vs_manifest",
                })
            continue

        target_index = {t: i for i, t in enumerate(used_targets)}
        factor_index = {f: j for j, f in enumerate(factor_universe)}

        B = np.zeros((len(used_targets), len(factor_universe)), dtype=float)
        for t, coef_map in coef_by_target.items():
            if t not in target_index:
                continue
            i = target_index[t]
            for f, c in coef_map.items():
                j = factor_index.get(f)
                if j is None:
                    continue
                B[i, j] = float(c)

        target_weights_sqrt = None
        if target_weighting_enabled:
            # Align weights to used_targets order
            w = np.array(weights_vec, dtype=float)
            w = np.clip(w, 0.0, 1.0)
            # Use sqrt for weighted least squares
            target_weights_sqrt = np.sqrt(w)

        # Persist matrix rows for audit
        for t in used_targets:
            i = target_index[t]
            for f in factor_universe:
                j = factor_index[f]
                c = float(B[i, j])
                if c == 0.0:
                    continue
                mapping_rows.append({"iso": iso_u, "target": t, "factor": f, "coef": c})

        # Precompute macro paths for every scenario (used for inversion + optional auto-lam selection)
        dq = int(args.days_per_quarter)
        if args.dailyize == "sqrt":
            daily_scale = 1.0 / math.sqrt(max(1, dq))
        else:
            daily_scale = 1.0

        macro_paths_by_scen: Dict[str, Dict[str, np.ndarray]] = {}
        v2x_anchor_status_by_scen: Dict[str, str] = {}
        v2x_target_daily_z_by_scen: Dict[str, Optional[float]] = {}
        severity_by_scen: Dict[str, float] = {}

        for scen in scenario_defs:
            scen_id = str(scen.get("scenario_id"))
            scen_mult = float(scen.get("scenario_severity_multiplier", 1.0))
            ladder_level = scen.get("ladder_level")
            ladder_level = str(ladder_level) if ladder_level is not None else None

            iso_mult = float((defaults.get("iso_multipliers") or {}).get(iso_u, 1.0))
            severity = 1.0 * scen_mult * iso_mult
            severity_by_scen[scen_id] = float(severity)

            mp: Dict[str, np.ndarray] = {}
            for v in variables:
                if not isinstance(v, dict) or not v.get("variable"):
                    continue
                mv = str(v.get("variable"))
                peak = float(v.get("peak_at_severity_1"))
                shape = str(v.get("shape"))
                mp[mv] = _build_shaped_macro_path(peak=peak, severity=severity, shape=shape, horizon_quarters=horizon_quarters)
            macro_paths_by_scen[scen_id] = mp

            v2x_target_daily_z = None
            v2x_anchor_status = "disabled"
            if anchor_enabled and ladder_level and ladder and ladder_level in ladder:
                v2x_target_daily_z = (ladder_calibration.get("targets") or {}).get(ladder_level, {}).get("z")
                try:
                    v2x_target_daily_z = float(v2x_target_daily_z)
                except Exception:
                    v2x_target_daily_z = None

                if v2x_target_daily_z is None or not np.isfinite(v2x_target_daily_z):
                    v2x_anchor_status = "skipped_missing_target"
                elif abs(v2x_target_daily_z) < 1e-8:
                    v2x_anchor_status = "skipped_target_near_zero"
                elif v2x_anchor_factor not in factor_index:
                    v2x_anchor_status = "skipped_missing_factor"
                else:
                    v2x_anchor_status = "active"

            v2x_anchor_status_by_scen[scen_id] = v2x_anchor_status
            v2x_target_daily_z_by_scen[scen_id] = v2x_target_daily_z

        lam_iso = float(lam_default)
        if str(args.auto_lam) != "off":
            grid = _auto_lam_grid(args.auto_lam_lam_min, args.auto_lam_lam_max, args.auto_lam_lam_n)

            def _eval(lam: float) -> Dict[str, Any]:
                return _evaluate_lambda_for_iso(
                    lam=float(lam),
                    B=B,
                    used_targets=used_targets,
                    target_index=target_index,
                    factor_index=factor_index,
                    macro_to_target=macro_to_target,
                    macro_paths_by_scen=macro_paths_by_scen,
                    horizon_quarters=horizon_quarters,
                    target_transform_by_target=target_transform_by_target,
                    macro_units_by_var=macro_units_by_var,
                    v2x_anchor_status_by_scen=v2x_anchor_status_by_scen,
                    v2x_target_daily_z_by_scen=v2x_target_daily_z_by_scen,
                    v2x_anchor_factor=v2x_anchor_factor,
                    daily_scale=float(daily_scale),
                    v2x_anchor_weight=float(args.v2x_anchor_weight),
                    target_weights_sqrt=target_weights_sqrt,
                )

            chosen, meta, grid_rows = _select_lambda(
                method=str(args.auto_lam),
                grid=grid,
                fit_eps=float(args.auto_lam_fit_eps),
                fit_agg=str(args.auto_lam_fit_agg),
                z_cap=float(args.auto_lam_z_cap),
                alpha=float(args.auto_lam_alpha),
                eval_fn=_eval,
            )

            if (
                str(args.auto_lam) == "constrained"
                and isinstance(meta, dict)
                and meta.get("note")
                and str(args.auto_lam_constrained_fallback) == "scalar"
            ):
                chosen2, meta2, _grid_rows2 = _select_lambda(
                    method="scalar",
                    grid=grid,
                    fit_eps=float(args.auto_lam_fit_eps),
                    fit_agg=str(args.auto_lam_fit_agg),
                    z_cap=float(args.auto_lam_z_cap),
                    alpha=float(args.auto_lam_alpha),
                    eval_fn=_eval,
                )
                meta["fallback_used"] = "scalar"
                meta["fallback_choice"] = meta2.get("chosen")
                chosen = float(chosen2)

            lam_iso = float(chosen)
            iso_diag["lam_effective"] = float(lam_iso)
            iso_diag["auto_lam_selection"] = meta

            for r in grid_rows:
                auto_lam_grid_rows_all.append({"run_id": run_id, "iso": iso_u, **r})
            auto_lam_selection_rows.append(
                {
                    "run_id": run_id,
                    "iso": iso_u,
                    "lam_selected": float(lam_iso),
                    "auto_lam_mode": str(args.auto_lam),
                    "fit_eps": float(args.auto_lam_fit_eps),
                    "fit_agg": str(args.auto_lam_fit_agg),
                    "z_cap": float(args.auto_lam_z_cap),
                    "alpha": float(args.auto_lam_alpha),
                }
            )
        else:
            iso_diag["lam_effective"] = float(lam_iso)

        for scen in scenario_defs:
            scen_id = str(scen.get("scenario_id"))
            scen_mult = float(scen.get("scenario_severity_multiplier", 1.0))
            ladder_level = scen.get("ladder_level")
            ladder_level = str(ladder_level) if ladder_level is not None else None

            # For now: macro narrative is deterministic with base severity=1, scaled by scenario + ISO multipliers
            iso_mult = float((defaults.get("iso_multipliers") or {}).get(iso_u, 1.0))
            severity = 1.0 * scen_mult * iso_mult
            severity = float(severity_by_scen.get(scen_id, severity))

            # Build quarterly macro deltas y_q in the same order as used_targets.
            # We map macro variable -> matched Step4 target, and take the variable's shaped delta.
            scen_diag = {
                "severity": float(severity),
                "ladder_level": ladder_level,
                "targets": {},
                "quarter_solutions": [],
            }

            macro_paths = macro_paths_by_scen.get(scen_id) or {}

            # For each quarter, solve for factor z-shock vector x_q
            q_to_h_start = {q: (q - 1) * dq + 1 for q in range(1, horizon_quarters + 1)}

            # Determine ladder-implied V2X target (daily z) for this scenario.
            v2x_target_daily_z = v2x_target_daily_z_by_scen.get(scen_id)
            v2x_anchor_status = str(v2x_anchor_status_by_scen.get(scen_id) or "disabled")

            quarter_x: list[np.ndarray] = []
            quarter_y: list[np.ndarray] = []
            quarter_diag: list[dict[str, Any]] = []
            v2x_daily_z_by_quarter: list[float] = []

            for q in range(1, horizon_quarters + 1):
                y = np.zeros((len(used_targets),), dtype=float)
                # Fill y using matched macro variables
                for mv, t in macro_to_target.items():
                    i = target_index[t]
                    if mv in macro_paths:
                        raw_delta = float(macro_paths[mv][q - 1])
                        units = str(macro_units_by_var.get(mv, "pp"))
                        tt = str(target_transform_by_target.get(t, ""))
                        delta, warn = _convert_macro_delta_to_target_units(raw_delta, macro_units=units, target_transform=tt)
                        if warn:
                            iso_diag.setdefault("unit_warnings", []).append(
                                {
                                    "scenario_id": scen_id,
                                    "quarter": int(q),
                                    "macro_variable": mv,
                                    "target": t,
                                    "units": units,
                                    "target_transform": tt,
                                    "warning": warn,
                                }
                            )
                        y[i] = float(delta)

                B_base = B
                y_base = y
                if target_weights_sqrt is not None and target_weights_sqrt.size == len(used_targets):
                    w = target_weights_sqrt.reshape(-1)
                    B_base = B * w[:, None]
                    y_base = y * w

                B_solve = B_base
                y_solve = y_base
                v2x_prior_quarter_z = None
                if v2x_anchor_status == "active":
                    # x_z is in factor-z units; daily z = x_z * daily_scale.
                    v2x_prior_quarter_z = float(v2x_target_daily_z) / float(daily_scale)
                    w = float(args.v2x_anchor_weight)
                    if np.isfinite(w) and w > 0:
                        jv = factor_index[v2x_anchor_factor]
                        extra_row = np.zeros((1, B.shape[1]), dtype=float)
                        extra_row[0, jv] = math.sqrt(w)
                        B_solve = np.vstack([B_base, extra_row])
                        y_solve = np.concatenate([y_base, np.array([math.sqrt(w) * v2x_prior_quarter_z], dtype=float)])

                x_z, solve_diag = _ridge_solve(B_solve, y_solve, lam=float(lam_iso))

                # Plausibility guardrail: keep factor-z within z_cap. Disable by setting --auto-lam-z-cap <= 0.
                z_cap = float(args.auto_lam_z_cap)
                clipped = False
                if np.isfinite(z_cap) and z_cap > 0 and x_z.size:
                    max_abs_before = float(np.max(np.abs(x_z)))
                    if np.isfinite(max_abs_before) and max_abs_before > z_cap:
                        clipped = True
                        x_z_clipped = np.clip(x_z, -z_cap, z_cap)
                        n_clipped = int(np.sum(np.abs(x_z) > z_cap))
                        solve_diag.setdefault("plausibility", {})
                        solve_diag["plausibility"].update(
                            {
                                "z_cap": float(z_cap),
                                "n_clipped": int(n_clipped),
                                "max_abs_z_before": float(max_abs_before),
                                "max_abs_z_after": float(np.max(np.abs(x_z_clipped))) if x_z_clipped.size else None,
                            }
                        )
                        x_z = x_z_clipped
                quarter_x.append(x_z)
                quarter_y.append(y)
                if v2x_anchor_factor in factor_index:
                    v2x_daily_z_by_quarter.append(float(x_z[factor_index[v2x_anchor_factor]]) * float(daily_scale))

                solve_diag_extra: Dict[str, Any] = {}
                if v2x_anchor_status == "active":
                    solve_diag_extra["v2x_anchor_weight"] = float(args.v2x_anchor_weight)
                    solve_diag_extra["v2x_target_daily_z"] = float(v2x_target_daily_z)
                    solve_diag_extra["v2x_prior_quarter_z"] = float(v2x_prior_quarter_z) if v2x_prior_quarter_z is not None else None
                # Residual over base macro targets only (excludes any anchor pseudo-observation)
                resid = B_base @ x_z - y_base
                solve_diag_extra["resid_l2_base_targets"] = float(np.linalg.norm(resid))
                solve_diag_extra["rel_resid_base_targets"] = float(np.linalg.norm(resid) / (np.linalg.norm(y) + 1e-12))

                quarter_diag.append({"quarter": q, **solve_diag, **solve_diag_extra})

            v2x_peak_daily_z = None
            v2x_anchor_multiplier = None
            if v2x_daily_z_by_quarter:
                v2x_peak_daily_z = float(np.max(np.array(v2x_daily_z_by_quarter, dtype=float)))
                if (
                    v2x_target_daily_z is not None
                    and np.isfinite(v2x_target_daily_z)
                    and v2x_peak_daily_z is not None
                    and np.isfinite(v2x_peak_daily_z)
                    and abs(v2x_peak_daily_z) > 1e-12
                ):
                    v2x_anchor_multiplier = float(v2x_target_daily_z) / float(v2x_peak_daily_z)

            applied_multiplier = float(v2x_anchor_multiplier) if v2x_anchor_multiplier is not None else 1.0
            if not np.isfinite(applied_multiplier) or applied_multiplier <= 0:
                applied_multiplier = 1.0

            scen_diag["v2x_anchor"] = {
                "status": v2x_anchor_status,
                "ladder_level": ladder_level,
                "daily_scale": float(daily_scale),
                "v2x_target_daily_z": v2x_target_daily_z,
                "v2x_peak_daily_z_after": float(v2x_peak_daily_z) * float(applied_multiplier) if v2x_peak_daily_z is not None else None,
                "anchor_weight": float(args.v2x_anchor_weight),
                "anchor_multiplier_implied": v2x_anchor_multiplier,
                "anchor_multiplier_applied": float(applied_multiplier),
            }

            # IMF Mode A diagnostic: implied severity percentile (do not force).
            implied_peak = None
            if v2x_peak_daily_z is not None and np.isfinite(v2x_peak_daily_z):
                implied_peak = float(v2x_peak_daily_z) * float(applied_multiplier)
            implied_pct = _empirical_percentile(residual_pool_vals, implied_peak) if implied_peak is not None else None
            scen_diag["implied_severity"] = {
                "factor": v2x_anchor_factor,
                "v2x_peak_daily_z": implied_peak,
                "empirical_percentile": implied_pct,
                "residual_pool_status": residual_pool_meta.get("status"),
                "n_obs": int(residual_pool_meta.get("n_obs") or 0),
                "t0": residual_pool_meta.get("t0"),
                "window_days": residual_pool_meta.get("window_days"),
                "ladder_level": ladder_level,
                "ladder_quantile": float(ladder.get(ladder_level)) if ladder_level and ladder_level in ladder else None,
            }

            # Write factor shocks.
            for q in range(1, horizon_quarters + 1):
                x_z = quarter_x[q - 1] * float(applied_multiplier)

                for j, f in enumerate(factor_universe):
                    z = float(x_z[j])
                    sigma = float(sigma_t0.get(f, 1.0))
                    shock = z * daily_scale * sigma
                    if not np.isfinite(shock) or shock == 0.0:
                        continue

                    h_start = q_to_h_start[q]
                    for h in range(h_start, h_start + dq):
                        rows.append(
                            {
                                "run_id": run_id,
                                "scenario_id": scen_id,
                                "iso": iso_u,
                                "h": int(h),
                                "quarter": int(q),
                                "factor": f,
                                "shock": float(shock),
                                "shock_units": "innovation",
                                "source": "macro_inversion_step4",
                                "scaling_method": scaling_method,
                                "ladder_level": ladder_level,
                                "anchor_v2x_multiplier": float(applied_multiplier),
                            }
                        )

                qd = dict(quarter_diag[q - 1])
                if abs(float(applied_multiplier) - 1.0) > 1e-12:
                    y = quarter_y[q - 1]
                    resid_scaled = B @ x_z - y
                    qd["resid_l2_after_v2x_scale"] = float(np.linalg.norm(resid_scaled))
                    qd["rel_resid_after_v2x_scale"] = float(np.linalg.norm(resid_scaled) / (np.linalg.norm(y) + 1e-12))
                scen_diag["quarter_solutions"].append(qd)

            iso_diag["scenarios"][scen_id] = scen_diag
            status_rows.append({"run_id": run_id, "iso": iso_u, "scenario_id": scen_id, "status": "ok", "reason": ""})

        diagnostics["iso"][iso_u] = iso_diag

    shocks_df = pd.DataFrame(rows)
    status_df = pd.DataFrame(status_rows)
    tag = str(args.tag or "").strip()
    suffix = f"_{tag}" if tag else ""

    status_path = out_dir / f"factor_shocks_from_macro_status{suffix}.csv"
    status_df.to_csv(status_path, index=False)

    if shocks_df.empty:
        # Still write diagnostics + status to make this a controlled "no-solution" outcome.
        mapping_df = pd.DataFrame(mapping_rows)
        mapping_path = out_dir / f"factor_shocks_from_macro_mapping{suffix}.csv"
        mapping_df.to_csv(mapping_path, index=False)
        diag_path = out_dir / f"factor_shocks_from_macro_diagnostics{suffix}.json"
        diag_path.write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")

        if str(args.auto_lam) != "off" and auto_lam_grid_rows_all:
            grid_path = out_dir / f"factor_shocks_from_macro_auto_lam_grid{suffix}.csv"
            pd.DataFrame(auto_lam_grid_rows_all).to_csv(grid_path, index=False)
            sel_path = out_dir / f"factor_shocks_from_macro_auto_lam_selection{suffix}.csv"
            pd.DataFrame(auto_lam_selection_rows).to_csv(sel_path, index=False)
        print(f"[WARN] No solvable macro inversions; wrote status/diagnostics only")
        print(f"[OK] Wrote: {status_path}")
        print(f"[OK] Wrote: {mapping_path}")
        print(f"[OK] Wrote: {diag_path}")
        if str(args.auto_lam) != "off" and auto_lam_grid_rows_all:
            print(f"[OK] Wrote: {grid_path}")
            print(f"[OK] Wrote: {sel_path}")
        return 0

    shocks_path = out_dir / f"factor_shocks_from_macro{suffix}.csv"
    shocks_df.to_csv(shocks_path, index=False)

    # Persist the macro paths used for inversion (quarterly deltas).
    macro_used_rows: List[Dict[str, Any]] = []
    defaults = dict(macro_cfg.get("defaults") or {})
    horizon_quarters = int(defaults.get("horizon_quarters") or 12)
    for scen in scenario_defs:
        scen_id = str(scen.get("scenario_id"))
        scen_mult = float(scen.get("scenario_severity_multiplier", 1.0))
        for iso in sorted(iso_inputs.keys()):
            iso_u = str(iso).upper()
            iso_mult = float((defaults.get("iso_multipliers") or {}).get(iso_u, 1.0))
            severity = 1.0 * scen_mult * iso_mult
            for v in variables:
                if not isinstance(v, dict) or not v.get("variable"):
                    continue
                mv = str(v.get("variable"))
                units = str(v.get("units") or "")
                peak = float(v.get("peak_at_severity_1"))
                shape = str(v.get("shape"))

                try:
                    path = _build_shaped_macro_path(
                        peak=peak,
                        severity=severity,
                        shape=shape,
                        horizon_quarters=horizon_quarters,
                    )
                except Exception:
                    continue

                for quarter_idx, delta in enumerate(path, start=1):
                    macro_used_rows.append(
                        {
                            "run_id": run_id,
                            "scenario_id": scen_id,
                            "iso": iso_u,
                            "quarter": int(quarter_idx),
                            "variable": mv,
                            "units": units,
                            "delta": float(delta),
                            "severity": float(severity),
                        }
                    )

    macro_used_path = out_dir / f"macro_narrative_paths_used_for_inversion{suffix}.csv"
    pd.DataFrame(macro_used_rows).to_csv(macro_used_path, index=False)

    mapping_df = pd.DataFrame(mapping_rows)
    mapping_path = out_dir / f"factor_shocks_from_macro_mapping{suffix}.csv"
    mapping_df.to_csv(mapping_path, index=False)

    diag_path = out_dir / f"factor_shocks_from_macro_diagnostics{suffix}.json"
    diag_path.write_text(json.dumps(diagnostics, indent=2), encoding="utf-8")

    if str(args.auto_lam) != "off" and auto_lam_grid_rows_all:
        grid_path = out_dir / f"factor_shocks_from_macro_auto_lam_grid{suffix}.csv"
        pd.DataFrame(auto_lam_grid_rows_all).to_csv(grid_path, index=False)
        sel_path = out_dir / f"factor_shocks_from_macro_auto_lam_selection{suffix}.csv"
        pd.DataFrame(auto_lam_selection_rows).to_csv(sel_path, index=False)

    print(f"[OK] Wrote: {shocks_path}")
    print(f"[OK] Wrote: {status_path}")
    print(f"[OK] Wrote: {mapping_path}")
    print(f"[OK] Wrote: {diag_path}")
    print(f"[OK] Wrote: {macro_used_path}")
    if str(args.auto_lam) != "off" and auto_lam_grid_rows_all:
        print(f"[OK] Wrote: {grid_path}")
        print(f"[OK] Wrote: {sel_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
