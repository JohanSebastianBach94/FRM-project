from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MacroVariableSpec:
    variable: str
    units: str
    # Peak deviation direction/magnitude for severity=1.0 (stylized, not calibrated)
    peak: float
    # Shape name controlling the quarter-by-quarter path
    shape: str


@dataclass(frozen=True)
class MacroTemplateConfig:
    horizon_quarters: int
    iso_multipliers: Dict[str, float]
    # scenario_id -> multiplier
    scenario_multipliers: Dict[str, float]
    variables: List[MacroVariableSpec]
    # baselines: variable -> iso -> baseline level
    baseline_levels: Dict[str, Dict[str, float]]


DEFAULT_MACRO_SPECS: List[MacroVariableSpec] = [
    MacroVariableSpec("gdp_growth_yoy", "pp", peak=-1.5, shape="hump_trough_recover"),
    MacroVariableSpec("cpi_infl_yoy", "pp", peak=-0.8, shape="hump_trough_recover"),
    MacroVariableSpec("unemployment_rate", "pp", peak=+0.7, shape="lagged_hump"),
    MacroVariableSpec("policy_rate", "bp", peak=-75.0, shape="front_loaded_revert"),
    MacroVariableSpec("equity_price", "%", peak=-18.0, shape="front_loaded_partial_recover"),
    MacroVariableSpec("fx_depreciation", "%", peak=+6.0, shape="front_loaded_partial_recover"),
    MacroVariableSpec("sovereign_spread", "bp", peak=+180.0, shape="front_loaded_revert"),
]


def _shape_path(shape: str, quarters: int, peak: float) -> np.ndarray:
    if quarters <= 0:
        return np.zeros(0, dtype=float)

    q = int(quarters)

    if shape == "hump_trough_recover":
        # Mild hump down then recovery toward 0
        # Peak around Q3, recovery by end.
        x = np.linspace(0.0, 1.0, q)
        trough = np.exp(-((x - 0.25) ** 2) / (2 * 0.12**2))
        trough = trough / trough.max()
        tail = np.linspace(1.0, 0.15, q)
        return peak * trough * tail

    if shape == "lagged_hump":
        # Unemployment responds with lag; peaks later and fades slowly
        x = np.linspace(0.0, 1.0, q)
        hump = np.exp(-((x - 0.45) ** 2) / (2 * 0.16**2))
        hump = hump / hump.max()
        tail = np.linspace(1.0, 0.35, q)
        return peak * hump * tail

    if shape == "front_loaded_revert":
        # Big initial move, then mean revert
        k = max(1, min(3, q))
        head = np.linspace(0.6, 1.0, k)
        tail = np.exp(-np.linspace(0.0, 2.0, q - k))
        return np.concatenate([peak * head, peak * tail])

    if shape == "front_loaded_partial_recover":
        # Instant hit then partial recovery (doesn't fully mean-revert)
        k = max(1, min(2, q))
        head = np.linspace(0.8, 1.0, k)
        tail = 0.45 + 0.55 * np.exp(-np.linspace(0.0, 2.2, q - k))
        return np.concatenate([peak * head, peak * tail])

    raise ValueError(f"Unknown macro path shape: {shape}")


def apply_baseline_levels(
    macro_deltas: pd.DataFrame,
    *,
    baseline_levels: Mapping[str, Mapping[str, float]],
) -> pd.DataFrame:
    """Convert macro deltas into baseline+stressed levels when baselines exist.

    Output columns:
    - baseline
    - stressed

    Interpretation rules:
    - units in {"pp", "bp"} => additive: stressed = baseline + delta
    - units == "%" => multiplicative: stressed = baseline * (1 + delta/100)

    If no baseline is provided for a row, baseline is NaN and stressed is NaN.
    """

    required = {"iso", "variable", "units", "delta"}
    missing = required - set(macro_deltas.columns)
    if missing:
        raise ValueError(f"macro_deltas missing required columns: {sorted(missing)}")

    df = macro_deltas.copy()
    df["iso"] = df["iso"].astype(str).str.upper()
    df["variable"] = df["variable"].astype(str)
    df["units"] = df["units"].astype(str)

    baselines: List[Optional[float]] = []
    stressed: List[Optional[float]] = []

    for _, r in df.iterrows():
        iso = str(r["iso"]).upper()
        var = str(r["variable"])
        units = str(r["units"]).strip()
        delta = float(r["delta"]) if pd.notna(r["delta"]) else np.nan

        b = None
        by_var = baseline_levels.get(var)
        if by_var is not None:
            b_val = by_var.get(iso)
            if b_val is not None:
                try:
                    b = float(b_val)
                except Exception:
                    b = None

        if b is None or not np.isfinite(b) or not np.isfinite(delta):
            baselines.append(np.nan)
            stressed.append(np.nan)
            continue

        if units in {"pp", "bp"}:
            baselines.append(b)
            stressed.append(b + delta)
        elif units == "%":
            baselines.append(b)
            stressed.append(b * (1.0 + delta / 100.0))
        else:
            # Unknown units => default to additive
            baselines.append(b)
            stressed.append(b + delta)

    df["baseline"] = baselines
    df["stressed"] = stressed
    return df


def build_template_config_from_dict(payload: Mapping[str, Any]) -> MacroTemplateConfig:
    """Parse the macro YAML/JSON payload into a typed config."""

    defaults = dict(payload.get("defaults") or {})
    horizon = int(defaults.get("horizon_quarters") or payload.get("horizon_quarters") or 12)

    iso_mult = {str(k).upper(): float(v) for k, v in (defaults.get("iso_multipliers") or {}).items()}

    scenario_multipliers: Dict[str, float] = {}
    for s in (payload.get("scenarios") or []):
        if not isinstance(s, dict):
            continue
        sid = s.get("scenario_id")
        if not sid:
            continue
        m = s.get("scenario_severity_multiplier")
        if m is None:
            continue
        scenario_multipliers[str(sid)] = float(m)

    variables: List[MacroVariableSpec] = []
    baseline_levels: Dict[str, Dict[str, float]] = {}
    for v in (payload.get("variables") or []):
        if not isinstance(v, dict):
            continue
        var = str(v.get("variable"))
        units = str(v.get("units"))
        peak = float(v.get("peak_at_severity_1"))
        shape = str(v.get("shape"))
        variables.append(MacroVariableSpec(variable=var, units=units, peak=peak, shape=shape))

        b = v.get("baseline_levels") or {}
        if isinstance(b, dict):
            baseline_levels[var] = {str(k).upper(): float(val) for k, val in b.items() if val is not None}

    if not variables:
        variables = list(DEFAULT_MACRO_SPECS)

    return MacroTemplateConfig(
        horizon_quarters=horizon,
        iso_multipliers=iso_mult,
        scenario_multipliers=scenario_multipliers,
        variables=variables,
        baseline_levels=baseline_levels,
    )


def severity_from_factor_shocks(
    factor_shocks: pd.DataFrame,
    *,
    reference_factor: str = "V2X",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Compute a per-ISO per-scenario severity scalar from factor shocks.

    We use peak absolute shock for `reference_factor` per (iso, scenario_id).
    Then for each scenario_id we normalize by the median peak across ISOs so
    severity is dimensionless and comparable.

    Returns
    - severity_df columns: iso, scenario_id, peak_shock, ref_peak, severity
    - diagnostics dict
    """

    required = {"iso", "scenario_id", "factor", "shock"}
    missing = required - set(factor_shocks.columns)
    if missing:
        raise ValueError(f"factor_shocks missing required columns: {sorted(missing)}")

    sub = factor_shocks.loc[factor_shocks["factor"].astype(str) == str(reference_factor)].copy()
    if sub.empty:
        raise ValueError(f"No rows for reference_factor={reference_factor}")

    peak = (
        sub.groupby(["iso", "scenario_id"], as_index=False)["shock"]
        .apply(lambda s: float(np.nanmax(np.abs(pd.to_numeric(s, errors="coerce").to_numpy()))))
        .rename(columns={"shock": "peak_shock"})
    )

    ref = peak.groupby("scenario_id", as_index=False)["peak_shock"].median().rename(columns={"peak_shock": "ref_peak"})
    out = peak.merge(ref, on="scenario_id", how="left")
    out["severity"] = out.apply(
        lambda r: float(r["peak_shock"]) / float(r["ref_peak"]) if float(r["ref_peak"]) > 0 else 1.0,
        axis=1,
    )

    # Keep it stable (avoid pathological scaling)
    out["severity"] = out["severity"].clip(lower=0.5, upper=2.0)

    diagnostics = {
        "reference_factor": reference_factor,
        "notes": [
            "severity = peak_abs(reference_factor shock) / median_peak_abs_by_scenario",
            "severity clipped to [0.5, 2.0]",
        ],
    }
    return out, diagnostics


def build_macro_narrative_paths(
    *,
    iso: str,
    scenario_id: str,
    severity: float,
    quarters: int,
    macro_specs: Optional[Iterable[MacroVariableSpec]] = None,
    iso_multiplier: float = 1.0,
) -> pd.DataFrame:
    specs = list(macro_specs) if macro_specs is not None else list(DEFAULT_MACRO_SPECS)

    rows: List[Dict[str, Any]] = []
    sev = float(severity) * float(iso_multiplier)

    for spec in specs:
        path = _shape_path(spec.shape, quarters, spec.peak * sev)
        for q in range(1, int(quarters) + 1):
            rows.append(
                {
                    "iso": str(iso),
                    "scenario_id": str(scenario_id),
                    "quarter": q,
                    "variable": spec.variable,
                    "delta": float(path[q - 1]),
                    "units": spec.units,
                    "severity": float(severity),
                    "iso_multiplier": float(iso_multiplier),
                }
            )

    return pd.DataFrame(rows)


def macro_definition_payload(*, quarters: int, severity_diagnostics: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "macro_narrative": {
            "quarters": int(quarters),
            "semantics": "All macro paths are deviations (delta) vs an implicit baseline.",
            "calibration": "Stylized/illustrative IMF/FSAP-style narrative (not calibrated to a specific country model).",
            "variables": [
                {
                    "variable": s.variable,
                    "units": s.units,
                    "peak_at_severity_1": s.peak,
                    "shape": s.shape,
                }
                for s in DEFAULT_MACRO_SPECS
            ],
        },
        "severity": dict(severity_diagnostics),
    }
