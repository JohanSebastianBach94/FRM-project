#!/usr/bin/env python3
"""Step 11.0 — Explainable scenario report (econ + stats).

Problem this solves
- Steps 9–10 emit many CSV/JSON artifacts that are hard for humans to interpret.
- This script collapses a run folder into a single, narrative markdown report plus
  a few compact visuals.

What it reads (for a given run_id)
- analysis_outputs/scenarios/<run_id>/manifest.json (run contract)
- analysis_outputs/scenarios/<run_id>/deterministic/scenario_definition.json
- analysis_outputs/scenarios/<run_id>/deterministic/scaling_diagnostics.csv
- analysis_outputs/scenarios/<run_id>/deterministic/factor_shocks.csv
- analysis_outputs/scenarios/<run_id>/deterministic/macro_narrative_paths.csv
- analysis_outputs/scenarios/<run_id>/deterministic/macro_narrative_levels.csv
- analysis_outputs/scenarios/<run_id>/deterministic/factor_shocks_from_macro*.{csv,json}
- analysis_outputs/feature_contributions_<ISO>.csv (Step 4 quality)

Outputs
- analysis_outputs/scenarios/<run_id>/reports/explainable_report.md
- analysis_outputs/scenarios/<run_id>/reports/plots/*.png

Notes on "IMF comparison"
- We avoid claiming that our stylized scenario equals any IMF/FSAP scenario.
- We compare magnitudes and sign patterns to common stress-testing stylized facts
  (GDP down, unemployment up, risk premia up, equity down, policy rate down, etc.).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import math
import re

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"

_PC_RE = re.compile(r"^(?P<block>[A-Za-z0-9_]+)_pc(?P<k>\d+)$")


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_yaml_or_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    if path.suffix.lower() == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore

            return yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


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
        v = float(x)
        if not np.isfinite(v):
            return None
        return v
    except Exception:
        return None


def _fmt(x: Any, ndp: int = 2) -> str:
    v = _safe_float(x)
    if v is None:
        return "NA"
    return f"{v:.{ndp}f}"


def _peak_metrics(series: pd.Series) -> Dict[str, float]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"min": float("nan"), "max": float("nan"), "min_abs": float("nan"), "max_abs": float("nan")}
    return {
        "min": float(s.min()),
        "max": float(s.max()),
        "min_abs": float(s.loc[s.abs().idxmin()]),
        "max_abs": float(s.loc[s.abs().idxmax()]),
    }


def _peak_abs_signed(series: pd.Series) -> float:
    """Return the signed value at max |series| (or NaN)."""

    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return float("nan")
    return float(s.loc[s.abs().idxmax()])


def _markdown_table(df: pd.DataFrame, max_rows: int = 12) -> str:
    """Render a small markdown table without optional dependencies (e.g., tabulate)."""

    if df.empty:
        return "(empty)\n"

    d = df.copy()
    if len(d) > max_rows:
        d = d.head(max_rows)

    # Convert to strings and compute widths
    headers = [str(c) for c in d.columns]
    rows = [["" if pd.isna(v) else str(v) for v in r] for r in d.itertuples(index=False, name=None)]
    cols = list(zip(*([headers] + rows))) if rows else [tuple([h]) for h in headers]
    widths = [max(len(x) for x in col) for col in cols]

    def fmt_row(values: List[str]) -> str:
        cells = [values[i].ljust(widths[i]) for i in range(len(values))]
        return "| " + " | ".join(cells) + " |"

    out: List[str] = []
    out.append(fmt_row(headers))
    out.append("| " + " | ".join(["-" * w for w in widths]) + " |")
    for r in rows:
        out.append(fmt_row(r))
    return "\n".join(out) + "\n"


def _load_step4_quality(iso: str) -> pd.DataFrame:
    path = PROJECT_ROOT / "analysis_outputs" / f"feature_contributions_{iso}.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Normalize a few expected cols (best-effort)
    for c in [
        "target",
        "train_r2",
        "test_r2",
        "macro_target",
        "target_transform",
        "feature_source_used",
        "train_ratio_used",
        "n_obs_total",
        "n_train",
        "n_test",
        "data_start",
        "data_end",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "cv_splits_requested",
        "cv_splits_used",
        "walk_forward_used",
        "walk_forward_min_train",
    ]:
        if c not in df.columns:
            df[c] = np.nan
    df["target"] = df["target"].astype(str)
    return df


def _parse_coefficients_blob(blob: Any) -> Dict[str, float]:
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
    return pd.read_csv(path, index_col=0)


def _expand_pc_coefficients(
    *,
    iso: str,
    coef_map: Dict[str, float],
    loadings_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]],
) -> Dict[str, float]:
    expanded: Dict[str, float] = {}
    for feat, beta in coef_map.items():
        m = _PC_RE.match(str(feat))
        if not m:
            expanded[str(feat)] = expanded.get(str(feat), 0.0) + float(beta)
            continue

        block = m.group("block")
        key = (iso, block)
        if key not in loadings_cache:
            loadings_cache[key] = _load_pca_loadings(iso, block)
        L = loadings_cache[key]
        if L is None or str(feat) not in L.columns:
            # If loadings are missing, we cannot expand; drop PC contribution.
            continue

        col = L[str(feat)]
        for raw_name, loading in col.items():
            try:
                w = float(loading)
            except Exception:
                continue
            if not np.isfinite(w) or w == 0.0:
                continue
            expanded[str(raw_name)] = expanded.get(str(raw_name), 0.0) + float(beta) * w

    return expanded


def _ridge_projection_weights(B: np.ndarray, lam: float) -> np.ndarray:
    """Return P such that x = P y for ridge solution min||Bx-y||^2 + lam||x||^2."""

    BtB = B.T @ B
    A = BtB + float(lam) * np.eye(B.shape[1])
    # Solve A^{-1} B^T without forming explicit inverse.
    return np.linalg.solve(A, B.T)


def _corr_rmse(a: pd.Series, b: pd.Series) -> Dict[str, float]:
    aa = pd.to_numeric(a, errors="coerce")
    bb = pd.to_numeric(b, errors="coerce")
    df = pd.DataFrame({"a": aa, "b": bb}).dropna()
    if df.empty:
        return {"corr": float("nan"), "rmse": float("nan")}
    corr = float(df["a"].corr(df["b"])) if len(df) >= 2 else float("nan")
    rmse = float(np.sqrt(np.mean((df["a"] - df["b"]) ** 2)))
    return {"corr": corr, "rmse": rmse}


def _scenario_severity_index(
    *,
    scen_def: Dict[str, Any],
    scaling_diag: pd.DataFrame,
    inv_diag: Dict[str, Any],
    macro_design: pd.DataFrame,
) -> pd.DataFrame:
    """Return best-effort severity index per scenario.

        Output columns: scenario_id, family, severity_index
            - family='quantile': uses scenario_definition.json calibration quantile if available (e.g. 0.95, 0.99)
            - family='narrative': uses inv_diag implied_severity empirical_percentile (0-1)
            - family='narrative_design': uses macro narrative design severity rank (0-1)

    This is a diagnostic axis for explaining why macro→target consistency may change with scenario severity.
    """

    rows: List[Dict[str, Any]] = []

    # Quantile family (Step 10.0)
    calib = scen_def.get("calibration") if isinstance(scen_def.get("calibration"), dict) else {}
    scen_list = scen_def.get("scenarios") if isinstance(scen_def.get("scenarios"), list) else []
    for s in scen_list:
        if not isinstance(s, dict):
            continue
        scen_id = str(s.get("scenario_id") or "").strip()
        z_shocks = s.get("z_shocks") if isinstance(s.get("z_shocks"), dict) else {}
        if not scen_id or not z_shocks:
            continue
        # Take the max quantile across shocked factors (usually just V2X)
        quantiles: List[float] = []
        for f in z_shocks.keys():
            key = f"{str(f)}_{scen_id}"
            q = _safe_float((calib.get(key) or {}).get("quantile") if isinstance(calib.get(key), dict) else None)
            if q is not None:
                quantiles.append(float(q))
        if quantiles:
            rows.append({"scenario_id": scen_id, "family": "quantile", "severity_index": float(max(quantiles))})
            continue

        # Fallback: if no calibration quantile exists, derive an ordering proxy from scaling diagnostics z_effective
        if not scaling_diag.empty and {"scenario_id", "z_effective"}.issubset(scaling_diag.columns):
            sub = scaling_diag.loc[scaling_diag["scenario_id"].astype(str) == scen_id].copy()
            if not sub.empty:
                z_eff = pd.to_numeric(sub["z_effective"], errors="coerce").dropna()
                if not z_eff.empty:
                    rows.append({"scenario_id": scen_id, "family": "quantile", "severity_index": float(z_eff.median())})

    # Narrative family (Step 10.2 macro_first)
    inv_iso = inv_diag.get("iso") if isinstance(inv_diag.get("iso"), dict) else {}
    for _, iso_v in (inv_iso or {}).items():
        if not isinstance(iso_v, dict):
            continue
        scen_map = iso_v.get("scenarios") if isinstance(iso_v.get("scenarios"), dict) else {}
        for scen_k, scen_v in (scen_map or {}).items():
            if not isinstance(scen_v, dict):
                continue
            sev = scen_v.get("implied_severity") if isinstance(scen_v.get("implied_severity"), dict) else {}
            p = _safe_float(sev.get("empirical_percentile"))
            if p is None:
                continue
            rows.append({"scenario_id": str(scen_k), "family": "narrative", "severity_index": float(p)})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["severity_index"] = pd.to_numeric(df["severity_index"], errors="coerce")
    df = df.dropna(subset=["severity_index"]).copy()
    if df.empty:
        return df
    # multiple entries per scenario possible (across ISOs); take median
    out = df.groupby(["scenario_id", "family"], dropna=False)["severity_index"].median().reset_index()

    # Fallback severity axis from narrative design (always scenario-distinct if multipliers differ).
    design = _macro_severity_score_from_design(macro_design)
    if not design.empty and {"scenario_id", "macro_severity_score"}.issubset(design.columns):
        d = design.copy()
        d["severity_index"] = pd.to_numeric(d["macro_severity_score"], errors="coerce")
        d = d.dropna(subset=["severity_index"]).copy()
        if not d.empty:
            d["severity_index"] = d["severity_index"].rank(method="average", pct=True)
            d["family"] = "narrative_design"
            out = pd.concat([out, d[["scenario_id", "family", "severity_index"]]], ignore_index=True)

    # If implied narrative severity is effectively constant across scenarios, prefer the design-based rank.
    narr = out.loc[out["family"] == "narrative", "severity_index"].dropna()
    if len(narr) >= 2 and narr.nunique() <= 1:
        out = out.loc[out["family"] != "narrative"].copy()

    # Choose ONE severity index per scenario_id to avoid merge duplication downstream.
    family_rank = {"narrative": 3, "narrative_design": 2, "quantile": 1}
    out["family_rank"] = out["family"].map(family_rank).fillna(0).astype(int)
    out = out.sort_values(["scenario_id", "family_rank"], ascending=[True, False])
    best = out.drop_duplicates(subset=["scenario_id"], keep="first").drop(columns=["family_rank"]).copy()
    return best.sort_values(["severity_index", "scenario_id"], ascending=[False, True])


def _macro_severity_score_from_design(macro_design: pd.DataFrame) -> pd.DataFrame:
    """Compute a simple narrative severity score from macro paths.

    For each (scenario_id, iso): sum of peak abs deltas across variables.
    Then report scenario-level median across ISOs.
    """

    need = {"iso", "scenario_id", "variable", "delta"}
    if macro_design.empty or not need.issubset(set(macro_design.columns)):
        return pd.DataFrame()

    df = macro_design.copy()
    df["iso"] = df["iso"].astype(str).str.upper()
    df["scenario_id"] = df["scenario_id"].astype(str)
    df["variable"] = df["variable"].astype(str)
    df["delta"] = pd.to_numeric(df["delta"], errors="coerce")
    df = df.dropna(subset=["delta"]).copy()
    if df.empty:
        return pd.DataFrame()

    peaks = (
        df.groupby(["scenario_id", "iso", "variable"], dropna=False)["delta"]
        .apply(lambda s: float(pd.to_numeric(s, errors="coerce").abs().max()))
        .reset_index(name="peak_abs_delta")
    )
    iso_score = peaks.groupby(["scenario_id", "iso"], dropna=False)["peak_abs_delta"].sum().reset_index(name="macro_severity_iso")
    scen_score = iso_score.groupby(["scenario_id"], dropna=False)["macro_severity_iso"].median().reset_index(name="macro_severity_score")
    return scen_score.sort_values(["macro_severity_score", "scenario_id"], ascending=[False, True])


def _target_comovement_summary(implied_targets: pd.DataFrame) -> pd.DataFrame:
    """Summarize target-vs-target correlation within each (iso, scenario).

    Uses quarter-level implied deltas (delta_hat). Off-diagonal mean correlation is a classic "correlations go to 1" stress diagnostic.
    """

    need = {"iso", "scenario_id", "quarter", "target", "delta_hat"}
    if implied_targets.empty or not need.issubset(set(implied_targets.columns)):
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    df = implied_targets.copy()
    df["iso"] = df["iso"].astype(str).str.upper()
    df["scenario_id"] = df["scenario_id"].astype(str)
    df["target"] = df["target"].astype(str)
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")
    df["delta_hat"] = pd.to_numeric(df["delta_hat"], errors="coerce")
    df = df.dropna(subset=["quarter", "delta_hat"]).copy()
    df["quarter"] = df["quarter"].astype(int)

    for (iso, scen), sub in df.groupby(["iso", "scenario_id"], dropna=False):
        pivot = sub.pivot_table(index="quarter", columns="target", values="delta_hat", aggfunc="mean")
        if pivot.shape[1] < 2 or pivot.shape[0] < 3:
            continue
        corr = pivot.corr()
        cov = pivot.cov()
        # off-diagonal upper triangle
        vals: List[float] = []
        abs_vals: List[float] = []
        cov_vals: List[float] = []
        abs_cov_vals: List[float] = []
        cols = list(corr.columns)
        for i in range(len(cols)):
            for j in range(i + 1, len(cols)):
                v = corr.iat[i, j]
                if pd.isna(v):
                    continue
                vv = float(v)
                if not np.isfinite(vv):
                    continue
                vals.append(vv)
                abs_vals.append(abs(vv))

                cv = cov.iat[i, j]
                if not pd.isna(cv):
                    cvv = float(cv)
                    if np.isfinite(cvv):
                        cov_vals.append(cvv)
                        abs_cov_vals.append(abs(cvv))
        if not vals:
            continue
        rows.append(
            {
                "iso": str(iso),
                "scenario_id": str(scen),
                "n_targets": int(pivot.shape[1]),
                "n_pairs": int(len(vals)),
                "mean_corr": float(np.mean(vals)),
                "mean_abs_corr": float(np.mean(abs_vals)),
                "mean_cov": (float(np.mean(cov_vals)) if cov_vals else float("nan")),
                "mean_abs_cov": (float(np.mean(abs_cov_vals)) if abs_cov_vals else float("nan")),
                "pct_abs_corr_ge_0_8": float(np.mean([1.0 if a >= 0.8 else 0.0 for a in abs_vals])),
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["scenario_id", "iso"], ascending=[True, True])


def _try_make_readable_macro_vs_implied_plots(
    *,
    report_dir: Path,
    macro_used: pd.DataFrame,
    implied_df: pd.DataFrame,
    consistency_df: pd.DataFrame,
) -> List[str]:
    """Readable per-ISO/per-scenario plots: macro driver vs implied mapped target (+ optional CI)."""

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return []

    if macro_used.empty or implied_df.empty or consistency_df.empty:
        return []

    out_paths: List[str] = []
    plots_dir = _ensure_dir(report_dir / "plots")

    mu = macro_used.copy()
    mu["iso"] = mu["iso"].astype(str).str.upper()
    mu["scenario_id"] = mu["scenario_id"].astype(str)
    mu["quarter"] = pd.to_numeric(mu["quarter"], errors="coerce")
    mu = mu.dropna(subset=["quarter"]).copy()
    mu["quarter"] = mu["quarter"].astype(int)

    im = implied_df.copy()
    im["iso"] = im["iso"].astype(str).str.upper()
    im["scenario_id"] = im["scenario_id"].astype(str)
    im["quarter"] = pd.to_numeric(im["quarter"], errors="coerce")
    im = im.dropna(subset=["quarter"]).copy()
    im["quarter"] = im["quarter"].astype(int)

    for _, row in consistency_df.iterrows():
        iso = str(row.get("iso") or "").upper()
        scen = str(row.get("scenario_id") or "")
        var = str(row.get("macro_variable") or "")
        tgt = str(row.get("mapped_target") or "")
        if not iso or not scen or not var or not tgt:
            continue

        a = mu.loc[(mu["iso"] == iso) & (mu["scenario_id"] == scen) & (mu["variable"] == var), ["quarter", "delta"]].copy()
        b = im.loc[(im["iso"] == iso) & (im["scenario_id"] == scen) & (im["target"] == tgt), ["quarter", "delta_hat"]].copy()
        if a.empty or b.empty:
            continue
        m = a.merge(b, on="quarter", how="inner")
        if m.empty:
            continue

        fig, ax = plt.subplots(figsize=(9.5, 4.2))
        ax.plot(m["quarter"], m["delta"], linewidth=2.2, label=f"Narrative macro: {var}")
        ax.plot(m["quarter"], m["delta_hat"], linewidth=2.0, linestyle="--", label=f"Implied target: {tgt}")

        ax.set_title(f"{iso} — {scen} — {var} vs {tgt}")
        ax.set_xlabel("Quarter")
        ax.set_ylabel("Delta")
        ax.grid(True, alpha=0.25)
        ax.legend(loc="best", fontsize=9)
        fig.tight_layout()
        out = plots_dir / f"macro_vs_implied_{iso}_{scen}_{var}.png"
        fig.savefig(out, dpi=170)
        plt.close(fig)
        out_paths.append(str(out))

    return out_paths


def _try_make_one_pagers(
    *,
    report_dir: Path,
    macro_used: pd.DataFrame,
    zq: pd.DataFrame,
    implied_df: pd.DataFrame,
    inv_diag: Dict[str, Any],
) -> List[str]:
    """Create a compact 1-page figure per ISO+scenario for decision use."""

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return []

    if macro_used.empty or zq.empty or implied_df.empty:
        return []

    plots_dir = _ensure_dir(report_dir / "plots")
    out_paths: List[str] = []

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _apply_plot_style() -> None:
        try:
            plt.rcParams.update(
                {
                    "figure.facecolor": "white",
                    "axes.facecolor": "white",
                    "savefig.facecolor": "white",
                    "axes.grid": True,
                    "grid.alpha": 0.22,
                    "grid.linestyle": "-",
                    "axes.spines.top": False,
                    "axes.spines.right": False,
                    "axes.titlesize": 12,
                    "axes.labelsize": 10,
                    "xtick.labelsize": 9,
                    "ytick.labelsize": 9,
                    "legend.fontsize": 8,
                    "legend.frameon": False,
                }
            )
        except Exception:
            pass

    _apply_plot_style()

    mu = macro_used.copy()
    mu["iso"] = mu["iso"].astype(str).str.upper()
    mu["scenario_id"] = mu["scenario_id"].astype(str)
    mu["quarter"] = pd.to_numeric(mu["quarter"], errors="coerce")
    mu = mu.dropna(subset=["quarter"]).copy()
    mu["quarter"] = mu["quarter"].astype(int)

    zz = zq.copy()
    zz["iso"] = zz["iso"].astype(str).str.upper()
    zz["scenario_id"] = zz["scenario_id"].astype(str)
    zz["quarter"] = pd.to_numeric(zz["quarter"], errors="coerce")
    zz = zz.dropna(subset=["quarter"]).copy()
    zz["quarter"] = zz["quarter"].astype(int)
    zz["factor"] = zz["factor"].astype(str)
    zz["z_quarter"] = pd.to_numeric(zz.get("z_quarter"), errors="coerce")

    # Scenario-level "global" factor paths: median across ISOs for display.
    # This is display-only (does not imply the ISO solve included that factor).
    global_zz = (
        zz.dropna(subset=["z_quarter"]).groupby(["scenario_id", "quarter", "factor"], dropna=False)["z_quarter"].median().reset_index()
    )

    im = implied_df.copy()
    im["iso"] = im["iso"].astype(str).str.upper()
    im["scenario_id"] = im["scenario_id"].astype(str)
    im["quarter"] = pd.to_numeric(im["quarter"], errors="coerce")
    im = im.dropna(subset=["quarter"]).copy()
    im["quarter"] = im["quarter"].astype(int)
    im["test_r2"] = pd.to_numeric(im.get("test_r2"), errors="coerce")
    im["macro_target"] = pd.to_numeric(im.get("macro_target"), errors="coerce").fillna(0.0)

    # Pick a stable set of macro drivers to show (prefer these if present)
    preferred_macro = [
        "gdp_growth_yoy",
        "unemployment_rate",
        "cpi_infl_yoy",
        "policy_rate",
        "equity_price",
        "sovereign_spread",
    ]

    days_per_quarter = int(inv_diag.get("days_per_quarter") or 63)
    macro_units = inv_diag.get("macro_units_by_var") if isinstance(inv_diag.get("macro_units_by_var"), dict) else {}
    z_cap = None
    try:
        z_cap = float(inv_diag.get("auto_lam", {}).get("z_cap")) if isinstance(inv_diag.get("auto_lam"), dict) else None
    except Exception:
        z_cap = None

    def _fmt_unit(var: str) -> str:
        u = macro_units.get(str(var)) if isinstance(macro_units, dict) else None
        return f" ({u})" if u else ""

    def _scenario_diag_text(iso: str, scen: str) -> str:
        parts: List[str] = []
        try:
            sev = inv_diag.get("iso", {}).get(iso, {}).get("scenarios", {}).get(scen, {}).get("severity")
            if sev is not None and np.isfinite(float(sev)):
                parts.append(f"severity={float(sev):.3f}")
        except Exception:
            pass
        # Best-effort lambda summary for the scenario (median across quarter solutions)
        try:
            qsol = inv_diag.get("iso", {}).get(iso, {}).get("scenarios", {}).get(scen, {}).get("quarter_solutions")
            if isinstance(qsol, list) and qsol:
                lams = [float(r.get("lam")) for r in qsol if isinstance(r, dict) and r.get("lam") is not None]
                lams = [x for x in lams if np.isfinite(x) and x > 0]
                if lams:
                    parts.append(f"λ~{float(np.median(lams)):.2e}")
        except Exception:
            pass
        if z_cap is not None and np.isfinite(z_cap):
            parts.append(f"z_cap={float(z_cap):.1f}")
        # Governance reminder
        parts.append("GDP excluded from inversion")
        return " | ".join(parts)

    # Preferred global/systemic factors to show if available (display-only; does not change the solve).
    # Note: factor naming is dataset-specific; we match by regex and then pin any matches.
    _force_factor_patterns: list[re.Pattern[str]] = [
        re.compile(r"^V2X$|VIX", re.IGNORECASE),
        re.compile(r"OIL|BRENT|WTI|GAS|ENERGY|COMMOD", re.IGNORECASE),
        re.compile(r"SOFR", re.IGNORECASE),
        re.compile(r"EURIBOR", re.IGNORECASE),
        re.compile(r"TEDRATE|TED", re.IGNORECASE),
        re.compile(r"GC\.DOD\.TOTL\.GD\.ZS", re.IGNORECASE),
        re.compile(r"SPREAD|BTP|BUND|OAS|CDS", re.IGNORECASE),
        re.compile(r"EQUITY|MSCI|SPX|DAX|FTSE", re.IGNORECASE),
    ]

    # Explicit anchor factor codes we want to display if present anywhere in the run.
    _force_factor_codes: list[str] = [
        "V2X",
        "DCOILBRENTEU",
        "DCOILWTICO",
        "SOFR_3m",
        "BTP_Bund_Spread",
    ]

    def _pick_factor_lines(zz_sub: pd.DataFrame, scen: str, k: int = 6) -> list[str]:
        if zz_sub.empty or "factor" not in zz_sub.columns:
            return []
        tmp = zz_sub.copy()
        tmp["factor"] = tmp["factor"].astype(str)
        tmp["absz"] = pd.to_numeric(tmp.get("z_quarter"), errors="coerce").abs()

        factors_all = tmp["factor"].dropna().astype(str).unique().tolist()

        forced: list[str] = []
        # First, explicit codes if they exist either locally or in global paths.
        for c in _force_factor_codes:
            if c in factors_all:
                forced.append(c)
            else:
                # If not in this ISO's factor set, include it if it's present in global factor paths for this scenario.
                try:
                    has_global = not global_zz.loc[(global_zz["scenario_id"] == str(scen)) & (global_zz["factor"] == str(c))].empty
                except Exception:
                    has_global = False
                if has_global:
                    forced.append(c)

        for pat in _force_factor_patterns:
            for f in factors_all:
                if pat.search(str(f)) and (str(f) not in forced):
                    forced.append(str(f))

        # Then add the biggest movers to complete the panel.
        top_by_abs = (
            tmp.groupby("factor", dropna=False)["absz"].max().sort_values(ascending=False).index.astype(str).tolist()
        )
        out: list[str] = []
        for f in forced:
            if f not in out:
                out.append(f)
            if len(out) >= int(k):
                return out[: int(k)]
        for f in top_by_abs:
            if f not in out:
                out.append(f)
            if len(out) >= int(k):
                break
        return out[: int(k)]

    # --- Storytelling-first target selection ---
    # We only have target codes (no rich metadata), so we use a pragmatic approach:
    # - Prefer macro-mapped targets (consistency checks)
    # - Prefer economically interpretable series (labor, inflation, rates, spreads, equity, housing)
    # - Prefer decent Step 4 quality (test_r2)
    # - Avoid internal/model-parameter-like targets (e.g., beta*) and near-zero movers

    _target_exclude = re.compile(r"(?:^|[_\-])(?:BETA\d+|BETA|ALPHA\d*|INTERCEPT)(?:[_\-]|$)", re.IGNORECASE)
    _target_exclude2 = re.compile(r"(?:^|[_\-])(?:COEF\d+|COEFFICIENT|PARAM)(?:[_\-]|$)", re.IGNORECASE)

    _story_patterns: list[tuple[str, re.Pattern[str], int]] = [
        ("labor", re.compile(r"UNEMP|UNRATE|JOBLESS|CLAIMS", re.IGNORECASE), 90),
        ("inflation", re.compile(r"CPI|INFL|PCE", re.IGNORECASE), 85),
        ("policy_rate", re.compile(r"POLICY[_\-]?RATE|FEDFUNDS|REFI|BASE[_\-]?RATE", re.IGNORECASE), 80),
        ("market_rates", re.compile(r"SOFR|OIS|EONIA|ESTR|YIELD|UST|BUND|GILT|\b10Y\b|\b2Y\b", re.IGNORECASE), 75),
        ("spreads", re.compile(r"SPREAD|CDS|OAS|SOV|SOVEREIGN|BANK[_\-]?SPREAD|CORP[_\-]?SPREAD", re.IGNORECASE), 78),
        ("equity", re.compile(r"EQUITY|STOCK|INDEX|SPX|DAX|FTSE|MSCI", re.IGNORECASE), 72),
        ("housing", re.compile(r"HOUSE|HPI|REAL[_\-]?ESTATE|MORTGAGE", re.IGNORECASE), 68),
        ("credit", re.compile(r"LOAN|CREDIT|NPL|PD|DEFAULT|CHARGE[_\-]?OFF", re.IGNORECASE), 65),
        ("activity", re.compile(r"GDP|INDPRO|IND[_\-]?PROD|PMI|IP\b", re.IGNORECASE), 45),
    ]

    def _story_priority(target_code: str) -> int:
        t = str(target_code or "")
        if not t:
            return 0
        score = 0
        for _name, pat, w in _story_patterns:
            if pat.search(t):
                score = max(score, int(w))
        # Penalize GDP-like targets when the model quality is often weak; keep macro narrative panel for GDP story.
        if re.search(r"GDP", t, flags=re.IGNORECASE):
            score = min(score, 35)
        return int(score)

    def _pick_story_targets(im_sub: pd.DataFrame, n: int = 4) -> list[str]:
        if im_sub.empty:
            return []
        df = im_sub.copy()
        df["target"] = df["target"].astype(str)
        df["delta_hat"] = pd.to_numeric(df.get("delta_hat"), errors="coerce")
        df["test_r2"] = pd.to_numeric(df.get("test_r2"), errors="coerce")
        df["macro_target"] = pd.to_numeric(df.get("macro_target"), errors="coerce").fillna(0.0)

        # Aggregate per target
        agg = (
            df.groupby("target", dropna=False)
            .agg(
                macro_target=("macro_target", "max"),
                test_r2=("test_r2", "max"),
                absd=("delta_hat", lambda s: float(pd.to_numeric(s, errors="coerce").abs().max())),
            )
            .reset_index()
        )
        if agg.empty:
            return []

        # Exclusions and low-signal filter
        agg["tcode"] = agg["target"].astype(str)
        agg = agg.loc[~agg["tcode"].str.contains(_target_exclude, na=False)].copy()
        agg = agg.loc[~agg["tcode"].str.contains(_target_exclude2, na=False)].copy()
        # Hard rule for one-pagers: never display GDP implied targets.
        agg = agg.loc[~agg["tcode"].str.contains(r"GDP", case=False, na=False)].copy()
        agg = agg.loc[pd.to_numeric(agg["absd"], errors="coerce").fillna(0.0) > 1e-10].copy()
        if agg.empty:
            return []

        # Story score from target code patterns
        agg["story"] = agg["tcode"].apply(_story_priority).astype(float)

        # Quality score: prefer non-negative test R²; downweight very negative.
        r2 = pd.to_numeric(agg["test_r2"], errors="coerce")
        agg["r2_score"] = np.where(r2.notna(), np.clip(r2, -0.5, 1.0), 0.0)

        # Size score within scenario, normalized to [0, 1]
        absd = pd.to_numeric(agg["absd"], errors="coerce").fillna(0.0)
        denom = float(absd.max()) if absd.max() > 0 else 1.0
        agg["size_score"] = (absd / denom).astype(float)

        # Final score: macro targets first, then story, then size, then quality
        # Keep macro targets but avoid low-skill GDP dominating the panel.
        agg["macro_bonus"] = np.where(agg["macro_target"].astype(float) >= 0.5, 10.0, 0.0)
        agg["is_gdp"] = agg["tcode"].str.contains(r"GDP", case=False, na=False)
        # Stronger penalty if GDP is also low-confidence.
        agg["gdp_penalty"] = np.where(agg["is_gdp"], np.where(agg["r2_score"] < 0.0, 12.0, 6.0), 0.0)
        agg["score"] = (
            100.0 * agg["macro_bonus"]
            + 1.5 * agg["story"]
            + 30.0 * agg["size_score"]
            + 10.0 * agg["r2_score"]
            - 10.0 * agg["gdp_penalty"]
        )

        chosen: list[str] = []
        # Ensure we show up to 2 macro-mapped targets (consistency anchors) if available.
        macro_pool = agg.loc[agg["macro_bonus"] > 0].sort_values(["score", "absd"], ascending=[False, False])
        for t in macro_pool["target"].astype(str).tolist():
            if t not in chosen:
                chosen.append(t)
            if len(chosen) >= min(2, int(n)):
                break

        # Fill remaining with best story score overall
        pool = agg.sort_values(["score", "absd"], ascending=[False, False])
        # Prefer non-negative test R² if we have enough options
        pool_good = pool.loc[pd.to_numeric(pool["test_r2"], errors="coerce").fillna(0.0) >= 0.0].copy()
        if pool_good.shape[0] >= max(1, int(n) - len(chosen)):
            pool_use = pool_good
        else:
            pool_use = pool
        for t in pool_use["target"].astype(str).tolist():
            if t in chosen:
                continue
            chosen.append(t)
            if len(chosen) >= int(n):
                break
        return chosen

    for iso in sorted(zz["iso"].unique().tolist()):
        for scen in sorted(zz.loc[zz["iso"] == iso, "scenario_id"].unique().tolist()):
            scen = str(scen)
            mu_sub = mu.loc[(mu["iso"] == iso) & (mu["scenario_id"] == scen)].copy()
            zz_sub = zz.loc[(zz["iso"] == iso) & (zz["scenario_id"] == scen)].copy()
            if mu_sub.empty or zz_sub.empty:
                continue

            # Macro panel: top 3 variables (stable ordering)
            macro_vars = [v for v in preferred_macro if v in mu_sub["variable"].unique().tolist()]
            if not macro_vars:
                macro_vars = sorted(mu_sub["variable"].astype(str).unique().tolist())
            macro_vars = macro_vars[:3]

            # Factor panel: show global/systemic anchors if present, then largest movers.
            factor_order = _pick_factor_lines(zz_sub, scen, k=8)

            # Target panel: storytelling-first selection (macro anchors + economically interpretable series)
            im_sub = im.loc[(im["iso"] == iso) & (im["scenario_id"] == scen)].copy()
            if im_sub.empty:
                continue
            target_order = _pick_story_targets(im_sub, n=4)

            # A4-ish landscape sizing; tuned for readability when pasted into a deck.
            fig, axes = plt.subplots(2, 2, figsize=(13.0, 7.6), constrained_layout=True)
            ax0, ax1, ax2, ax3 = axes.flatten()

            # Panel 1: macro drivers (quarterly deltas)
            for v in macro_vars:
                s = mu_sub.loc[mu_sub["variable"] == v, ["quarter", "delta"]].sort_values("quarter")
                ax0.step(s["quarter"], s["delta"], where="mid", linewidth=2.2, label=f"{v}{_fmt_unit(v)}")
            ax0.axhline(0.0, color="#333", linewidth=0.8, alpha=0.55)
            ax0.set_title("Macro narrative (input deltas)")
            ax0.set_xlabel("Quarter")
            ax0.set_ylabel("Δ")
            ax0.grid(True, alpha=0.25)
            ax0.legend(loc="best")

            # Panel 2: factor shocks (quarter z)
            for f in factor_order:
                s = zz_sub.loc[zz_sub["factor"].astype(str) == f, ["quarter", "z_quarter"]].sort_values("quarter")
                label = str(f)
                if s.empty:
                    # Fallback to scenario-level global path if available.
                    g = global_zz.loc[(global_zz["scenario_id"] == str(scen)) & (global_zz["factor"].astype(str) == str(f)), ["quarter", "z_quarter"]]
                    g = g.sort_values("quarter")
                    if not g.empty:
                        s = g
                        label = f"{f} (global)"
                if not s.empty:
                    ax1.step(s["quarter"], s["z_quarter"], where="mid", linewidth=2.2, label=label)
            ax1.axhline(0.0, color="#333", linewidth=0.8, alpha=0.55)
            if z_cap is not None and np.isfinite(z_cap):
                ax1.axhline(float(z_cap), color="#888", linewidth=0.9, linestyle=":", alpha=0.7)
                ax1.axhline(-float(z_cap), color="#888", linewidth=0.9, linestyle=":", alpha=0.7)
            ax1.set_title(f"Implied factor shocks (z per quarter; {days_per_quarter}d/q)")
            ax1.set_xlabel("Quarter")
            ax1.set_ylabel("z")
            ax1.grid(True, alpha=0.25)
            ax1.legend(loc="best")

            # Panel 3: key implied targets (Step 4 implied deltas)
            plotted_any = False
            for t in target_order:
                s = im_sub.loc[im_sub["target"].astype(str) == t, ["quarter", "delta_hat", "test_r2"]].sort_values("quarter")
                if s.empty:
                    continue
                r2 = pd.to_numeric(s["test_r2"].iloc[0], errors="coerce") if "test_r2" in s.columns else float("nan")
                label = f"{t} (R²={r2:.2f})" if np.isfinite(r2) else str(t)
                ls = "--" if (np.isfinite(r2) and float(r2) < 0.0) else "-"
                alpha = 0.75 if (np.isfinite(r2) and float(r2) < 0.0) else 1.0
                ax2.step(s["quarter"], s["delta_hat"], where="mid", linewidth=2.2, linestyle=ls, alpha=alpha, label=label)
                plotted_any = True
            ax2.axhline(0.0, color="#333", linewidth=0.8, alpha=0.55)
            ax2.set_title("Implied targets (Step 4; dashed = low-confidence)")
            ax2.set_xlabel("Quarter")
            ax2.set_ylabel("Δ̂")
            ax2.grid(True, alpha=0.25)
            if plotted_any:
                ax2.legend(loc="best")
            else:
                msg = "No targets available"
                ax2.text(0.5, 0.5, msg, transform=ax2.transAxes, ha="center", va="center", fontsize=10, alpha=0.8)

            # Panel 4: compact diagnostic (z at max-|z| per factor; preserves sign)
            def _peak_z_from_df(df_in: pd.DataFrame, factor_code: str) -> Optional[float]:
                if df_in.empty:
                    return None
                sub = df_in.loc[df_in["factor"].astype(str) == str(factor_code)].copy()
                if sub.empty:
                    return None
                sub["absz"] = pd.to_numeric(sub.get("z_quarter"), errors="coerce").abs()
                if sub["absz"].isna().all():
                    return None
                idx2 = sub["absz"].idxmax()
                try:
                    z = float(sub.loc[idx2, "z_quarter"])
                except Exception:
                    return None
                return z if np.isfinite(z) else None

            peak_rows: List[Tuple[str, float]] = []
            # Local peaks for all factors in this ISO/scenario.
            for fac, g in zz_sub.groupby("factor", dropna=False):
                zpk = _peak_z_from_df(g, str(fac))
                if zpk is None:
                    continue
                peak_rows.append((str(fac), float(zpk)))

            # Ensure forced anchors show up in the peak panel if available locally or globally.
            forced_for_peak: list[str] = []
            for c in _force_factor_codes:
                if c not in forced_for_peak:
                    forced_for_peak.append(c)
            try:
                factors_all = zz_sub["factor"].dropna().astype(str).unique().tolist()
            except Exception:
                factors_all = []
            for pat in _force_factor_patterns:
                for f in factors_all:
                    if pat.search(str(f)) and (str(f) not in forced_for_peak):
                        forced_for_peak.append(str(f))

            # Add global fallback peaks for forced factors missing locally.
            for f in forced_for_peak:
                if any(r[0] == str(f) for r in peak_rows):
                    continue
                try:
                    gdf = global_zz.loc[
                        (global_zz["scenario_id"] == str(scen))
                        & (global_zz["factor"].astype(str) == str(f)),
                        ["quarter", "factor", "z_quarter"],
                    ].copy()
                except Exception:
                    gdf = pd.DataFrame()
                zpk = _peak_z_from_df(gdf, str(f))
                if zpk is not None:
                    peak_rows.append((str(f), float(zpk)))

            if peak_rows:
                peak_df = pd.DataFrame(peak_rows, columns=["factor", "z_peak"]).drop_duplicates(subset=["factor"], keep="first")
                peak_df["abs_peak"] = pd.to_numeric(peak_df["z_peak"], errors="coerce").abs()
                peak_df = peak_df.dropna(subset=["abs_peak"]).copy()
                peak_df = peak_df.sort_values("abs_peak", ascending=False)

                n_peak = 12
                ordered: list[str] = []
                for f in forced_for_peak:
                    if f in peak_df["factor"].astype(str).tolist() and f not in ordered:
                        ordered.append(str(f))
                for f in peak_df["factor"].astype(str).tolist():
                    if f not in ordered:
                        ordered.append(str(f))
                    if len(ordered) >= int(n_peak):
                        break

                show = peak_df.set_index("factor").reindex(ordered).dropna(subset=["z_peak"]).head(int(n_peak))
                ax3.barh(show.index.astype(str), show["z_peak"].astype(float).values, alpha=0.88)
            ax3.axvline(0.0, color="#333", linewidth=0.8, alpha=0.55)
            if z_cap is not None and np.isfinite(z_cap):
                ax3.axvline(float(z_cap), color="#888", linewidth=0.9, linestyle=":", alpha=0.7)
                ax3.axvline(-float(z_cap), color="#888", linewidth=0.9, linestyle=":", alpha=0.7)
            ax3.set_title("Peak factor z (top 12; value at max |z|)")
            ax3.set_xlabel("z")
            ax3.grid(True, axis="x", alpha=0.25)

            fig.suptitle(f"Scenario one-pager — {iso} — {scen}", fontsize=14)
            # Small header diagnostics box
            header = _scenario_diag_text(iso, scen)
            fig.text(0.01, 0.985, header, ha="left", va="top", fontsize=9, color="#333")
            fig.text(0.99, 0.985, f"generated {generated_at}", ha="right", va="top", fontsize=8, color="#555")
            out = plots_dir / f"one_pager_{iso}_{scen}.png"
            fig.savefig(out, dpi=220)
            plt.close(fig)
            out_paths.append(str(out))

    return out_paths


def _quarterly_factor_z_from_daily_shocks(
    *,
    factor_macro: pd.DataFrame,
    inv_diag: Dict[str, Any],
    days_per_quarter: int,
) -> pd.DataFrame:
    """Recover quarter-level factor z for macro-derived shocks.

    factor_shocks_from_macro.csv stores daily innovation shocks:
      shock = x_z_quarter * daily_scale * sigma_t0
    where daily_scale is either 1/sqrt(days_per_quarter) or 1.
    """

    if factor_macro.empty:
        return pd.DataFrame()

    need = {"iso", "scenario_id", "quarter", "factor", "shock"}
    if not need.issubset(set(factor_macro.columns)):
        return pd.DataFrame()

    df = factor_macro.copy()
    df["iso"] = df["iso"].astype(str).str.upper()
    df["scenario_id"] = df["scenario_id"].astype(str)
    df["factor"] = df["factor"].astype(str)
    df["quarter"] = pd.to_numeric(df["quarter"], errors="coerce")
    df["shock"] = pd.to_numeric(df["shock"], errors="coerce")
    df = df.dropna(subset=["quarter", "shock"]).copy()
    df["quarter"] = df["quarter"].astype(int)

    # Take one row per (iso, scenario, quarter, factor) since shock is constant within the quarter.
    df = df.sort_values(["iso", "scenario_id", "quarter", "factor", "h"]) if "h" in df.columns else df
    df = df.groupby(["iso", "scenario_id", "quarter", "factor"], dropna=False, as_index=False).first()

    # daily_scale per iso/scenario from diagnostics; fallback from config
    default_daily_scale = 1.0 / math.sqrt(max(1, int(days_per_quarter)))

    def get_daily_scale(iso: str, scen: str) -> float:
        try:
            return float(inv_diag["iso"][iso]["scenarios"][scen]["v2x_anchor"]["daily_scale"])
        except Exception:
            return float(default_daily_scale)

    def get_sigma(iso: str, factor: str) -> float:
        try:
            s = inv_diag["iso"][iso].get("sigma_t0") or {}
            return float(s.get(factor, 1.0))
        except Exception:
            return 1.0

    df["daily_scale"] = [get_daily_scale(i, s) for i, s in zip(df["iso"], df["scenario_id"])]
    df["sigma_t0"] = [get_sigma(i, f) for i, f in zip(df["iso"], df["factor"])]

    # Avoid division by zero; sigma_t0 defaults to 1.0 when unknown.
    denom = df["daily_scale"].astype(float) * df["sigma_t0"].astype(float)
    denom = denom.replace(0.0, np.nan)
    df["z_quarter"] = df["shock"].astype(float) / denom
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["z_quarter"]).copy()
    return df[["iso", "scenario_id", "quarter", "factor", "z_quarter", "daily_scale", "sigma_t0"]]


def _try_make_plots(
    *,
    report_dir: Path,
    factor_macro: pd.DataFrame,
    macro_peaks: pd.DataFrame,
    step4_summary: pd.DataFrame,
) -> List[str]:
    """Create a couple of compact plots if matplotlib is available."""

    try:
        import matplotlib.pyplot as plt  # type: ignore
    except Exception:
        return []

    out_paths: List[str] = []
    plots_dir = _ensure_dir(report_dir / "plots")

    # Plot 1: heatmap-like grid of implied factor shocks (macro->factor) for each scenario.
    if not factor_macro.empty and {"iso", "scenario_id", "factor", "shock"}.issubset(factor_macro.columns):
        for scen_id in sorted(factor_macro["scenario_id"].astype(str).unique().tolist()):
            sub = factor_macro.loc[factor_macro["scenario_id"].astype(str) == scen_id].copy()
            if sub.empty:
                continue
            pivot = sub.pivot_table(index="iso", columns="factor", values="shock", aggfunc="mean")
            # Focus: top factors by overall abs magnitude to stay readable.
            abs_rank = pivot.abs().sum(axis=0).sort_values(ascending=False)
            keep = abs_rank.head(12).index.tolist()
            pivot = pivot.loc[:, keep]

            fig, ax = plt.subplots(figsize=(max(6, 0.6 * len(keep) + 2), max(2.5, 0.4 * len(pivot) + 1.5)))
            im = ax.imshow(pivot.values, aspect="auto", cmap="RdBu_r")
            ax.set_title(f"Implied factor shocks (macro→factor) — {scen_id}")
            ax.set_yticks(np.arange(len(pivot.index)))
            ax.set_yticklabels(pivot.index.tolist())
            ax.set_xticks(np.arange(len(pivot.columns)))
            ax.set_xticklabels(pivot.columns.tolist(), rotation=45, ha="right")
            cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
            cbar.set_label("shock")
            fig.tight_layout()
            out = plots_dir / f"implied_factor_shocks_{scen_id}.png"
            fig.savefig(out, dpi=170)
            plt.close(fig)
            out_paths.append(str(out))

    # Plot 2: macro peak deltas by ISO (small multiples as bar chart for GDP/CPI/UNRATE if present).
    if not macro_peaks.empty and {"iso", "scenario_id", "variable", "peak"}.issubset(macro_peaks.columns):
        focus_vars = [v for v in ["gdp_growth_yoy", "cpi_infl_yoy", "unemployment_rate"] if v in set(macro_peaks["variable"])]
        for scen_id in sorted(macro_peaks["scenario_id"].astype(str).unique().tolist()):
            sub = macro_peaks.loc[(macro_peaks["scenario_id"].astype(str) == scen_id) & (macro_peaks["variable"].isin(focus_vars))].copy()
            if sub.empty:
                continue
            pivot = sub.pivot_table(index="iso", columns="variable", values="peak", aggfunc="mean")
            fig, ax = plt.subplots(figsize=(7.5, 3.5))
            pivot.plot(kind="bar", ax=ax)
            ax.set_title(f"Macro narrative peak deltas — {scen_id}")
            ax.set_ylabel("delta (template units)")
            ax.grid(True, axis="y", alpha=0.25)
            fig.tight_layout()
            out = plots_dir / f"macro_peak_deltas_{scen_id}.png"
            fig.savefig(out, dpi=170)
            plt.close(fig)
            out_paths.append(str(out))

    # Plot 3: Step 4 macro target OOS R2 (quality gate context).
    if not step4_summary.empty and {"iso", "target", "test_r2"}.issubset(step4_summary.columns):
        sub = step4_summary.copy()
        sub = sub.loc[sub["macro_target"].fillna(False).astype(bool)].copy()
        sub["test_r2"] = pd.to_numeric(sub["test_r2"], errors="coerce")
        sub = sub.dropna(subset=["test_r2"])
        if not sub.empty:
            fig, ax = plt.subplots(figsize=(8.5, 4.0))
            # show worst-to-best to highlight gating risk
            sub = sub.sort_values(["test_r2"], ascending=True)
            labels = (sub["iso"].astype(str) + ":" + sub["target"].astype(str)).tolist()
            ax.barh(labels, sub["test_r2"].values)
            ax.axvline(0.0, color="black", linewidth=1)
            ax.set_title("Step 4 macro target out-of-sample R² (higher is better)")
            ax.set_xlabel("test_r2")
            ax.grid(True, axis="x", alpha=0.25)
            fig.tight_layout()
            out = plots_dir / "step4_macro_test_r2.png"
            fig.savefig(out, dpi=170)
            plt.close(fig)
            out_paths.append(str(out))

    return out_paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 11.0 — Explainable scenario report")
    parser.add_argument("--run-id", default=None, help="Scenario run id created by Step 9")
    parser.add_argument(
        "--macro-tag",
        default="",
        help=(
            "Optional tag for Step 10.2 macro inversion outputs. When set, reads "
            "deterministic/factor_shocks_from_macro_<tag>.csv and matching *_diagnostics/_mapping/_status/_paths files."
        ),
    )
    args = parser.parse_args()

    run_id = _infer_run_id(args.run_id)
    run_dir = SCENARIOS_DIR / run_id
    det_dir = run_dir / "deterministic"
    base_report_dir = _ensure_dir(run_dir / "reports")

    macro_tag = str(args.macro_tag or "").strip()
    macro_suffix = f"_{macro_tag}" if macro_tag else ""

    # If a macro tag is provided, write reports into a dedicated subfolder to avoid overwriting
    # the default (hybrid) report outputs.
    report_dir = _ensure_dir(base_report_dir / macro_tag) if macro_tag else base_report_dir

    manifest = _read_json(run_dir / "manifest.json")
    iso_inputs = manifest.get("iso_inputs") or {}

    scen_def = _read_json(det_dir / "scenario_definition.json") if (det_dir / "scenario_definition.json").exists() else {}
    scaling_diag = pd.read_csv(det_dir / "scaling_diagnostics.csv") if (det_dir / "scaling_diagnostics.csv").exists() else pd.DataFrame()

    factor_shocks = pd.read_csv(det_dir / "factor_shocks.csv") if (det_dir / "factor_shocks.csv").exists() else pd.DataFrame()
    macro_paths = pd.read_csv(det_dir / "macro_narrative_paths.csv") if (det_dir / "macro_narrative_paths.csv").exists() else pd.DataFrame()
    macro_levels = pd.read_csv(det_dir / "macro_narrative_levels.csv") if (det_dir / "macro_narrative_levels.csv").exists() else pd.DataFrame()

    macro_shocks_path = det_dir / f"factor_shocks_from_macro{macro_suffix}.csv"
    macro_status_path = det_dir / f"factor_shocks_from_macro_status{macro_suffix}.csv"
    macro_diag_path = det_dir / f"factor_shocks_from_macro_diagnostics{macro_suffix}.json"
    macro_mapping_path = det_dir / f"factor_shocks_from_macro_mapping{macro_suffix}.csv"
    macro_used_path = det_dir / f"macro_narrative_paths_used_for_inversion{macro_suffix}.csv"

    factor_macro = pd.read_csv(macro_shocks_path) if macro_shocks_path.exists() else pd.DataFrame()
    status = pd.read_csv(macro_status_path) if macro_status_path.exists() else pd.DataFrame()
    inv_diag = _read_json(macro_diag_path) if macro_diag_path.exists() else {}

    # Optional: the macro paths that were actually fed into inversion (Step 10.2)
    macro_used = pd.read_csv(macro_used_path) if macro_used_path.exists() else pd.DataFrame()

    mapping_df = pd.read_csv(macro_mapping_path) if macro_mapping_path.exists() else pd.DataFrame()

    # Step 4 quality summary across ISOs
    step4_rows: List[pd.DataFrame] = []
    for iso in sorted(iso_inputs.keys()):
        df = _load_step4_quality(str(iso).upper())
        if df.empty:
            continue
        df = df.copy()
        df["iso"] = str(iso).upper()
        step4_rows.append(df)
    step4_summary = pd.concat(step4_rows, ignore_index=True) if step4_rows else pd.DataFrame()

    # Scenario design source: prefer the macro paths actually used for inversion (tag-aware)
    macro_design = pd.DataFrame()
    if not macro_used.empty and {"iso", "scenario_id", "variable", "quarter", "delta"}.issubset(macro_used.columns):
        macro_design = macro_used
    elif not macro_paths.empty and {"iso", "scenario_id", "variable", "quarter", "delta"}.issubset(macro_paths.columns):
        macro_design = macro_paths

    # Compact macro peak table per iso/scenario/variable
    macro_peaks_rows: List[Dict[str, Any]] = []
    if not macro_design.empty:
        for (iso, scen, var), sub in macro_design.groupby(["iso", "scenario_id", "variable"], dropna=False):
            m = _peak_metrics(sub["delta"])
            # Keep a single signed peak at max abs
            peak = float(m["max_abs"]) if np.isfinite(m["max_abs"]) else float("nan")
            macro_peaks_rows.append({"iso": iso, "scenario_id": scen, "variable": var, "peak": peak})
    macro_peaks = pd.DataFrame(macro_peaks_rows)

    # Executive-level summary: scenario lists + ISO list
    isos = sorted([str(k).upper() for k in iso_inputs.keys()])
    scenarios = []  # quantile / deterministic family
    if isinstance(scen_def.get("scenarios"), list):
        scenarios = [str(s.get("scenario_id")) for s in scen_def.get("scenarios") if isinstance(s, dict) and s.get("scenario_id")]
    if not scenarios and not factor_shocks.empty and "scenario_id" in factor_shocks.columns:
        scenarios = sorted(factor_shocks["scenario_id"].astype(str).unique().tolist())

    macro_scenarios: List[str] = []
    if not macro_design.empty and "scenario_id" in macro_design.columns:
        macro_scenarios = sorted(macro_design["scenario_id"].astype(str).unique().tolist())
    elif not factor_macro.empty and "scenario_id" in factor_macro.columns:
        macro_scenarios = sorted(factor_macro["scenario_id"].astype(str).unique().tolist())

    # Basic factor shock summary (deterministic)
    det_focus = pd.DataFrame()
    if not factor_shocks.empty and {"iso", "scenario_id", "factor", "shock"}.issubset(factor_shocks.columns):
        tmp = factor_shocks.copy()
        tmp["abs_shock"] = pd.to_numeric(tmp["shock"], errors="coerce").abs()
        det_focus = (
            tmp.groupby(["scenario_id", "factor"], dropna=False)["abs_shock"]
            .sum()
            .reset_index()
            .sort_values(["scenario_id", "abs_shock"], ascending=[True, False])
        )

    # Inversion status summary
    status_summary = pd.DataFrame()
    if not status.empty and {"iso", "scenario_id", "status"}.issubset(status.columns):
        status_summary = status.groupby(["iso", "status"], dropna=False).size().reset_index(name="n")

    # Econ/stat commentary heuristics (kept conservative)
    econ_notes: List[str] = []
    econ_notes.append(
        "**Interpretation reminder:** the canonical stress input is *factor innovations*. The macro narrative is a reporting layer. "
        "Step 10.2 is an *approximate inversion* (ridge least squares) of Step 4 sparse mappings; it is not a structural macro model."
    )

    # Flag “coverage realism” based on factor universe composition.
    example_factors = []
    if isos:
        example_factors = list((iso_inputs.get(isos[0]) or {}).get("factors") or [])[:12]
    if example_factors:
        econ_notes.append(
            "**Factor universe reality check:** in this run the governed factor set is relatively small and includes several slow-moving balance-sheet style series (e.g. BIS loans, public debt ratios) plus V2X. "
            "That makes the resulting macro→factor translation less intuitive than IMF/FSAP factor sets (which typically include richer market spreads, credit, house prices, etc.)."
        )

    # IMF/FSAP-style plausibility ranges (very rough) for peak deltas
    plausibility = {
        "gdp_growth_yoy": {"adverse_typical_pp": (1.0, 5.0)},
        "unemployment_rate": {"adverse_typical_pp": (0.5, 3.0)},
        "cpi_infl_yoy": {"adverse_typical_pp": (0.5, 3.0)},
    }

    plaus_rows: List[Dict[str, Any]] = []
    if not macro_peaks.empty:
        for scen in sorted(macro_peaks["scenario_id"].astype(str).unique().tolist()):
            for var, pr in plausibility.items():
                sub = macro_peaks.loc[(macro_peaks["scenario_id"].astype(str) == scen) & (macro_peaks["variable"].astype(str) == var)]
                if sub.empty:
                    continue
                # absolute peak for comparability
                peaks = pd.to_numeric(sub["peak"], errors="coerce").abs().dropna()
                if peaks.empty:
                    continue
                lo, hi = float(peaks.min()), float(peaks.max())
                tlo, thi = pr["adverse_typical_pp"]

                # Where does this scenario sit within the rough typical band? (use the worst-case ISO peak)
                pos = None
                if thi > tlo and np.isfinite(hi):
                    pos = (hi - float(tlo)) / (float(thi) - float(tlo))
                    pos = float(np.clip(pos, 0.0, 1.0))

                if hi < float(tlo):
                    comment = "below typical (mild)"
                elif lo > float(thi):
                    comment = "above typical"
                else:
                    # In-range, but still distinguish low-end vs high-end
                    if pos is not None and pos < 0.25:
                        comment = "in-range (low-end)"
                    elif pos is not None and pos > 0.75:
                        comment = "in-range (high-end)"
                    else:
                        comment = "in-range"

                plaus_rows.append({
                    "scenario_id": scen,
                    "variable": var,
                    "abs_peak_range": f"{lo:.2f}–{hi:.2f}",
                    "rough_IMF_style_range": f"{tlo:.1f}–{thi:.1f} pp",
                    "position_in_range": ("NA" if pos is None else f"{pos:.2f}"),
                    "comment": comment,
                })
    plaus_df = pd.DataFrame(plaus_rows)

    # Run plots
    plot_paths = _try_make_plots(report_dir=report_dir, factor_macro=factor_macro, macro_peaks=macro_peaks, step4_summary=step4_summary)

    # --- IMF narrative findings: interactions + forward target behavior ---
    narrative_md: List[str] = []
    narrative_md.append(f"# IMF-Style Narrative Findings — {run_id}\n")
    narrative_md.append("## What this section answers\n")
    narrative_md.append(
        "- How the **macro drivers** (GDP/CPI/UNRATE paths you specified) translate into the factor-innovation space we can simulate.\n"
        "- How those drivers **interact** (trade-offs) inside the Step 10.2 ridge inversion.\n"
        "- How **all Step 4 targets** would behave forward given the implied factor path (including targets not explicitly pinned by the macro narrative).\n"
    )

    narrative_md.append("## Inputs used\n")
    narrative_md.append(f"- Macro paths used for inversion: deterministic/macro_narrative_paths_used_for_inversion{macro_suffix}.csv\n")
    narrative_md.append(f"- Implied factor shocks: deterministic/factor_shocks_from_macro{macro_suffix}.csv\n")
    narrative_md.append("\n")

    # IMF Mode A: show implied severity (empirical percentile of V2X peak z) if available.
    implied_rows = []
    inv_iso = inv_diag.get("iso") if isinstance(inv_diag.get("iso"), dict) else {}
    for iso_k, iso_v in (inv_iso or {}).items():
        if not isinstance(iso_v, dict):
            continue
        scen_map = iso_v.get("scenarios") if isinstance(iso_v.get("scenarios"), dict) else {}
        for scen_k, scen_v in (scen_map or {}).items():
            if not isinstance(scen_v, dict):
                continue
            sev = scen_v.get("implied_severity") if isinstance(scen_v.get("implied_severity"), dict) else {}
            if not sev:
                continue
            implied_rows.append(
                {
                    "iso": str(iso_k).upper(),
                    "scenario_id": str(scen_k),
                    "factor": sev.get("factor"),
                    "v2x_peak_daily_z": sev.get("v2x_peak_daily_z"),
                    "empirical_percentile": sev.get("empirical_percentile"),
                    "ladder_level": sev.get("ladder_level"),
                    "ladder_quantile": sev.get("ladder_quantile"),
                    "n_obs": sev.get("n_obs"),
                    "t0": sev.get("t0"),
                    "window_days": sev.get("window_days"),
                }
            )

    implied_df = pd.DataFrame(implied_rows)
    if not implied_df.empty:
        narrative_md.append("## Implied risk severity (diagnostic, not forced)\n")
        narrative_md.append(
            "This reports the empirical percentile of the implied peak daily factor-z (typically `V2X`) against the governed residual window. "
            "In `--imf-mode=macro_first`, this is an *output* (severity implied by the narrative), not a constraint.\n\n"
        )
        show = implied_df.copy()
        # compact formatting
        show["empirical_percentile"] = pd.to_numeric(show["empirical_percentile"], errors="coerce").round(4)
        show["v2x_peak_daily_z"] = pd.to_numeric(show["v2x_peak_daily_z"], errors="coerce").round(4)
        show["ladder_quantile"] = pd.to_numeric(show["ladder_quantile"], errors="coerce").round(4)
        narrative_md.append(_markdown_table(show.sort_values(["iso", "scenario_id"]).head(60), max_rows=60))
        narrative_md.append("\n\n")

    # Compute quarter-level factor z for macro-derived shocks.
    days_per_quarter = int(inv_diag.get("days_per_quarter") or 63)
    zq = _quarterly_factor_z_from_daily_shocks(factor_macro=factor_macro, inv_diag=inv_diag, days_per_quarter=days_per_quarter)

    # Compact transmission summary table: peak quarterly factor z per ISO/scenario/factor.
    zq_peaks = pd.DataFrame()
    if not zq.empty and {"iso", "scenario_id", "factor", "z_quarter"}.issubset(zq.columns):
        tmp = zq.copy()
        tmp["absz"] = pd.to_numeric(tmp["z_quarter"], errors="coerce").abs()
        zq_peaks = (
            tmp.groupby(["iso", "scenario_id", "factor"], dropna=False)
            .agg({"z_quarter": _peak_abs_signed, "absz": "max"})
            .reset_index()
            .rename(columns={"z_quarter": "peak_z_quarter", "absz": "peak_abs_z_quarter"})
        )
        zq_peaks = zq_peaks.sort_values(["scenario_id", "iso", "peak_abs_z_quarter"], ascending=[True, True, False])

    # Build implied target paths for all Step4 targets and write CSV.
    implied_rows: List[Dict[str, Any]] = []
    weight_rows: List[Dict[str, Any]] = []
    consistency_rows: List[Dict[str, Any]] = []

    if not zq.empty:
        loadings_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}

        # Load macro config (best-effort) for later consistency checks.
        macro_cfg_path = None
        if inv_diag.get("macro_config"):
            try:
                macro_cfg_path = Path(str(inv_diag.get("macro_config")))
                if not macro_cfg_path.is_absolute():
                    macro_cfg_path = PROJECT_ROOT / macro_cfg_path
            except Exception:
                macro_cfg_path = None
        macro_cfg = _read_yaml_or_json(macro_cfg_path) if macro_cfg_path else {}
        step10_2_cfg = macro_cfg.get("step10_2") if isinstance(macro_cfg.get("step10_2"), dict) else {}
        map_by_iso = step10_2_cfg.get("macro_to_target_by_iso") if isinstance(step10_2_cfg.get("macro_to_target_by_iso"), dict) else {}
        use_vars_default = step10_2_cfg.get("use_variables") if isinstance(step10_2_cfg.get("use_variables"), list) else []
        use_by_iso = step10_2_cfg.get("use_variables_by_iso") if isinstance(step10_2_cfg.get("use_variables_by_iso"), dict) else {}

        for iso in sorted(zq["iso"].unique().tolist()):
            iso_u = str(iso).upper()
            # Build quarter factor vector per scenario.
            iso_factors = sorted(zq.loc[zq["iso"] == iso_u, "factor"].astype(str).unique().tolist())
            factor_index = {f: j for j, f in enumerate(iso_factors)}

            step4 = _load_step4_quality(iso_u)
            if step4.empty:
                continue

            # Precompute expanded coefficient dict per target
            coef_by_target: Dict[str, Dict[str, float]] = {}
            for _, r in step4.iterrows():
                t = str(r.get("target"))
                coef = _parse_coefficients_blob(r.get("coefficients"))
                coef = _expand_pc_coefficients(iso=iso_u, coef_map=coef, loadings_cache=loadings_cache)
                coef_by_target[t] = coef

            # Compute interaction weights from the actual coefficient matrix used (factor_shocks_from_macro_mapping.csv).
            if not mapping_df.empty and {"iso", "target", "factor", "coef"}.issubset(mapping_df.columns):
                m_iso = mapping_df.loc[mapping_df["iso"].astype(str).str.upper() == iso_u].copy()
                if not m_iso.empty:
                    used_targets = sorted(set(m_iso["target"].astype(str).tolist()))
                    # Build B by pivoting mapping_df (already expanded in Step 10.2).
                    pivot = m_iso.pivot_table(index="target", columns="factor", values="coef", aggfunc="first").fillna(0.0)
                    # Align columns to iso_factors
                    pivot = pivot.reindex(columns=iso_factors, fill_value=0.0)
                    B = pivot.to_numpy(dtype=float)
                    if B.size > 0 and np.isfinite(B).all():
                        iso_diag = (inv_diag.get("iso") or {}).get(iso_u) if isinstance(inv_diag, dict) else None
                        lam_iso = _safe_float((iso_diag or {}).get("lam_effective"))
                        if lam_iso is None:
                            lam_iso = _safe_float(inv_diag.get("lam_default") if isinstance(inv_diag, dict) else None)
                        if lam_iso is None:
                            lam_iso = _safe_float(inv_diag.get("lam") if isinstance(inv_diag, dict) else None)
                        if lam_iso is None:
                            lam_iso = 2.0

                        P = _ridge_projection_weights(B, lam=float(lam_iso))  # (n_factors, n_targets)
                        key_factors = [f for f in ["V2X"] if f in factor_index]
                        for f in key_factors:
                            j = factor_index[f]
                            w = P[j, :]
                            order = np.argsort(np.abs(w))[::-1][:8]
                            for k in order:
                                if float(w[k]) == 0.0:
                                    continue
                                weight_rows.append(
                                    {
                                        "run_id": run_id,
                                        "iso": iso_u,
                                        "factor": f,
                                        "target": used_targets[int(k)],
                                        "weight": float(w[k]),
                                        "abs_weight": float(abs(w[k])),
                                        "lam": float(lam_iso),
                                    }
                                )

            # Implied target deltas quarter-by-quarter
            for scen in sorted(zq.loc[zq["iso"] == iso_u, "scenario_id"].unique().tolist()):
                sub = zq.loc[(zq["iso"] == iso_u) & (zq["scenario_id"] == str(scen))].copy()
                for q in sorted(sub["quarter"].unique().tolist()):
                    x = np.zeros((len(iso_factors),), dtype=float)
                    subq = sub.loc[sub["quarter"] == int(q)]
                    for _, rr in subq.iterrows():
                        x[factor_index[str(rr["factor"])]] = float(rr["z_quarter"])

                    for _, r in step4.iterrows():
                        tgt = str(r.get("target"))
                        coef = coef_by_target.get(tgt) or {}
                        if not coef:
                            continue
                        yhat = 0.0
                        for feat, beta in coef.items():
                            j = factor_index.get(feat)
                            if j is None:
                                continue
                            yhat += float(beta) * float(x[j])

                        if not np.isfinite(yhat):
                            continue
                        implied_rows.append(
                            {
                                "run_id": run_id,
                                "iso": iso_u,
                                "scenario_id": str(scen),
                                "quarter": int(q),
                                "target": tgt,
                                "delta_hat": float(yhat),
                                "test_r2": _safe_float(r.get("test_r2")),
                                "train_r2": _safe_float(r.get("train_r2")),
                                "macro_target": bool(r.get("macro_target", False)) if "macro_target" in r else False,
                            }
                        )

            # Consistency checks are computed after implied_df is constructed.

    implied_df = pd.DataFrame(implied_rows)
    weights_df = pd.DataFrame(weight_rows)

    # Consistency check: narrative macro deltas (variables inverted) vs implied deltas on mapped Step4 targets.
    macro_cfg_path = None
    if inv_diag.get("macro_config"):
        try:
            macro_cfg_path = Path(str(inv_diag.get("macro_config")))
            if not macro_cfg_path.is_absolute():
                macro_cfg_path = PROJECT_ROOT / macro_cfg_path
        except Exception:
            macro_cfg_path = None
    macro_cfg = _read_yaml_or_json(macro_cfg_path) if macro_cfg_path else {}
    step10_2_cfg = macro_cfg.get("step10_2") if isinstance(macro_cfg.get("step10_2"), dict) else {}
    map_by_iso = step10_2_cfg.get("macro_to_target_by_iso") if isinstance(step10_2_cfg.get("macro_to_target_by_iso"), dict) else {}
    use_vars_default = step10_2_cfg.get("use_variables") if isinstance(step10_2_cfg.get("use_variables"), list) else []
    use_by_iso = step10_2_cfg.get("use_variables_by_iso") if isinstance(step10_2_cfg.get("use_variables_by_iso"), dict) else {}

    if (not macro_used.empty) and (not implied_df.empty) and isinstance(map_by_iso, dict):
        macro_used_local = macro_used.copy()
        macro_used_local["iso"] = macro_used_local["iso"].astype(str).str.upper()
        macro_used_local["scenario_id"] = macro_used_local["scenario_id"].astype(str)
        macro_used_local["variable"] = macro_used_local["variable"].astype(str)
        macro_used_local["quarter"] = pd.to_numeric(macro_used_local["quarter"], errors="coerce")
        macro_used_local = macro_used_local.dropna(subset=["quarter"]).copy()
        macro_used_local["quarter"] = macro_used_local["quarter"].astype(int)

        implied_local = implied_df.copy()
        implied_local["iso"] = implied_local["iso"].astype(str).str.upper()
        implied_local["scenario_id"] = implied_local["scenario_id"].astype(str)
        implied_local["target"] = implied_local["target"].astype(str)

        for iso_u, iso_map in map_by_iso.items():
            if not isinstance(iso_map, dict):
                continue
            iso_u = str(iso_u).upper()
            used_vars = list(use_vars_default)
            if isinstance(use_by_iso, dict) and iso_u in use_by_iso and isinstance(use_by_iso.get(iso_u), list):
                used_vars = use_by_iso.get(iso_u)
            used_vars = [str(v) for v in used_vars]

            for scen in sorted(macro_used_local.loc[macro_used_local["iso"] == iso_u, "scenario_id"].unique().tolist()):
                for var in used_vars:
                    tgt = iso_map.get(var)
                    if not tgt:
                        continue
                    mu = macro_used_local.loc[
                        (macro_used_local["iso"] == iso_u)
                        & (macro_used_local["scenario_id"] == str(scen))
                        & (macro_used_local["variable"] == str(var))
                    ].copy()
                    if mu.empty:
                        continue
                    im = implied_local.loc[
                        (implied_local["iso"] == iso_u)
                        & (implied_local["scenario_id"] == str(scen))
                        & (implied_local["target"] == str(tgt))
                    ].copy()
                    if im.empty:
                        continue
                    merged = mu.merge(im, on=["iso", "scenario_id", "quarter"], how="inner", suffixes=("_macro", "_implied"))
                    if merged.empty:
                        continue
                    m = _corr_rmse(merged["delta"], merged["delta_hat"])

                    peak_idx = pd.to_numeric(merged["delta"], errors="coerce").abs().idxmax() if len(merged) else None
                    sign_match = None
                    if peak_idx is not None and peak_idx in merged.index:
                        a = float(merged.loc[peak_idx, "delta"])
                        b = float(merged.loc[peak_idx, "delta_hat"])
                        if np.isfinite(a) and np.isfinite(b) and abs(a) > 1e-12 and abs(b) > 1e-12:
                            sign_match = (a > 0) == (b > 0)

                    consistency_rows.append(
                        {
                            "iso": iso_u,
                            "scenario_id": str(scen),
                            "macro_variable": str(var),
                            "mapped_target": str(tgt),
                            "corr": float(m["corr"]),
                            "rmse": float(m["rmse"]),
                            "sign_match_at_peak": sign_match,
                        }
                    )

    consistency_df = pd.DataFrame(consistency_rows)
    implied_path = report_dir / "targets_implied_from_macro.csv"
    weights_path = report_dir / "macro_driver_interaction_weights.csv"
    consistency_path = report_dir / "macro_target_consistency.csv"
    if not implied_df.empty:
        implied_df.to_csv(implied_path, index=False)
    if not weights_df.empty:
        weights_df = weights_df.sort_values(["iso", "factor", "abs_weight"], ascending=[True, True, False])
        weights_df.to_csv(weights_path, index=False)
    if not consistency_df.empty:
        consistency_df.to_csv(consistency_path, index=False)

    # Severity vs consistency summary (deterministic diagnostic)
    severity_by_scenario = _scenario_severity_index(
        scen_def=scen_def,
        scaling_diag=scaling_diag,
        inv_diag=inv_diag,
        macro_design=macro_design,
    )
    consistency_summary = pd.DataFrame()
    if not consistency_df.empty and {"scenario_id", "corr", "rmse"}.issubset(consistency_df.columns):
        tmp = consistency_df.copy()
        tmp["corr"] = pd.to_numeric(tmp["corr"], errors="coerce")
        tmp["abs_corr"] = tmp["corr"].abs()
        tmp["rmse"] = pd.to_numeric(tmp["rmse"], errors="coerce")
        if "sign_match_at_peak" in tmp.columns:
            tmp["sign_match_at_peak"] = tmp["sign_match_at_peak"].astype("float")

        consistency_summary = (
            tmp.groupby(["scenario_id"], dropna=False)
            .agg(
                median_abs_corr=("abs_corr", "median"),
                median_corr=("corr", "median"),
                median_rmse=("rmse", "median"),
                sign_match_rate=("sign_match_at_peak", "mean"),
                n_pairs=("corr", "count"),
            )
            .reset_index()
        )
        if not severity_by_scenario.empty:
            consistency_summary = consistency_summary.merge(severity_by_scenario, on="scenario_id", how="left")

        # Write diagnostics for downstream analysis
        if not severity_by_scenario.empty:
            severity_by_scenario.to_csv(report_dir / "severity_index_by_scenario.csv", index=False)
        consistency_summary.to_csv(report_dir / "macro_target_consistency_summary.csv", index=False)

        # Optional plot
        if (not severity_by_scenario.empty) and ("severity_index" in consistency_summary.columns):
            try:
                import matplotlib.pyplot as plt  # type: ignore

                plots_dir = _ensure_dir(report_dir / "plots")
                fig, ax = plt.subplots(figsize=(7.8, 4.2))
                if "family" in consistency_summary.columns:
                    for fam, sub in consistency_summary.groupby("family", dropna=False):
                        ax.scatter(sub["severity_index"], sub["median_abs_corr"], label=str(fam), alpha=0.85)
                        for _, r in sub.iterrows():
                            ax.annotate(str(r["scenario_id"]), (r["severity_index"], r["median_abs_corr"]), fontsize=8, alpha=0.7)
                    ax.legend(loc="best", fontsize=9)
                else:
                    ax.scatter(consistency_summary["severity_index"], consistency_summary["median_abs_corr"], alpha=0.85)
                    for _, r in consistency_summary.iterrows():
                        ax.annotate(str(r["scenario_id"]), (r["severity_index"], r["median_abs_corr"]), fontsize=8, alpha=0.7)

                ax.set_title("Macro→target consistency vs scenario severity (diagnostic)")
                ax.set_xlabel("Severity index (quantile or implied percentile)")
                ax.set_ylabel("Median |corr| (narrative macro vs implied target)")
                ax.grid(True, alpha=0.25)
                fig.tight_layout()
                out = plots_dir / "consistency_vs_severity.png"
                fig.savefig(out, dpi=170)
                plt.close(fig)
                plot_paths.append(str(out))
            except Exception:
                pass

    # Target co-movement diagnostic: do correlations across targets increase with macro severity?
    macro_severity = _macro_severity_score_from_design(macro_design)
    target_comovement_iso = _target_comovement_summary(implied_df)
    target_comovement = pd.DataFrame()
    if not target_comovement_iso.empty:
        target_comovement = (
            target_comovement_iso.groupby(["scenario_id"], dropna=False)
            .agg(
                mean_abs_corr_median_iso=("mean_abs_corr", "median"),
                mean_corr_median_iso=("mean_corr", "median"),
                mean_abs_cov_median_iso=("mean_abs_cov", "median"),
                mean_cov_median_iso=("mean_cov", "median"),
                pct_abs_corr_ge_0_8_median_iso=("pct_abs_corr_ge_0_8", "median"),
                n_targets_median_iso=("n_targets", "median"),
                n_pairs_median_iso=("n_pairs", "median"),
                n_isos=("iso", "nunique"),
            )
            .reset_index()
        )
        if not macro_severity.empty:
            target_comovement = target_comovement.merge(macro_severity, on="scenario_id", how="left")
        # Write CSVs for downstream analysis
        target_comovement_iso.to_csv(report_dir / "target_comovement_by_iso.csv", index=False)
        target_comovement.to_csv(report_dir / "target_comovement_summary.csv", index=False)

        # Optional plot
        if ("macro_severity_score" in target_comovement.columns) and target_comovement["macro_severity_score"].notna().any():
            try:
                import matplotlib.pyplot as plt  # type: ignore

                plots_dir = _ensure_dir(report_dir / "plots")
                fig, ax = plt.subplots(figsize=(7.8, 4.2))
                x = pd.to_numeric(target_comovement["macro_severity_score"], errors="coerce")
                # Covariance is scale-sensitive; correlation is largely invariant if scenarios are just scaled templates.
                y = pd.to_numeric(target_comovement.get("mean_abs_cov_median_iso"), errors="coerce")
                ax.scatter(x, y, alpha=0.85)
                for _, r in target_comovement.iterrows():
                    ax.annotate(
                        str(r.get("scenario_id")),
                        (
                            float(pd.to_numeric(r.get("macro_severity_score"), errors="coerce")),
                            float(pd.to_numeric(r.get("mean_abs_cov_median_iso"), errors="coerce")),
                        ),
                        fontsize=8,
                        alpha=0.7,
                    )
                ax.set_title("Target co-movement vs macro severity (diagnostic; abs covariance)")
                ax.set_xlabel("Macro severity score (sum of abs peaks; median ISO)")
                ax.set_ylabel("Median ISO mean |cov| across targets")
                ax.grid(True, alpha=0.25)
                fig.tight_layout()
                out = plots_dir / "target_comovement_vs_severity.png"
                fig.savefig(out, dpi=170)
                plt.close(fig)
                plot_paths.append(str(out))
            except Exception:
                pass

    # Narrative markdown content
    narrative_md.append("## Driver interactions (what pushes what)\n")
    if weights_df.empty:
        narrative_md.append(
            "(No interaction weights available — likely because the macro→target mapping file was missing, or V2X was not in the ISO factor set used for inversion.)\n"
        )
    else:
        narrative_md.append(
            "This is a *linear accounting* of how changes in the macro targets used by Step 10.2 tend to load into the implied factor shocks under ridge inversion.\n"
            "Interpretation: a positive weight means that increasing that target delta increases the implied factor z; negative means it decreases it.\n"
        )
        show = weights_df.copy()
        show["weight"] = show["weight"].map(lambda x: f"{x:.4f}")
        narrative_md.append(_markdown_table(show[["iso", "factor", "target", "weight"]].head(30), max_rows=30))

    narrative_md.append("## Does the model-implied macro match the narrative?\n")
    if consistency_df.empty:
        narrative_md.append(
            "(No consistency diagnostics computed. If Step 10.2 was run with V2X anchoring, you may see conflicts where the ladder anchor forces V2X severity even when Step 4 macro-target mappings imply the opposite sign.)\n"
        )
    else:
        show = consistency_df.copy()
        show["corr"] = show["corr"].map(lambda x: "NA" if not np.isfinite(x) else f"{x:.2f}")
        show["rmse"] = show["rmse"].map(lambda x: "NA" if not np.isfinite(x) else f"{x:.3f}")
        narrative_md.append(
            "This compares the *input* macro deltas (the driver path you specified) to the *implied* Step 4 target deltas generated by the macro-derived factor shocks.\n"
            "Low correlation / sign mismatches mean the current Step 4 mapping cannot support that narrative *at the same time* as the anchored risk severity.\n"
        )
        narrative_md.append(_markdown_table(show[["iso", "scenario_id", "macro_variable", "mapped_target", "corr", "rmse", "sign_match_at_peak"]].head(40), max_rows=40))

        if not consistency_summary.empty:
            narrative_md.append("\n### Does consistency improve with severity? (diagnostic)\n")
            narrative_md.append(
                "As scenarios become more adverse (worse quantile / higher implied percentile), the driver signal tends to be larger. "
                "Often that increases macro→target correlation, but it can also expose structural mismatches in Step 4 mappings.\n\n"
            )
            show2 = consistency_summary.copy()
            if "severity_index" in show2.columns:
                show2 = show2.sort_values(["severity_index", "scenario_id"], ascending=[False, True])
            narrative_md.append(_markdown_table(show2[[c for c in ["family", "severity_index", "scenario_id", "median_abs_corr", "sign_match_rate", "median_rmse", "n_pairs"] if c in show2.columns]].head(30), max_rows=30))

    if not target_comovement.empty:
        narrative_md.append("\n## Do targets move more together under stress? (correlations go to 1)\n")
        narrative_md.append(
            "A classic stress-test stylized fact is that **cross-asset/target correlations rise in bad states** (systemic co-movement). "
            "This diagnostic computes, for each scenario, the off-diagonal correlation matrix of implied target deltas across quarters, then summarizes the average |corr|.\n\n"
            "Caveats: only a few quarters of deterministic path are available, and Step 4 is linear; treat this as a *directional* sanity check.\n\n"
        )
        show3 = target_comovement.copy()
        if "macro_severity_score" in show3.columns:
            show3["macro_severity_score"] = pd.to_numeric(show3["macro_severity_score"], errors="coerce").round(4)
            show3 = show3.sort_values(["macro_severity_score", "scenario_id"], ascending=[False, True])
        narrative_md.append(
            _markdown_table(
                show3[[c for c in ["scenario_id", "macro_severity_score", "mean_abs_corr_median_iso", "mean_abs_cov_median_iso", "pct_abs_corr_ge_0_8_median_iso", "n_targets_median_iso", "n_isos"] if c in show3.columns]].head(30),
                max_rows=30,
            )
        )

    narrative_md.append("## Forward behavior of targets (given the drivers)\n")
    if implied_df.empty:
        narrative_md.append("(No implied target projection could be computed.)\n")
    else:
        best = implied_df.copy()
        best["test_r2"] = pd.to_numeric(best.get("test_r2"), errors="coerce")

        top_targets_by_iso: Dict[str, List[str]] = {}
        # Auto: macro targets + a few highest-quality non-macro targets
        tgt_rank = (
            best.groupby(["iso", "target"], dropna=False)[["test_r2", "macro_target"]]
            .agg({"test_r2": "max", "macro_target": "max"})
            .reset_index()
        )
        tgt_rank = tgt_rank.sort_values(["iso", "macro_target", "test_r2"], ascending=[True, False, False])
        for iso in tgt_rank["iso"].unique().tolist():
            top_targets_by_iso[str(iso)] = tgt_rank.loc[tgt_rank["iso"] == iso, "target"].head(8).astype(str).tolist()

        # Create compact peak table by scenario/iso/target
        peak_rows: List[Dict[str, Any]] = []
        for (iso, scen, tgt), sub in implied_df.groupby(["iso", "scenario_id", "target"], dropna=False):
            if str(tgt) not in set(top_targets_by_iso.get(str(iso), [])):
                continue
            m = _peak_metrics(sub["delta_hat"])
            peak = float(m["max_abs"]) if np.isfinite(m["max_abs"]) else float("nan")
            peak_rows.append({"iso": iso, "scenario_id": scen, "target": tgt, "peak_delta_hat": peak})
        peaks = pd.DataFrame(peak_rows)
        if peaks.empty:
            narrative_md.append("(No peaks computed.)\n")
        else:
            peaks = peaks.sort_values(["scenario_id", "iso", "peak_delta_hat"], ascending=[True, True, False])
            narrative_md.append("Peak implied deltas (signed, at max |delta|) for a compact target set:\n")
            narrative_md.append(_markdown_table(peaks.head(40), max_rows=40))

        narrative_md.append(
            "\nPractical reading of the forward paths:\n"
            "- The macro narrative defines a *joint path* (coherent bundle) across GDP/CPI/UNRATE (and optionally policy/spreads/equity in Step 10.1).\n"
            "- Step 10.2 translates that bundle into the factor shocks we can simulate; targets not pinned by the narrative move endogenously via the Step 4 linear map.\n"
            "- Treat large moves on targets with low/negative Step 4 test R² as *low-confidence* projections.\n"
        )


    narrative_out = report_dir / "narrative_imf_findings.md"
    narrative_out.write_text("\n".join(narrative_md) + "\n", encoding="utf-8")

    # Readable plots: macro driver vs implied mapped target (with optional CI shading).
    more_plots = _try_make_readable_macro_vs_implied_plots(
        report_dir=report_dir,
        macro_used=macro_used,
        implied_df=implied_df,
        consistency_df=consistency_df,
    )
    plot_paths.extend(more_plots)

    one_pagers = _try_make_one_pagers(
        report_dir=report_dir,
        macro_used=macro_used,
        zq=zq,
        implied_df=implied_df,
        inv_diag=inv_diag,
    )
    plot_paths.extend(one_pagers)

    # Impact summary table: peak implied deltas per ISO/scenario/target
    impact_peaks = pd.DataFrame()
    if not implied_df.empty:
        tmp = implied_df.copy()
        tmp["absd"] = pd.to_numeric(tmp["delta_hat"], errors="coerce").abs()
        tmp["test_r2"] = pd.to_numeric(tmp.get("test_r2"), errors="coerce")
        tmp["macro_target"] = tmp.get("macro_target").fillna(False).astype(bool) if "macro_target" in tmp.columns else False
        impact_peaks = (
            tmp.groupby(["iso", "scenario_id", "target"], dropna=False)
            .agg({"delta_hat": _peak_abs_signed, "absd": "max", "test_r2": "max", "macro_target": "max"})
            .reset_index()
            .rename(columns={"delta_hat": "peak_delta_hat", "absd": "peak_abs_delta_hat"})
        )
        impact_peaks = impact_peaks.sort_values(
            ["scenario_id", "iso", "macro_target", "peak_abs_delta_hat", "test_r2"],
            ascending=[True, True, False, False, False],
        )

    # Assemble markdown report (deterministic; IMF/FSAP-style structure)
    md: List[str] = []
    md.append(f"# Explainable Scenario Report — {run_id}\n")
    md.append("## Executive summary\n")
    md.append(f"- ISOs: {', '.join(isos) if isos else 'NA'}")
    md.append(f"- Quantile scenarios (Step 10.0): {', '.join(scenarios) if scenarios else 'NA'}")
    md.append(f"- Narrative scenarios (Step 10.2{macro_suffix}): {', '.join(macro_scenarios) if macro_scenarios else 'NA'}")
    md.append("- Deterministic scenario compilation + explainability (no Monte Carlo in this chapter).\n")

    md.append("## What this run is (and is not)\n")
    for n in econ_notes:
        md.append(f"- {n}")
    md.append("\n")

    # --- Scenario design ---
    md.append("## Scenario design (narrative macro paths)\n")
    if macro_peaks.empty:
        md.append("(No macro_narrative_paths.csv found or empty.)\n")
    else:
        show = macro_peaks.copy()
        show["abs_peak"] = pd.to_numeric(show["peak"], errors="coerce").abs()
        show = show.sort_values(["scenario_id", "variable", "abs_peak"], ascending=[True, True, False])
        md.append("Peak delta per ISO/variable (signed value at max |delta|):\n")
        md.append(_markdown_table(show[["iso", "scenario_id", "variable", "peak"]].head(30), max_rows=30))
        md.append(
            "Note: the narrative templates in this repo are typically *front-loaded* and then *recover* toward baseline, "
            "so lines in later quarters often converge by design (deltas move back toward 0).\n"
        )

    if not plaus_df.empty:
        md.append("### Plausibility check (rough, IMF-style heuristics)\n")
        md.append(
            "These ranges are *heuristic* and meant as a quick smell-test (magnitude + sign), not a validation. "
            "IMF/FSAP adverse scenarios are typically multi-year with GDP below baseline and unemployment above baseline; inflation response is regime-dependent.\n"
        )
        md.append(_markdown_table(plaus_df, max_rows=30))

    # --- Transmission ---
    md.append("## Transmission (macro → factors)\n")
    if det_focus.empty:
        md.append("(No Step 10.0 factor_shocks.csv found or empty.)\n")
    else:
        md.append("Step 10.0 (quantile family) — top shocked factors by scenario (sum abs across ISOs):\n")
        md.append(_markdown_table(det_focus.groupby("scenario_id").head(8)))

    if status_summary.empty:
        md.append("Step 10.2 inversion status: (missing)\n")
    else:
        md.append("Step 10.2 inversion status counts (should be all ok; otherwise investigate):\n")
        md.append(_markdown_table(status_summary, max_rows=40))

    if zq_peaks.empty:
        md.append("Peak implied factor shocks (quarter z): (not available)\n")
    else:
        md.append("Peak implied factor shocks (quarter z; signed at max |z|):\n")
        md.append(_markdown_table(zq_peaks[["iso", "scenario_id", "factor", "peak_z_quarter"]].head(40), max_rows=40))

    if weights_df.empty:
        md.append("Driver interaction weights (V2X): (not available)\n")
    else:
        show = weights_df.copy()
        show["weight"] = pd.to_numeric(show["weight"], errors="coerce").map(lambda x: "NA" if not np.isfinite(x) else f"{x:.4f}")
        md.append("Driver interaction weights (linear accounting; larger |weight| = stronger push):\n")
        md.append(_markdown_table(show[["iso", "factor", "target", "weight"]].head(30), max_rows=30))

    # --- Impact ---
    md.append("## Impact (implied Step 4 targets)\n")
    if impact_peaks.empty:
        md.append("(No implied target projections computed.)\n")
    else:
        md.append("Peak implied deltas for a compact set of targets (ranked by macro_target, then |peak|, then test R²):\n")
        md.append(_markdown_table(impact_peaks[["iso", "scenario_id", "target", "peak_delta_hat", "test_r2", "macro_target"]].head(60), max_rows=60))

    # --- Sensitivity & limitations ---
    md.append("## Sensitivity & limitations\n")

    md.append("### Step 4 macro target quality (out-of-sample R²)\n")
    if step4_summary.empty:
        md.append("(Missing Step 4 feature contribution summaries.)\n")
    else:
        q = step4_summary.copy()
        q = q.loc[q["macro_target"].fillna(False).astype(bool)].copy()
        q["test_r2"] = pd.to_numeric(q["test_r2"], errors="coerce")
        q["train_r2"] = pd.to_numeric(q["train_r2"], errors="coerce")
        q = q.sort_values(["iso", "test_r2"], ascending=[True, True])
        md.append(
            "Method summary: Step 4 uses a single **chronological holdout split** (default train_ratio=0.7): first ~70% of usable monthly observations are in-sample, last ~30% are out-of-sample.\n"
            "For macro targets, it uses **walk-forward (expanding window) cross-validation** on the training set to pick regularization strength (min_train_size=12 months).\n"
            "Interpretation: $R^2<0$ means the mapping is worse than a mean forecast; Step 10.2 should refuse those targets when min_test_r2=0.\n"
        )
        show_cols = [
            "iso",
            "target",
            "test_r2",
            "train_r2",
            "n_obs_total",
            "n_train",
            "n_test",
            "train_start",
            "test_start",
            "target_transform",
            "feature_source_used",
        ]
        show_cols = [c for c in show_cols if c in q.columns]
        md.append(_markdown_table(q[show_cols].head(40), max_rows=40))

        md.append("\n### Low-confidence targets (test R² < 0)\n")
        q_neg = q.copy()
        q_neg["test_r2"] = pd.to_numeric(q_neg.get("test_r2"), errors="coerce")
        q_neg = q_neg.dropna(subset=["test_r2"])
        q_neg = q_neg.loc[q_neg["test_r2"] < 0].copy()
        if q_neg.empty:
            md.append("(None detected among Step 4 macro targets in this run.)\n")
        else:
            md.append(
                "Targets with negative out-of-sample $R^2$ have **no predictive skill relative to a mean forecast** on the held-out period. "
                "They can still appear in the implied forward projections (because Step 4 defines them), but they should be treated as **interpretation cautions**, not reliable scenario claims.\n"
            )
            neg_cols = ["iso", "target", "test_r2", "train_r2", "target_transform", "feature_source_used"]
            neg_cols = [c for c in neg_cols if c in q_neg.columns]
            q_neg = q_neg.sort_values(["test_r2"], ascending=True)
            md.append(_markdown_table(q_neg[neg_cols].head(30), max_rows=30))

    md.append("### Macro-target consistency (narrative vs implied)\n")
    if consistency_df.empty:
        md.append("(No consistency diagnostics computed.)\n")
    else:
        show = consistency_df.copy()
        show["corr"] = pd.to_numeric(show["corr"], errors="coerce")
        show["rmse"] = pd.to_numeric(show["rmse"], errors="coerce")
        show = show.sort_values(["scenario_id", "iso", "corr"], ascending=[True, True, True])
        md.append(_markdown_table(show.head(40), max_rows=40))

        if not consistency_summary.empty:
            md.append("\n### Severity vs consistency (does correlation rise in adverse scenarios?)\n")
            md.append(
                "This is a run-level diagnostic: in more severe scenarios, correlations often increase because the macro driver signal is larger. "
                "If correlations do *not* increase (or flip sign), it is a red flag that the Step 4 mapping cannot support the narrative under stress.\n\n"
            )
            show2 = consistency_summary.copy()
            if "severity_index" in show2.columns:
                show2 = show2.sort_values(["severity_index", "scenario_id"], ascending=[False, True])
            cols = [c for c in ["family", "severity_index", "scenario_id", "median_abs_corr", "median_corr", "sign_match_rate", "median_rmse", "n_pairs"] if c in show2.columns]
            md.append(_markdown_table(show2[cols].head(30), max_rows=30))

    if not target_comovement.empty:
        md.append("\n### Target co-movement vs severity (systemic correlation)\n")
        md.append(
            "This is a scenario-level systemic diagnostic: it summarizes **average off-diagonal correlation across implied target deltas**. "
            "If this rises with macro severity, it supports the narrative that stress makes outcomes more tightly linked.\n"
        )
        show3 = target_comovement.copy()
        if "macro_severity_score" in show3.columns:
            show3["macro_severity_score"] = pd.to_numeric(show3["macro_severity_score"], errors="coerce").round(4)
            show3 = show3.sort_values(["macro_severity_score", "scenario_id"], ascending=[False, True])
        cols3 = [c for c in ["scenario_id", "macro_severity_score", "mean_abs_corr_median_iso", "mean_abs_cov_median_iso", "pct_abs_corr_ge_0_8_median_iso", "n_targets_median_iso", "n_isos"] if c in show3.columns]
        md.append(_markdown_table(show3[cols3].head(30), max_rows=30))

    md.append("## Economic interpretation vs IMF / FSAP stylized facts\n")
    md.append(
        "What the literature typically expects in a global risk-off / adverse macrofinancial scenario:\n"
        "- Financial conditions tighten: volatility/risk premia up, equity down, spreads up, funding strains.\n"
        "- Real activity falls below baseline: GDP growth down (or level below baseline), unemployment up.\n"
        "- Policy reacts: short rates often fall (or rise under inflation-dominance); inflation can go either way.\n\n"
        "How to read *this* run relative to that:\n"
        "- Step 10.0 shocks are currently very sparse and centered on V2X (by design). That matches the ‘risk-off’ trigger commonly used in stress test narratives.\n"
        "- Step 10.1 macro paths are stylized and severity-scaled; they resemble the *shape* used in FSAP/IMF narratives (front-loaded deterioration, gradual recovery), but they are not calibrated to a country macro model.\n"
        "- Step 10.2 inversion is best read as a *translation layer* into the factor space we can simulate, not as proof that those factors structurally cause the macro outcomes.\n"
    )

    md.append("## Limitations (actionable)\n")
    md.append(
        "- **Factor set coverage:** if the governed factor universe is dominated by slow-moving balance sheet series, the implied factor shocks will look ‘weird’ compared to IMF factor decompositions.\n"
        "- **GDP mapping remains fragile in some ISOs:** even after transforms and AR lags, GDP is hard to predict out-of-sample. Treat GDP as narrative-only unless Step 4 improves.\n"
        "- **Model is linear and static:** Step 4 is not a dynamic macro-financial model; regime changes and nonlinearities are not captured.\n"
    )

    if plot_paths:
        md.append("\n## Plots\n")
        md.append("Generated under reports/plots/. (These are meant to replace dozens of raw lists.)\n")
        for p in plot_paths:
            rel = Path(p).resolve().relative_to(PROJECT_ROOT)
            md.append(f"- {rel.as_posix()}")

    out_md = report_dir / "explainable_report.md"
    out_md.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] Wrote: {out_md}")
    print(f"[OK] Wrote: {narrative_out}")
    if not implied_df.empty:
        print(f"[OK] Wrote: {implied_path}")
    if not weights_df.empty:
        print(f"[OK] Wrote: {weights_path}")
    if not consistency_df.empty:
        print(f"[OK] Wrote: {consistency_path}")
    if plot_paths:
        print(f"[OK] Wrote plots under: {report_dir / 'plots'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
