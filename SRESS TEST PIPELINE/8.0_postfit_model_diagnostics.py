"""Step 8.0 — Post-fit diagnostics and stress-test readiness gate.

Consumes the blockwise/global outputs produced by Step 7.2 (DCC-GARCH fit) and
writes a consolidated diagnostics bundle so we can decide whether the model is
usable for stress testing.

Outputs (default under analysis_outputs/postfit_model_diagnostics/)
- block_postfit_diagnostics.csv
- postfit_summary.json
- postfit_report.md

This step is intentionally lightweight: it reuses already-produced outputs and
avoids recomputing full correlation paths.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class BlockJudgement:
    block_id: str
    label: str
    n_series: int
    status: str
    judgement: str
    reasons: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Post-fit diagnostics + readiness gate (Step 8.0)")
    parser.add_argument(
        "--dcc-results-dir",
        default=str(PROJECT_ROOT / "DCC GARCH MODEL" / "results"),
        help="Path to DCC GARCH MODEL results directory",
    )
    parser.add_argument(
        "--output-dir",
        default=str(PROJECT_ROOT / "analysis_outputs" / "postfit_model_diagnostics"),
        help="Where to write diagnostics outputs",
    )
    parser.add_argument(
        "--min-eigen",
        type=float,
        default=0.01,
        help="Minimum acceptable eigenvalue for unconditional correlation matrices",
    )
    parser.add_argument(
        "--min-update-success",
        type=float,
        default=0.95,
        help="Minimum acceptable update_success_rate for DCC recursion",
    )
    parser.add_argument(
        "--min-garch-convergence",
        type=float,
        default=0.5,
        help="Minimum acceptable GARCH convergence rate per block",
    )
    parser.add_argument(
        "--gamma-warn",
        type=float,
        default=0.7,
        help="Gamma threshold above which ADCC asymmetry is flagged",
    )
    return parser.parse_args()


def _safe_read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safe_read_csv(path: Path, **kwargs) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return pd.DataFrame()


def _min_eigen_from_corr(csv_path: Path) -> float | None:
    df = _safe_read_csv(csv_path, index_col=0)
    if df.empty:
        return None
    try:
        mat = df.values.astype(float)
        mat = (mat + mat.T) / 2
        np.fill_diagonal(mat, 1.0)
        w = np.linalg.eigvalsh(mat)
        return float(np.min(w))
    except Exception:
        return None


def _fitted_series_from_residuals(block_dir: Path) -> list[str]:
    """Infer which series were actually fitted for a block.

    We rely on standardized_residuals.csv because it is produced for both
    univariate-only and multivariate runs, and its header is cheap to read.
    """

    residuals_path = block_dir / "standardized_residuals.csv"
    if not residuals_path.exists():
        return []
    try:
        header = pd.read_csv(residuals_path, nrows=0)
        cols = [c for c in header.columns if c and c != "Unnamed: 0"]
        return [str(c) for c in cols]
    except Exception:
        return []


def judge_block(row: pd.Series, args: argparse.Namespace, block_min_eig: float | None) -> BlockJudgement:
    block_id = str(row.get("block_id") or row.get("label") or "")
    label = str(row.get("label") or block_id)
    n_series = int(row.get("n_series") or 0)
    status = str(row.get("status") or "")

    reasons: list[str] = []

    if status != "completed":
        reasons.append(f"status={status}")

    model_used = str(row.get("correlation_model_used") or "")
    if model_used == "univariate_only":
        if bool(row.get("garch_quality_gate_triggered")):
            reasons.append("univariate: garch_quality_gate_triggered")
        judgement = "WARN" if reasons else "PASS"
        return BlockJudgement(block_id=block_id, label=label, n_series=n_series, status=status, judgement=judgement, reasons=reasons)

    # Multivariate block expectations
    update_success = row.get("update_success_rate")
    try:
        update_success_val = float(update_success) if update_success is not None else None
    except Exception:
        update_success_val = None

    garch_conv = row.get("garch_convergence_rate")
    try:
        garch_conv_val = float(garch_conv) if garch_conv is not None else None
    except Exception:
        garch_conv_val = None

    a_plus_b = row.get("a_plus_b")
    try:
        a_plus_b_val = float(a_plus_b) if a_plus_b is not None else None
    except Exception:
        a_plus_b_val = None

    gamma = row.get("adcc_gamma")
    try:
        gamma_val = float(gamma) if gamma is not None else None
    except Exception:
        gamma_val = None

    if update_success_val is None or update_success_val < args.min_update_success:
        reasons.append(f"update_success_rate<{args.min_update_success}")

    if garch_conv_val is None or garch_conv_val < args.min_garch_convergence:
        reasons.append(f"garch_convergence_rate<{args.min_garch_convergence}")

    if a_plus_b_val is not None and a_plus_b_val >= 0.98:
        reasons.append("high_persistence(a+b>=0.98)")

    if bool(row.get("dcc_a_at_upper_bound")):
        reasons.append("dcc_a_at_upper_bound")

    if gamma_val is not None and gamma_val >= args.gamma_warn:
        reasons.append(f"high_gamma(g>={args.gamma_warn})")

    # Floating point tolerance: eigenvalues computed via numpy can land microscopically
    # below the theoretical target (e.g. 0.00999999999999999 vs 0.01).
    eig_tol = 1e-12
    if block_min_eig is None:
        reasons.append("missing_unconditional_corr")
    elif block_min_eig + eig_tol < args.min_eigen:
        reasons.append(f"min_eigen<{args.min_eigen}")

    # Fail conditions are the hard gates; other items are warnings.
    hard_fail = False
    if update_success_val is None or update_success_val < args.min_update_success:
        hard_fail = True
    if garch_conv_val is None or garch_conv_val < args.min_garch_convergence:
        hard_fail = True
    if block_min_eig is None or (block_min_eig + eig_tol < args.min_eigen):
        hard_fail = True

    judgement = "FAIL" if hard_fail else ("WARN" if reasons else "PASS")
    return BlockJudgement(block_id=block_id, label=label, n_series=n_series, status=status, judgement=judgement, reasons=reasons)


def _warn_detail(row: pd.Series, block_dir: Path) -> str:
    """Build a concise, actionable detail string for WARN blocks."""

    fitted = _fitted_series_from_residuals(block_dir)
    fitted_str = ";".join(fitted) if fitted else ""

    missing = str(row.get("missing_series") or "")
    dupes = str(row.get("already_assigned") or "")

    a_val = row.get("dcc_a")
    max_a = row.get("dcc_max_a")
    try:
        a_num = float(a_val) if a_val is not None and a_val != "" else None
    except Exception:
        a_num = None
    try:
        max_a_num = float(max_a) if max_a is not None and max_a != "" else None
    except Exception:
        max_a_num = None

    parts: list[str] = []
    if bool(row.get("dcc_a_at_upper_bound")):
        if a_num is not None and max_a_num is not None:
            parts.append(f"dcc_a_at_upper_bound(a={a_num:.6g},cap={max_a_num:.6g})")
        else:
            parts.append("dcc_a_at_upper_bound")

    if fitted_str:
        parts.append(f"fitted={fitted_str}")
    if missing:
        parts.append(f"missing={missing}")
    if dupes:
        parts.append(f"dupes={dupes}")

    return " | ".join(parts)


def _describe_series(s: pd.Series) -> dict:
    s = pd.to_numeric(s, errors="coerce")
    s_clean = s.dropna()
    if s_clean.empty:
        return {
            "n": int(s.shape[0]),
            "n_nonnull": 0,
            "min": None,
            "p01": None,
            "p05": None,
            "median": None,
            "p95": None,
            "p99": None,
            "max": None,
            "mean": None,
            "std": None,
            "pct_abs_gt_095": None,
            "pct_abs_gt_099": None,
        }

    q = s_clean.quantile([0.01, 0.05, 0.5, 0.95, 0.99]).to_dict()
    abs_s = s_clean.abs()
    return {
        "n": int(s.shape[0]),
        "n_nonnull": int(s_clean.shape[0]),
        "min": float(s_clean.min()),
        "p01": float(q.get(0.01)),
        "p05": float(q.get(0.05)),
        "median": float(q.get(0.5)),
        "p95": float(q.get(0.95)),
        "p99": float(q.get(0.99)),
        "max": float(s_clean.max()),
        "mean": float(s_clean.mean()),
        "std": float(s_clean.std(ddof=0)),
        "pct_abs_gt_095": float((abs_s > 0.95).mean()),
        "pct_abs_gt_099": float((abs_s > 0.99).mean()),
    }


def _write_warn_corr_sanity(warn_blocks: list[str], blocks_root: Path, out_dir: Path) -> None:
    """Write correlation sanity stats for WARN blocks.

    This is a diagnostics helper only. It reads the *actual* correlation columns from
    each block's correlation_time_series.csv so the reported pairs match what the
    fitter produced.
    """

    rows: list[dict] = []
    json_out: dict[str, dict] = {}

    for block_id in warn_blocks:
        block_dir = blocks_root / block_id
        corr_path = block_dir / "correlation_time_series.csv"
        fitted = _fitted_series_from_residuals(block_dir)
        fitted_str = ";".join(fitted) if fitted else ""

        if not corr_path.exists():
            rows.append({"block_id": block_id, "status": "missing", "pair": None, "fitted_series": fitted_str})
            json_out[block_id] = {"status": "missing", "path": str(corr_path), "fitted_series": fitted}
            continue

        try:
            corr_ts = pd.read_csv(corr_path, index_col=0, parse_dates=True)
        except Exception:
            corr_ts = pd.DataFrame()

        if corr_ts.empty or corr_ts.shape[1] == 0:
            rows.append({"block_id": block_id, "status": "empty", "pair": None, "fitted_series": fitted_str})
            json_out[block_id] = {"status": "empty", "path": str(corr_path), "fitted_series": fitted}
            continue

        block_summary = {
            "status": "ok",
            "path": str(corr_path),
            "fitted_series": fitted,
            "pairs": {},
        }

        numeric_cols = [str(c) for c in corr_ts.columns if str(c).strip()]
        for col in numeric_cols:
            stats = _describe_series(corr_ts[col])
            rows.append({"block_id": block_id, "status": "ok", "pair": col, "fitted_series": fitted_str, **stats})
            block_summary["pairs"][col] = stats

        stacked = pd.concat([pd.to_numeric(corr_ts[c], errors="coerce") for c in numeric_cols], axis=0, ignore_index=True)
        block_summary["block_level"] = _describe_series(stacked)
        json_out[block_id] = block_summary

    (out_dir / "warn_corr_sanity.csv").write_text(pd.DataFrame(rows).to_csv(index=False), encoding="utf-8")
    (out_dir / "warn_corr_sanity.json").write_text(json.dumps(json_out, indent=2), encoding="utf-8")


def main() -> None:
    args = parse_args()
    dcc_results = Path(args.dcc_results_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Primary sources
    summary_csv = dcc_results / "block_fit_summary.csv"
    metrics_json = dcc_results / "block_fit_metrics.json"
    fit_summary_json = dcc_results / "fit_summary.json"
    fit_metrics_json = dcc_results / "fit_metrics.json"

    df = _safe_read_csv(summary_csv)
    if df.empty:
        raise FileNotFoundError(f"Missing/empty: {summary_csv}")

    blocks_root = dcc_results / "blocks"

    rows = []
    judgements: list[BlockJudgement] = []

    for _, row in df.iterrows():
        block_id = str(row.get("block_id") or "")
        block_dir = blocks_root / block_id

        block_min_eig = _min_eigen_from_corr(block_dir / "unconditional_correlation_matrix.csv")

        # GARCH per-series loglike, convergence, alpha+beta
        garch_params = _safe_read_csv(block_dir / "dcc_garch_parameters.csv", index_col=0)
        if not garch_params.empty:
            for c in ["loglikelihood", "alpha_beta_sum"]:
                if c in garch_params.columns:
                    garch_params[c] = pd.to_numeric(garch_params[c], errors="coerce")

        garch_ll_mean = float(garch_params["loglikelihood"].mean()) if (not garch_params.empty and "loglikelihood" in garch_params.columns) else None
        garch_ll_median = float(garch_params["loglikelihood"].median()) if (not garch_params.empty and "loglikelihood" in garch_params.columns) else None
        garch_ab_mean = float(garch_params["alpha_beta_sum"].mean()) if (not garch_params.empty and "alpha_beta_sum" in garch_params.columns) else None

        judgement = judge_block(row, args, block_min_eig)
        judgements.append(judgement)

        warn_detail = ""
        if judgement.judgement == "WARN":
            warn_detail = _warn_detail(row, block_dir)

        record = dict(row)
        record.update(
            {
                "min_eigen_unconditional_corr": block_min_eig,
                "garch_loglikelihood_mean": garch_ll_mean,
                "garch_loglikelihood_median": garch_ll_median,
                "garch_alpha_beta_sum_mean": garch_ab_mean,
                "readiness_judgement": judgement.judgement,
                "readiness_reasons": ";".join(judgement.reasons),
                "readiness_warn_detail": warn_detail,
            }
        )
        rows.append(record)

    out_csv = out_dir / "block_postfit_diagnostics.csv"
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    # Aggregate summary
    judgement_counts = pd.Series([j.judgement for j in judgements]).value_counts().to_dict()
    n_blocks = len(judgements)
    n_fail = int(judgement_counts.get("FAIL", 0))
    n_warn = int(judgement_counts.get("WARN", 0))
    ready = bool(n_fail == 0)

    extra = {
        "block_fit_metrics": _safe_read_json(metrics_json),
        "fit_summary": _safe_read_json(fit_summary_json),
        "fit_metrics": _safe_read_json(fit_metrics_json),
    }

    summary = {
        "timestamp": datetime.now().isoformat(),
        "dcc_results_dir": str(dcc_results),
        "output_dir": str(out_dir),
        "blocks": n_blocks,
        "judgement_counts": judgement_counts,
        "ready_for_stress_testing": ready,
        "notes": {
            "min_eigen_floor": args.min_eigen,
            "min_update_success": args.min_update_success,
            "min_garch_convergence": args.min_garch_convergence,
            "gamma_warn": args.gamma_warn,
        },
        "high_level": {
            "hard_fail_blocks": [j.block_id for j in judgements if j.judgement == "FAIL"],
            "warn_blocks": [j.block_id for j in judgements if j.judgement == "WARN"],
        },
    }

    out_json = out_dir / "postfit_summary.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    # Report markdown
    report_lines = []
    report_lines.append(f"# Post-fit diagnostics (Step 8)\n")
    report_lines.append(f"Generated: {summary['timestamp']}\n")
    report_lines.append(f"Source: {dcc_results}\n")
    report_lines.append(f"\n## Readiness verdict\n")
    report_lines.append(f"- Ready for stress testing: **{ready}**\n")
    report_lines.append(f"- Blocks: {n_blocks} (FAIL={n_fail}, WARN={n_warn})\n")

    if n_fail:
        report_lines.append("\n### FAIL blocks\n")
        for j in judgements:
            if j.judgement == "FAIL":
                report_lines.append(f"- {j.block_id}: {j.label} — {', '.join(j.reasons)}\n")

    if n_warn:
        report_lines.append("\n### WARN blocks\n")
        for j in judgements:
            if j.judgement == "WARN":
                report_lines.append(f"- {j.block_id}: {j.label} — {', '.join(j.reasons)}\n")

        # Extra diagnostics for WARN blocks: correlation sanity report.
        _write_warn_corr_sanity([j.block_id for j in judgements if j.judgement == "WARN"], blocks_root, out_dir)

    report_lines.append("\n## Notes\n")
    report_lines.append("This report summarizes parameter stability (a,b,g), persistence, GARCH convergence/loglike, and unconditional correlation eigenvalues.\n")

    (out_dir / "postfit_report.md").write_text("".join(report_lines), encoding="utf-8")

    print(f"[Step 8] Wrote: {out_csv}")
    print(f"[Step 8] Wrote: {out_json}")
    print(f"[Step 8] Wrote: {out_dir / 'postfit_report.md'}")


if __name__ == "__main__":
    main()
