#!/usr/bin/env python3
"""Step 7 report-only evaluator for PCA/FCI settings.

Runs a small grid of PCA/FCI configurations (6–10 candidates), re-runs:
  - Step 3 monthly factor preparation (governed, uses frozen blocks)
  - Step 4 Lasso mapping diagnostics
  - Phase 3 ISO ADCC diagnostics (for persistence + min eigen)

and writes a ranked table under analysis_outputs/ without modifying any repo configs.

Notes:
- This script does NOT change config/factor_settings.yaml.
- It writes candidate YAMLs under analysis_outputs/pca_fci_tuning/<run_id>/candidates/.
- It copies key artifacts per candidate so results are preserved even though the
  underlying pipeline writes to fixed analysis_outputs locations.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import shutil
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BASE_FACTOR_SETTINGS = PROJECT_ROOT / "config" / "factor_settings.yaml"

MONTHLY_STEP3 = PROJECT_ROOT / "scripts" / "prepare_country_factors.py"
STEP4_LASSO = PROJECT_ROOT / "SRESS TEST PIPELINE" / "4.0_lasso_pipeline.py"
PHASE3_ADCC = PROJECT_ROOT / "SRESS TEST PIPELINE" / "iso_adcc_diagnostics.py"

FACTOR_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation"
FEATURE_CONTRIB = PROJECT_ROOT / "analysis_outputs" / "feature_contributions"
DIAG_CORR_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr"
ADCC_META_DIR = PROJECT_ROOT / "models" / "adcc"


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp) or {}
    return payload if isinstance(payload, dict) else {}


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        yaml.safe_dump(payload, fp, sort_keys=False)


def _deep_update(base: dict, updates: dict) -> dict:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_update(base[key], value)
        else:
            base[key] = value
    return base


def _run(cmd: List[str], label: str, *, timeout_s: int | None = None, allow_fail: bool = False) -> None:
    print(f"\n--- {label} ---")
    print(" ".join(cmd))
    try:
        subprocess.run(cmd, cwd=PROJECT_ROOT, check=not allow_fail, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        msg = f"Timed out after {timeout_s}s: {label}" if timeout_s else f"Timed out: {label}"
        if allow_fail:
            print(msg)
            return
        raise


def _safe_read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _copy_if_exists(src: Path, dst_dir: Path) -> None:
    if not src.exists():
        return
    dst_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst_dir / src.name)


def _summarize_step4(iso: str) -> Dict[str, float]:
    # Step 4 writes analysis_outputs/feature_contributions_{ISO}.csv
    summary_path = PROJECT_ROOT / "analysis_outputs" / f"feature_contributions_{iso}.csv"
    if not summary_path.exists():
        return {
            "step4_targets": 0.0,
            "step4_mean_cv_r2": float("nan"),
            "step4_mean_test_r2": float("nan"),
            "step4_instability_rate": float("nan"),
            "step4_overfitting_rate": float("nan"),
            "step4_eigen_warn_rate": float("nan"),
            "step4_condition_rate": float("nan"),
        }

    df = pd.read_csv(summary_path)
    if df.empty:
        return {
            "step4_targets": 0.0,
            "step4_mean_cv_r2": float("nan"),
            "step4_mean_test_r2": float("nan"),
            "step4_instability_rate": float("nan"),
            "step4_overfitting_rate": float("nan"),
            "step4_eigen_warn_rate": float("nan"),
            "step4_condition_rate": float("nan"),
        }

    def _mean_bool(col: str) -> float:
        if col not in df.columns:
            return float("nan")
        s = df[col]
        # tolerate True/False strings
        s = s.replace({"True": True, "False": False})
        try:
            return float(pd.to_numeric(s, errors="coerce").fillna(0).mean())
        except Exception:
            return float("nan")

    return {
        "step4_targets": float(len(df)),
        "step4_mean_cv_r2": float(pd.to_numeric(df.get("mean_cv_r2"), errors="coerce").mean()),
        "step4_mean_test_r2": float(pd.to_numeric(df.get("test_r2"), errors="coerce").mean()),
        "step4_instability_rate": _mean_bool("instability_flag"),
        "step4_overfitting_rate": _mean_bool("overfitting_flag"),
        "step4_eigen_warn_rate": _mean_bool("eigen_warning"),
        "step4_condition_rate": _mean_bool("condition_flag"),
    }


def _summarize_adcc(iso: str) -> Dict[str, float]:
    meta_path = ADCC_META_DIR / f"{iso}_adcc.json"
    meta = _safe_read_json(meta_path)
    persistence = meta.get("persistence")
    min_eigen = meta.get("min_eigen")
    return {
        "adcc_persistence": float(persistence) if persistence is not None else float("nan"),
        "adcc_min_eigen": float(min_eigen) if min_eigen is not None else float("nan"),
    }


def _summarize_complexity(iso: str) -> Dict[str, float]:
    factors_path = FACTOR_DIR / f"{iso}_factors.csv"
    pca_path = FACTOR_DIR / f"{iso}_pca_components.csv"

    fci_included = 0.0
    if factors_path.exists():
        try:
            header = pd.read_csv(factors_path, nrows=0)
            cols = list(header.columns)
            fci_included = 1.0 if any(str(c).startswith(f"FCI_{iso}") for c in cols) else 0.0
        except Exception:
            fci_included = 0.0

    pc_count = 0.0
    if pca_path.exists():
        try:
            header = pd.read_csv(pca_path, nrows=0)
            # index column counted in header; subtract 1 if unnamed index
            cols = [c for c in header.columns if not str(c).startswith("Unnamed")]
            # Heuristic: if index was saved, first column likely date; keep all others
            pc_count = float(max(0, len(cols) - 1))
        except Exception:
            pc_count = 0.0

    return {"pc_count": pc_count, "fci_included": fci_included}


def _score(metrics: Dict[str, float]) -> Tuple[float, Dict[str, float]]:
    mean_cv_r2 = metrics.get("step4_mean_cv_r2", float("nan"))
    mean_test_r2 = metrics.get("step4_mean_test_r2", float("nan"))
    instability = metrics.get("step4_instability_rate", float("nan"))
    overfit = metrics.get("step4_overfitting_rate", float("nan"))
    eigen_warn = metrics.get("step4_eigen_warn_rate", float("nan"))
    condition = metrics.get("step4_condition_rate", float("nan"))

    persistence = metrics.get("adcc_persistence", float("nan"))
    min_eigen = metrics.get("adcc_min_eigen", float("nan"))

    pc_count = metrics.get("pc_count", 0.0)
    fci_included = metrics.get("fci_included", 0.0)

    def nz(x: float, fallback: float) -> float:
        return fallback if pd.isna(x) else float(x)

    # Step 4 mapping quality (primary)
    mapping_score = (
        nz(mean_cv_r2, -1.0)
        + 0.30 * nz(mean_test_r2, -1.0)
        - 0.25 * nz(instability, 1.0)
        - 0.10 * nz(overfit, 1.0)
        - 0.10 * nz(eigen_warn, 1.0)
        - 0.05 * nz(condition, 1.0)
    )

    # Step 7 correlation health (secondary): penalize high persistence and low eigenvalues.
    pers = nz(persistence, 1.5)
    eig = nz(min_eigen, -1.0)
    persistence_penalty = max(0.0, pers - 0.995) * 20.0
    min_eigen_penalty = max(0.0, 0.02 - eig) * 50.0
    corr_score = -(persistence_penalty + min_eigen_penalty)

    # Complexity penalty (tertiary)
    complexity_penalty = 0.02 * float(pc_count) + (0.05 if float(fci_included) > 0 else 0.0)

    total = mapping_score + corr_score - complexity_penalty
    return total, {
        "mapping_score": mapping_score,
        "corr_score": corr_score,
        "complexity_penalty": complexity_penalty,
    }


def _candidate_specs() -> List[Tuple[str, dict]]:
    # 8 candidates: baseline + PCA aggressiveness + caps + FCI mode.
    return [
        ("baseline", {}),
        ("pca_more_0p90", {"pca": {"monthly": {"corr_cutoff": 0.90}}}),
        ("pca_less_0p98", {"pca": {"monthly": {"corr_cutoff": 0.98}}}),
        ("pca_var_0p95", {"pca": {"monthly": {"variance_target": 0.95}}}),
        ("pca_cap_3", {"pca": {"monthly": {"max_components": 3}}}),
        ("fci_mean", {"fci": {"monthly": {"method": "mean"}}}),
        ("fci_pca", {"fci": {"monthly": {"method": "pca"}}}),
        (
            "pca_more_fci_mean",
            {"pca": {"monthly": {"corr_cutoff": 0.90, "max_components": 3}}, "fci": {"monthly": {"method": "mean"}}},
        ),
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 7 report-only PCA/FCI tuning evaluator")
    parser.add_argument("--iso", type=str, default="USA", help="ISO to evaluate (default: USA)")
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=8,
        help="How many candidates to run (default: 8)",
    )
    parser.add_argument(
        "--output-root",
        type=str,
        default=str(PROJECT_ROOT / "analysis_outputs" / "pca_fci_tuning"),
        help="Root folder for evaluator reports/artifacts",
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="Use lighter Step 4 settings (faster, less stable diagnostics)",
    )
    parser.add_argument(
        "--skip-adcc",
        action="store_true",
        help="Skip Phase 3 ADCC diagnostics (score will exclude correlation health)",
    )
    parser.add_argument(
        "--adcc-timeout",
        type=int,
        default=240,
        help="Max seconds to allow Phase 3 ADCC per candidate before timing out (default: 240)",
    )
    parser.add_argument(
        "--adcc-grid-a",
        nargs="+",
        type=float,
        default=[0.02, 0.05],
        help="Small grid for Phase 3 ADCC parameter a (default: 0.02 0.05)",
    )
    parser.add_argument(
        "--adcc-grid-b",
        nargs="+",
        type=float,
        default=[0.90, 0.95],
        help="Small grid for Phase 3 ADCC parameter b (default: 0.90 0.95)",
    )
    parser.add_argument(
        "--adcc-grid-g",
        nargs="+",
        type=float,
        default=[0.0, 0.05],
        help="Small grid for Phase 3 ADCC parameter g (default: 0.0 0.05)",
    )
    parser.add_argument(
        "--feature-source",
        choices=["pca", "full"],
        default="pca",
        help="Step 4 feature source (default: pca)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    iso = args.iso.strip().upper()
    print(f"Evaluator settings: iso={iso}, fast={args.fast}, skip_adcc={args.skip_adcc}")

    base = _load_yaml(BASE_FACTOR_SETTINGS)
    if not base:
        raise FileNotFoundError(f"Missing base factor settings at {BASE_FACTOR_SETTINGS}")

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.output_root)
    run_dir = out_root / run_id
    candidates_dir = run_dir / "candidates"
    artifacts_dir = run_dir / "artifacts"
    run_dir.mkdir(parents=True, exist_ok=True)

    candidates = _candidate_specs()[: max(1, int(args.max_candidates))]
    rows: List[Dict[str, Any]] = []

    for idx, (candidate_id, override) in enumerate(candidates, start=1):
        print(f"\n============================")
        print(f"Candidate {idx}/{len(candidates)}: {candidate_id}")
        print(f"============================")

        candidate_settings = deepcopy(base)
        _deep_update(candidate_settings, override)

        candidate_path = candidates_dir / f"{candidate_id}.yaml"
        _write_yaml(candidate_path, candidate_settings)

        # Step 3 monthly factor preparation
        _run(
            [sys.executable, str(MONTHLY_STEP3), "--iso", iso, "--factor-settings", str(candidate_path)],
            "Step 3 (monthly) factor preparation",
        )

        # Step 4 mapping diagnostics
        step4_cmd = [
            sys.executable,
            str(STEP4_LASSO),
            "--countries",
            iso,
            "--feature-source",
            args.feature_source,
        ]
        if args.fast:
            step4_cmd += [
                "--alpha-steps",
                "25",
                "--permutation-trials",
                "25",
                "--stability-bootstraps",
                "25",
            ]
        _run(step4_cmd, "Step 4 Lasso mapping")

        # Phase 3 ADCC diagnostics (optional)
        if not args.skip_adcc:
            _run(
                [
                    sys.executable,
                    str(PHASE3_ADCC),
                    "--isos",
                    iso,
                    "--model",
                    "adcc",
                    "--overwrite",
                    "--grid-a",
                    *[str(x) for x in (args.adcc_grid_a or [])],
                    "--grid-b",
                    *[str(x) for x in (args.adcc_grid_b or [])],
                    "--grid-g",
                    *[str(x) for x in (args.adcc_grid_g or [])],
                ],
                "Phase 3 ADCC diagnostics",
                timeout_s=int(args.adcc_timeout) if args.adcc_timeout else None,
                allow_fail=True,
            )

        # Collect metrics
        metrics: Dict[str, Any] = {
            "iso": iso,
            "candidate": candidate_id,
            "candidate_path": str(candidate_path),
            "pca_monthly_corr_cutoff": float(
                (((candidate_settings.get("pca") or {}).get("monthly") or {}).get("corr_cutoff"))
                or float("nan")
            ),
            "pca_monthly_variance_target": float(
                (((candidate_settings.get("pca") or {}).get("monthly") or {}).get("variance_target"))
                or float("nan")
            ),
            "pca_monthly_max_components": float(
                (((candidate_settings.get("pca") or {}).get("monthly") or {}).get("max_components"))
                or float("nan")
            ),
            "fci_monthly_method": str((((candidate_settings.get("fci") or {}).get("monthly") or {}).get("method")) or ""),
        }
        metrics.update(_summarize_step4(iso))
        metrics.update(_summarize_adcc(iso) if not args.skip_adcc else {"adcc_persistence": float("nan"), "adcc_min_eigen": float("nan")})
        metrics.update(_summarize_complexity(iso))

        total_score, parts = _score(metrics)
        metrics["score_total"] = float(total_score)
        metrics.update({f"score_{k}": float(v) for k, v in parts.items()})

        # Preserve artifacts for this candidate
        cand_art_dir = artifacts_dir / candidate_id
        cand_art_dir.mkdir(parents=True, exist_ok=True)
        _copy_if_exists(FACTOR_DIR / f"{iso}_factors.csv", cand_art_dir)
        _copy_if_exists(FACTOR_DIR / f"{iso}_pca_components.csv", cand_art_dir)
        _copy_if_exists(PROJECT_ROOT / "analysis_outputs" / f"feature_contributions_{iso}.csv", cand_art_dir)
        _copy_if_exists(ADCC_META_DIR / f"{iso}_adcc.json", cand_art_dir)
        _copy_if_exists(DIAG_CORR_DIR / f"{iso}_Sigma_eigenvalues.csv", cand_art_dir)
        _copy_if_exists(DIAG_CORR_DIR / f"{iso}_Rt.csv", cand_art_dir)

        rows.append(metrics)

    df = pd.DataFrame(rows)
    df = df.sort_values(["score_total"], ascending=False, na_position="last")

    ranked_csv = run_dir / "pca_fci_tuning_ranked.csv"
    df.to_csv(ranked_csv, index=False)

    # Markdown summary
    top_cols = [
        "candidate",
        "score_total",
        "score_mapping_score",
        "score_corr_score",
        "score_complexity_penalty",
        "step4_mean_cv_r2",
        "step4_mean_test_r2",
        "step4_instability_rate",
        "adcc_persistence",
        "adcc_min_eigen",
        "pc_count",
        "fci_included",
        "pca_monthly_corr_cutoff",
        "pca_monthly_variance_target",
        "pca_monthly_max_components",
        "fci_monthly_method",
    ]
    present_cols = [c for c in top_cols if c in df.columns]
    md = [
        f"# PCA/FCI tuning report ({iso})",
        "",
        f"Run: `{run_id}`",
        f"Candidates: {len(candidates)}",
        "",
        "## Ranked results",
        "",
        df[present_cols].to_markdown(index=False),
        "",
        f"CSV: {ranked_csv}",
        f"Artifacts: {artifacts_dir}",
    ]
    (run_dir / "pca_fci_tuning_ranked.md").write_text("\n".join(md), encoding="utf-8")

    print(f"\nWrote ranked results to: {ranked_csv}")


if __name__ == "__main__":
    main()
