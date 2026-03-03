#!/usr/bin/env python3
"""Step 9.0 — Scenario governance + run contract.

This script freezes the scenario inputs required by downstream steps:
- ISO universe + governed factor shortlists
- Rt / Sigma pair paths (and eigenvalues) produced by Step 6 daily ADCC
- Readiness gate evidence (Step 8 postfit summary + pipeline run status)

It writes a reproducible run folder under:
  analysis_outputs/scenarios/<run_id>/

Design intent (literature-aligned)
- Scenarios are defined in factor-shock space (return-like innovations).
- This step does not generate scenarios; it only freezes inputs + provenance.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import os
from pathlib import Path
import sys
from typing import Iterable, Optional

import pandas as pd

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from scenario_io import copy_file, ensure_dir, read_json, safe_relpath, sha256_file, write_json
from scenario_spec import (
    IsoFrozenInputs,
    ScenarioRunConfig,
    ScenarioRunManifest,
    default_run_id,
    utc_now_iso,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"
RUNLOG_STATUS_PATH = PROJECT_ROOT / "analysis_outputs" / "runlogs" / "pipeline_0_to_8" / "latest" / "status.json"
POSTFIT_SUMMARY_PATH = PROJECT_ROOT / "analysis_outputs" / "postfit_model_diagnostics" / "postfit_summary.json"
POSTFIT_BLOCKS_PATH = PROJECT_ROOT / "analysis_outputs" / "postfit_model_diagnostics" / "block_postfit_diagnostics.csv"

SHORTLIST_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "factors_daily_shortlist"
SHORTLIST_MONTHLY_DIR = PROJECT_ROOT / "analysis_outputs" / "feature_shortlist"

LITERATURE_FACTORS_DIR = PROJECT_ROOT / "analysis_outputs" / "literature_factors"

DIAG_CORR_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"
ADCC_MODELS_DIR = PROJECT_ROOT / "models" / "adcc"


def _iso_prefix(iso: str) -> str:
    return iso.strip().lower()


def _load_block_readiness(*, postfit_blocks_path: Path) -> Optional[pd.DataFrame]:
    if not postfit_blocks_path.exists():
        return None
    df = pd.read_csv(postfit_blocks_path)
    for col in ["block_id", "readiness_judgement"]:
        if col not in df.columns:
            raise ValueError(f"Step 8 block diagnostics missing required column: {col}")
    df["block_id"] = df["block_id"].astype(str)
    df["readiness_judgement"] = df["readiness_judgement"].astype(str)
    return df


def _enforce_readiness_for_iso(
    iso: str,
    blocks_df: Optional[pd.DataFrame],
    *,
    allow_warn: bool,
    filter_not_ready_blocks: bool,
    postfit_blocks_path: Path,
) -> tuple[Optional[list[str]], Optional[dict[str, object]]]:
    """Return (allowed_block_ids, excluded_map) or raise if gating fails."""
    if blocks_df is None:
        return None, None

    prefix = _iso_prefix(iso) + "_"
    sub = blocks_df.loc[blocks_df["block_id"].str.lower().str.startswith(prefix)].copy()
    if sub.empty:
        raise SystemExit(
            f"Step 8 readiness gating: no block rows found for ISO {iso}. "
            f"Expected block_id prefix '{prefix}' in {postfit_blocks_path}."
        )

    allowed: list[str] = []
    excluded: dict[str, object] = {}
    for _, row in sub.iterrows():
        block_id = str(row.get("block_id"))
        judgement = str(row.get("readiness_judgement", "")).upper().strip()
        warn_detail = row.get("readiness_warn_detail")
        reasons = row.get("readiness_reasons")

        if judgement == "PASS":
            allowed.append(block_id)
        elif judgement == "WARN" and allow_warn:
            allowed.append(block_id)
        else:
            excluded[block_id] = {
                "readiness_judgement": judgement or None,
                "readiness_reasons": reasons if isinstance(reasons, str) else None,
                "readiness_warn_detail": warn_detail if isinstance(warn_detail, str) else None,
            }

    if excluded and not filter_not_ready_blocks:
        raise SystemExit(
            f"Step 8 readiness gating: ISO {iso} has non-allowed blocks: {sorted(excluded.keys())}. "
            "Pass --filter-not-ready-blocks to proceed while recording exclusions (not recommended)."
        )

    return allowed, (excluded or None)


def _expected_pair_columns(used_cols: list[str]) -> set[str]:
    cols = list(used_cols)
    exp: set[str] = set()
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            exp.add(f"{cols[i]}_{cols[j]}")
    return exp


def _read_csv_header_columns(path: Path) -> list[str]:
    # Fast header read to avoid loading full dataframe
    first = path.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    return [c.strip() for c in first.split(",")]


def _parse_isos(text: Optional[str]) -> Optional[list[str]]:
    if not text:
        return None
    parts = [p.strip().upper() for p in text.split(",") if p.strip()]
    return sorted(set(parts)) if parts else None


def _infer_isos_from_shortlists(frequency: str) -> list[str]:
    if frequency == "daily":
        paths = sorted(SHORTLIST_DAILY_DIR.glob("*_factors_daily_shortlist.csv"))
        if paths:
            return sorted({p.stem.split("_")[0].upper() for p in paths})

    # Fallback: monthly shortlist convention (Step 5)
    paths = sorted(SHORTLIST_MONTHLY_DIR.glob("factors_*.csv"))
    return sorted({p.stem.split("_")[1].upper() for p in paths})


def _infer_isos_from_literature(*, literature_dir: Path) -> list[str]:
    paths = sorted(literature_dir.glob("*_block_factors_*_daily.csv"))
    return sorted({p.name.split("_", 1)[0].upper() for p in paths})


def _load_daily_factors(iso: str) -> tuple[list[str], Path]:
    path = SHORTLIST_DAILY_DIR / f"{iso}_factors_daily_shortlist.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing daily shortlist for {iso}: {path}")
    df = pd.read_csv(path)
    factors = [c for c in df.columns if c.lower() != "date"]
    if not factors:
        raise ValueError(f"No factors found in {path}")
    return factors, path


def _freq_label(freq: str) -> str:
    return str(freq).replace("/", "-")


def _find_literature_factor_file(iso: str, *, literature_dir: Path, literature_freq: str) -> Path:
    literature_dir = literature_dir.resolve()
    freq_label = _freq_label(literature_freq)
    exact = literature_dir / f"{iso}_block_factors_{freq_label}_daily.csv"
    if exact.exists():
        return exact
    candidates = sorted(
        literature_dir.glob(f"{iso}_block_factors_*_daily.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if candidates:
        return candidates[0]
    raise FileNotFoundError(
        f"Missing literature daily block-factor panel for {iso} under {literature_dir}. "
        "Generate them via Step 3 with DCC_LITERATURE=1 and DCC_LITERATURE_DAILY=1 (or run scripts/prepare_country_factors.py --literature --literature-expand-to-daily)."
    )


def _load_daily_factors_literature(iso: str, *, literature_dir: Path, literature_freq: str) -> tuple[list[str], Path]:
    path = _find_literature_factor_file(iso, literature_dir=literature_dir, literature_freq=literature_freq)
    df = pd.read_csv(path, index_col=0)
    factors = [c for c in df.columns if str(c).strip()]
    if not factors:
        raise ValueError(f"No factors found in {path}")
    return factors, path


def _load_monthly_factors(iso: str) -> tuple[list[str], Path]:
    path = SHORTLIST_MONTHLY_DIR / f"factors_{iso}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing monthly shortlist for {iso}: {path}")
    df = pd.read_csv(path)
    # Monthly shortlist files are typically a single-row wide table.
    factors = [c for c in df.columns if c.lower() != "date"]
    return factors, path


def _read_pairs_window(path: Path, *, t0: Optional[str], window_days: int) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0)
    # Best-effort date parsing
    try:
        df.index = pd.to_datetime(df.index)
    except Exception:
        return df

    df = df.sort_index()
    if t0:
        t0_ts = pd.to_datetime(t0)
        df = df.loc[df.index <= t0_ts]
    if window_days and window_days > 0:
        df = df.tail(int(window_days))
    return df


def _maybe_copy_windowed_csv(src: Path, dst: Path, *, t0: Optional[str], window_days: int) -> tuple[Optional[str], Optional[str], Optional[int]]:
    if not src.exists():
        raise FileNotFoundError(f"Missing required input: {src}")

    df = _read_pairs_window(src, t0=t0, window_days=window_days)
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst)

    start = end = None
    n_obs = None
    try:
        if hasattr(df.index, "min") and hasattr(df.index, "max"):
            start = pd.to_datetime(df.index.min()).strftime("%Y-%m-%d")
            end = pd.to_datetime(df.index.max()).strftime("%Y-%m-%d")
            n_obs = int(df.shape[0])
    except Exception:
        pass
    return start, end, n_obs


def _iter_daily_paths(iso: str) -> tuple[Path, Path, Path, Path]:
    dt = DIAG_CORR_DAILY_DIR / f"{iso}_Dt_daily.csv"
    sigma_pairs = DIAG_CORR_DAILY_DIR / f"{iso}_Sigma_daily_pairs.csv"
    rt_pairs = DIAG_CORR_DAILY_DIR / f"{iso}_Rt_daily_pairs.csv"
    eigen = DIAG_CORR_DAILY_DIR / f"{iso}_Sigma_daily_eigenvalues.csv"
    return dt, sigma_pairs, rt_pairs, eigen


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 9.0 — Freeze scenario inputs and write run manifest")
    parser.add_argument("--run-id", default=None, help="Run ID (default: auto timestamp)")
    parser.add_argument("--frequency", default="daily", choices=["daily", "monthly"], help="Scenario frequency")
    parser.add_argument("--isos", default=None, help="Comma-separated ISO list (default: infer from shortlists)")
    parser.add_argument(
        "--daily-shortlist-source",
        default=os.environ.get("DCC_DAILY_FACTOR_SPACE", "shortlist"),
        choices=["shortlist", "literature"],
        help="Daily factor list source: governed daily shortlist (default) or literature block-factor panel.",
    )
    parser.add_argument(
        "--literature-dir",
        default=os.environ.get("DCC_LITERATURE_DIR", str(LITERATURE_FACTORS_DIR)),
        help="Directory containing literature factor panels (analysis_outputs/literature_factors by default).",
    )
    parser.add_argument(
        "--literature-freq",
        default=os.environ.get("DCC_LITERATURE_FREQ", "M"),
        help="Frequency label used when generating literature factors (default: M).",
    )
    parser.add_argument("--t0", default=None, help="End date for frozen windows (YYYY-MM-DD). Default: latest available")
    parser.add_argument("--window-days", type=int, default=756, help="Tail window length to freeze (default: 756 ≈ 3y)")
    parser.add_argument("--allow-warn", action="store_true", help="Allow running when Step 8 reports WARN blocks")
    parser.add_argument(
        "--allow-not-ready",
        action="store_true",
        help="Allow running even when Step 8 postfit summary says not ready (not recommended)",
    )
    parser.add_argument(
        "--filter-not-ready-blocks",
        action="store_true",
        help="Instead of failing, exclude non-allowed blocks per ISO and record exclusions in the manifest",
    )
    parser.add_argument(
        "--freeze-step6-columns",
        action="store_true",
        help="Freeze/validate exactly the columns Step 6 used (from models/adcc/{ISO}_daily_adcc.json)",
    )
    parser.add_argument(
        "--postfit-summary-path",
        default=str(POSTFIT_SUMMARY_PATH),
        help="Path to Step 8 postfit_summary.json (default: analysis_outputs/postfit_model_diagnostics/postfit_summary.json)",
    )
    parser.add_argument(
        "--postfit-blocks-path",
        default=str(POSTFIT_BLOCKS_PATH),
        help="Path to Step 8 block_postfit_diagnostics.csv (default: analysis_outputs/postfit_model_diagnostics/block_postfit_diagnostics.csv)",
    )
    parser.add_argument(
        "--runlog-status-path",
        default=str(RUNLOG_STATUS_PATH),
        help="Path to pipeline status.json (default: analysis_outputs/runlogs/pipeline_0_to_8/latest/status.json)",
    )
    args = parser.parse_args()

    run_id = args.run_id or default_run_id(prefix="scenarios")
    daily_source = str(args.daily_shortlist_source)
    literature_dir = Path(str(args.literature_dir))
    literature_freq = str(args.literature_freq)

    if args.frequency == "daily" and daily_source == "literature":
        isos = _parse_isos(args.isos) or _infer_isos_from_literature(literature_dir=literature_dir)
    else:
        isos = _parse_isos(args.isos) or _infer_isos_from_shortlists(args.frequency)

    postfit_summary_path = Path(str(args.postfit_summary_path))
    postfit_blocks_path = Path(str(args.postfit_blocks_path))
    runlog_status_path = Path(str(args.runlog_status_path))

    postfit_summary = read_json(postfit_summary_path)
    pipeline_status = read_json(runlog_status_path)
    blocks_df = _load_block_readiness(postfit_blocks_path=postfit_blocks_path)

    allow_warn_effective = bool(args.allow_warn)

    if postfit_summary and not args.allow_not_ready:
        ready = bool(postfit_summary.get("ready_for_stress_testing", False))
        warn_blocks = set((postfit_summary.get("high_level", {}) or {}).get("warn_blocks", []) or [])
        if not ready:
            raise SystemExit(
                "Step 8 readiness gate: ready_for_stress_testing=false. "
                "Re-run/repair models or pass --allow-not-ready if you really want to proceed."
            )
        if warn_blocks and not allow_warn_effective:
            # If WARNs are purely due to dcc_a hitting its configured cap, treat this as an
            # informational regularization signal rather than a governance stopper.
            warn_only_cap = False
            if blocks_df is not None and "readiness_reasons" in blocks_df.columns:
                warn_only_cap = True
                for bid in warn_blocks:
                    sub = blocks_df.loc[blocks_df["block_id"].astype(str) == str(bid)]
                    if sub.empty:
                        # If we cannot look up the block reason, do not auto-allow.
                        warn_only_cap = False
                        break
                    reasons_str = str(sub.iloc[0].get("readiness_reasons") or "")
                    reasons = [r.strip() for r in reasons_str.split(";") if r and str(r).strip()]
                    # Auto-allow only when the *only* reason is the cap-binding flag.
                    if any(r != "dcc_a_at_upper_bound" for r in reasons):
                        warn_only_cap = False
                        break

            if warn_only_cap:
                allow_warn_effective = True
                print(
                    "[INFO] Step 8 WARN blocks are only 'dcc_a_at_upper_bound' (cap binding). "
                    "Proceeding while recording warnings in the manifest."
                )
            else:
                raise SystemExit(
                    "Step 8 reports WARN blocks (go-with-warnings). "
                    "Pass --allow-warn to proceed while recording the warnings in the run manifest."
                )

    run_dir = ensure_dir(SCENARIOS_DIR / run_id)
    inputs_dir = ensure_dir(run_dir / "inputs")

    frozen_inputs: dict[str, IsoFrozenInputs] = {}

    for iso in isos:
        if args.frequency == "daily":
            if daily_source == "literature":
                factors, shortlist_path = _load_daily_factors_literature(
                    iso,
                    literature_dir=literature_dir,
                    literature_freq=literature_freq,
                )
            else:
                factors, shortlist_path = _load_daily_factors(iso)
            dt_src, sigma_src, rt_src, eigen_src = _iter_daily_paths(iso)
            adcc_meta_src = ADCC_MODELS_DIR / f"{iso}_daily_adcc.json"
        else:
            factors, shortlist_path = _load_monthly_factors(iso)
            # Monthly mode is not yet wired to full Sigma paths; we still snapshot available daily artifacts if present.
            dt_src, sigma_src, rt_src, eigen_src = _iter_daily_paths(iso)
            adcc_meta_src = ADCC_MODELS_DIR / f"{iso}_daily_adcc.json"

        allowed_blocks, excluded_blocks = _enforce_readiness_for_iso(
            iso,
            blocks_df,
            allow_warn=allow_warn_effective,
            filter_not_ready_blocks=bool(args.filter_not_ready_blocks),
            postfit_blocks_path=postfit_blocks_path,
        )

        iso_dir = ensure_dir(inputs_dir / iso)
        ensure_dir(iso_dir / "shortlist")
        ensure_dir(iso_dir / "covariance")
        ensure_dir(iso_dir / "models")

        frozen_files: dict[str, object] = {}

        frozen_shortlist_path = iso_dir / "shortlist" / shortlist_path.name
        copy_file(shortlist_path, frozen_shortlist_path)
        frozen_files["shortlist"] = {
            "source": safe_relpath(shortlist_path, PROJECT_ROOT),
            "frozen": safe_relpath(frozen_shortlist_path, PROJECT_ROOT),
            **sha256_file(frozen_shortlist_path),
        }

        dt_rel = None
        if dt_src.exists():
            frozen_dt_path = iso_dir / "covariance" / dt_src.name
            _maybe_copy_windowed_csv(dt_src, frozen_dt_path, t0=args.t0, window_days=args.window_days)
            dt_rel = safe_relpath(dt_src, PROJECT_ROOT)
            frozen_files["dt"] = {
                "source": safe_relpath(dt_src, PROJECT_ROOT),
                "frozen": safe_relpath(frozen_dt_path, PROJECT_ROOT),
                **sha256_file(frozen_dt_path),
            }

        resid_src = DIAG_CORR_DAILY_DIR / f"{iso}_standardized_residuals_daily.csv"
        if resid_src.exists():
            frozen_resid_path = iso_dir / "covariance" / resid_src.name
            _maybe_copy_windowed_csv(resid_src, frozen_resid_path, t0=args.t0, window_days=args.window_days)
            frozen_files["standardized_residuals"] = {
                "source": safe_relpath(resid_src, PROJECT_ROOT),
                "frozen": safe_relpath(frozen_resid_path, PROJECT_ROOT),
                **sha256_file(frozen_resid_path),
            }

        frozen_sigma_path = iso_dir / "covariance" / sigma_src.name
        frozen_rt_path = iso_dir / "covariance" / rt_src.name
        frozen_eigen_path = iso_dir / "covariance" / eigen_src.name

        start, end, n_obs = _maybe_copy_windowed_csv(sigma_src, frozen_sigma_path, t0=args.t0, window_days=args.window_days)
        _maybe_copy_windowed_csv(rt_src, frozen_rt_path, t0=args.t0, window_days=args.window_days)
        _maybe_copy_windowed_csv(eigen_src, frozen_eigen_path, t0=args.t0, window_days=args.window_days)

        frozen_files["sigma_pairs"] = {
            "source": safe_relpath(sigma_src, PROJECT_ROOT),
            "frozen": safe_relpath(frozen_sigma_path, PROJECT_ROOT),
            **sha256_file(frozen_sigma_path),
        }
        frozen_files["rt_pairs"] = {
            "source": safe_relpath(rt_src, PROJECT_ROOT),
            "frozen": safe_relpath(frozen_rt_path, PROJECT_ROOT),
            **sha256_file(frozen_rt_path),
        }
        frozen_files["sigma_eigenvalues"] = {
            "source": safe_relpath(eigen_src, PROJECT_ROOT),
            "frozen": safe_relpath(frozen_eigen_path, PROJECT_ROOT),
            **sha256_file(frozen_eigen_path),
        }

        meta_rel = None
        used_cols: Optional[list[str]] = None
        if adcc_meta_src.exists():
            frozen_meta_path = iso_dir / "models" / adcc_meta_src.name
            copy_file(adcc_meta_src, frozen_meta_path)
            meta_rel = safe_relpath(adcc_meta_src, PROJECT_ROOT)
            frozen_files["adcc_meta"] = {
                "source": safe_relpath(adcc_meta_src, PROJECT_ROOT),
                "frozen": safe_relpath(frozen_meta_path, PROJECT_ROOT),
                **sha256_file(frozen_meta_path),
            }
            meta_json = read_json(adcc_meta_src) or {}
            cols = meta_json.get("columns")
            if isinstance(cols, list) and all(isinstance(x, str) for x in cols):
                used_cols = list(cols)

        shortlist_factors: Optional[list[str]] = None
        freeze_step6_cols = bool(args.freeze_step6_columns) or (args.frequency == "daily" and daily_source == "literature")
        if freeze_step6_cols:
            if not used_cols:
                raise SystemExit(
                    f"--freeze-step6-columns requested but Step 6 meta is missing/invalid for {iso}: {adcc_meta_src}"
                )
            shortlist_factors = list(factors)
            used_set = set(used_cols)
            missing_in_used = sorted(set(shortlist_factors) - used_set)
            if missing_in_used:
                raise SystemExit(
                    f"Step 6 columns validation failed for {iso}: shortlist factors missing in adcc meta columns: {missing_in_used}"
                )

            exp_pairs = _expected_pair_columns(used_cols)
            rt_header = _read_csv_header_columns(rt_src)
            sigma_header = _read_csv_header_columns(sigma_src)
            rt_pairs = set(rt_header[1:])
            sigma_pairs = set(sigma_header[1:])

            extra_rt = sorted(rt_pairs - exp_pairs)
            extra_sigma = sorted(sigma_pairs - exp_pairs)
            missing_rt = sorted(exp_pairs - rt_pairs)
            missing_sigma = sorted(exp_pairs - sigma_pairs)
            if extra_rt or extra_sigma or missing_rt or missing_sigma:
                raise SystemExit(
                    f"Step 6 columns validation failed for {iso}: pair header mismatch. "
                    f"extra_rt={len(extra_rt)} missing_rt={len(missing_rt)} extra_sigma={len(extra_sigma)} missing_sigma={len(missing_sigma)}"
                )

            # Downstream scenario factor space should match what Step 6 actually modeled (excluding the return target)
            factors = [c for c in used_cols if c != "Rt_daily"]

        asof_t0 = args.t0 or end
        frozen_inputs[iso] = IsoFrozenInputs(
            iso=iso,
            factors=factors,
            shortlist_factors=shortlist_factors,
            shortlist_source=safe_relpath(shortlist_path, PROJECT_ROOT),
            dt_source=dt_rel,
            sigma_pairs_source=safe_relpath(sigma_src, PROJECT_ROOT),
            rt_pairs_source=safe_relpath(rt_src, PROJECT_ROOT),
            eigenvalues_source=safe_relpath(eigen_src, PROJECT_ROOT),
            adcc_meta_source=meta_rel,
            asof_t0=asof_t0,
            frozen_start=start,
            frozen_end=end,
            n_obs=n_obs,
            readiness_allowed_blocks=allowed_blocks,
            readiness_excluded_blocks=excluded_blocks,
            frozen_files=frozen_files,
        )

    config = ScenarioRunConfig(
        run_id=run_id,
        created_at=utc_now_iso(),
        frequency=args.frequency,
        isos=isos,
        t0=args.t0,
        window_days=int(args.window_days),
        allow_warn=allow_warn_effective,
        allow_not_ready=bool(args.allow_not_ready),
        filter_not_ready_blocks=bool(args.filter_not_ready_blocks),
        freeze_step6_columns=bool(args.freeze_step6_columns) or (args.frequency == "daily" and daily_source == "literature"),
    )

    manifest = ScenarioRunManifest(
        config=config.__dict__,
        pipeline_status=pipeline_status,
        postfit_summary=postfit_summary,
        iso_inputs={iso: frozen_inputs[iso].__dict__ for iso in frozen_inputs},
    )

    write_json(run_dir / "manifest.json", manifest.to_dict())
    write_json(run_dir / "inputs_snapshot.json", {iso: frozen_inputs[iso].__dict__ for iso in frozen_inputs})

    print(f"[OK] Scenario inputs frozen: {run_dir}")
    print(f"[OK] ISOs: {', '.join(isos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
