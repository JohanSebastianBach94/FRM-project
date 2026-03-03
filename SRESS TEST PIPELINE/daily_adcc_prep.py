#!/usr/bin/env python3
"""Daily ADCC preparation (Step 6.4) fully contained within SRESS TEST PIPELINE."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

from iso_adcc_diagnostics import (
    ADCC_MODELS_DIR,
    DIAG_CORR_DIR,
    GARCH_MODELS_DIR,
    build_covariance_series,
    compute_dynamic_correlations,
    fit_adcc_params,
    fit_series_garch,
    make_correlation_matrix,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SHORTLIST_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "factors_daily_shortlist"
ALIGNED_SHORTLIST_DIR = PROJECT_ROOT / "analysis_outputs" / "factors_daily_shortlist_aligned"
RT_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr_daily"
FACTOR_PREP_DAILY_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation_daily"
LITERATURE_FACTORS_DIR = PROJECT_ROOT / "analysis_outputs" / "literature_factors"

DEFAULT_VOL_MODELS = ["GARCH"]
LONG_VOL_MODELS = ["GARCH", "FIGARCH", "HAR"]
# Optional raw proxies to carry through the daily modeling panel (when available).
# Note: V2X is an EU implied-vol proxy; USA typically uses VIX (VIXCLS). We never
# force V2X onto USA; we simply append whichever of these exists in the upstream
# daily factor-prep output for that ISO.
REQUIRED_SERIES = {"SOFR_3m", "V2X", "VIXCLS"}


def _env_default(name: str, default: str) -> str:
    val = os.environ.get(name)
    return default if val is None or str(val).strip() == "" else str(val).strip()


def _freq_label(freq: str) -> str:
    return str(freq).replace("/", "-")


def _find_literature_factor_file(iso: str, *, literature_dir: Path, literature_freq: str) -> Path:
    literature_dir = literature_dir.resolve()
    freq_label = _freq_label(literature_freq)
    exact = literature_dir / f"{iso}_block_factors_{freq_label}_daily.csv"
    if exact.exists():
        return exact
    # Fallback: take the most recent daily literature panel for this ISO.
    candidates = sorted(literature_dir.glob(f"{iso}_block_factors_*_daily.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if candidates:
        return candidates[0]
    raise FileNotFoundError(
        f"No literature factor panel found for {iso} under {literature_dir}. "
        "Generate them via Step 3 with DCC_LITERATURE=1 and DCC_LITERATURE_DAILY=1 (or run scripts/prepare_country_factors.py --literature --literature-expand-to-daily)."
    )


def _sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _file_provenance(path: Path) -> dict:
    stat = path.stat()
    mtime_utc = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
    return {
        "path": str(path),
        "mtime_epoch": float(stat.st_mtime),
        "mtime_utc": mtime_utc,
        "size_bytes": int(stat.st_size),
        "sha256": _sha256_file(path),
    }


def _literature_input_paths(iso: str, *, literature_dir: Path, literature_freq: str) -> dict:
    freq_label = _freq_label(literature_freq)
    daily_path = _find_literature_factor_file(iso, literature_dir=literature_dir, literature_freq=literature_freq)
    resampled_path = literature_dir.resolve() / f"{iso}_block_factors_{freq_label}_resampled.csv"
    manifest_path = literature_dir.resolve() / f"{iso}_literature_manifest.json"
    return {
        "daily": daily_path,
        "resampled": resampled_path if resampled_path.exists() else None,
        "manifest": manifest_path if manifest_path.exists() else None,
    }


def _preflight_check_literature_inputs(
    iso: str,
    *,
    literature_dir: Path,
    literature_freq: str,
    force: bool,
) -> dict:
    paths = _literature_input_paths(iso, literature_dir=literature_dir, literature_freq=literature_freq)
    daily_path: Path = paths["daily"]
    daily_mtime = daily_path.stat().st_mtime

    reference_paths: list[Path] = []
    if paths.get("resampled") is not None:
        reference_paths.append(paths["resampled"])
    if paths.get("manifest") is not None:
        reference_paths.append(paths["manifest"])

    newest_ref = None
    if reference_paths:
        newest_ref = max(reference_paths, key=lambda p: p.stat().st_mtime)

    stale = False
    reason = None
    if newest_ref is not None:
        ref_mtime = newest_ref.stat().st_mtime
        if daily_mtime < ref_mtime:
            stale = True
            reason = (
                f"Daily literature panel is older than {newest_ref.name}. "
                f"({daily_path.name} mtime={datetime.fromtimestamp(daily_mtime, tz=timezone.utc).isoformat()} < "
                f"{newest_ref.name} mtime={datetime.fromtimestamp(ref_mtime, tz=timezone.utc).isoformat()})"
            )

    if stale:
        msg = (
            f"[STALE INPUT] {iso}: {reason}\n"
            "Step 6 literature-mode reads the daily-expanded file "
            f"'{daily_path.name}'. Regenerate it via Step 3 before running Step 6:\n"
            f"  python scripts/prepare_country_factors.py --literature --literature-freq {literature_freq} --literature-expand-to-daily --iso {iso}\n"
            "Re-run Step 6 with --force only if you intentionally want to proceed anyway."
        )
        if not force:
            raise RuntimeError(msg)
        print(f"[WARN] {msg}")

    return {
        "iso": iso,
        "literature_dir": str(literature_dir),
        "literature_freq": str(literature_freq),
        "daily_path": str(daily_path),
        "resampled_path": str(paths["resampled"]) if paths.get("resampled") is not None else None,
        "manifest_path": str(paths["manifest"]) if paths.get("manifest") is not None else None,
        "stale_daily_detected": bool(stale),
        "force": bool(force),
    }


def align_daily_shortlists(
    shortlist_dir: Path,
    output_dir: Path,
    date_column: str | None = "date",
) -> list[str]:
    output_dir = output_dir.resolve()
    if not shortlist_dir.is_dir():
        raise FileNotFoundError(f"Daily shortlist directory missing: {shortlist_dir}")

    iso_paths = sorted(shortlist_dir.glob("*_factors_daily_shortlist.csv"))
    if not iso_paths:
        raise FileNotFoundError(f"No daily shortlists found under {shortlist_dir}")

    feature_sets = []
    data_frames: dict[str, pd.DataFrame] = {}

    for path in iso_paths:
        iso = path.stem.split("_")[0].upper()
        df = pd.read_csv(path)
        if date_column and date_column not in df.columns:
            # Common fallback when a DataFrame index was written to CSV.
            first = str(df.columns[0]) if len(df.columns) else ""
            if first.lower() == "date":
                df = df.rename(columns={first: date_column})
            elif first.startswith("Unnamed") or first.strip() == "":
                df = df.rename(columns={first: date_column})
            else:
                raise ValueError(f"Expected column {date_column!r} missing in {path}")
        features = [col for col in df.columns if col != date_column]
        data_frames[iso] = df
        feature_sets.append(set(features))

    union_features = set.union(*feature_sets)
    keep_features = sorted(union_features)
    if not keep_features:
        raise ValueError("No features remain in the union of shortlists")

    output_dir.mkdir(parents=True, exist_ok=True)
    for path in iso_paths:
        iso = path.stem.split("_")[0].upper()
        df = data_frames[iso]
        missing = [feature for feature in keep_features if feature not in df.columns]
        for feature in missing:
            df[feature] = pd.NA
        columns = [date_column] + keep_features if date_column else keep_features
        aligned = df.loc[:, columns].dropna(axis=1, how="all")
        aligned.to_csv(output_dir / path.name, index=False)

    print(f"Aligned {len(keep_features)} drivers across {len(iso_paths)} ISOs.")
    return keep_features


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase 6 daily ADCC preparation")
    parser.add_argument("--isos", nargs="*", default=["ITA", "FRA", "DEU", "USA", "ESP"], help="ISO codes")
    parser.add_argument(
        "--factor-space",
        choices=["shortlist", "literature"],
        default=_env_default("DCC_DAILY_FACTOR_SPACE", "shortlist"),
        help="Which factor space to use for daily ADCC: governed daily shortlists (default) or literature block factors.",
    )
    parser.add_argument(
        "--literature-dir",
        type=str,
        default=_env_default("DCC_LITERATURE_DIR", str(LITERATURE_FACTORS_DIR)),
        help="Directory containing literature factor panels (analysis_outputs/literature_factors by default).",
    )
    parser.add_argument(
        "--literature-freq",
        type=str,
        default=_env_default("DCC_LITERATURE_FREQ", "M"),
        help="Frequency label used when generating literature factors (default: M).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute even if daily ADCC artifacts already exist on disk.",
    )
    parser.add_argument(
        "--dt-only",
        action="store_true",
        help="Only compute and persist Dt (conditional volatilities) for scenario scaling; skip ADCC/DCC recursion.",
    )
    parser.add_argument(
        "--model",
        choices=["adcc", "dcc"],
        default="adcc",
        help="Correlation specification to estimate",
    )
    parser.add_argument(
        "--fit-method",
        choices=["opt", "grid"],
        default="opt",
        help="Parameter estimation method (opt uses constrained optimization; grid uses coarse search)",
    )
    parser.add_argument(
        "--max-columns",
        type=int,
        default=0,
        help="Optional cap on number of columns used (keeps Rt_daily + required series first).",
    )
    parser.add_argument(
        "--long-vol",
        action="store_true",
        help="Run the richer GARCH/FIGARCH/HAR set for overnight long-volatility validation (slow).",
    )
    parser.add_argument(
        "--garch-p",
        type=int,
        default=1,
        help="ARCH order (p) used for every GARCH fit (default: 1).",
    )
    parser.add_argument(
        "--garch-q",
        type=int,
        default=1,
        help="GARCH order (q) used for every GARCH fit (default: 1).",
    )
    parser.add_argument(
        "--mean-lags",
        type=int,
        choices=[0, 1, 2],
        default=0,
        help="Number of AR lags in the GARCH mean equation (0=zero mean).",
    )
    return parser.parse_args(argv)


def ensure_aligned_shortlists() -> list[str]:
    return align_daily_shortlists(
        shortlist_dir=SHORTLIST_DAILY_DIR,
        output_dir=ALIGNED_SHORTLIST_DIR,
    )


def load_daily_shortlist(iso: str, *, aligned_dir: Path) -> pd.DataFrame:
    path = aligned_dir / f"{iso}_factors_daily_shortlist.csv"
    if not path.exists():
        raise FileNotFoundError(f"Daily shortlist missing for {iso}: {path}")
    df = pd.read_csv(path, index_col=0, parse_dates=True).sort_index()
    df.index.name = "date"
    return df.dropna(axis=1, how="all")


def load_factor_prep_daily(iso: str) -> pd.DataFrame:
    path = FACTOR_PREP_DAILY_DIR / f"{iso}_factors_daily.csv"
    if not path.exists():
        raise FileNotFoundError(f"Daily factor prep missing for {iso}: {path}")
    df = pd.read_csv(path, index_col=0, parse_dates=True).sort_index()
    df.index.name = "date"
    return df


def load_literature_factors_daily(
    iso: str,
    *,
    literature_dir: Path,
    literature_freq: str,
) -> pd.DataFrame:
    path = _find_literature_factor_file(iso, literature_dir=literature_dir, literature_freq=literature_freq)
    df = pd.read_csv(path, index_col=0, parse_dates=True).sort_index()
    df.index.name = df.index.name or "date"
    return df.dropna(axis=1, how="all")


def load_daily_factors(
    iso: str,
    *,
    factor_space: str,
    aligned_shortlist_dir: Path,
    literature_dir: Path,
    literature_freq: str,
) -> pd.DataFrame:
    if str(factor_space) == "literature":
        return load_literature_factors_daily(
            iso,
            literature_dir=literature_dir,
            literature_freq=literature_freq,
        )
    return load_daily_shortlist(iso, aligned_dir=aligned_shortlist_dir)


def load_daily_rt(iso: str) -> pd.Series:
    path = RT_DAILY_DIR / f"{iso}_Rt_daily.csv"
    if not path.exists():
        raise FileNotFoundError(f"Daily Rt missing for {iso}: {path}")
    df = pd.read_csv(path, index_col=0, parse_dates=True).sort_index()
    if "Rt_daily" in df.columns:
        s = df["Rt_daily"]
    else:
        s = df.iloc[:, 0]
    s.name = "Rt_daily"
    return s


def build_rt_panel(
    iso: str,
    *,
    factor_space: str,
    aligned_shortlist_dir: Path,
    literature_dir: Path,
    literature_freq: str,
) -> pd.DataFrame:
    factors = load_daily_factors(
        iso,
        factor_space=factor_space,
        aligned_shortlist_dir=aligned_shortlist_dir,
        literature_dir=literature_dir,
        literature_freq=literature_freq,
    )

    # In literature/block-factor mode, append required raw proxies (if available)
    # so deterministic scenarios remain compatible (e.g., V2X templates).
    if str(factor_space) == "literature":
        try:
            prep = load_factor_prep_daily(iso)
            add_cols = [c for c in sorted(REQUIRED_SERIES) if c in prep.columns and c not in factors.columns]
            if add_cols:
                factors = factors.join(prep[add_cols], how="left")
        except Exception:
            # Best-effort: literature factors can still run without these.
            pass

    rt = load_daily_rt(iso)
    panel = factors.join(rt, how="inner")
    panel = panel.dropna(axis=1, how="all")
    target_name = rt.name
    feature_columns = [col for col in panel.columns if col != target_name]
    if feature_columns:
        # Forward-fill only: do NOT backfill (prevents lookahead).
        panel.loc[:, feature_columns] = panel.loc[:, feature_columns].ffill()
    panel = panel.dropna(how="any")
    return panel


def limit_panel_columns(panel: pd.DataFrame, max_columns: int) -> pd.DataFrame:
    if max_columns <= 0 or panel.shape[1] <= max_columns:
        return panel
    keep: list[str] = []
    if "Rt_daily" in panel.columns:
        keep.append("Rt_daily")
    for series in sorted(REQUIRED_SERIES):
        if series in panel.columns:
            keep.append(series)
    for col in panel.columns:
        if len(keep) >= max_columns:
            break
        if col not in keep:
            keep.append(col)
    return panel[keep]


def estimate_daily_garch_and_adcc(
    iso: str,
    clip: float = 10.0,
    use_gjr: bool = True,
    max_columns: int = 0,
    vol_models: list[str] | None = None,
    garch_p: int = 1,
    garch_q: int = 1,
    mean_lags: int = 0,
    model_type: str = "adcc",
    fit_method: str = "opt",
    force: bool = False,
    dt_only: bool = False,
    factor_space: str = "shortlist",
    aligned_shortlist_dir: Path = ALIGNED_SHORTLIST_DIR,
    literature_dir: Path = LITERATURE_FACTORS_DIR,
    literature_freq: str = "M",
) -> None:
    dt_path = RT_DAILY_DIR / f"{iso}_Dt_daily.csv"
    resid_path = RT_DAILY_DIR / f"{iso}_standardized_residuals_daily.csv"

    preflight = None
    if str(factor_space) == "literature":
        preflight = _preflight_check_literature_inputs(
            iso,
            literature_dir=literature_dir,
            literature_freq=literature_freq,
            force=bool(force),
        )

    expected_artifacts = [
        ADCC_MODELS_DIR / f"{iso}_daily_adcc.json",
        RT_DAILY_DIR / f"{iso}_Rt_daily_pairs.csv",
        RT_DAILY_DIR / f"{iso}_Sigma_daily_pairs.csv",
        RT_DAILY_DIR / f"{iso}_Sigma_daily_eigenvalues.csv",
        DIAG_CORR_DIR / f"{iso}_Sigma_daily_corr.csv",
    ]
    if dt_only:
        if not force and dt_path.exists() and resid_path.exists():
            print(f"[SKIP] Dt already exists for {iso} (use --force to recompute): {dt_path}")
            return
    else:
        if not force and resid_path.exists() and all(path.exists() for path in expected_artifacts):
            print(f"[SKIP] Daily ADCC artifacts already exist for {iso} (use --force to recompute).")
            return

    provenance = None

    panel = build_rt_panel(
        iso,
        factor_space=factor_space,
        aligned_shortlist_dir=aligned_shortlist_dir,
        literature_dir=literature_dir,
        literature_freq=literature_freq,
    )
    if max_columns:
        panel = limit_panel_columns(panel, max_columns)
    if panel.shape[0] < 200:
        print(f"[SKIP] Not enough daily observations for ADCC for {iso}")
        return

    residuals = pd.DataFrame(index=panel.index)
    cond_vols = pd.DataFrame(index=panel.index)
    vol_models = vol_models or DEFAULT_VOL_MODELS
    print(f"[DAILY {iso}] Trying vol models: {vol_models}")
    for col in panel.columns:
        print(f"[DAILY {iso}] GARCH -> {col}")
        try:
            std_resid, vol, diag, _, _ = fit_series_garch(
                panel[col],
                vol_models=vol_models,
                use_gjr=use_gjr,
                clip_resid=clip,
                garch_p=garch_p,
                garch_q=garch_q,
                mean_lags=mean_lags,
            )
            residuals[col] = std_resid
            cond_vols[col] = vol
        except Exception as exc:
            print(f"  SKIP {col}: {exc}")

    residuals = residuals.dropna(how="all")
    cond_vols = cond_vols.loc[residuals.index]
    # ADCC/DCC recursion assumes fully observed standardized residual vectors.
    residuals = residuals.dropna(how="any")
    cond_vols = cond_vols.loc[residuals.index]
    if residuals.shape[1] < 2 or residuals.shape[0] < 200:
        print(f"[SKIP] Not enough valid residuals for ADCC for {iso}")
        return

    # Persist standardized residuals for downstream empirical calibration (Step 10.x).
    residuals.index.name = residuals.index.name or "date"
    resid_path = RT_DAILY_DIR / f"{iso}_standardized_residuals_daily.csv"
    residuals.to_csv(resid_path)
    print(f"[DAILY {iso}] Saved standardized residuals to {resid_path}")

    # Persist D_t (conditional volatilities) explicitly for scenario scaling.
    RT_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    cond_vols.index.name = cond_vols.index.name or "date"
    cond_vols.to_csv(dt_path)
    print(f"[DAILY {iso}] Saved Dt (conditional vols) to {dt_path}")

    if dt_only:
        return

    params, loglik, fit_info = fit_adcc_params(
        residuals,
        model_type=model_type,
        fit_method=fit_method,
        grid_a=None,
        grid_b=None,
        grid_g=None,
    )
    corr_out = compute_dynamic_correlations(residuals, model_type, params)
    corr_df = corr_out[0]
    persistence = corr_out[1]
    corr_diag = corr_out[2] if len(corr_out) > 2 else {}
    cov_df, eigen_df = build_covariance_series(corr_df, cond_vols.loc[residuals.index], residuals)

    # Persist full Rt / Sigma_t paths for scenario generation.
    corr_pairs_path = RT_DAILY_DIR / f"{iso}_Rt_daily_pairs.csv"
    sigma_pairs_path = RT_DAILY_DIR / f"{iso}_Sigma_daily_pairs.csv"
    eigen_path = RT_DAILY_DIR / f"{iso}_Sigma_daily_eigenvalues.csv"
    corr_df.to_csv(corr_pairs_path)
    cov_df.to_csv(sigma_pairs_path)
    eigen_df.to_csv(eigen_path)
    print(f"[DAILY {iso}] Saved Rt pair paths to {corr_pairs_path}")
    print(f"[DAILY {iso}] Saved Sigma pair paths to {sigma_pairs_path}")

    # Save average daily correlation matrix (backwards-compatible artifact).
    avg_pairs = corr_df.mean(axis=0)
    avg_matrix = make_correlation_matrix(avg_pairs, residuals.columns)
    avg_df = pd.DataFrame(avg_matrix, index=residuals.columns, columns=residuals.columns)
    out_path = DIAG_CORR_DIR / f"{iso}_Sigma_daily_corr.csv"
    avg_df.to_csv(out_path)
    print(f"[DAILY {iso}] Saved avg daily correlation matrix to {out_path}")

    # Persist fitted parameters + metadata.
    model_meta_path = ADCC_MODELS_DIR / f"{iso}_daily_adcc.json"
    if str(factor_space) == "literature":
        paths = _literature_input_paths(iso, literature_dir=literature_dir, literature_freq=literature_freq)
        provenance = {
            "literature_daily": _file_provenance(paths["daily"]),
            "literature_resampled": _file_provenance(paths["resampled"]) if paths.get("resampled") is not None else None,
            "literature_manifest": _file_provenance(paths["manifest"]) if paths.get("manifest") is not None else None,
        }
    meta = {
        "iso": iso,
        "frequency": "daily",
        "factor_space": str(factor_space),
        "inputs": provenance,
        "preflight": preflight,
        "model": model_type.upper(),
        "fit_method": fit_method,
        "params": {"a": float(params[0]), "b": float(params[1]), "g": float(params[2])},
        "persistence": float(persistence) if np.isfinite(persistence) else None,
        "loglikelihood": float(loglik) if np.isfinite(loglik) else None,
        "fit_info": fit_info,
        "corr_diag": corr_diag,
        "columns": list(residuals.columns),
        "start": residuals.index.min().strftime("%Y-%m-%d"),
        "end": residuals.index.max().strftime("%Y-%m-%d"),
        "residuals_shape": list(residuals.shape),
        "pair_count": int(len(corr_df.columns)),
    }
    model_meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"[DAILY {iso}] Saved fitted params to {model_meta_path}")

    if "Rt_daily" in cond_vols.columns:
        rt_vol = cond_vols["Rt_daily"].dropna()
        if rt_vol.shape[0] >= 200:
            hi_thresh = rt_vol.quantile(0.8)
            lo_thresh = rt_vol.quantile(0.2)

            high_idx = rt_vol.index[rt_vol >= hi_thresh]
            low_idx = rt_vol.index[rt_vol <= lo_thresh]

            def regime_corr(idxs):
                common = residuals.index.intersection(idxs)
                if len(common) < 50:
                    return None
                return residuals.loc[common].corr()

            R_high = regime_corr(high_idx)
            R_low = regime_corr(low_idx)

            if R_high is not None:
                R_high.to_csv(DIAG_CORR_DIR / f"{iso}_Sigma_daily_corr_highvol.csv")
            if R_low is not None:
                R_low.to_csv(DIAG_CORR_DIR / f"{iso}_Sigma_daily_corr_lowvol.csv")

            print(
                f"[DAILY {iso}] Regime diagnostics: high-vol days={len(high_idx)}, low-vol days={len(low_idx)}"
            )


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    factor_space = str(args.factor_space)

    # Only the governed shortlist mode relies on pre-aligning the daily shortlist directory.
    if factor_space == "shortlist":
        ensure_aligned_shortlists()

    vol_models = LONG_VOL_MODELS if args.long_vol else DEFAULT_VOL_MODELS
    for iso in args.isos:
        estimate_daily_garch_and_adcc(
            iso,
            max_columns=args.max_columns,
            vol_models=vol_models,
            garch_p=args.garch_p,
            garch_q=args.garch_q,
            mean_lags=args.mean_lags,
            force=bool(args.force),
            model_type=args.model,
            fit_method=args.fit_method,
            dt_only=bool(args.dt_only),
            factor_space=factor_space,
            aligned_shortlist_dir=ALIGNED_SHORTLIST_DIR,
            literature_dir=Path(str(args.literature_dir)),
            literature_freq=str(args.literature_freq),
        )


if __name__ == "__main__":  # pragma: no cover
    main()
