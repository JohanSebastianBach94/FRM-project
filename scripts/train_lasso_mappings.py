#!/usr/bin/env python3
"""Train Lasso mappings from PCA-ready country factors to target series."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Lasso, LassoCV, RidgeCV
from sklearn.metrics import r2_score
from sklearn.model_selection import TimeSeriesSplit, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import yaml
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.stats.stattools import jarque_bera


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

COUNTRY_BLOCKS_YAML = Path("config") / "country_blocks_extended.yaml"
FACTOR_DIR = Path("analysis_outputs") / "factor_preparation"
TARGET_PANEL_PATH = Path("data") / "cleaned_monthly_panel.parquet"
MODEL_DIR = Path("models")
OUTPUT_DIR = Path("outputs")
PCA_COMPONENT_FILE_SUFFIX = "_pca_components.csv"
ANALYSIS_DIR = Path("analysis_outputs")
FEATURE_OUTPUT_DIR = ANALYSIS_DIR / "feature_contributions"


def load_country_targets(path: Path) -> Dict[str, List[str]]:
    with path.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp)
    mapping: Dict[str, List[str]] = {}
    for entry in payload.get("country_blocks", []):
        iso = entry.get("iso_code")
        if not iso:
            continue
        codes = []
        for block in entry.get("blocks", []):
            codes.extend(block.get("series_codes", []))
        mapping[iso] = sorted(set(codes))
    return mapping


def load_target_panel() -> pd.DataFrame:
    if not TARGET_PANEL_PATH.exists():
        raise FileNotFoundError(f"Target panel not found at {TARGET_PANEL_PATH}")
    df = pd.read_parquet(TARGET_PANEL_PATH)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    return df.sort_index()


def load_factor_panel(iso: str, source: str = "full") -> pd.DataFrame:
    if source == "pca":
        path = FACTOR_DIR / f"{iso}{PCA_COMPONENT_FILE_SUFFIX}"
        if not path.exists():
            fallback = FACTOR_DIR / f"{iso}_factors.csv"
            if fallback.exists():
                logger.warning("PCA components missing for %s; falling back to factor file", iso)
                path = fallback
            else:
                raise FileNotFoundError(f"Neither PCA components nor factor panel found for {iso}")
    else:
        path = FACTOR_DIR / f"{iso}_factors.csv"
    if not path.exists():
            return {
                "pipeline": pipeline,
                "train_r2": train_r2,
                "test_r2": test_r2,
                "alpha": alpha_value,
                "num_features": len(X_train.columns),
                "contributions": contributions,
                "estimator": estimator_name,
                "feature_names": X_train.columns.tolist(),
                "mse_path": getattr(est, "mse_path_", None),
                "coef_path": getattr(est, "coef_path_", None),
                "alpha_path": getattr(est, "alphas_", None),
                "scaler": scaler,
                "estimator_obj": est,
            }
    feature_cols = [col for col in df.columns if col != target and not col.startswith(drop_prefix)]
    if not feature_cols:
        raise ValueError("No eligible features remain after excluding target columns")
    features = df[feature_cols]
    target_series = target_panel[target].reindex(df.index)
    data = pd.concat([features, target_series], axis=1).dropna()
    if data.shape[0] < 24 or data.shape[1] <= 1:
        raise ValueError("Insufficient aligned data for modeling")
    clean_features = data[feature_cols]
    clean_target = data[target]
    return clean_features, clean_target


def prune_correlated_features(X: pd.DataFrame, threshold: float = 0.98) -> pd.DataFrame:
    if X.shape[1] <= 1:
        return X
    corr = X.corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    to_drop = [column for column in upper.columns if (upper[column] > threshold).any()]
    if to_drop:
        return X.drop(columns=to_drop)
    return X


def fit_pipeline(
    estimator_name: str,
    estimator,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
) -> Dict[str, object]:
    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            (estimator_name, estimator),
        ]
    )
    pipeline.fit(X_train, y_train)
    train_pred = pipeline.predict(X_train)
    test_pred = pipeline.predict(X_test)
    train_r2 = float(r2_score(y_train, train_pred)) if len(y_train) > 1 else float("nan")
    test_r2 = float(r2_score(y_test, test_pred)) if len(y_test) > 1 else float("nan")
    scaler = pipeline.named_steps["scaler"]
    est = pipeline.named_steps[estimator_name]
    coef_std = est.coef_
    scale = getattr(scaler, "scale_", np.ones(len(X_train.columns)))
    safe_scale = np.where(scale == 0, 1.0, scale)
    coef_original = coef_std / safe_scale
    contributions = sorted(
        ((name, float(coef)) for name, coef in zip(X_train.columns, coef_original)),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    alpha_value = float(getattr(est, "alpha_", float("nan")))
    return {
        "pipeline": pipeline,
        "train_r2": train_r2,
        "test_r2": test_r2,
        "alpha": alpha_value,
        "num_features": len(X_train.columns),
        "contributions": contributions,
        "estimator": estimator_name,
    }


def annotate_cv_test_r2(stats: Dict[str, object], X_full: pd.DataFrame, y_full: pd.Series, cv: TimeSeriesSplit) -> Dict[str, object]:
    scores = cross_val_score(stats["pipeline"], X_full, y_full, cv=cv, scoring="r2")
    if scores.size == 0 or np.all(np.isnan(scores)):
        stats["test_r2"] = float("nan")
    else:
        stats["test_r2"] = float(np.nanmean(scores))
    return stats


def train_target_model(
    X: pd.DataFrame,
    y: pd.Series,
    train_ratio: float,
    cv_splits: int,
    alpha_grid: Tuple[float, float, int],
    max_iter: int,
    tol: float,
    prune_threshold: float,
    allow_ridge_fallback: bool,
) -> Dict[str, object]:
    split_idx = int(len(y) * train_ratio)
    split_idx = max(1, min(split_idx, len(y) - 1))
    X_pruned = prune_correlated_features(X, threshold=prune_threshold)
    X_train, X_test = X_pruned.iloc[:split_idx], X_pruned.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    max_splits = min(cv_splits, len(y_train) - 1)
    if max_splits < 2:
        raise ValueError("Not enough observations to build time-series splits")
    cv = TimeSeriesSplit(n_splits=max_splits)
    raw_alphas = np.logspace(alpha_grid[0], alpha_grid[1], alpha_grid[2])
    cov = X_train.T @ y_train
    denom = len(y_train)
    alpha_max = float(np.max(np.abs(cov))) / denom if denom > 0 else 1.0
    alpha_max = max(alpha_max, 1e-8)
    alphas = raw_alphas * alpha_max
    lasso = LassoCV(cv=cv, alphas=alphas, random_state=42, max_iter=max_iter, tol=tol)
    stats = fit_pipeline("lasso", lasso, X_train, y_train, X_test, y_test)
    stats = annotate_cv_test_r2(stats, X_pruned, y, cv)
    unstable, _ = evaluate_instability(stats["train_r2"], stats["test_r2"])
    if unstable and allow_ridge_fallback:
        logger.debug("Lasso flagged unstable for target; retrying with Ridge")
        ridge = RidgeCV(cv=cv, alphas=alphas, scoring="r2")
        stats = fit_pipeline("ridge", ridge, X_train, y_train, X_test, y_test)
        stats = annotate_cv_test_r2(stats, X_pruned, y, cv)
    return stats


def format_contribution_string(contributions: List[Tuple[str, float]], limit: int = 10) -> Tuple[str, str]:
    nonzero = [(name, coef) for name, coef in contributions if abs(coef) > 1e-9]
    top = nonzero[:limit]
    top_str = ";".join(f"{name}:{coef:.6g}" for name, coef in top)
    coeff_str = ";".join(f"{name}:{coef:.6g}" for name, coef in nonzero)
    return top_str, coeff_str


def evaluate_instability(train_r2: float, test_r2: float) -> Tuple[bool, str]:
    if np.isnan(train_r2) or np.isnan(test_r2):
        return True, "insufficient_r2"
    diff = abs(train_r2 - test_r2)
    if test_r2 < 0:
        return True, "negative_test_r2"
    if diff > 0.35:
        return True, "r2_diff_large"
    return False, "stable"


def main() -> None:
    parser = argparse.ArgumentParser(description="Train Lasso mappings for country factor targets")
    parser.add_argument("--countries", type=str, nargs="*", help="Optional ISO codes to filter")
    parser.add_argument("--train-ratio", type=float, default=0.7, help="Train split ratio")
    parser.add_argument("--cv-splits", type=int, default=5, help="Max TimeSeriesSplit folds")
    parser.add_argument("--alpha-start", type=float, default=-6.0, help="Logspace alpha start exponent")
    parser.add_argument("--alpha-end", type=float, default=0.0, help="Logspace alpha end exponent")
    parser.add_argument("--alpha-steps", type=int, default=50, help="Number of alphas")
    parser.add_argument("--prune-corr-threshold", type=float, default=0.98,
                        help="Drop one of features with |corr| above this threshold")
    parser.add_argument("--feature-source", choices=["full", "pca"], default="pca",
                        help="Feature panel type to feed into Lasso (PCA components vs. full factor panel)")
    parser.add_argument("--lasso-max-iter", type=int, default=100_000,
                        help="Maximum coordinate descent iterations for LassoCV")
    parser.add_argument("--lasso-tol", type=float, default=1e-4,
                        help="Convergence tolerance for LassoCV")
    parser.add_argument("--ridge-fallback", dest="ridge_fallback", action="store_true", default=True,
                        help="Allow RidgeCV fallback for unstable targets")
    parser.add_argument("--no-ridge-fallback", dest="ridge_fallback", action="store_false",
                        help="Skip Ridge fallback even when Lasso flags instability")
    args = parser.parse_args()

    targets_map = load_country_targets(COUNTRY_BLOCKS_YAML)
    target_panel = load_target_panel()
    selected_countries = args.countries or list(targets_map.keys())
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for iso in selected_countries:
        if iso not in targets_map:
            logger.warning("No config entry for %s; skipping", iso)
            continue
        try:
            panel = load_factor_panel(iso, source=args.feature_source)
        except FileNotFoundError as exc:
            logger.warning(str(exc))
            continue
        diagnostics: List[Dict[str, object]] = []
        for target in targets_map[iso]:
            try:
                features, target_series = build_feature_sets(panel, target_panel, target)
            except (KeyError, ValueError) as exc:
                logger.warning("%s %s: %s", iso, target, exc)
                continue
            try:
                stats = train_target_model(
                    features,
                    target_series,
                    train_ratio=args.train_ratio,
                    cv_splits=args.cv_splits,
                    alpha_grid=(args.alpha_start, args.alpha_end, args.alpha_steps),
                    max_iter=args.lasso_max_iter,
                    tol=args.lasso_tol,
                    prune_threshold=args.prune_corr_threshold,
                    allow_ridge_fallback=args.ridge_fallback,
                )
            except ValueError as exc:
                logger.warning("%s %s: modeling failed - %s", iso, target, exc)
                continue
            pipeline = stats["pipeline"]
            model_path = MODEL_DIR / f"lasso_{iso}_{target}.joblib"
            joblib.dump(pipeline, model_path)
            logger.info("Saved model for %s %s to %s", iso, target, model_path)
            top_str, coeff_str = format_contribution_string(stats["contributions"])
            instability_flag, instability_reason = evaluate_instability(stats["train_r2"], stats["test_r2"])
            diagnostics.append(
                {
                    "country": iso,
                    "target": target,
                    "train_r2": stats["train_r2"],
                    "test_r2": stats["test_r2"],
                    "alpha": stats["alpha"],
                    "num_features": stats["num_features"],
                    "estimator": stats["estimator"],
                    "top_contributions": top_str,
                    "coefficients": coeff_str,
                    "instability_flag": instability_flag,
                    "instability_reason": instability_reason,
                    "model_path": str(model_path),
                }
            )
        if diagnostics:
            out_path = OUTPUT_DIR / f"feature_contributions_{iso}.csv"
            pd.DataFrame(diagnostics).to_csv(out_path, index=False)
            logger.info("Wrote contribution report for %s to %s", iso, out_path)
        else:
            logger.warning("No models trained for %s", iso)


if __name__ == "__main__":
    main()
