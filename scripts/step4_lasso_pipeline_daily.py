#!/usr/bin/env python3
"""Daily ElasticNet pipeline for factor drivers with diagnostics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Set

import numpy as np
import pandas as pd
import yaml
from sklearn.linear_model import ElasticNet, ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

FACTOR_DIR_DAILY = Path("analysis_outputs") / "factor_preparation_daily"
RT_DAILY_DIR = Path("analysis_outputs") / "diag_corr_daily"
FEATURE_DIR_DAILY = Path("analysis_outputs") / "feature_contributions_daily"
MODEL_DIAG_DIR = Path("analysis_outputs") / "model_diagnostics_daily"
BLOCK_CONFIG = Path("config") / "country_blocks_extended.yaml"
CATALOG_PATH = Path("catalog.csv")

MIN_ALPHA = 1e-4
ALPHA_POINTS = 25
ELASTIC_NET_L1_RATIO = 0.2
MAX_NONZERO_PER_BLOCK = 3
HOLDOUT_WINDOW = 126  # ≈ six months
MIN_HOLDOUT = 30

SIGN_PRIORS = (
    ("spread", 1),
    ("baml", 1),
    ("hy", 1),
    ("vix", 1),
    ("unrate", 1),
    ("credit", 1),
    ("gdpc", -1),
    ("gdp", -1),
    ("industrial_production", -1),
)

try:
    with BLOCK_CONFIG.open("r", encoding="utf-8") as fp:
        COUNTRY_BLOCKS = yaml.safe_load(fp).get("country_blocks", [])
except FileNotFoundError:
    COUNTRY_BLOCKS = []


def ensure_dirs() -> None:
    FEATURE_DIR_DAILY.mkdir(parents=True, exist_ok=True)
    MODEL_DIAG_DIR.mkdir(parents=True, exist_ok=True)


def load_do_not_use_series() -> Set[str]:
    if not CATALOG_PATH.exists():
        return set()
    try:
        df = pd.read_csv(CATALOG_PATH)
    except Exception:
        return set()
    cols = {c.lower(): c for c in df.columns}
    series_col = cols.get("series")
    dnu_col = cols.get("do_not_use")
    if not series_col or not dnu_col:
        return set()
    out: Set[str] = set()
    truthy = {"1", "true", "yes", "y", "t"}
    for series, flag in zip(df[series_col], df[dnu_col]):
        if pd.isna(series):
            continue
        flag_str = "" if pd.isna(flag) else str(flag).strip().lower()
        if flag_str in truthy:
            out.add(str(series).strip())
    return out


def build_series_block_map(iso: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for entry in COUNTRY_BLOCKS:
        if entry.get("iso_code") != iso:
            continue
        for block in entry.get("blocks", []):
            key = block.get("key")
            for code in block.get("series_codes", []):
                mapping[code] = key
    return mapping


def base_feature_name(feature: str) -> str:
    return feature.split("_lag")[0] if "_lag" in feature else feature


def infer_block(feature: str, iso: str, lookup: Dict[str, str]) -> str:
    base_name = base_feature_name(feature)
    if base_name in lookup:
        return lookup[base_name]
    if feature.startswith(f"{iso}_") and "_pc" in feature:
        remainder = feature[len(f"{iso}_"):]
        return remainder.split("_pc")[0]
    if "FCI" in feature:
        return "financial_markets"
    return "unknown"


def expected_sign(feature: str) -> int | None:
    name = base_feature_name(feature).lower()
    for token, sign in SIGN_PRIORS:
        if token in name:
            return sign
    return None


def run_lasso_for_iso(
    iso: str,
    target: str,
    *,
    max_iter: int,
    tol: float,
    min_alpha: float,
    alpha_points: int,
) -> None:
    factors_path = FACTOR_DIR_DAILY / f"{iso}_factors_daily.csv"
    rt_path = RT_DAILY_DIR / f"{iso}_Rt_daily.csv"
    if not factors_path.exists():
        print(f"[SKIP] Daily factors missing for {iso}: {factors_path}")
        return
    if not rt_path.exists():
        print(f"[SKIP] Daily Rt missing for {iso}: {rt_path}")
        return

    X_df = pd.read_csv(factors_path, index_col=0, parse_dates=True).sort_index()
    y_df = pd.read_csv(rt_path, index_col=0, parse_dates=True).sort_index()

    manifest_path = FACTOR_DIR_DAILY / f"{iso}_manifest_daily.json"
    if not manifest_path.exists():
        print(
            f"[SKIP] Step 3 daily manifest missing for {iso}: {manifest_path} (rerun Step 3 daily so Step 4 cannot reintroduce trimmed series)"
        )
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"[SKIP] Failed to parse daily manifest for {iso}: {manifest_path}: {exc}")
        return
    allowed: Set[str] = set()
    if isinstance(manifest, list):
        for item in manifest:
            if isinstance(item, dict) and item.get("column"):
                allowed.add(str(item["column"]))
            elif isinstance(item, str):
                allowed.add(str(item))
    if allowed:
        X_df = X_df[[c for c in X_df.columns if c in allowed]]

    if target not in y_df.columns:
        if y_df.shape[1] == 1:
            y_series = y_df.iloc[:, 0]
        else:
            print(f"[SKIP] Target {target} not in Rt file for {iso}")
            return
    else:
        y_series = y_df[target]

    panel = X_df.join(y_series, how="inner").dropna(how="any")
    if panel.shape[0] < 60:
        print(f"[SKIP] Not enough aligned daily observations for {iso}")
        return

    excluded_bases = load_do_not_use_series()
    feature_names = [
        c
        for c in panel.columns
        if c != y_series.name and base_feature_name(c) not in excluded_bases
    ]
    dropped = panel.shape[1] - 1 - len(feature_names)
    if dropped > 0:
        print(f"[INFO] Dropped {dropped} do_not_use daily features for {iso}")
    X = panel[feature_names].values
    y = panel[y_series.name].values

    tscv = TimeSeriesSplit(n_splits=5)
    effective_min_alpha = max(min_alpha, MIN_ALPHA)
    if alpha_points < 3:
        alpha_points = 3
    alphas = np.logspace(np.log10(effective_min_alpha), 0, alpha_points)

    enet_cv = ElasticNetCV(
        alphas=alphas,
        l1_ratio=ELASTIC_NET_L1_RATIO,
        cv=tscv,
        max_iter=max_iter,
        tol=tol,
        n_jobs=None,
    )

    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("model", enet_cv),
        ]
    )
    pipeline.fit(X, y)
    enet_cv_fitted = pipeline.named_steps["model"]

    out_dir = FEATURE_DIR_DAILY
    out_dir.mkdir(parents=True, exist_ok=True)

    cv_path = out_dir / f"{iso}_{target}_lasso_cv_daily.csv"
    coef_path = out_dir / f"{iso}_{target}_coeffs_daily.csv"

    pd.DataFrame({"alphas": enet_cv_fitted.alphas_, "mse_path_mean": enet_cv_fitted.mse_path_.mean(axis=1)}).to_csv(cv_path, index=False)

    series_block_map = build_series_block_map(iso)
    feature_blocks = [infer_block(name, iso, series_block_map) for name in feature_names]

    coefs = pd.Series(enet_cv_fitted.coef_, index=feature_names)
    block_cap_applied = False
    block_groups: Dict[str, List[str]] = {}
    for name, block in zip(feature_names, feature_blocks):
        block_groups.setdefault(block, []).append(name)

    for block, names in block_groups.items():
        if block == "unknown":
            continue
        nonzero = [n for n in names if not np.isclose(coefs[n], 0.0)]
        if len(nonzero) <= MAX_NONZERO_PER_BLOCK:
            continue
        keep = sorted(nonzero, key=lambda n: abs(coefs[n]), reverse=True)[:MAX_NONZERO_PER_BLOCK]
        for name in set(nonzero) - set(keep):
            coefs[name] = 0.0
            block_cap_applied = True

    coefs.to_csv(coef_path, header=["coefficient"])

    holdout_size = min(HOLDOUT_WINDOW, max(MIN_HOLDOUT, panel.shape[0] // 5))
    if panel.shape[0] - holdout_size < 50:
        holdout_size = 0

    diagnostics = {
        "iso": iso,
        "target": target,
        "alpha": float(enet_cv_fitted.alpha_),
        "l1_ratio": ELASTIC_NET_L1_RATIO,
        "max_iter": max_iter,
        "tol": tol,
        "block_cap_applied": block_cap_applied,
    }

    if holdout_size > 0:
        train_X = X[:-holdout_size]
        train_y = y[:-holdout_size]
        test_X = X[-holdout_size:]
        test_y = y[-holdout_size:]

        holdout_scaler = StandardScaler()
        train_X_scaled = holdout_scaler.fit_transform(train_X)
        test_X_scaled = holdout_scaler.transform(test_X)

        oot_model = ElasticNet(
            alpha=enet_cv_fitted.alpha_,
            l1_ratio=ELASTIC_NET_L1_RATIO,
            max_iter=max_iter,
            tol=tol,
        )
        oot_model.fit(train_X_scaled, train_y)

        train_pred = oot_model.predict(train_X_scaled)
        test_pred = oot_model.predict(test_X_scaled)
        train_rmse = float(np.sqrt(np.mean((train_y - train_pred) ** 2)))
        test_rmse = float(np.sqrt(np.mean((test_y - test_pred) ** 2)))
        ratio = float(test_rmse / train_rmse) if train_rmse > 0 else float("inf")
        diagnostics.update(
            {
                "holdout_size": holdout_size,
                "rmse_train": train_rmse,
                "rmse_holdout": test_rmse,
                "rmse_ratio": ratio,
                "rmse_ratio_threshold": 1.2,
                "rmse_ratio_passed": ratio <= 1.2,
            }
        )
    else:
        diagnostics.update(
            {
                "holdout_size": 0,
                "rmse_train": None,
                "rmse_holdout": None,
                "rmse_ratio": None,
                "rmse_ratio_threshold": 1.2,
                "rmse_ratio_passed": True,
            }
        )

    sign_warnings = []
    for feature, coef in coefs.items():
        if np.isclose(coef, 0.0):
            continue
        sign_hint = expected_sign(feature)
        if sign_hint is None:
            continue
        if np.sign(coef) == 0:
            continue
        if np.sign(coef) != np.sign(sign_hint):
            sign_warnings.append(
                {
                    "feature": feature,
                    "coefficient": float(coef),
                    "expected_sign": "positive" if sign_hint > 0 else "negative",
                }
            )

    diagnostics["sign_warnings"] = sign_warnings

    diag_path = MODEL_DIAG_DIR / f"{iso}_{target}_diagnostics_daily.json"
    with diag_path.open("w", encoding="utf-8") as fp:
        json.dump(diagnostics, fp, indent=2)

    print(f"[DONE] Daily ElasticNet for {iso} target {target}")


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily ElasticNet driver mappings with diagnostics")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO codes to process")
    parser.add_argument("--target", type=str, default="Rt_daily", help="Target column in Rt file")
    parser.add_argument("--max-iter", type=int, default=50000, help="Maximum iterations for the solver")
    parser.add_argument("--tol", type=float, default=1e-5, help="Optimizer tolerance for solver convergence")
    parser.add_argument(
        "--min-alpha",
        type=float,
        default=1e-3,
        help="Lowest alpha to try in the ElasticNetCV grid",
    )
    parser.add_argument(
        "--alpha-points",
        type=int,
        default=ALPHA_POINTS,
        help="Number of alphas in the log-space grid",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    ensure_dirs()
    args = parse_args(argv)
    for iso in args.isos:
        run_lasso_for_iso(
            iso,
            args.target,
            max_iter=args.max_iter,
            tol=args.tol,
            min_alpha=args.min_alpha,
            alpha_points=args.alpha_points,
        )


if __name__ == "__main__":  # pragma: no cover
    main()
