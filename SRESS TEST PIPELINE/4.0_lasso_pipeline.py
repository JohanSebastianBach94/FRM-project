#!/usr/bin/env python3
"""Step 4 sparse mapping runner (Lasso/ElasticNet) with diagnostics."""
from __future__ import annotations

import argparse
import json
import logging
import warnings
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import joblib
import numpy as np
import pandas as pd
import yaml
from sklearn.linear_model import ElasticNet, ElasticNetCV, Lasso, LassoCV
from sklearn.exceptions import ConvergenceWarning
from sklearn.metrics import r2_score
from sklearn.model_selection import BaseCrossValidator, TimeSeriesSplit, cross_validate
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch
from statsmodels.stats.stattools import jarque_bera
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform


warnings.filterwarnings("ignore", category=ConvergenceWarning)


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

COUNTRY_BLOCKS_PATH = Path("config") / "country_blocks_extended.yaml"
FROZEN_BLOCKS_PATH = Path("outputs") / "country_block_definition.json"
TARGETS_CONFIG_PATH = Path("config") / "step4_targets.yaml"
CATALOG_PATH = Path("catalog.csv")
FACTOR_DIR = Path("analysis_outputs") / "factor_preparation"
DAILY_SHORTLIST_DIR = Path("analysis_outputs") / "factors_daily_shortlist"
FULL_TARGET_PANEL_PATH = Path("data") / "cleaned_monthly_panel_full.parquet"
TARGET_PANEL_PATH = Path("data") / "cleaned_monthly_panel.parquet"
MODEL_DIR = Path("models") / "lasso"
FEATURE_OUTPUT_DIR = Path("analysis_outputs") / "feature_contributions"
WALK_FORWARD_MIN_TRAIN = 12


def is_macro_target(target: str) -> bool:
    t = str(target).upper()
    if "_BETA" in t or t.endswith("BETA0") or t.endswith("BETA1") or t.endswith("BETA2"):
        return False
    return any(key in t for key in ("GDP", "CPI", "UNRATE"))


def transform_target_series(target: str, s: pd.Series) -> Tuple[pd.Series, str]:
    """Return (transformed_series, transform_name).

    Motivation: macro level series are often non-stationary and hard to predict.
    For GDP and CPI we train on YoY log changes (approx %).
    Unemployment and DNSS betas are kept in levels.
    """

    t = str(target).upper()
    y = pd.to_numeric(s, errors="coerce").astype(float)
    if ("GDP" in t) or (t == "GDPC1"):
        # YoY % change (log approximation when strictly positive).
        y_pos = y.where(y > 0)
        yoy = 100.0 * (np.log(y_pos) - np.log(y_pos.shift(12)))
        if yoy.notna().sum() < 24:
            yoy = 100.0 * (y / y.shift(12) - 1.0)
        return yoy, "yoy_log_pct"
    if "CPI" in t:
        y_pos = y.where(y > 0)
        yoy = 100.0 * (np.log(y_pos) - np.log(y_pos.shift(12)))
        if yoy.notna().sum() < 24:
            yoy = 100.0 * (y / y.shift(12) - 1.0)
        return yoy, "yoy_log_pct"
    return y, "level"


def ensure_dirs() -> None:
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    FEATURE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def base_feature_name(feature: str) -> str:
    return feature.split("_lag")[0] if "_lag" in feature else feature


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


def load_country_targets(config_path: Path | None = None) -> Tuple[Dict[str, List[str]], List[str]]:
    """Return (per_iso_targets, global_targets).

    Preferred: explicit targets config at config/step4_targets.yaml.
    Fallback: old behavior (derive targets from blocks) for compatibility.
    """

    config_path = config_path or TARGETS_CONFIG_PATH
    if config_path.exists():
        try:
            with config_path.open("r", encoding="utf-8") as fp:
                payload = yaml.safe_load(fp) or {}
        except Exception:
            payload = {}
        if isinstance(payload, dict):
            targets = payload.get("targets", {})
            if isinstance(targets, dict):
                global_targets = targets.get("global", [])
                if not isinstance(global_targets, list):
                    global_targets = []
                per_iso = targets.get("per_iso", {})
                if not isinstance(per_iso, dict):
                    per_iso = {}
                per_iso_map: Dict[str, List[str]] = {}
                for iso, series in per_iso.items():
                    if not iso or not isinstance(series, list):
                        continue
                    cleaned = [str(x).strip() for x in series if str(x).strip()]
                    per_iso_map[str(iso).upper()] = cleaned
                global_cleaned = [str(x).strip() for x in global_targets if str(x).strip()]
                return per_iso_map, global_cleaned

    # Fallback: treat block series codes as "targets" (legacy behavior).
    logger.warning(
        "No explicit Step 4 targets config found at %s; falling back to block-derived targets (legacy).",
        config_path,
    )
    if FROZEN_BLOCKS_PATH.exists():
        payload = json.loads(FROZEN_BLOCKS_PATH.read_text(encoding="utf-8"))
        mapping: Dict[str, List[str]] = {}
        for iso, entry in payload.items():
            codes: List[str] = []
            for block in entry.get("blocks", []) or []:
                codes.extend(block.get("series_codes", []) or [])
            mapping[str(iso).upper()] = sorted(set(str(x) for x in codes))
        return mapping, []

    with COUNTRY_BLOCKS_PATH.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp)
    mapping = {}
    for entry in payload.get("country_blocks", []):
        iso = entry.get("iso_code")
        if not iso:
            continue
        codes: List[str] = []
        for block in entry.get("blocks", []):
            codes.extend(block.get("series_codes", []))
        mapping[str(iso).upper()] = sorted(set(codes))
    return mapping, []


def load_target_panel() -> pd.DataFrame:
    for path in (FULL_TARGET_PANEL_PATH, TARGET_PANEL_PATH):
        if path.exists():
            panel = pd.read_parquet(path)
            if not isinstance(panel.index, pd.DatetimeIndex):
                panel.index = pd.to_datetime(panel.index)
            logger.info("Loaded target panel from %s", path)
            return panel.sort_index()
    raise FileNotFoundError(f"Target panel missing at {FULL_TARGET_PANEL_PATH} or {TARGET_PANEL_PATH}")


def _to_month_end_business_day(df: pd.DataFrame) -> pd.DataFrame:
    """Downsample a daily panel to the last *available* date in each month.

    The cleaned target panel is indexed by month-end business day (e.g. 2025-10-31, 2025-11-28),
    not calendar month-end. Using groupby(period).tail(1) preserves those exact dates.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Expected DatetimeIndex")
    df = df.sort_index()
    month = df.index.to_period("M")
    out = df.groupby(month, sort=False).tail(1)
    out = out.loc[~out.index.duplicated(keep="last")]
    return out


def load_factor_panel(iso: str, source: str) -> pd.DataFrame:
    if source == "daily_shortlist":
        path = DAILY_SHORTLIST_DIR / f"{iso}_factors_daily_shortlist.csv"
        if not path.exists():
            raise FileNotFoundError(f"Daily shortlist factor panel missing at {path}")

        df = pd.read_csv(path, parse_dates=["date"])
        if "date" not in df.columns:
            raise ValueError(f"Daily shortlist file missing 'date' column: {path}")
        df = df.set_index("date")
        df = df.loc[:, ~df.columns.duplicated()]
        # Align to target panel's month-end business-day index.
        df = _to_month_end_business_day(df)
        return df

    if source == "pca":
        path = FACTOR_DIR / f"{iso}_pca_components.csv"
        if not path.exists():
            fallback = FACTOR_DIR / f"{iso}_factors.csv"
            if fallback.exists():
                logger.warning("PCA components missing for %s; falling back to factor panel", iso)
                path = fallback
            else:
                raise FileNotFoundError(f"Factor panel not found for {iso}")
    else:
        path = FACTOR_DIR / f"{iso}_factors.csv"
    if not path.exists():
        raise FileNotFoundError(f"Factor panel missing at {path}")
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df = df.loc[:, ~df.columns.duplicated()]

    if source == "pca":
        meta_path = FACTOR_DIR / f"{iso}_pca_component_metadata.json"
        if not meta_path.exists():
            raise FileNotFoundError(
                f"Step 3 PCA component metadata missing for {iso} at {meta_path}. "
                "Re-run Step 3 factor preparation so Step 4 cannot consume ungoverned PCA outputs."
            )
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to parse PCA component metadata for {iso}: {meta_path}: {exc}")
        allowed: Set[str] = set()
        if isinstance(meta, list):
            for item in meta:
                if isinstance(item, dict) and item.get("component"):
                    allowed.add(str(item["component"]))
        if not allowed:
            raise ValueError(f"PCA component metadata for {iso} contains no components: {meta_path}")
        keep = [c for c in df.columns if c in allowed]
        if not keep:
            raise ValueError(f"No PCA component columns in {path} match metadata {meta_path}")
        return df[keep]

    manifest_path = FACTOR_DIR / f"{iso}_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Step 3 manifest missing for {iso} at {manifest_path}. "
            "Re-run Step 3 factor preparation so Step 4 cannot reintroduce trimmed series."
        )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Failed to parse manifest for {iso}: {manifest_path}: {exc}")
    allowed: Set[str] = set()
    if isinstance(manifest, list):
        for item in manifest:
            if isinstance(item, dict) and item.get("column"):
                allowed.add(str(item["column"]))
            elif isinstance(item, str):
                allowed.add(str(item))
    if not allowed:
        raise ValueError(f"Manifest for {iso} contains no columns: {manifest_path}")
    keep = [c for c in df.columns if c in allowed]
    if not keep:
        raise ValueError(f"No factor columns in {path} match manifest {manifest_path}")
    return df[keep]


def build_feature_target_pair(
    df: pd.DataFrame,
    targets: pd.Series,
    target: str,
    *,
    excluded_bases: Set[str] | None = None,
    exclude_target_contemporaneous: bool = True,
) -> Tuple[pd.DataFrame, pd.Series]:
    drop_prefix = f"{target}_"
    excluded_bases = excluded_bases or set()
    feature_cols: List[str] = []
    for col in df.columns:
        if col == target or col.startswith(drop_prefix):
            continue
        base = base_feature_name(col)
        if base in excluded_bases:
            continue
        if exclude_target_contemporaneous and base == target:
            # Allow AR terms (lag1+) but forbid contemporaneous leakage.
            if col == target or col.endswith("_lag0"):
                continue
        feature_cols.append(col)
    if not feature_cols:
        raise ValueError("No eligible features after removing target columns")
    panel = pd.concat([df[feature_cols], targets.rename(target)], axis=1)
    panel = panel.dropna()
    if panel.shape[0] < 24 or panel.shape[1] <= 1:
        raise ValueError("Insufficient aligned data for modeling")
    return panel[feature_cols], panel[target]


def prune_correlated_features(X: pd.DataFrame, threshold: float = 0.98) -> Tuple[pd.DataFrame, List[str]]:
    if X.shape[1] <= 1:
        return X, []
    corr = X.corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    to_drop = [col for col in upper.columns if (upper[col] > threshold).any()]
    if to_drop:
        return X.drop(columns=to_drop), to_drop
    return X, []


def build_alpha_grid(X_train: pd.DataFrame, y_train: pd.Series, start: float, end: float, steps: int) -> np.ndarray:
    """Alpha grid computed in standardized feature space.

    The Step 4 estimators are always trained behind StandardScaler, so the alpha
    grid should be scaled using standardized X to avoid pathological grids.
    """

    steps = max(3, int(steps))
    raw = np.logspace(start, end, steps)
    denom = len(y_train)
    if denom <= 0:
        return raw
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X_train.values)
    yc = np.asarray(y_train, dtype=float)
    yc = yc - float(np.nanmean(yc))
    cov = Xs.T @ yc
    alpha_max = float(np.max(np.abs(cov))) / denom
    alpha_max = max(alpha_max, 1e-8)
    return raw * alpha_max


def build_lasso_pipeline(
    alphas: np.ndarray,
    cv: BaseCrossValidator,
    max_iter: int,
    tol: float,
    *,
    model: str = "lasso",
    l1_ratio: float = 0.2,
) -> Tuple[Pipeline, object]:
    if model == "elasticnet":
        estimator = ElasticNetCV(
            alphas=alphas,
            l1_ratio=float(l1_ratio),
            cv=cv,
            random_state=42,
            max_iter=max_iter,
            tol=tol,
        )
        pipeline = Pipeline([("scaler", StandardScaler()), ("enet", estimator)])
        return pipeline, estimator
    lasso = LassoCV(alphas=alphas, cv=cv, random_state=42, max_iter=max_iter, tol=tol)
    pipeline = Pipeline([("scaler", StandardScaler()), ("lasso", lasso)])
    return pipeline, lasso


def split_train_test(X: pd.DataFrame, y: pd.Series, ratio: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    split_idx = int(len(y) * ratio)
    split_idx = max(1, min(split_idx, len(y) - 1))
    return X.iloc[:split_idx], X.iloc[split_idx:], y.iloc[:split_idx], y.iloc[split_idx:]


def cross_validate_r2(pipeline: Pipeline, X_train: pd.DataFrame, y_train: pd.Series, cv: BaseCrossValidator) -> Tuple[float, float, List[float]]:
    scores = cross_validate(
        pipeline,
        X_train,
        y_train,
        cv=cv,
        scoring="r2",
        return_train_score=False,
        error_score=np.nan,
    )["test_score"]
    if scores.size == 0 or np.all(np.isnan(scores)):
        return float("nan"), float("nan"), []
    return float(np.nanmean(scores)), float(np.nanstd(scores)), [float(s) for s in scores]


def permutation_test_pvalue(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    alphas: np.ndarray,
    cv: BaseCrossValidator,
    max_iter: int,
    tol: float,
    trials: int,
    random_state: int = 42,
    *,
    model: str = "lasso",
    l1_ratio: float = 0.2,
) -> float:
    if int(trials) <= 0:
        return float("nan")
    X_train_np = np.asarray(X_train)
    y_train_np = np.asarray(y_train)
    X_test_np = np.asarray(X_test)
    y_test_np = np.asarray(y_test)

    base_pipeline, _ = build_lasso_pipeline(alphas, cv, max_iter, tol, model=model, l1_ratio=l1_ratio)
    base_pipeline.fit(X_train_np, y_train_np)
    base_r2 = r2_score(y_test_np, base_pipeline.predict(X_test_np))
    rng = np.random.default_rng(random_state)
    exceed = 0
    for trial in range(trials):
        # Time-series-safe null: circular shift instead of i.i.d. shuffle.
        if len(y_train_np) < 3:
            permuted = rng.permutation(y_train_np)
        else:
            shift = int(rng.integers(1, len(y_train_np)))
            permuted = np.roll(y_train_np, shift)
        pipeline, _ = build_lasso_pipeline(alphas, cv, max_iter, tol, model=model, l1_ratio=l1_ratio)
        pipeline.fit(X_train_np, permuted)
        perm_r2 = r2_score(y_test_np, pipeline.predict(X_test_np))
        if perm_r2 >= base_r2:
            exceed += 1
    return float((exceed + 1) / (trials + 1))


def moving_block_bootstrap_indices(n: int, block_len: int, rng: np.random.Generator) -> np.ndarray:
    if n <= 0:
        return np.array([], dtype=int)
    block_len = int(max(1, min(block_len, n)))
    needed = int(np.ceil(n / block_len))
    indices: List[int] = []
    for _ in range(needed):
        start = int(rng.integers(0, n))
        indices.extend(((start + j) % n) for j in range(block_len))
    return np.asarray(indices[:n], dtype=int)


def stability_selection(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    alpha: float,
    bootstraps: int,
    max_iter: int,
    tol: float,
    random_state: int = 42,
    *,
    model: str = "lasso",
    l1_ratio: float = 0.2,
) -> Dict[str, float]:
    if int(bootstraps) <= 0:
        return {}
    rng = np.random.default_rng(random_state)
    counters: Counter[str] = Counter()
    n = len(y_train)
    block_len = max(3, min(12, n // 10 if n else 3))
    for _ in range(bootstraps):
        idx = moving_block_bootstrap_indices(n, block_len, rng)
        Xb = X_train.iloc[idx]
        yb = y_train.iloc[idx]
        if model == "elasticnet":
            reg = ElasticNet(alpha=alpha, l1_ratio=float(l1_ratio), max_iter=max_iter, tol=tol)
            estimator = Pipeline([("scaler", StandardScaler()), ("enet", reg)])
            coef = "enet"
        else:
            reg = Lasso(alpha=alpha, max_iter=max_iter, tol=tol)
            estimator = Pipeline([("scaler", StandardScaler()), ("lasso", reg)])
            coef = "lasso"
        estimator.fit(Xb, yb)
        coef_values = estimator.named_steps[coef].coef_
        for name, value in zip(X_train.columns, coef_values):
            if abs(value) > 1e-9:
                counters[name] += 1
    if not counters:
        return {}
    return {name: counters[name] / bootstraps for name in sorted(counters)}


def residual_diagnostics(residuals: pd.Series) -> Dict[str, float]:
    diag: Dict[str, float] = {}
    if len(residuals) < 12:
        diag["note"] = "not enough observations"
        return diag
    try:
        ljung = acorr_ljungbox(residuals, lags=[12], return_df=True)
        diag["ljung_box_pvalue"] = float(ljung["lb_pvalue"].iloc[-1])
    except Exception:
        diag["ljung_box_pvalue"] = float("nan")
    try:
        arch = het_arch(residuals)
        diag["arch_lm_pvalue"] = float(arch[1])
    except Exception:
        diag["arch_lm_pvalue"] = float("nan")
    try:
        jb = jarque_bera(residuals)
        diag["jarque_bera_pvalue"] = float(jb[1])
    except Exception:
        diag["jarque_bera_pvalue"] = float("nan")
    return diag


def contributions_summary(scaler: StandardScaler, coefficients: np.ndarray, feature_names: Iterable[str]) -> Tuple[str, str, Dict[str, float]]:
    scale = np.asarray(getattr(scaler, "scale_", np.ones(len(feature_names))))
    safe_scale = np.where(scale == 0, 1.0, scale)
    original_coefs = coefficients / safe_scale
    ranked = sorted(
        ((name, float(coef)) for name, coef in zip(feature_names, original_coefs)),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    nonzero = [(name, coef) for name, coef in ranked if abs(coef) > 1e-9]
    top10 = nonzero[:10]
    top_str = ";".join(f"{name}:{coef:.6g}" for name, coef in top10)
    coeff_str = ";".join(f"{name}:{coef:.6g}" for name, coef in nonzero)
    return top_str, coeff_str, {name: coef for name, coef in nonzero}


def save_mse_records(alpha_grid: np.ndarray, mse_path: np.ndarray, iso: str, target: str) -> List[Dict[str, object]]:
    folds = mse_path.shape[1]
    rows: List[Dict[str, object]] = []
    for a_idx, alpha in enumerate(alpha_grid):
        for fold in range(folds):
            rows.append(
                {
                    "iso": iso,
                    "target": target,
                    "alpha": float(alpha),
                    "fold": fold + 1,
                    "mse": float(mse_path[a_idx, fold]),
                }
            )
    return rows


def save_path_df(alpha_grid: np.ndarray, coef_path: np.ndarray, columns: List[str], path: Path) -> None:
    df = pd.DataFrame(coef_path, index=alpha_grid, columns=columns)
    df.index.name = "alpha"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path)


def write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)


class WalkForwardSplit(BaseCrossValidator):
    def __init__(self, n_splits: int, min_train_size: int = 12) -> None:
        self.n_splits = max(1, n_splits)
        self.min_train_size = max(1, min_train_size)

    def get_n_splits(self, X=None, y=None, groups=None) -> int:
        return self.n_splits

    def split(self, X, y=None, groups=None):
        n_samples = len(X)
        remaining = max(0, n_samples - self.min_train_size)
        if remaining < self.n_splits:
            raise ValueError("Not enough observations for walk-forward splits")
        test_size = max(1, remaining // (self.n_splits + 1))
        for split_idx in range(self.n_splits):
            train_end = min(self.min_train_size + split_idx * test_size, n_samples - 1)
            test_start = train_end
            test_end = min(test_start + test_size, n_samples)
            if test_end <= test_start:
                break
            yield np.arange(train_end), np.arange(test_start, test_end)


def compute_cluster_summary(X: pd.DataFrame, threshold: float = 0.5) -> Dict[str, object]:
    if X.shape[1] < 2:
        return {"labels": {}, "big_groups": [], "top_pairs": []}
    corr = X.corr().fillna(0)
    dist = (1 - np.abs(corr)).values
    condensed = squareform(dist)
    link = linkage(condensed, method="average")
    labels = fcluster(link, t=threshold, criterion="distance")
    group_map: Dict[int, List[str]] = {}
    for label, name in zip(labels, X.columns):
        group_map.setdefault(int(label), []).append(name)
    big_groups = [sorted(members) for members in group_map.values() if len(members) > 1]
    big_groups.sort(key=len, reverse=True)
    pair_list: List[Dict[str, object]] = []
    abs_corr = corr.abs()
    for i in range(len(X.columns)):
        for j in range(i + 1, len(X.columns)):
            pair_list.append({
                "pair": [X.columns[i], X.columns[j]],
                "corr": float(abs_corr.iat[i, j]),
            })
    pair_list.sort(key=lambda item: item["corr"], reverse=True)
    return {
        "labels": dict(zip(X.columns, labels.tolist())),
        "big_groups": big_groups[:5],
        "top_pairs": pair_list[:5],
        "threshold": threshold,
    }


def format_cluster_summary(cluster_info: Dict[str, object]) -> str:
    return json.dumps(cluster_info, ensure_ascii=False)


def compute_condition_number(X: pd.DataFrame) -> float:
    try:
        return float(np.linalg.cond(X.values))
    except Exception:
        return float("nan")


def min_eigenvalue_selected(X: pd.DataFrame, selected: List[str]) -> float | None:
    if len(selected) < 2:
        return None
    corr = X.loc[:, selected].corr().fillna(0)
    try:
        eigvals = np.linalg.eigvalsh(corr.values)
        return float(np.min(eigvals))
    except Exception:
        return None


def build_cv_split(n_splits: int, walk_forward: bool) -> BaseCrossValidator:
    if walk_forward:
        return WalkForwardSplit(n_splits=n_splits, min_train_size=WALK_FORWARD_MIN_TRAIN)
    return TimeSeriesSplit(n_splits=n_splits)


def evaluate_instability(train_r2: float, test_r2: float) -> Tuple[bool, str]:
    if np.isnan(train_r2) or np.isnan(test_r2):
        return True, "insufficient_r2"
    diff = abs(train_r2 - test_r2)
    if test_r2 < 0:
        return True, "negative_test_r2"
    if diff > 0.35:
        return True, "r2_diff_large"
    return False, "stable"


def train_target(
    iso: str,
    target: str,
    features: pd.DataFrame,
    target_series: pd.Series,
    train_ratio: float,
    cv_splits: int,
    alpha_start: float,
    alpha_end: float,
    alpha_steps: int,
    max_iter: int,
    tol: float,
    prune_threshold: float,
    permutation_trials: int,
    stability_bootstraps: int,
    walk_forward: bool,
    condition_threshold: float,
    eigen_threshold: float,
    cluster_threshold: float,
    model: str = "lasso",
    l1_ratio: float = 0.2,
    excluded_bases: Set[str] | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    transformed_target, transform_name = transform_target_series(target, target_series)
    target_series = transformed_target

    # Macro targets: prefer walk-forward CV and stronger regularization.
    macro = is_macro_target(target)
    effective_walk_forward = bool(walk_forward or macro)
    effective_model = model
    effective_l1_ratio = float(l1_ratio)
    if macro and model == "lasso":
        effective_model = "elasticnet"
        effective_l1_ratio = 0.2

    X, y = build_feature_target_pair(
        features,
        target_series,
        target,
        excluded_bases=excluded_bases,
        exclude_target_contemporaneous=True,
    )

    # Macro targets are typically persistent; add lagged target terms (AR) to avoid
    # forcing the model to proxy persistence via correlated factors.
    ar_lags_added: List[str] = []
    if macro:
        tmp = pd.concat([X, y.rename(target)], axis=1)
        for lag in (1, 2, 3):
            col = f"{target}_lag{lag}"
            tmp[col] = tmp[target].shift(lag)
            ar_lags_added.append(col)
        tmp = tmp.dropna()
        y = tmp[target]
        X = tmp.drop(columns=[target])
    cluster_info = compute_cluster_summary(X, threshold=cluster_threshold)
    cluster_out = FEATURE_OUTPUT_DIR / f"clusters_{iso}_{target}.json"
    write_json(cluster_out, cluster_info)
    X_pruned, pruned_features = prune_correlated_features(X, threshold=prune_threshold)
    n_obs_total = int(len(y))
    y_start = y.index.min() if len(y.index) else None
    y_end = y.index.max() if len(y.index) else None

    X_train, X_test, y_train, y_test = split_train_test(X_pruned, y, train_ratio)
    n_train = len(y_train)
    n_test = len(y_test)

    def _fmt_idx(v: object) -> str:
        if v is None:
            return ""
        try:
            ts = pd.to_datetime(v)
            if pd.isna(ts):
                return ""
            return ts.date().isoformat()
        except Exception:
            return str(v)

    train_start = y_train.index.min() if len(y_train.index) else None
    train_end = y_train.index.max() if len(y_train.index) else None
    test_start = y_test.index.min() if len(y_test.index) else None
    test_end = y_test.index.max() if len(y_test.index) else None
    if effective_walk_forward:
        available_splits = max(0, n_train - WALK_FORWARD_MIN_TRAIN)
        max_splits = min(cv_splits, available_splits)
        if max_splits < 1:
            raise ValueError("Not enough data for walk-forward cross-validation")
    else:
        available_splits = max(1, n_train - 1)
        max_splits = min(cv_splits, available_splits)
        if max_splits < 2:
            raise ValueError("Not enough data for the requested cross-validation splits")
    cv = build_cv_split(max_splits, effective_walk_forward)
    alphas = build_alpha_grid(X_train, y_train, alpha_start, alpha_end, alpha_steps)
    pipeline, cv_estimator = build_lasso_pipeline(alphas, cv, max_iter, tol, model=effective_model, l1_ratio=effective_l1_ratio)
    pipeline.fit(X_train, y_train)
    train_r2 = float(r2_score(y_train, pipeline.predict(X_train)))
    test_r2 = float(r2_score(y_test, pipeline.predict(X_test)))
    # Clean CV design: select alpha via CV estimator, then evaluate a fixed-alpha model.
    best_alpha = float(getattr(cv_estimator, "alpha_", np.nan))
    if effective_model == "elasticnet":
        fixed = Pipeline([
            ("scaler", StandardScaler()),
            ("enet", ElasticNet(alpha=best_alpha, l1_ratio=float(effective_l1_ratio), max_iter=max_iter, tol=tol)),
        ])
    else:
        fixed = Pipeline([
            ("scaler", StandardScaler()),
            ("lasso", Lasso(alpha=best_alpha, max_iter=max_iter, tol=tol)),
        ])
    cv_mean, cv_std, cv_scores = cross_validate_r2(fixed, X_train, y_train, cv)
    perm_pvalue = permutation_test_pvalue(
        X_train,
        y_train,
        X_test,
        y_test,
        alphas,
        cv,
        max_iter,
        tol,
        trials=permutation_trials,
        model=effective_model,
        l1_ratio=effective_l1_ratio,
    )
    stability = stability_selection(
        X_train,
        y_train,
        best_alpha,
        stability_bootstraps,
        max_iter,
        tol,
        model=effective_model,
        l1_ratio=effective_l1_ratio,
    )
    scaler: StandardScaler = pipeline.named_steps["scaler"]
    if effective_model == "elasticnet":
        coef_map = pipeline.named_steps["enet"].coef_
    else:
        coef_map = pipeline.named_steps["lasso"].coef_
    feature_names = list(X_train.columns)
    top_contribs, coeff_str, _ = contributions_summary(scaler, coef_map, feature_names)
    mse_path_attr = getattr(cv_estimator, "mse_path_", None)
    if isinstance(mse_path_attr, np.ndarray) and mse_path_attr.ndim == 3:
        mse_path = mse_path_attr[0]
    else:
        mse_path = mse_path_attr
    mse_records = save_mse_records(alphas, mse_path, iso, target) if isinstance(mse_path, np.ndarray) else []
    path_alpha = FEATURE_OUTPUT_DIR / f"{iso}_{target}_path.csv"
    coef_path_attr = getattr(cv_estimator, "coef_path_", None)
    if coef_path_attr is not None:
        save_path_df(getattr(cv_estimator, "alphas_", alphas), coef_path_attr, feature_names, path_alpha)
        path_alpha_str = str(path_alpha)
    else:
        path_alpha_str = ""
    stability_out = FEATURE_OUTPUT_DIR / f"stability_{iso}_{target}.csv"
    if stability:
        pd.DataFrame({"feature": list(stability.keys()), "frequency": list(stability.values())}).to_csv(stability_out, index=False)
    else:
        pd.DataFrame(columns=["feature", "frequency"]).to_csv(stability_out, index=False)
    perm_out = FEATURE_OUTPUT_DIR / f"permutation_test_{iso}_{target}.json"
    write_json(perm_out, {"p_value": perm_pvalue, "trials": permutation_trials})
    residual_out = FEATURE_OUTPUT_DIR / f"residuals_{iso}_{target}.json"
    write_json(residual_out, {
        "train": residual_diagnostics(y_train - pipeline.predict(X_train)),
        "test": residual_diagnostics(y_test - pipeline.predict(X_test)),
    })
    model_prefix = "enet" if effective_model == "elasticnet" else "lasso"
    model_path = MODEL_DIR / f"{model_prefix}_{iso}_{target}.joblib"
    joblib.dump(pipeline, model_path)
    instability_flag, instability_reason = evaluate_instability(train_r2, test_r2)
    overfitting_flag = cv_mean < train_r2 - 0.05 or cv_mean <= 0
    degrees_of_freedom = int(np.count_nonzero(coef_map))
    condition_number = compute_condition_number(X_train)
    condition_flag = condition_number > condition_threshold
    selected_features = [name for name, coef in zip(feature_names, coef_map) if abs(coef) > 1e-9]
    min_eig = min_eigenvalue_selected(X_pruned, selected_features)
    eigen_flag = min_eig is not None and min_eig < eigen_threshold
    cv_mse_file = FEATURE_OUTPUT_DIR / f"{iso}_{model_prefix}_cv.csv"
    return (
        {
            "iso": iso,
            "target": target,
            "target_transform": transform_name,
            "macro_target": macro,
            "ar_lags_added": ar_lags_added,
            "walk_forward_used": effective_walk_forward,
            "train_ratio_used": float(train_ratio),
            "n_obs_total": int(n_obs_total),
            "n_train": int(n_train),
            "n_test": int(n_test),
            "data_start": _fmt_idx(y_start),
            "data_end": _fmt_idx(y_end),
            "train_start": _fmt_idx(train_start),
            "train_end": _fmt_idx(train_end),
            "test_start": _fmt_idx(test_start),
            "test_end": _fmt_idx(test_end),
            "cv_splits_requested": int(cv_splits),
            "cv_splits_used": int(max_splits),
            "walk_forward_min_train": int(WALK_FORWARD_MIN_TRAIN),
            "train_r2": train_r2,
            "test_r2": test_r2,
            "mean_cv_r2": cv_mean,
            "std_cv_r2": cv_std,
            "cv_scores": cv_scores,
            "alpha": float(getattr(cv_estimator, "alpha_", float("nan"))),
            **({"l1_ratio": float(effective_l1_ratio)} if effective_model == "elasticnet" else {}),
            "num_features": len(feature_names),
            "degrees_of_freedom": degrees_of_freedom,
            "permutation_pvalue": perm_pvalue,
            "stability_file": str(stability_out),
            "coef_path": path_alpha_str,
            "cv_mse_file": str(cv_mse_file),
            "top_contributions": top_contribs,
            "coefficients": coeff_str,
            "instability_flag": instability_flag,
            "instability_reason": instability_reason,
            "overfitting_flag": overfitting_flag,
            "permutation_path": str(perm_out),
            "residual_path": str(residual_out),
            "condition_number": condition_number,
            "condition_flag": condition_flag,
            "cluster_summary": str(cluster_out),
            "pruned_features": pruned_features,
            "pruned_count": len(pruned_features),
            "selected_features": selected_features,
            "min_eigenvalue": min_eig,
            "eigen_warning": eigen_flag,
            "model_path": str(model_path),
        },
        mse_records,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train sparse mappings (Step 4) with diagnostics")
    parser.add_argument("--countries", nargs="*", help="ISO codes to process")
    parser.add_argument("--feature-source", choices=["full", "pca", "daily_shortlist"], default="pca")
    parser.add_argument(
        "--macro-feature-source",
        choices=["full", "pca", "daily_shortlist"],
        default=None,
        help="Optional feature source override used only for macro targets (e.g. align macro inversion to Step 6 factors)",
    )
    parser.add_argument("--train-ratio", type=float, default=0.7)
    parser.add_argument("--cv-splits", type=int, default=5)
    parser.add_argument(
        "--targets-config",
        type=str,
        default=str(TARGETS_CONFIG_PATH),
        help="YAML file defining explicit Step 4 targets",
    )
    parser.add_argument(
        "--model",
        choices=["lasso", "elasticnet"],
        default="lasso",
        help="Estimator family for Step 4",
    )
    parser.add_argument(
        "--l1-ratio",
        type=float,
        default=0.5,
        help="ElasticNet l1_ratio (only used when --model elasticnet)",
    )
    parser.add_argument("--alpha-start", type=float, default=-6.0)
    parser.add_argument("--alpha-end", type=float, default=0.0)
    parser.add_argument("--alpha-steps", type=int, default=50)
    parser.add_argument("--lasso-max-iter", type=int, default=10_000)
    parser.add_argument("--lasso-tol", type=float, default=1e-4)
    parser.add_argument("--prune-corr-threshold", type=float, default=0.98)
    parser.add_argument("--permutation-trials", type=int, default=100, help="Set to 0 to skip permutation p-value")
    parser.add_argument("--stability-bootstraps", type=int, default=100, help="Set to 0 to skip bootstrap stability selection")
    parser.add_argument("--walk-forward", action="store_true", help="Use walk-forward cross-validation instead of TimeSeriesSplit")
    parser.add_argument("--condition-threshold", type=float, default=1e6)
    parser.add_argument("--eigen-threshold", type=float, default=0.02)
    parser.add_argument("--cluster-threshold", type=float, default=0.5)
    args = parser.parse_args()

    ensure_dirs()
    excluded_bases = load_do_not_use_series()
    if excluded_bases:
        logger.info("Loaded %d do_not_use series from %s", len(excluded_bases), CATALOG_PATH)
    per_iso_targets, global_targets = load_country_targets(Path(args.targets_config))
    target_panel = load_target_panel()
    selected = args.countries or sorted(per_iso_targets.keys())
    if not selected:
        logger.error("No ISO codes selected (and targets config has no per-ISO entries).")
        return

    for iso in selected:
        iso = str(iso).upper()
        iso_targets = list(global_targets) + list(per_iso_targets.get(iso, []))
        if not iso_targets:
            logger.warning("No targets configured for %s; skipping", iso)
            continue
        try:
            factor_panel_default = load_factor_panel(iso, args.feature_source)
        except FileNotFoundError as exc:
            logger.warning(str(exc))
            continue
        factor_panel_macro = None
        if args.macro_feature_source:
            try:
                factor_panel_macro = load_factor_panel(iso, str(args.macro_feature_source))
            except FileNotFoundError as exc:
                logger.warning("Macro feature source not available for %s: %s", iso, exc)
                factor_panel_macro = None
        missing_targets = [t for t in iso_targets if t not in target_panel.columns]
        if missing_targets:
            head = missing_targets[:5]
            tail = f" (+{len(missing_targets) - len(head)} more)" if len(missing_targets) > len(head) else ""
            logger.warning(
                "Coverage gap for %s: %d targets not in cleaned panel (%s%s); downstream models will skip them",
                iso,
                len(missing_targets),
                ", ".join(head),
                tail,
            )
        diagnostics: List[Dict[str, object]] = []
        mse_rows: List[Dict[str, object]] = []
        for target in iso_targets:
            if target not in target_panel.columns:
                continue
            target_series = target_panel[target]
            try:
                features_for_target = factor_panel_default
                feature_source_used = str(args.feature_source)
                if is_macro_target(target) and factor_panel_macro is not None:
                    features_for_target = factor_panel_macro
                    feature_source_used = str(args.macro_feature_source)

                stats, mse = train_target(
                    iso,
                    target,
                    features_for_target,
                    target_series,
                    train_ratio=args.train_ratio,
                    cv_splits=args.cv_splits,
                    alpha_start=args.alpha_start,
                    alpha_end=args.alpha_end,
                    alpha_steps=args.alpha_steps,
                    max_iter=args.lasso_max_iter,
                    tol=args.lasso_tol,
                    prune_threshold=args.prune_corr_threshold,
                    permutation_trials=args.permutation_trials,
                    stability_bootstraps=args.stability_bootstraps,
                    walk_forward=args.walk_forward,
                    condition_threshold=args.condition_threshold,
                    eigen_threshold=args.eigen_threshold,
                    cluster_threshold=args.cluster_threshold,
                    model=args.model,
                    l1_ratio=args.l1_ratio,
                    excluded_bases=excluded_bases,
                )
            except ValueError as exc:
                logger.warning("Skipping %s %s: %s", iso, target, exc)
                continue
            stats["feature_source_used"] = feature_source_used
            diagnostics.append(stats)
            mse_rows.extend(mse)
        if diagnostics:
            pd.DataFrame(diagnostics).to_csv(Path("analysis_outputs") / f"feature_contributions_{iso}.csv", index=False)
            logger.info("Wrote feature contribution summary for %s", iso)
        if mse_rows:
            model_prefix = "enet" if args.model == "elasticnet" else "lasso"
            pd.DataFrame(mse_rows).to_csv(FEATURE_OUTPUT_DIR / f"{iso}_{model_prefix}_cv.csv", index=False)
            logger.info("Saved CV MSE path for %s", iso)


if __name__ == "__main__":
    main()
