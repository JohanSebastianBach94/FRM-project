"""Pipeline runner for HAR, GARCH, and FIGARCH volatility forecasts."""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

import numpy as np
import pandas as pd

RUNNER_DIR = Path(__file__).resolve().parent
if str(RUNNER_DIR) not in sys.path:
    sys.path.insert(0, str(RUNNER_DIR))

DEFAULT_CONFIG_PATH = (RUNNER_DIR / "config" / "settings.json").resolve()

from data_prep import DataConfig, load_returns, prepare_har_inputs, realised_volatility
from diagnostics import DiagnosticResult, run_diagnostics
from models import FIGARCHModel, ForecastEvaluator, GARCHModel, HARModel
from models.selector import fit_all_models, score_models


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def resolve_split_index(index: pd.Index, split_cfg: Dict) -> int:
    """Resolve the train/test split index based on config."""
    length = len(index)
    if length == 0:
        raise ValueError("Empty index provided to resolve_split_index")

    min_points = int(split_cfg.get("min_train_points", 0) or 0)
    test_start = split_cfg.get("test_start_date")
    if test_start:
        dt = pd.to_datetime(test_start)
        if not isinstance(index, pd.DatetimeIndex):
            raise ValueError("test_start_date requires a DatetimeIndex")
        split_idx = int(index.searchsorted(dt))
        base = max(min_points, split_idx)
    else:
        ratio = float(split_cfg.get("train_ratio", 0.75) or 0.75)
        base = max(int(length * ratio), min_points)
    return max(1, min(base, length - 1))


def _min_variance(cfg: Dict | None) -> float:
    return float((cfg or {}).get("min_variance", 1e-8))


def _figarch_scale(cfg: Dict) -> float:
    fig_cfg = cfg.get("figarch", {})
    scale = float(fig_cfg.get("input_scale", 1) or 1)
    if scale <= 0:
        raise ValueError("figarch.input_scale must be positive")
    return scale


def _har_log_settings(cfg: Dict) -> Tuple[bool, float]:
    har_cfg = cfg.get("har", {})
    return bool(har_cfg.get("use_log", False)), float(har_cfg.get("log_epsilon", 1e-8))


def _restore_har_scale(pred: pd.Series, use_log: bool, eps: float) -> pd.Series:
    return pd.Series(np.exp(pred) - eps, index=pred.index) if use_log else pred


def _compute_adj_r2_from_aligned(aligned: pd.DataFrame) -> float | None:
    if aligned.empty:
        return None
    n = len(aligned)
    X = np.column_stack([np.ones(n), aligned.iloc[:, 1].values])
    y = aligned.iloc[:, 0].values
    try:
        coef, *_ = np.linalg.lstsq(X, y, rcond=None)
        yhat = X @ coef
        ss_res = float(np.nansum((y - yhat) ** 2))
        ss_tot = float(np.nansum((y - np.nanmean(y)) ** 2))
        if ss_tot <= 0:
            return None
        r2 = float(1 - ss_res / ss_tot)
        return float(1 - (1 - r2) * (n - 1) / (n - 2)) if n > 2 else float(r2)
    except np.linalg.LinAlgError:
        return None


def _compute_adj_r2_from_conditional(model, returns: pd.Series, cfg: Dict) -> float | None:
    cond = getattr(model, "conditional_volatility", None)
    if cond is None and getattr(model, "res", None) is not None:
        cond = getattr(model.res, "conditional_volatility", None)
        if cond is not None:
            cond = np.square(cond)
    if cond is None:
        return None
    aligned = pd.concat(
        [returns.iloc[-len(cond) :], pd.Series(cond, index=returns.index[-len(cond) :])],
        axis=1,
    ).dropna()
    aligned.columns = ["actual", "forecast"]
    return _compute_adj_r2_from_aligned(aligned)


def _process_forecasts(
    actual: pd.Series,
    forecast: pd.Series,
    min_var: float,
    cfg: Dict | None = None,
) -> Tuple[pd.Series, pd.Series, Dict[str, float | int | None]]:
    actual_aligned = actual.clip(lower=min_var)
    cleaned = forecast.replace([np.inf, -np.inf], np.nan).clip(lower=min_var)
    if cleaned.isna().any():
        fallback = actual_aligned.reindex(cleaned.index)
        cleaned = cleaned.fillna(fallback)
        if cleaned.isna().any():
            cleaned = cleaned.fillna(method="ffill")
        if cleaned.isna().any():
            cleaned = cleaned.fillna(min_var)
    cap_info = {
        "min_variance": min_var,
        "initial_nan_count": int(forecast.isna().sum()),
        "post_fill_nan_count": int(cleaned.isna().sum()),
    }
    cfg = cfg or {}
    cap_quantile = cfg.get("variance_cap_quantile")
    cap_multiplier = cfg.get("variance_cap_multiplier")
    max_variance = cfg.get("max_variance")
    cap = None
    if max_variance is not None:
        cap = max(min_var, float(max_variance))
    elif cap_quantile is not None or cap_multiplier is not None:
        ref = actual_aligned.reindex(cleaned.index)
        qv = ref.quantile(float(cap_quantile) if cap_quantile is not None else 0.999)
        mult = float(cap_multiplier) if cap_multiplier is not None else 5.0
        if np.isfinite(qv) and qv > 0:
            cap = max(min_var, float(qv) * mult)
    if cap is not None:
        cleaned = cleaned.clip(upper=cap)
        actual_aligned = actual_aligned.clip(upper=cap)
        cap_info.update(
            {
                "cap_value": cap,
                "cap_quantile": cap_quantile,
                "cap_multiplier": cap_multiplier,
            }
        )
    return actual_aligned, cleaned, cap_info


def batched_har_forecasts(
    features_level: pd.DataFrame,
    features_model: pd.DataFrame,
    split: int,
    batch_size: int,
    min_var: float,
    use_log: bool,
    log_epsilon: float,
    limit: int | None = None,
) -> Tuple[pd.Series, pd.Series]:
    start = split
    stop = len(features_level) if limit is None else min(len(features_level), split + limit)
    if start >= stop:
        empty = features_level.index[start:start]
        return (
            pd.Series(dtype=float, index=empty, name="rv_actual"),
            pd.Series(dtype=float, index=empty, name="har_forecast"),
        )
    actual_parts: List[pd.Series] = []
    forecast_parts: List[pd.Series] = []
    ptr = start
    while ptr < stop:
        end = min(stop, ptr + batch_size)
        idx = features_level.index[ptr:end]
        if idx.empty:
            break
        train = features_model.iloc[:ptr]
        if train.empty:
            break
        model = HARModel(train)
        model.fit()
        subset = features_model.loc[idx].drop(columns=["rv_target"])
        pred_log = model.predict(subset)
        pred = _restore_har_scale(pred_log, use_log, log_epsilon).clip(lower=min_var)
        forecast_parts.append(pred.rename("har_forecast"))
        actual_batch = features_level.loc[idx, "rv_target"].clip(lower=min_var).rename("rv_actual")
        actual_parts.append(actual_batch)
        ptr = end
    actual = pd.concat(actual_parts) if actual_parts else pd.Series(dtype=float, index=features_level.index[start:start])
    forecast = pd.concat(forecast_parts) if forecast_parts else pd.Series(dtype=float, index=features_level.index[start:start])
    return actual, forecast


def batched_figarch_forecasts(
    returns: pd.Series,
    order: Tuple[int, float, int],
    split: int,
    batch_size: int,
    limit: int | None = None,
    scale: float = 1.0,
) -> Tuple[pd.Series, pd.Series]:
    scale = float(scale or 1.0)
    if scale <= 0:
        raise ValueError("figarch scale_factor must be positive")
    start = split
    stop = len(returns) if limit is None else min(len(returns), split + limit)
    if start >= stop:
        empty = returns.index[start:start]
        return (
            pd.Series(dtype=float, index=empty, name="rv_actual"),
            pd.Series(dtype=float, index=empty, name="figarch_forecast"),
        )
    actual_parts: List[pd.Series] = []
    forecast_parts: List[pd.Series] = []
    ptr = start
    scaled_returns = returns * scale
    while ptr < stop:
        end = min(stop, ptr + batch_size)
        idx = returns.index[ptr:end]
        if idx.empty:
            break
        train = scaled_returns.iloc[:ptr]
        if train.empty:
            break
        model = FIGARCHModel(train, order)
        model.fit(disp=False)
        fc = model.forecast_series(index=idx)
        if isinstance(fc, pd.DataFrame):
            fc = fc.iloc[:, 0]
        if scale != 1:
            fc = fc.div(scale ** 2)
        forecast_parts.append(fc.rename("figarch_forecast"))
        actual_batch = returns.iloc[ptr:end].pow(2).rename("rv_actual")
        actual_parts.append(actual_batch)
        ptr = end
    actual = pd.concat(actual_parts) if actual_parts else pd.Series(dtype=float, index=returns.index[start:start])
    forecast = pd.concat(forecast_parts) if forecast_parts else pd.Series(dtype=float, index=returns.index[start:start])
    return actual, forecast


def export_dynamic_correlations(
    forecasts: Dict[str, pd.Series],
    window: int,
    out_dir: Path,
    filename: str,
    label: str,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not forecasts:
        (out_dir / filename).write_text("")
        warnings.warn(f"[{label}] No forecasts available; wrote empty file", RuntimeWarning)
        return
    df = pd.DataFrame(forecasts)
    df = df.sort_index(axis=1)
    cols = list(df.columns)
    pair_series: Dict[str, pd.Series] = {}
    for i, left in enumerate(cols):
        for right in cols[i + 1 :]:
            pair_series[f"{left}_{right}"] = df[left].rolling(window).corr(df[right])
    if not pair_series:
        (out_dir / filename).write_text("")
        warnings.warn(f"[{label}] Not enough series for correlations", RuntimeWarning)
        return
    out = pd.DataFrame(pair_series)
    out = out.dropna(how="all")
    if out.empty:
        (out_dir / filename).write_text("")
        warnings.warn(f"[{label}] Correlation matrix empty after dropping NaNs", RuntimeWarning)
        return
    out.to_csv(out_dir / filename, index_label="date")
    print(f"[{label}] Exported {len(out.columns)} correlation pairs to {filename}")


def _collect_factor_isos(factor_dir: Path, cfg: Dict) -> List[str]:
    if not factor_dir.exists():
        raise FileNotFoundError(f"Factor preparation directory not found: {factor_dir}")
    iso_cfg = cfg.get("factor_isos")
    if iso_cfg:
        if not isinstance(iso_cfg, list) or not iso_cfg:
            raise ValueError("factor_isos must be a non-empty list of ISO codes")
        return iso_cfg
    manifests = sorted(factor_dir.glob("*_manifest.json"))
    if not manifests:
        raise FileNotFoundError(f"No factor manifests found under {factor_dir}")
    return sorted({path.stem.split("_")[0] for path in manifests})


def _load_factor_manifest_columns(manifest_path: Path) -> List[str]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing factor manifest: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as fh:
        manifest = json.load(fh)
    columns = [entry.get("column") for entry in manifest if entry.get("column")]
    if not columns:
        raise ValueError(f"Factor manifest {manifest_path} contains no columns")
    return columns

_LAG_RE = re.compile(r"^(?P<base>.+)_lag(?P<lag>\d+)$")


def _filter_factor_columns_for_volatility(columns: List[str]) -> List[str]:
    """Step 7.1 semantics: benchmark volatility on return-like series, not on lag features.

    - Drops lagged columns (lag>=1)
    - Maps *_lag0 -> base name
    - Preserves order while de-duplicating
    """
    filtered: List[str] = []
    seen: set[str] = set()
    for col in columns:
        match = _LAG_RE.match(col)
        if match:
            lag = int(match.group("lag"))
            if lag != 0:
                continue
            col = match.group("base")
        if col not in seen:
            filtered.append(col)
            seen.add(col)
    return filtered


# ---------------------------------------------------------------------------
# Model blocks
# ---------------------------------------------------------------------------


def evaluate_har(asset: str, df: pd.DataFrame, cfg: Dict, outputs_dir: Path) -> Tuple[Dict, pd.Series]:
    rv_method = cfg.get("rv_method", "bipower")
    features_path = prepare_har_inputs(
        df,
        asset,
        cfg["har"]["lags"],
        outputs_dir / "features",
        rv_method=rv_method,
    )
    features_level = pd.read_csv(features_path, index_col=0, parse_dates=True)
    split = resolve_split_index(features_level.index, cfg["split"])
    use_log, log_eps = _har_log_settings(cfg)
    features_model = np.log(features_level + log_eps) if use_log else features_level.copy()
    min_var = _min_variance(cfg)
    train = features_model.iloc[:split]
    if train.empty:
        raise ValueError(f"Insufficient training observations for HAR model on {asset}")
    model = HARModel(train)
    model.fit()
    har_cfg = cfg.get("har", {})
    refit_interval = int(har_cfg.get("refit_interval", 0) or 0)
    max_forecasts = har_cfg.get("max_forecasts")
    if refit_interval > 0:
        actual, forecast = batched_har_forecasts(
            features_level,
            features_model,
            split,
            refit_interval,
            min_var,
            use_log,
            log_eps,
            max_forecasts,
        )
    else:
        test_features = features_model.iloc[split:].drop(columns=["rv_target"])
        pred_log = model.predict(test_features)
        forecast = _restore_har_scale(pred_log, use_log, log_eps).rename("har_forecast")
        actual = features_level.iloc[split:]["rv_target"].rename("rv_actual")
    actual_aligned, cleaned_forecast, cap = _process_forecasts(
        actual,
        forecast.rename(asset),
        min_var,
        cfg.get("variance_capping", {}),
    )
    evaluator = ForecastEvaluator()
    test_eval = evaluator.evaluate(actual_aligned, cleaned_forecast)
    aligned = pd.concat([actual_aligned, cleaned_forecast], axis=1, join="inner").dropna()
    adj_r2 = _compute_adj_r2_from_aligned(aligned)
    corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1])) if not aligned.empty else None
    rmse = float(np.sqrt(test_eval.mse)) if not aligned.empty else None
    mean_actual = float(aligned.iloc[:, 0].mean()) if not aligned.empty else None
    norm_rmse = float(rmse / mean_actual) if (rmse is not None and mean_actual not in (None, 0)) else None
    metrics = {
        "test": ForecastEvaluator.to_dict(test_eval),
        "split_index": split,
        "min_variance": min_var,
        "model_stats": {"adj_r2": adj_r2, "corr": corr, "norm_rmse": norm_rmse},
        "cap_info": cap,
    }
    metrics_path = outputs_dir / "har" / f"har_metrics_{asset}.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    forecast_path = outputs_dir / "har" / f"har_forecasts_{asset}.csv"
    forecast_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned_forecast.rename(asset).to_csv(forecast_path, header=["forecast"])
    return metrics, cleaned_forecast.rename(asset)


def evaluate_garch(asset: str, df: pd.DataFrame, cfg: Dict, outputs_dir: Path) -> Tuple[Dict, pd.Series]:
    returns = df[asset].dropna()
    if returns.empty:
        raise ValueError(f"No returns for asset {asset}")
    split = resolve_split_index(returns.index, cfg["split"])
    train = returns.iloc[:split]
    if train.empty:
        raise ValueError(f"Insufficient training set for GARCH on {asset}")
    min_var = _min_variance(cfg)
    model = GARCHModel(train)
    model.fit()
    garch_cfg = cfg.get("garch", {})
    max_fc = garch_cfg.get("max_forecasts")
    idx = returns.index[split: len(returns) if max_fc is None else min(len(returns), split + int(max_fc))]
    if idx.empty:
        forecast_series = pd.Series(dtype=float, index=idx, name=asset)
    else:
        fc = model.forecast_series(index=idx)
        if isinstance(fc, pd.DataFrame):
            fc = fc.iloc[:, 0]
        forecast_series = fc.rename(asset)
    rv_actual = realised_volatility(returns, method=cfg.get("rv_method", "bipower")).reindex(forecast_series.index)
    actual_aligned, cleaned, cap = _process_forecasts(rv_actual, forecast_series, min_var, cfg.get("variance_capping", {}))
    evaluator = ForecastEvaluator()
    test_eval = evaluator.evaluate(actual_aligned, cleaned)
    aligned = pd.concat([actual_aligned, cleaned], axis=1, join="inner").dropna()
    adj = _compute_adj_r2_from_aligned(aligned) or _compute_adj_r2_from_conditional(model, returns, cfg)
    metrics = {
        "test": ForecastEvaluator.to_dict(test_eval),
        "split_index": split,
        "min_variance": min_var,
        "model_stats": {"adj_r2": adj},
        "cap_info": cap,
    }
    metrics_path = outputs_dir / "garch" / f"garch_metrics_{asset}.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    forecast_path = outputs_dir / "garch" / f"garch_forecasts_{asset}.csv"
    forecast_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned.rename(asset).to_csv(forecast_path, header=["forecast"])
    return metrics, cleaned.rename(asset)


def evaluate_figarch(asset: str, df: pd.DataFrame, cfg: Dict, outputs_dir: Path) -> Tuple[Dict, pd.Series]:
    returns = df[asset].dropna()
    if returns.empty:
        raise ValueError(f"No returns for asset {asset}")
    split = resolve_split_index(returns.index, cfg["split"])
    train = returns.iloc[:split]
    if train.empty:
        raise ValueError(f"Insufficient training set for FIGARCH on {asset}")
    fig_cfg = cfg.get("figarch", {})
    order = (fig_cfg.get("p", 1), fig_cfg.get("d", 0.4), fig_cfg.get("q", 1))
    batch = int(fig_cfg.get("refit_interval", 21) or 21)
    max_fc = fig_cfg.get("max_forecasts")
    scale = _figarch_scale(cfg)
    actual, forecast = batched_figarch_forecasts(returns, order, split, batch, max_fc, scale=scale)
    min_var = _min_variance(cfg)
    actual_aligned, cleaned, cap = _process_forecasts(actual, forecast.rename(asset), min_var, fig_cfg)
    evaluator = ForecastEvaluator()
    test_eval = evaluator.evaluate(actual_aligned, cleaned)
    aligned = pd.concat([actual_aligned, cleaned], axis=1, join="inner").dropna()
    adj = _compute_adj_r2_from_aligned(aligned)
    metrics = {
        "test": ForecastEvaluator.to_dict(test_eval),
        "split_index": split,
        "min_variance": min_var,
        "model_stats": {"adj_r2": adj},
        "cap_info": cap,
        "input_scale": scale,
    }
    metrics_path = outputs_dir / "figarch" / f"figarch_metrics_{asset}.json"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as fh:
        json.dump(metrics, fh, indent=2)
    forecast_path = outputs_dir / "figarch" / f"figarch_forecasts_{asset}.csv"
    forecast_path.parent.mkdir(parents=True, exist_ok=True)
    cleaned.rename(asset).to_csv(forecast_path, header=["forecast"])
    return metrics, cleaned.rename(asset)


def save_diagnostics(asset: str, result: DiagnosticResult, outputs_dir: Path) -> None:
    diag_dir = outputs_dir / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "hurst": result.hurst,
        "ljung_box_pvalue": result.ljung_box_pvalue,
        "long_memory_flag": result.long_memory_flag,
        "variance_breaks": result.variance_breaks,
    }
    with (diag_dir / f"diagnostics_{asset}.json").open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _run_volatility_pipeline(
    df: pd.DataFrame,
    assets: List[str],
    cfg: Dict,
    outputs_dir: Path,
    base_dir: Path,
    run_label: Optional[str] = None,
) -> None:
    if not assets:
        warnings.warn("No assets provided to the volatility pipeline", RuntimeWarning)
        return
    outputs_dir.mkdir(parents=True, exist_ok=True)
    har_forecasts: Dict[str, pd.Series] = {}
    garch_forecasts: Dict[str, pd.Series] = {}
    figarch_forecasts: Dict[str, pd.Series] = {}
    vol_selection: Dict[str, pd.Series] = {}
    for asset in assets:
        print(f"[RUN] Diagnostics for {asset}")
        rv = realised_volatility(df[asset], method=cfg.get("rv_method", "bipower"))
        diag = run_diagnostics(rv, cfg.get("diagnostics", {}))
        save_diagnostics(asset, diag, outputs_dir)
        try:
            print(f"[RUN] HAR {asset}")
            _, har_fc = evaluate_har(asset, df, cfg, outputs_dir)
            har_forecasts[asset] = har_fc
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] HAR failed for {asset}: {exc}")
            har_fc = pd.Series(index=df.index, dtype=float)
        try:
            print(f"[RUN] GARCH {asset}")
            _, garch_fc = evaluate_garch(asset, df, cfg, outputs_dir)
            garch_forecasts[asset] = garch_fc
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] GARCH failed for {asset}: {exc}")
            garch_fc = pd.Series(index=df.index, dtype=float)
        try:
            print(f"[RUN] FIGARCH {asset}")
            _, figarch_fc = evaluate_figarch(asset, df, cfg, outputs_dir)
            figarch_forecasts[asset] = figarch_fc
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] FIGARCH failed for {asset}: {exc}")
            figarch_fc = pd.Series(index=df.index, dtype=float)

        rv_target = realised_volatility(df[asset], method=cfg.get("rv_method", "bipower"))
        idx = rv_target.index
        returns = df[asset].reindex(idx)
        model_cfg = cfg
        model_results = fit_all_models(returns, rv_target, model_cfg)
        best_key, best_var = score_models(model_results)
        print(f"[SELECT] {asset}: best volatility model = {best_key}")
        vol_selection[asset] = best_var.reindex(idx)
    corr_cfg = cfg.get("correlations", {})
    window = int(corr_cfg.get("window", cfg.get("correlation_window", 60)))

    def _corr_output(kind: str, default_dir: str) -> Tuple[Path, str]:
        kind_cfg = corr_cfg.get(kind, {})
        out_dir = (base_dir / kind_cfg.get("output_dir", default_dir)).resolve()
        if run_label:
            out_dir = out_dir / run_label
        filename = kind_cfg.get("filename", f"dynamic_correlations_{kind}.csv")
        if run_label:
            filename = f"{run_label}_{filename}"
        return out_dir, filename

    har_out_dir, har_filename = _corr_output("har", "../Output/har_correlations")
    figarch_out_dir, figarch_filename = _corr_output("figarch", "../Output/figarch_correlations")
    garch_out_dir, garch_filename = _corr_output("garch", "../Output/garch_correlations")

    export_dynamic_correlations(
        har_forecasts,
        window,
        har_out_dir,
        har_filename,
        "HAR",
    )
    export_dynamic_correlations(
        figarch_forecasts,
        window,
        figarch_out_dir,
        figarch_filename,
        "FIGARCH",
    )
    export_dynamic_correlations(
        garch_forecasts,
        window,
        garch_out_dir,
        garch_filename,
        "GARCH",
    )

    if vol_selection:
        sel_dir = outputs_dir / "volatility_selection"
        sel_dir.mkdir(parents=True, exist_ok=True)
        sel_df = pd.DataFrame(vol_selection)
        sel_df = sel_df.sort_index(axis=1)
        sel_df = sel_df.copy()
        filename = "vol_selection.csv" if not run_label else f"vol_selection_{run_label}.csv"
        sel_df.to_csv(sel_dir / filename)


def load_config(path: Path) -> Dict:
    with path.open() as fh:
        return json.load(fh)


def run_models(cfg: Dict) -> None:
    base_dir = Path(__file__).parent
    outputs_root = (base_dir / cfg.get("outputs_dir", "outputs")).resolve()
    factor_dir_cfg = cfg.get("factor_preparation_dir")
    if factor_dir_cfg:
        factor_dir = (base_dir / factor_dir_cfg).resolve()
        isos = _collect_factor_isos(factor_dir, cfg)
        for iso in isos:
            factor_csv = factor_dir / f"{iso}_factors.csv"
            manifest = factor_dir / f"{iso}_manifest.json"
            if not factor_csv.exists():
                warnings.warn(f"Missing factor CSV for {iso}: {factor_csv}", RuntimeWarning)
                continue
            try:
                columns = _load_factor_manifest_columns(manifest)
            except Exception as exc:
                warnings.warn(f"Skipping {iso} due to manifest error: {exc}", RuntimeWarning)
                continue

            columns = _filter_factor_columns_for_volatility(columns)

            data_cfg = DataConfig(
                input_csv=factor_csv,
                date_column=cfg.get("date_column", "date"),
                asset_columns=columns,
                outputs_dir=outputs_root / iso,
                enforce_return_like=bool(cfg.get("enforce_return_like_inputs", True)),
                stationarity_alpha=float(cfg.get("stationarity_alpha", 0.05)),
                nonstationary_transform=str(cfg.get("nonstationary_transform", "diff")),
            )
            data_cfg.outputs_dir.mkdir(parents=True, exist_ok=True)
            try:
                df = load_returns(data_cfg)
            except Exception as exc:
                warnings.warn(f"Unable to load data for {iso}: {exc}", RuntimeWarning)
                continue
            available = [col for col in columns if col in df.columns]
            if not available:
                warnings.warn(f"Skipping {iso}: no manifest columns available after load", RuntimeWarning)
                continue
            _run_volatility_pipeline(df, available, cfg, data_cfg.outputs_dir, base_dir, run_label=iso)
        return
    data_cfg = DataConfig(
        input_csv=(base_dir / cfg["input_csv"]).resolve(),
        date_column=cfg.get("date_column", "date"),
        asset_columns=cfg.get("asset_columns"),
        outputs_dir=outputs_root,
    )
    data_cfg.outputs_dir.mkdir(parents=True, exist_ok=True)
    df = load_returns(data_cfg)
    assets = cfg.get("asset_columns") or list(df.columns)
    _run_volatility_pipeline(df, assets, cfg, data_cfg.outputs_dir, base_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HAR/GARCH/FIGARCH pipeline")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Path to the pipeline JSON config",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config(Path(args.config))
    run_models(cfg)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
