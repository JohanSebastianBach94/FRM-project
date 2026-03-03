"""Data preparation utilities for the mean-reversion volatility suite."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller


@dataclass
class DataConfig:
    input_csv: Path
    date_column: str
    asset_columns: Iterable[str]
    outputs_dir: Path

    # Input semantics controls
    # - If True: attempt to ensure inputs are return-like/stationary.
    # - Non-stationary series (ADF p-value > alpha) are transformed.
    enforce_return_like: bool = False
    stationarity_alpha: float = 0.05
    nonstationary_transform: str = "diff"  # "diff" | "pct_change"


def _adf_pvalue(series: pd.Series) -> float | None:
    series = series.dropna()
    if len(series) < 25:
        return None
    try:
        result = adfuller(series.to_numpy(), autolag="AIC")
    except Exception:
        return None
    try:
        return float(result[1])
    except Exception:
        return None


def _transform_nonstationary(series: pd.Series, mode: str) -> pd.Series:
    mode = (mode or "diff").lower()
    if mode == "pct_change":
        out = series.pct_change() * 100.0
    elif mode == "diff":
        out = series.diff()
    else:
        raise ValueError(f"Unknown nonstationary_transform: {mode}")
    return out.replace([np.inf, -np.inf], np.nan)


def load_returns(config: DataConfig) -> pd.DataFrame:
    """Load the input CSV, parse the date column, and enforce asset availability."""
    asset_columns = list(config.asset_columns)

    df = pd.read_csv(config.input_csv)
    if config.date_column not in df.columns:
        # Many pipeline CSVs are written with the date as the (unnamed) index.
        # Pandas reads that column back as 'Unnamed: 0' by default.
        fallback = None
        if "Unnamed: 0" in df.columns:
            fallback = "Unnamed: 0"
        elif len(df.columns) > 0 and df.columns[0] not in asset_columns:
            fallback = df.columns[0]

        if fallback is None:
            raise KeyError(f"Missing date column '{config.date_column}' in {config.input_csv}")

        df = df.rename(columns={fallback: config.date_column})

    df[config.date_column] = pd.to_datetime(df[config.date_column], errors="coerce")
    df = df.dropna(subset=[config.date_column])
    df.sort_values(config.date_column, inplace=True)
    df.set_index(config.date_column, inplace=True)

    # Resolve common alias patterns between the factor pipeline and manifests.
    resolved: dict[str, str] = {}
    used_actual: set[str] = set()
    for desired in asset_columns:
        if desired in df.columns:
            resolved[desired] = desired
            used_actual.add(desired)
            continue

        actual = None
        if desired.endswith("_lag0"):
            base = desired[: -len("_lag0")]
            if base in df.columns:
                actual = base
        elif desired.endswith("_FCI") and desired.count("_") == 1:
            iso = desired.split("_")[0]
            swapped = f"FCI_{iso}"
            if swapped in df.columns:
                actual = swapped

        if actual is not None and actual not in used_actual:
            resolved[desired] = actual
            used_actual.add(actual)

    missing = [col for col in asset_columns if col not in resolved]
    available = [col for col in asset_columns if col in resolved]
    if not available:
        raise KeyError(f"Input file missing required asset columns: {missing}")
    if missing:
        print(
            f"[load_returns] Warning: {config.input_csv.name} missing {len(missing)} manifest columns; continuing with {len(available)} available"
        )

    rename_map = {
        actual: desired
        for desired, actual in resolved.items()
        if desired != actual and actual in df.columns and desired not in df.columns
    }
    if rename_map:
        df = df.rename(columns=rename_map)

    df = df.dropna(subset=available, how="all")

    # Optional: enforce return-like/stationary inputs.
    if config.enforce_return_like:
        alpha = float(config.stationarity_alpha or 0.05)
        transform_mode = config.nonstationary_transform
        transformed = 0
        skipped = 0
        for col in available:
            series = df[col]
            pval = _adf_pvalue(series)
            if pval is None:
                skipped += 1
                continue
            if pval > alpha:
                df[col] = _transform_nonstationary(series, transform_mode)
                transformed += 1
        if transformed:
            print(
                f"[load_returns] Enforced return-like inputs: transformed {transformed} non-stationary series (ADF p>{alpha:.3f}); skipped {skipped}"
            )

    return df


def realised_volatility(returns: pd.Series, method: str = "squared") -> pd.Series:
    """Compute a proxy for realised volatility from daily returns.

    Supported methods:
    - 'squared' : squared returns (default)
    - 'bipower' : simple bipower-style proxy using adjacent absolute returns

    Note: true bipower variation normally requires intraday returns. For daily
    data we use the common daily proxy bpv_t = (pi/2) * |r_t| * |r_{t-1}| which
    preserves index alignment and is useful as a robustness experiment.
    """
    method = (method or "squared").lower()
    if method == "squared":
        return returns.pow(2)
    if method == "bipower":
        abs_r = returns.abs()
        bpv = (np.pi / 2.0) * abs_r * abs_r.shift(1)
        return bpv
    raise ValueError(f"Unknown rv method: {method}")


def build_har_features(rv: pd.Series, lags: Dict[str, int]) -> pd.DataFrame:
    """Construct daily/weekly/monthly realised volatility averages for HAR models."""
    features = pd.DataFrame(index=rv.index)
    features["rv_daily"] = rv.shift(1)
    features["rv_weekly"] = rv.rolling(lags["weekly"], min_periods=lags["weekly"]).mean().shift(1)
    features["rv_monthly"] = rv.rolling(lags["monthly"], min_periods=lags["monthly"]).mean().shift(1)
    features["rv_target"] = rv
    return features.dropna()


def save_feature_set(features: pd.DataFrame, asset: str, outputs_dir: Path) -> Path:
    outputs_dir.mkdir(parents=True, exist_ok=True)
    out_path = outputs_dir / f"har_features_{asset}.csv"
    features.to_csv(out_path, index=True)
    return out_path


def prepare_har_inputs(
    df: pd.DataFrame,
    asset: str,
    lags: Dict[str, int],
    outputs_dir: Path,
    rv_method: str = "squared",
) -> Path:
    rv = realised_volatility(df[asset], method=rv_method)
    features = build_har_features(rv, lags)
    return save_feature_set(features, asset, outputs_dir)


__all__ = [
    "DataConfig",
    "load_returns",
    "realised_volatility",
    "build_har_features",
    "save_feature_set",
    "prepare_har_inputs",
]
