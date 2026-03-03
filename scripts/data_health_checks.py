#!/usr/bin/env python3
"""Compute data health diagnostics for the cleaned monthly panel."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DEFAULT_COVERAGE_THRESHOLD = 0.62
THRESHOLD_CONFIG_PATH = PROJECT_ROOT / "analysis_outputs" / "coverage_threshold_config.json"


def _load_default_coverage_threshold(fallback: float = DEFAULT_COVERAGE_THRESHOLD) -> float:
    if not THRESHOLD_CONFIG_PATH.exists():
        return fallback
    try:
        payload = json.loads(THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return fallback
    if not isinstance(payload, dict):
        return fallback
    value = payload.get("series_threshold", payload.get("threshold", fallback))
    try:
        return float(value)
    except Exception:
        return fallback
# Keep economically essential drivers even if coverage is low.
REQUIRED_SERIES = {"SOFR_3m", "V2X"}
OUTPUT_DIR = Path("analysis_outputs") / "diagnostics"
LOG_DIR = Path("logs")
DATA_PATH = Path("data") / "cleaned_monthly_panel.parquet"
RESAMPLING_LOG_PATH = OUTPUT_DIR / "resampling_log.csv"
TRANSFORM_SUGGESTIONS_PATH = OUTPUT_DIR / "global_transform_suggestions.csv"

OUTPUT_DIR = PROJECT_ROOT / OUTPUT_DIR
LOG_DIR = PROJECT_ROOT / LOG_DIR
DATA_PATH = PROJECT_ROOT / DATA_PATH
RESAMPLING_LOG_PATH = PROJECT_ROOT / "analysis_outputs" / "diagnostics" / "resampling_log.csv"
TRANSFORM_SUGGESTIONS_PATH = PROJECT_ROOT / "analysis_outputs" / "diagnostics" / "global_transform_suggestions.csv"


def ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def load_panel() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Cleaned panel missing at {DATA_PATH}")
    df = pd.read_parquet(DATA_PATH)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df


def compute_coverage(df: pd.DataFrame) -> pd.Series:
    return df.count() / len(df)


def run_stationarity(series: pd.Series) -> Dict[str, Optional[float]]:
    # Import here to avoid hard dependency when running in fast mode.
    from statsmodels.tsa.stattools import adfuller, kpss

    values = series.dropna()
    if len(values) < 20:
        return {"adf_pvalue": None, "kpss_pvalue": None, "suggested_transform": "insufficient_data"}
    try:
        adf_result = adfuller(values, autolag="AIC")
        adf_p = float(adf_result[1])
    except Exception:
        adf_p = None
    try:
        kpss_result = kpss(values, regression="c", nlags="auto")
        kpss_p = float(kpss_result[1])
    except Exception:
        kpss_p = None
    transform = suggest_transform(values, adf_p, kpss_p)
    return {"adf_pvalue": adf_p, "kpss_pvalue": kpss_p, "suggested_transform": transform}


def suggest_transform(values: pd.Series, adf_p: Optional[float], kpss_p: Optional[float]) -> str:
    if adf_p is None and kpss_p is None:
        return "none"
    if adf_p is not None and adf_p <= 0.05 and (kpss_p is None or kpss_p > 0.05):
        return "none"
    if (values > 0).all():
        return "log_diff"
    return "pct_change(1)"


def seasonal_ratio(series: pd.Series) -> Optional[float]:
    # Import here to avoid hard dependency when running in fast mode.
    from statsmodels.tsa.seasonal import seasonal_decompose

    values = series.dropna()
    if len(values) < 24:
        return None
    try:
        result = seasonal_decompose(values, model="additive", period=12, extrapolate_trend="freq")
        seasonal_var = float(np.nanvar(result.seasonal))
        total_var = float(np.nanvar(result.observed))
    except Exception:
        return None
    return seasonal_var / total_var if total_var > 0 else None


def compute_outliers(series: pd.Series) -> Dict[str, Optional[float]]:
    values = series.dropna()
    if values.empty:
        return {"extreme_frac": None, "flagged": False}
    median = float(np.nanmedian(values))
    mad = float(np.nanmedian(np.abs(values - median)))
    threshold = 8 * mad
    lower = median - threshold
    upper = median + threshold
    extreme = ((values < lower) | (values > upper)).sum()
    frac = extreme / len(values)
    return {"extreme_frac": float(frac), "flagged": frac > 0.001}


def interpolate_short_gaps(series: pd.Series) -> pd.Series:
    """Impute short gaps without lookahead.

    We intentionally avoid linear interpolation and backfill here because they
    can introduce lookahead bias. Default behavior is step-hold / LOCF.
    """

    return series.ffill(limit=2)


def build_summary(df: pd.DataFrame) -> Dict[str, object]:
    summary = {
        "start_date": df.index.min().strftime("%Y-%m-%d"),
        "end_date": df.index.max().strftime("%Y-%m-%d"),
        "observations": len(df),
        "frequency": pd.infer_freq(df.index) or "custom",
    }
    return summary


def apply_transform(series: pd.Series, transform: str) -> pd.Series:
    if transform == "log_diff":
        positive = series.where(series > 0)
        logged = np.log(positive)
        return logged.diff()
    if transform == "pct_change(1)":
        return series.pct_change()
    return series


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute data health diagnostics for the cleaned monthly panel")
    parser.add_argument("--coverage-threshold", type=float, default=_load_default_coverage_threshold())
    parser.add_argument(
        "--skip-heavy-diagnostics",
        action="store_true",
        help="Skip stationarity/seasonality/outlier diagnostics and only rebuild transformed_panel.parquet.",
    )
    parser.add_argument(
        "--reuse-transform-suggestions",
        action="store_true",
        help="When skipping heavy diagnostics, reuse existing global_transform_suggestions.csv if present.",
    )
    return parser.parse_args()


def main(args: argparse.Namespace | None = None) -> None:
    ensure_dirs()
    args = args or parse_args()
    df = load_panel()
    numeric = df.select_dtypes(include=[np.number]).copy()
    coverage = compute_coverage(numeric)
    coverage.to_csv(OUTPUT_DIR / "global_coverage.csv")
    dropped = [
        series
        for series in coverage[coverage < args.coverage_threshold].index
        if series not in REQUIRED_SERIES
    ]
    pd.Series(dropped).to_csv(OUTPUT_DIR / "global_dropped_columns.csv", index=False, header=["column"])
    cleaned = numeric.drop(columns=dropped)

    if cleaned.empty:
        print("Cleaned panel is empty; skipping data health diagnostics.")
        return

    if args.skip_heavy_diagnostics:
        transforms_dict: Dict[str, str]
        if args.reuse_transform_suggestions and TRANSFORM_SUGGESTIONS_PATH.exists():
            transform_df = pd.read_csv(TRANSFORM_SUGGESTIONS_PATH)
            transforms_dict = transform_df.set_index("target")["suggested_transform"].to_dict()
        else:
            transforms_dict = {col: "none" for col in cleaned.columns}

        imputed_df = cleaned.copy()
        for column in cleaned.columns:
            series = cleaned[column]
            imputed_df[column] = interpolate_short_gaps(series)

        transformed = pd.DataFrame(index=imputed_df.index)
        for column in cleaned.columns:
            transform = transforms_dict.get(column, "none")
            transformed[column] = apply_transform(imputed_df[column], transform)

        transformed_path = OUTPUT_DIR / "transformed_panel.parquet"
        transformed.to_parquet(transformed_path)
        print(f"Fast rebuild complete; transformed panel stored at {transformed_path}")
        return
    basic_stats = cleaned.agg(["mean", "std", "min", "max"], axis=0).transpose()
    basic_stats["skew"] = cleaned.skew()
    basic_stats["kurtosis"] = cleaned.kurtosis()
    basic_stats["zeros"] = (cleaned == 0).sum()
    basic_stats["infs"] = cleaned.isin([np.inf, -np.inf]).sum()
    basic_stats.to_csv(OUTPUT_DIR / "global_basic_stats.csv")
    outlier_rows = []
    stationarity_rows = []
    transform_recs = []
    seasonality_rows = []
    imputation_rows = []
    imputed_df = cleaned.copy()
    for column in cleaned.columns:
        series = cleaned[column]
        out = compute_outliers(series)
        seasonality = seasonal_ratio(series)
        stationarity = run_stationarity(series)
        outlier_rows.append({"target": column, **out})
        stationarity_rows.append({"target": column, **stationarity})
        transform_recs.append({"target": column, "suggested_transform": stationarity["suggested_transform"]})
        seasonality_rows.append({"target": column, "seasonal_ratio": seasonality, "flagged": (seasonality or 0) > 0.3})
        before_missing = series.isna().sum()
        imputed_series = interpolate_short_gaps(series)
        after_missing = imputed_series.isna().sum()
        if after_missing < before_missing:
            imputation_rows.append({"target": column, "imputed": before_missing - after_missing})
            imputed_df[column] = imputed_series
    pd.DataFrame(outlier_rows).to_csv(OUTPUT_DIR / "global_outliers.csv", index=False)
    pd.DataFrame(stationarity_rows).to_csv(OUTPUT_DIR / "global_stationarity.csv", index=False)
    pd.DataFrame(transform_recs).to_csv(OUTPUT_DIR / "global_transform_suggestions.csv", index=False)
    pd.DataFrame(seasonality_rows).to_csv(OUTPUT_DIR / "global_seasonality.csv", index=False)
    if imputation_rows:
        pd.DataFrame(imputation_rows).to_csv(OUTPUT_DIR / "global_imputation_summary.csv", index=False)
    transform_df = pd.DataFrame(transform_recs)
    transforms_dict = transform_df.set_index("target")["suggested_transform"].to_dict()
    transformed = pd.DataFrame(index=imputed_df.index)
    for column in cleaned.columns:
        transform = transforms_dict.get(column, "none")
        transformed[column] = apply_transform(imputed_df[column], transform)
    transformed_path = OUTPUT_DIR / "transformed_panel.parquet"
    transformed.to_parquet(transformed_path)
    seasonal_flags = sum(row["flagged"] for row in seasonality_rows if row["seasonal_ratio"] is not None)
    outlier_flags = sum(row["flagged"] for row in outlier_rows)
    summary = build_summary(imputed_df)
    summary.update({
        "total_series": int(numeric.shape[1]),
        "kept_series": int(cleaned.shape[1]),
        "dropped_series": dropped,
        "seasonal_flags": int(seasonal_flags),
        "outlier_flags": int(outlier_flags),
    })
    summary_extra = {
        "transformed_panel": str(transformed_path),
        "transform_usage": transforms_dict,
    }
    resampling_info = None
    if RESAMPLING_LOG_PATH.exists():
        resample_df = pd.read_csv(RESAMPLING_LOG_PATH)
        action_counts = resample_df["resampling_action"].value_counts().to_dict()
        resampling_info = {
            "path": str(RESAMPLING_LOG_PATH),
            "entries": int(len(resample_df)),
            "action_counts": {action: int(count) for action, count in action_counts.items()},
        }
    summary.update(summary_extra)
    summary["resampling_log"] = resampling_info
    json_path = OUTPUT_DIR / "global_data_health.json"
    md_path = OUTPUT_DIR / "global_data_health.md"
    with json_path.open("w", encoding="utf-8") as fp:
        json.dump(summary, fp, indent=2)
    with md_path.open("w", encoding="utf-8") as fp:
        fp.write("# Data Health Summary\n\n")
        fp.write(f"**Date range**: {summary['start_date']} → {summary['end_date']} ({summary['observations']} obs)\n\n")
        fp.write("## Coverage & statistics\n")
        fp.write(f"Kept {summary['kept_series']} of {summary['total_series']} series (dropped {len(summary['dropped_series'])}).\n")
        if summary['dropped_series']:
            fp.write("Dropped series:\n")
            for col in summary['dropped_series']:
                fp.write(f"- {col}\n")
        fp.write("\n---\n")
        fp.write("## Stationarity flags\n")
        stationarity_df = pd.DataFrame(stationarity_rows)
        adf_fail = stationarity_df['adf_pvalue'].dropna() > 0.05
        adf_fail = adf_fail.sum() if not adf_fail.empty else 0
        kpss_fail = stationarity_df['kpss_pvalue'].dropna() < 0.05
        kpss_fail = kpss_fail.sum() if not kpss_fail.empty else 0
        fp.write(f"ADF p>0.05: {adf_fail} series. KPSS p<0.05: {kpss_fail} series.\n")
        fp.write("## Seasonality checks\n")
        fp.write(f"Seasonal ratio >0.3 flagged for {summary['seasonal_flags']} series.\n")
        fp.write("## Outlier coverage\n")
        fp.write(f"Extreme values (>8×MAD) flagged in {summary['outlier_flags']} series.\n")
        fp.write("## Transform policy\n")
        fp.write(f"Applied recommended transforms; transformed panel stored at `{summary['transformed_panel']}`.\n")
        fp.write("Check `analysis_outputs/diagnostics/global_transform_suggestions.csv` for the per-series advice.\n")
        fp.write("## Outlier policy\n")
        fp.write("Outliers are retained to preserve crisis tail dynamics for stress/GARCH/ADCC modeling. No winsorization unless clear data errors arise.\n")
        fp.write("## Resampling log\n")
        resample_info = summary.get("resampling_log")
        if resample_info:
            fp.write(f"Log file: `{resample_info['path']}`\n")
            fp.write(f"Total series logged: {resample_info['entries']}\n")
            fp.write("Resampling actions:\n")
            for action, count in resample_info['action_counts'].items():
                fp.write(f"- {action}: {count}\n")
        else:
            fp.write("No resampling log was found.\n")
    print(f"Data health diagnostics written to {json_path}")


if __name__ == "__main__":
    main()