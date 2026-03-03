#!/usr/bin/env python3
"""Daily variant of factor preparation for stress indicators.

This script mirrors `scripts/factor_preparation.py` but operates on the
already-constructed daily stress-indicator panel
`stress_indicators_expanded.csv` instead of the monthly
`transformed_panel.parquet`.

It builds standardized daily blocks, daily lags, PCA components, and a
manifest per ISO, writing
`analysis_outputs/factor_preparation_daily/{ISO}_factors_daily.csv`.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import joblib
import numpy as np
import pandas as pd
import yaml
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import statsmodels.api as sm

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DAILY_PANEL_PRIMARY = PROJECT_ROOT / "stress_indicators_expanded.csv"
DAILY_PANEL_FALLBACK = PROJECT_ROOT / "data" / "stress_indicators_expanded.csv"

BLOCKS_PATH = PROJECT_ROOT / "config" / "country_blocks_extended.yaml"
FACTOR_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation_daily"
DIAG_DIR = PROJECT_ROOT / "analysis_outputs" / "diagnostics_daily"
SCALER_DIR = PROJECT_ROOT / "models" / "scalers_daily"
FCI_DIR = PROJECT_ROOT / "analysis_outputs"

THRESHOLD_CONFIG_PATH = PROJECT_ROOT / "analysis_outputs" / "coverage_threshold_config.json"


@dataclass(frozen=True)
class PCASettings:
    correlation_threshold: float = 0.55
    min_series: int = 3
    series_trigger: int = 5
    component_max: int = 6
    variance_target: float = 0.9
    block_component_cap: int = 2


PCA_DEFAULTS = PCASettings()


def ensure_dirs() -> None:
    FACTOR_DIR.mkdir(parents=True, exist_ok=True)
    SCALER_DIR.mkdir(parents=True, exist_ok=True)
    DIAG_DIR.mkdir(parents=True, exist_ok=True)


def load_yaml(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def compute_vif(df: pd.DataFrame) -> pd.Series:
    vif_values = []
    for column in df.columns:
        y = df[column]
        X = df.drop(columns=column)
        if X.shape[1] == 0:
            vif_values.append(np.nan)
            continue
        X_const = sm.add_constant(X, has_constant="add")
        model = sm.OLS(y, X_const, missing="drop").fit()
        r2 = model.rsquared
        vif_values.append(np.inf if r2 >= 0.999 else 1.0 / (1 - r2))
    return pd.Series(vif_values, index=df.columns)


def needs_pca(block_df: pd.DataFrame, settings: PCASettings) -> bool:
    if block_df.shape[1] < settings.min_series:
        return False
    corr = block_df.corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    mean_corr = upper.stack().mean() if not upper.stack().empty else 0.0
    return mean_corr > settings.correlation_threshold or block_df.shape[1] >= settings.series_trigger


def build_lags(series: pd.Series, n_lags: int = 5) -> pd.DataFrame:
    """Daily lags for HAR-style features (lag0..lag5)."""

    lags = {"lag0": series}
    for lag in range(1, n_lags + 1):
        lags[f"lag{lag}"] = series.shift(lag)
    return pd.DataFrame(lags)


def build_fci(components: pd.DataFrame, iso: str) -> Optional[pd.Series]:
    if components.shape[1] < 3:
        return None
    filler = components.ffill().fillna(0)
    pca = PCA(n_components=1)
    scores = pca.fit_transform(filler)
    return pd.Series(scores.ravel(), index=filler.index, name=f"{iso}_FCI")


def _load_daily_upsampling_policy() -> dict:
    defaults = {
        "default_method": "step_hold",
        "allow_backfill": False,
        "interpolate_opt_in_series": [],
        "interpolate_opt_in_blocks": [],
        "interpolate_opt_in_block_series": [],
    }

    if not THRESHOLD_CONFIG_PATH.exists():
        return defaults

    try:
        payload = json.loads(THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return defaults

    policy = payload.get("daily_upsampling_policy", {}) if isinstance(payload, dict) else {}
    if not isinstance(policy, dict):
        return defaults

    merged = dict(defaults)
    for key in defaults:
        if key in policy:
            merged[key] = policy[key]
    return merged


def _should_interpolate(iso: str, block_key: str, series: str, policy: dict) -> bool:
    opt_in_series = set(policy.get("interpolate_opt_in_series") or [])
    if series in opt_in_series:
        return True

    opt_in_blocks = policy.get("interpolate_opt_in_blocks") or []
    for item in opt_in_blocks:
        if not isinstance(item, dict):
            continue
        if item.get("iso") == iso and item.get("block") == block_key:
            return True

    opt_in_block_series = policy.get("interpolate_opt_in_block_series") or []
    for item in opt_in_block_series:
        if not isinstance(item, dict):
            continue
        if item.get("iso") == iso and item.get("block") == block_key and item.get("series") == series:
            return True

    return False


def _apply_fill_policy(df: pd.DataFrame, iso: str, block_key: str, policy: dict) -> pd.DataFrame:
    allow_backfill = bool(policy.get("allow_backfill", False))
    out = pd.DataFrame(index=df.index)
    for col in df.columns:
        s = pd.to_numeric(df[col], errors="coerce")
        s = s.ffill()
        if _should_interpolate(iso, block_key, col, policy):
            s = s.interpolate(method="time")
            s = s.ffill()
        if allow_backfill:
            s = s.bfill()
        out[col] = s
    return out


def main(settings: PCASettings | None = None) -> None:
    ensure_dirs()
    settings = settings or PCA_DEFAULTS
    print(f"Daily PCA settings: {settings}")

    policy = _load_daily_upsampling_policy()
    print(f"Daily upsampling policy: {policy}")

    panel_path = DAILY_PANEL_PRIMARY if DAILY_PANEL_PRIMARY.exists() else DAILY_PANEL_FALLBACK
    if not panel_path.exists():
        raise FileNotFoundError(
            "Daily stress-indicator panel missing: expected stress_indicators_expanded.csv "
            "(repo root or data/)."
        )

    panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    panel.index.name = "date"
    panel = panel.sort_index()

    # Calendar governance: treat the daily pipeline as business-day aligned.
    panel = panel.loc[panel.index.dayofweek < 5]

    country_blocks = load_yaml(BLOCKS_PATH).get("country_blocks", [])
    components_records: List[Dict[str, object]] = []
    loadings_records: List[Dict[str, object]] = []

    if not country_blocks:
        raise ValueError("No country blocks declared in config/country_blocks_extended.yaml")

    for entry in country_blocks:
        iso = entry.get("iso_code")
        if not iso:
            continue
        blocks = entry.get("blocks", [])
        series_list = sorted({series for block in blocks for series in block.get("series_codes", [])})
        iso_df = panel.loc[:, [s for s in series_list if s in panel.columns]].copy()
        if iso_df.empty:
            continue

        scaled_data = pd.DataFrame(index=iso_df.index)
        lags = pd.DataFrame(index=iso_df.index)
        block_diagnostics = []
        pca_frames = []
        manifest: List[Dict[str, object]] = []

        for block in blocks:
            key = block.get("key")
            if not key:
                continue
            series_codes = [s for s in block.get("series_codes", []) if s in iso_df.columns]
            if not series_codes:
                continue
            block_values = iso_df[series_codes].copy()
            block_values = _apply_fill_policy(block_values, iso=iso, block_key=key, policy=policy)

            # Critical: do NOT force missing values to 0.0. Zero-imputation injects
            # artificial levels into the daily panel and can materially distort
            # volatility/correlation estimates downstream.
            fit_values = block_values.dropna(how="any")
            if fit_values.empty:
                print(f"[WARN] No non-missing rows for {iso}/{key}; leaving block as NaN")
                scaled_block = pd.DataFrame(index=block_values.index, columns=series_codes, dtype=float)
            else:
                scaler = StandardScaler()
                scaler.fit(fit_values)
                means = pd.Series(scaler.mean_, index=series_codes)
                scales = pd.Series(scaler.scale_, index=series_codes).replace(0.0, 1.0)
                scaled_block = (block_values - means) / scales
                joblib.dump(scaler, SCALER_DIR / f"{iso}_{key}.pkl")
            block_diagnostics.append((key, scaled_block))
            for series in series_codes:
                scaled_data[series] = scaled_block[series]

        # Daily coverage/seasonality diagnostics would ideally be recomputed;
        # for now, we treat all series as eligible for lagging.
        eligible = list(scaled_data.columns)

        for series in eligible:
            if series in scaled_data.columns:
                lag_df = build_lags(scaled_data[series])
                lag_df = lag_df.add_prefix(f"{series}_")
                lags = pd.concat([lags, lag_df], axis=1)
                for col in lag_df.columns:
                    manifest.append({"column": col, "origin": "lag_daily"})

        collin_rows = []
        for key, scaled_block in block_diagnostics:
            corr = scaled_block.corr().abs()
            upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
            high_pairs = upper.stack().reset_index()
            high_pairs = high_pairs[high_pairs[0] > 0.95]
            for _, row in high_pairs.iterrows():
                collin_rows.append(
                    {
                        "iso": iso,
                        "block": key,
                        "series_1": row["level_0"],
                        "series_2": row["level_1"],
                        "correlation": float(row[0]),
                    }
                )
            vif = compute_vif(scaled_block)
            for series, value in vif.items():
                collin_rows.append(
                    {
                        "iso": iso,
                        "block": key,
                        "series_1": series,
                        "series_2": "VIF",
                        "correlation": float(value) if np.isfinite(value) else None,
                    }
                )

        if collin_rows:
            pd.DataFrame(collin_rows).to_csv(DIAG_DIR / f"{iso}_collinearity_daily.csv", index=False)

        for key, scaled_block in block_diagnostics:
            candidates = scaled_block.dropna(axis=1, how="all")
            if needs_pca(candidates, settings):
                max_comp = min(
                    candidates.shape[1],
                    max(1, len(candidates) // 10),
                    settings.component_max,
                )
                if max_comp == 0:
                    continue
                pca = PCA(n_components=max_comp)
                filled = candidates.fillna(0)
                comps = pca.fit_transform(filled)
                cum = np.cumsum(pca.explained_variance_ratio_)
                keep = int(np.searchsorted(cum, settings.variance_target, side="right") + 1)
                keep = max(1, min(keep, max_comp, settings.block_component_cap))
                names = [f"{iso}_{key}_pc{i+1}_daily" for i in range(keep)]
                comps_df = pd.DataFrame(comps[:, :keep], index=candidates.index, columns=names)
                pca_frames.append(comps_df)
                for idx, name in enumerate(names):
                    components_records.append(
                        {
                            "iso": iso,
                            "block": key,
                            "component": name,
                            "explained_variance_ratio": float(pca.explained_variance_ratio_[idx]),
                            "cumulative_variance": float(cum[idx]),
                        }
                    )
                loadings = pd.DataFrame(
                    pca.components_[:keep, :],
                    index=names,
                    columns=candidates.columns,
                ).stack()
                for (component, series), value in loadings.items():
                    loadings_records.append(
                        {
                            "iso": iso,
                            "block": key,
                            "component": component,
                            "series": series,
                            "loading": float(value),
                        }
                    )

        final_parts = [scaled_data, lags]
        if pca_frames:
            final_parts.extend(pca_frames)
            for pc_df in pca_frames:
                for col in pc_df.columns:
                    manifest.append({"column": col, "origin": "pca_daily"})

        fci = None
        financial_blocks = [scaled for key, scaled in block_diagnostics if "financial" in key.lower()]
        if financial_blocks:
            fci_candidates = pd.concat(financial_blocks, axis=1)
            fci = build_fci(fci_candidates, iso)
            if fci is not None:
                final_parts.append(fci.to_frame())
                manifest.append({"column": fci.name, "origin": "FCI_daily"})
                fci_path = FCI_DIR / f"FCI_{iso}_daily.csv"
                fci.to_csv(fci_path)

        final_df = pd.concat(final_parts, axis=1).dropna(how="all")

        if iso == "DEU":
            spread_cols = [
                "BTP_Bund_Spread",
                "Bonos_Bund_Spread",
                "OAT_Bund_Spread",
                "Treasury_Bund_Spread",
            ]
            present_spreads = [col for col in spread_cols if col in final_df.columns]
            if present_spreads:
                # Collapse the multi-country spreads into one equally weighted proxy
                combined_name = f"{iso}_combined_spread_daily"
                final_df[combined_name] = final_df[present_spreads].mean(axis=1)
                manifest.append({"column": combined_name, "origin": "combined_spread_daily"})
                final_df = final_df.drop(columns=present_spreads)

        final_path = FACTOR_DIR / f"{iso}_factors_daily.csv"
        final_df.to_csv(final_path)

        manifest_path = FACTOR_DIR / f"{iso}_manifest_daily.json"
        with manifest_path.open("w", encoding="utf-8") as fp:
            json.dump(manifest, fp, indent=2)

        print(f"Wrote daily factors for {iso} to {final_path}")

    components_path = FACTOR_DIR / "pca_components_summary_daily.csv"
    loadings_path = FACTOR_DIR / "pca_loadings_summary_daily.csv"
    pd.DataFrame(components_records).to_csv(components_path, index=False)
    pd.DataFrame(loadings_records).to_csv(loadings_path, index=False)


def parse_args() -> PCASettings:
    parser = argparse.ArgumentParser(description="Prepare DAILY factors with optional PCA heuristics.")
    parser.add_argument("--pca-correlation-threshold", type=float, default=PCA_DEFAULTS.correlation_threshold)
    parser.add_argument("--pca-min-series", type=int, default=PCA_DEFAULTS.min_series)
    parser.add_argument("--pca-series-trigger", type=int, default=PCA_DEFAULTS.series_trigger)
    parser.add_argument("--pca-component-max", type=int, default=PCA_DEFAULTS.component_max)
    parser.add_argument("--pca-variance-target", type=float, default=PCA_DEFAULTS.variance_target)
    parser.add_argument("--pca-block-component-cap", type=int, default=PCA_DEFAULTS.block_component_cap)
    args = parser.parse_args()
    return PCASettings(
        correlation_threshold=args.pca_correlation_threshold,
        min_series=args.pca_min_series,
        series_trigger=args.pca_series_trigger,
        component_max=args.pca_component_max,
        variance_target=args.pca_variance_target,
        block_component_cap=args.pca_block_component_cap,
    )


if __name__ == "__main__":  # pragma: no cover
    main(parse_args())
