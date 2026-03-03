#!/usr/bin/env python3
"""Standardize transformed panel, build lags, PCA components, and manifest."""

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

TRANSFORMED_PANEL = Path("analysis_outputs") / "diagnostics" / "transformed_panel.parquet"
BLOCKS_PATH = Path("config") / "country_blocks_extended.yaml"
COVERAGE_PATH = Path("analysis_outputs") / "diagnostics" / "global_coverage.csv"
SEASONALITY_PATH = Path("analysis_outputs") / "diagnostics" / "global_seasonality.csv"
FACTOR_DIR = Path("analysis_outputs") / "factor_preparation"
DIAG_DIR = Path("analysis_outputs") / "diagnostics"
SCALER_DIR = Path("models") / "scalers"
FCI_DIR = Path("analysis_outputs")


@dataclass(frozen=True)
class PCASettings:
    correlation_threshold: float = 0.55
    min_series: int = 3
    series_trigger: int = 5
    component_max: int = 6
    variance_target: float = 0.9


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


def build_lags(series: pd.Series, n_lags: int = 3) -> pd.DataFrame:
    lags = {"lag0": series}
    for lag in range(1, n_lags + 1):
        lags[f"lag{lag}"] = series.shift(lag)
    return pd.DataFrame(lags)


def build_fci(components: pd.DataFrame, iso: str) -> Optional[pd.Series]:
    if components.shape[1] < 3:
        return None
    filler = components.ffill().bfill().fillna(0)
    pca = PCA(n_components=1)
    scores = pca.fit_transform(filler)
    return pd.Series(scores.ravel(), index=filler.index, name=f"{iso}_FCI")


def main(settings: PCASettings | None = None) -> None:
    ensure_dirs()
    settings = settings or PCA_DEFAULTS
    print(f"PCA settings: {settings}")
    if not TRANSFORMED_PANEL.exists():
        raise FileNotFoundError("Transformed panel missing; run data health checks first")
    panel = pd.read_parquet(TRANSFORMED_PANEL)
    country_blocks = load_yaml(BLOCKS_PATH).get("country_blocks", [])
    coverage = pd.read_csv(COVERAGE_PATH).set_index("series_code").iloc[:, 0].to_dict()
    seasonality = pd.read_csv(SEASONALITY_PATH).set_index("target")["flagged"].astype(bool).to_dict()
    components_records = []
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
        iso_df = iso_df.ffill().bfill()
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
            block_values = block_values.ffill().bfill().fillna(0)
            scaler = StandardScaler()
            scaled_block = pd.DataFrame(scaler.fit_transform(block_values), index=block_values.index, columns=series_codes)
            joblib.dump(scaler, SCALER_DIR / f"{iso}_{key}.pkl")
            block_diagnostics.append((key, scaled_block))
            for series in series_codes:
                scaled_data[series] = scaled_block[series]
        coverage_series = {series: coverage.get(series, 0.0) for series in scaled_data.columns}
        seasonal_flags = {series: seasonality.get(series, False) for series in scaled_data.columns}
        eligible = [series for series, cov in coverage_series.items() if cov >= 0.8 and not seasonal_flags.get(series, False)]
        for series in eligible:
            if series in scaled_data.columns:
                lag_df = build_lags(scaled_data[series])
                lag_df = lag_df.add_prefix(f"{series}_")
                lags = pd.concat([lags, lag_df], axis=1)
                for col in lag_df.columns:
                    manifest.append({"column": col, "origin": "lag"})
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
            pd.DataFrame(collin_rows).to_csv(DIAG_DIR / f"{iso}_collinearity.csv", index=False)
        for key, scaled_block in block_diagnostics:
            candidates = scaled_block.dropna(axis=1, how="all")
            if needs_pca(candidates, settings):
                max_comp = min(candidates.shape[1], max(1, len(candidates) // 10), settings.component_max)
                if max_comp == 0:
                    continue
                pca = PCA(n_components=max_comp)
                filled = candidates.fillna(0)
                comps = pca.fit_transform(filled)
                cum = np.cumsum(pca.explained_variance_ratio_)
                keep = int(np.searchsorted(cum, settings.variance_target, side="right") + 1)
                keep = max(1, min(keep, max_comp))
                names = [f"{iso}_{key}_pc{i+1}" for i in range(keep)]
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
                    manifest.append({"column": col, "origin": "pca"})
        fci = None
        financial_blocks = [scaled for key, scaled in block_diagnostics if "financial" in key.lower()]
        if financial_blocks:
            fci_candidates = pd.concat(financial_blocks, axis=1)
            fci = build_fci(fci_candidates, iso)
            if fci is not None:
                final_parts.append(fci.to_frame())
                manifest.append({"column": fci.name, "origin": "FCI"})
                fci_path = FCI_DIR / f"FCI_{iso}.csv"
                fci.to_csv(fci_path)
        final_df = pd.concat(final_parts, axis=1).dropna(how="all")
        final_path = FACTOR_DIR / f"{iso}_factors.csv"
        final_df.to_csv(final_path)
        manifest_path = FACTOR_DIR / f"{iso}_manifest.json"
        with manifest_path.open("w", encoding="utf-8") as fp:
            json.dump(manifest, fp, indent=2)
        print(f"Wrote factors for {iso} to {final_path}")
    components_path = FACTOR_DIR / "pca_components_summary.csv"
    loadings_path = FACTOR_DIR / "pca_loadings_summary.csv"
    pd.DataFrame(components_records).to_csv(components_path, index=False)
    pd.DataFrame(loadings_records).to_csv(loadings_path, index=False)


def parse_args() -> PCASettings:
    parser = argparse.ArgumentParser(description="Prepare factors with optional PCA heuristics.")
    parser.add_argument("--pca-correlation-threshold", type=float, default=PCA_DEFAULTS.correlation_threshold)
    parser.add_argument("--pca-min-series", type=int, default=PCA_DEFAULTS.min_series)
    parser.add_argument("--pca-series-trigger", type=int, default=PCA_DEFAULTS.series_trigger)
    parser.add_argument("--pca-component-max", type=int, default=PCA_DEFAULTS.component_max)
    parser.add_argument("--pca-variance-target", type=float, default=PCA_DEFAULTS.variance_target)
    args = parser.parse_args()
    return PCASettings(
        correlation_threshold=args.pca_correlation_threshold,
        min_series=args.pca_min_series,
        series_trigger=args.pca_series_trigger,
        component_max=args.pca_component_max,
        variance_target=args.pca_variance_target,
    )


if __name__ == "__main__":
    main(parse_args())