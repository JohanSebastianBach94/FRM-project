#!/usr/bin/env python3
"""Generate correlation and PCA diagnostics for each block."""

from pathlib import Path
import json
from pprint import pprint
from typing import Dict, Iterable

import numpy as np
import pandas as pd
import yaml
from scipy.cluster.hierarchy import linkage
from scipy.spatial.distance import squareform
from sklearn.decomposition import PCA

DATA_PATH = Path("data") / "cleaned_monthly_panel.parquet"
TRANSFORMED_PATH = Path("analysis_outputs") / "diagnostics" / "transformed_panel.parquet"
BLOCKS_PATH = Path("config") / "country_blocks_extended.yaml"


def load_series(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    return df


def mean_corr_stats(df: pd.DataFrame) -> Dict[str, float] | None:
    if df.shape[1] < 2:
        return None
    corr = df.corr()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    stack = upper.stack()
    if stack.empty:
        return None
    return {
        "mean": float(stack.mean()),
        "min": float(stack.min()),
        "max": float(stack.max()),
        "count": int(stack.count()),
    }


def first_pc_variance(df: pd.DataFrame) -> float | None:
    df_clean = df.dropna()
    if df_clean.shape[0] < 2 or df_clean.shape[1] < 2:
        return None
    pca = PCA(n_components=1)
    pca.fit(df_clean)
    return float(pca.explained_variance_ratio_[0])


def rolling_corr_stats(
    df: pd.DataFrame, window: int = 36, min_periods: int = 24
) -> Dict[str, float] | None:
    if df.shape[1] < 2 or len(df) < min_periods:
        return None
    mean_corrs: list[float] = []
    for end in range(window, len(df) + 1):
        window_df = df.iloc[end - window : end].ffill().bfill()
        if window_df.shape[0] < min_periods:
            continue
        corr = window_df.corr()
        upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
        stack = upper.stack()
        if stack.empty:
            continue
        mean_corrs.append(float(stack.mean()))
    if not mean_corrs:
        return None
    return {
        "mean": float(np.mean(mean_corrs)),
        "std": float(np.std(mean_corrs)),
        "windows": len(mean_corrs),
    }


def clustering_distances(df: pd.DataFrame) -> Dict[str, object] | None:
    if df.shape[1] < 2:
        return None
    corr = df.corr()
    dist = (1 - corr).fillna(1.0)
    triu = dist.where(np.triu(np.ones(dist.shape), k=1).astype(bool))
    stack = triu.stack()
    if stack.empty:
        return None
    try:
        condensed = squareform(dist.values)
        link = linkage(condensed, method="average")
        return {
            "dist_stats": {
                "mean": float(stack.mean()),
                "min": float(stack.min()),
                "max": float(stack.max()),
            },
            "linkage_head": link[:3].tolist(),
        }
    except ValueError:
        return {"dist_stats": {"mean": float(stack.mean()), "min": float(stack.min()), "max": float(stack.max())}, "linkage_head": []}


def subset(df: pd.DataFrame, series: Iterable[str]) -> pd.DataFrame | None:
    keep = [s for s in series if s in df.columns]
    if not keep:
        return None
    return df.loc[:, keep]


def summarize_block(name: str, levels: pd.DataFrame, transformed: pd.DataFrame) -> None:
    level_corr = mean_corr_stats(levels)
    trans_corr = mean_corr_stats(transformed)
    level_pc = first_pc_variance(levels)
    trans_pc = first_pc_variance(transformed)
    rolling = rolling_corr_stats(levels)
    clustering = clustering_distances(levels)
    summary = {
        "levels_correlation": level_corr,
        "transformed_correlation": trans_corr,
        "first_pc_levels": level_pc,
        "first_pc_transformed": trans_pc,
        "rolling_36m_levels": rolling,
        "clustering_distances": clustering,
    }
    print(f"\nBlock {name} ({levels.shape[1]} series)")
    pprint(summary)


def main() -> None:
    levels = load_series(DATA_PATH)
    transformed = load_series(TRANSFORMED_PATH)
    blocks = yaml.safe_load(BLOCKS_PATH.read_text(encoding="utf-8")).get("country_blocks", [])
    print("Loaded", levels.shape, "levels series;", transformed.shape, "transformed")
    for entry in blocks:
        iso = entry.get("iso_code")
        if not iso:
            continue
        for block in entry.get("blocks", []):
            key = block.get("key")
            series = block.get("series_codes", [])
            if not series:
                continue
            lvl = subset(levels, series)
            trn = subset(transformed, series)
            if lvl is None or trn is None:
                continue
            if lvl.shape[1] < 2:
                print(f"{iso}/{key}: insufficient series ({lvl.shape[1]})")
                continue
            summarize_block(f"{iso}/{key}", lvl, trn)


if __name__ == "__main__":
    main()
