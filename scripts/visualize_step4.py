#!/usr/bin/env python3
"""Generate validation plots and tables for Step 4 diagnostics."""
from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Dict, Iterable, List

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import yaml

sns.set_theme(style="whitegrid")

BASE_DIR = Path(__file__).resolve().parent.parent
FEATURE_DIR = BASE_DIR / "analysis_outputs" / "feature_contributions"
FEATURE_CONTRIB_PATH = BASE_DIR / "analysis_outputs" / "feature_contributions_USA.csv"
PLOTS_DIR = FEATURE_DIR / "plots"
TARGET_PANEL_PATH = BASE_DIR / "data" / "cleaned_monthly_panel.parquet"
COUNTRY_BLOCKS_PATH = BASE_DIR / "config" / "country_blocks_extended.yaml"

PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def load_targets_map() -> Dict[str, List[str]]:
    with COUNTRY_BLOCKS_PATH.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp)
    result: Dict[str, List[str]] = {}
    for block in payload.get("country_blocks", []):
        iso = block.get("iso_code")
        if not iso:
            continue
        codes: List[str] = []
        for subset in block.get("blocks", []):
            codes.extend(subset.get("series_codes", []))
        result[iso] = sorted(set(codes))
    return result


def collect_residuals() -> pd.DataFrame:
    rows = []
    for path in sorted(FEATURE_DIR.glob("residuals_USA_*.json")):
        target = path.stem.split("_")[-1]
        payload = json.loads(path.read_text())
        for set_label in ("train", "test"):
            diag = payload.get(set_label, {})
            rows.append(
                {
                    "target": target,
                    "set": set_label,
                    "ljung_box": diag.get("ljung_box_pvalue", float("nan")),
                    "arch": diag.get("arch_lm_pvalue", float("nan")),
                    "jarque_bera": diag.get("jarque_bera_pvalue", float("nan")),
                }
            )
    return pd.DataFrame(rows)


def plot_residual_pvalues(df: pd.DataFrame) -> Path:
    melted = df.melt(id_vars=["target", "set"], value_vars=["ljung_box", "arch", "jarque_bera"],
                     var_name="test", value_name="p_value")
    fig, axes = plt.subplots(1, 3, figsize=(16, 4), sharey=True)
    for ax, test in zip(axes, ["ljung_box", "arch", "jarque_bera"]):
        subset = melted[melted["test"] == test]
        sns.barplot(data=subset, x="target", y="p_value", hue="set", ax=ax)
        ax.set_title(test.replace("_", " ").title())
        ax.set_yscale("log")
        ax.set_xlabel("")
        ax.set_ylabel("p-value" if ax is axes[0] else None)
        ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    out_path = PLOTS_DIR / "residual_pvalues.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


def collect_stability() -> pd.DataFrame:
    frames = []
    for path in sorted(FEATURE_DIR.glob("stability_USA_*.csv")):
        target = path.stem.replace("stability_USA_", "")
        df = pd.read_csv(path)
        if df.empty:
            continue
        df["target"] = target
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["frequency"] = combined["frequency"].astype(float)
    return combined.sort_values(["target", "frequency"], ascending=[True, False])


def plot_stability(df: pd.DataFrame) -> Path:
    if df.empty:
        raise ValueError("No stability data to plot")
    top = df.groupby("target").head(5)
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=top, x="frequency", y="feature", hue="target", dodge=False, ax=ax)
    ax.set_title("Top stability features per target")
    ax.set_xscale("linear")
    ax.set_xlabel("Selection frequency")
    fig.tight_layout()
    out_path = PLOTS_DIR / "stability_top_features.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


def summarize_clusters(contrib: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for path in sorted(FEATURE_DIR.glob("clusters_USA_*.json")):
        target = path.stem.replace("clusters_USA_", "")
        payload = json.loads(path.read_text())
        rows.append(
            {
                "target": target,
                "big_groups": payload.get("big_groups", []),
                "top_pairs": payload.get("top_pairs", []),
            }
        )
    return pd.DataFrame(rows)


def summarize_condition(contrib: pd.DataFrame) -> pd.DataFrame:
    if contrib.empty:
        return contrib
    contrib = contrib.copy()
    contrib["selected_count"] = contrib["selected_features"].apply(lambda value: len(ast.literal_eval(value)) if isinstance(value, str) else 0)
    contrib["pruned_count"] = contrib["pruned_count"].astype(int)
    condition_df = contrib[["target", "condition_number", "condition_flag", "min_eigenvalue", "eigen_warning", "pruned_features", "pruned_count", "selected_count"]]
    return condition_df


def plot_condition(condition_df: pd.DataFrame) -> Path:
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.barplot(data=condition_df, x="target", y="condition_number", palette="viridis", ax=ax)
    ax.set_yscale("log")
    ax.set_title("Condition number by target")
    ax.set_xlabel("")
    ax.set_ylabel("Condition number")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    out_path = PLOTS_DIR / "condition_numbers.png"
    fig.savefig(out_path)
    plt.close(fig)
    return out_path


def plot_walkforward_cv(contrib: pd.DataFrame, cv_df: pd.DataFrame) -> Path:
    mean_mse = cv_df.groupby(["target", "alpha"]).mse.mean().reset_index()
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.lineplot(data=mean_mse, x="alpha", y="mse", hue="target", ax=ax)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_title("Walk-forward CV MSE paths")
    ax.set_xlabel("alpha")
    ax.set_ylabel("Mean MSE")
    fig.tight_layout()
    out_path = PLOTS_DIR / "walkforward_mse.png"
    fig.savefig(out_path)
    plt.close(fig)
    summary = contrib[["target", "train_r2", "test_r2"]]
    summary.to_csv(PLOTS_DIR / "walkforward_r2_summary.csv", index=False)
    return out_path


def check_panel_coverage(targets_map: Dict[str, List[str]]) -> Dict[str, Iterable[str]]:
    if not TARGET_PANEL_PATH.exists():
        return {}
    panel = pd.read_parquet(TARGET_PANEL_PATH)
    missing: Dict[str, List[str]] = {}
    columns = set(panel.columns)
    for iso, targets in targets_map.items():
        missing_targets = [t for t in targets if t not in columns]
        if missing_targets:
            missing[iso] = missing_targets
    return missing


def main() -> None:
    residuals = collect_residuals()
    residual_plot = plot_residual_pvalues(residuals)

    stability = collect_stability()
    stability_plot = plot_stability(stability)

    if not FEATURE_CONTRIB_PATH.exists():
        raise FileNotFoundError(FEATURE_CONTRIB_PATH)
    contrib = pd.read_csv(FEATURE_CONTRIB_PATH)
    condition_summary = summarize_condition(contrib)
    condition_plot = plot_condition(condition_summary)
    condition_summary.to_csv(FEATURE_DIR / "condition_summary_USA.csv", index=False)

    clusters = summarize_clusters(contrib)
    clusters.to_csv(FEATURE_DIR / "cluster_summary_USA.csv", index=False)

    cv_df = pd.read_csv(FEATURE_DIR / "USA_lasso_cv.csv")
    walk_plot = plot_walkforward_cv(contrib, cv_df)

    targets_map = load_targets_map()
    missing = check_panel_coverage(targets_map)
    (FEATURE_DIR / "missing_targets.json").write_text(json.dumps(missing, indent=2))

    print("Residual plot:", residual_plot)
    print("Stability plot:", stability_plot)
    print("Condition plot:", condition_plot)
    print("Walk-forward plot:", walk_plot)
    print("Missing target summary:", FEATURE_DIR / "missing_targets.json")


if __name__ == "__main__":
    main()
