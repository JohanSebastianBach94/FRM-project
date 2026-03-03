"""Rank Step 4 drivers, enforce PCA/FCI includes, and prune collinearity.

Key governance rule:
- Step 5 does not invent a new feature universe; it ranks/prunes only features
    that appear in Step 4 outputs (i.e., the feature space Step 4 actually trained
    on and selected from).
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import yaml
from statsmodels.stats.outliers_influence import variance_inflation_factor


BASE_DIR = Path(__file__).resolve().parent.parent
FEATURE_OUTPUT_DIR = BASE_DIR / "analysis_outputs"
SHORTLIST_DIR = FEATURE_OUTPUT_DIR / "feature_shortlist"
MANIFEST_PATH = SHORTLIST_DIR / "manifest.json"
FACTOR_PREP_DIR = FEATURE_OUTPUT_DIR / "factor_preparation"
FCI_DIR = FEATURE_OUTPUT_DIR
SHORTLIST_DIR.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = BASE_DIR / "config" / "step5_shortlist.yaml"
PIPELINE_CONFIG_PATH = BASE_DIR / "SRESS TEST PIPELINE" / "step5_shortlist.yaml"

DEFAULT_CONFIG = {
    "monthly": {
        "selection": {"min_features": 8, "max_features": 12},
        "ranking_weights": {
            "contribution": 0.45,
            "stability": 0.30,
            "coverage": 0.15,
            "oos_contribution": 0.10,
        },
        "performance_weighting": {"test_r2_clip_low": 0.0, "test_r2_clip_high": 1.0},
        "pruning": {
            "correlation_cluster_threshold": 0.85,
            "vif_threshold": 8.0,
            "dropna_for_corr": True,
        },
    }
}


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Step 5 monthly shortlist + collinearity pruning")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to Step 5 config YAML (overrides defaults)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def resolve_config_path(config_override: Optional[str]) -> Optional[Path]:
    if config_override:
        return (BASE_DIR / config_override).resolve() if not Path(config_override).is_absolute() else Path(config_override)
    if PIPELINE_CONFIG_PATH.exists():
        return PIPELINE_CONFIG_PATH
    if CONFIG_PATH.exists():
        return CONFIG_PATH
    return None


def load_config(config_override: Optional[str] = None) -> Tuple[dict, Optional[Path]]:
    cfg = DEFAULT_CONFIG
    path = resolve_config_path(config_override)
    if path and path.exists():
        try:
            loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to parse Step 5 config at {path}: {exc}")
        if isinstance(loaded, dict):
            cfg = {**cfg, **loaded}
    return cfg, path


def list_isos() -> List[str]:
    isos: Set[str] = set()
    for path in FEATURE_OUTPUT_DIR.glob("feature_contributions_*.csv"):
        stem = path.stem
        iso = stem.replace("feature_contributions_", "")
        if iso:
            isos.add(iso)
    return sorted(isos)


def normalize_series(series: pd.Series) -> pd.Series:
    min_val = series.min()
    max_val = series.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series(0.0, index=series.index)
    return (series - min_val) / (max_val - min_val)


def parse_coefficients(value: str) -> Dict[str, float]:
    parsed: Dict[str, float] = {}
    if not isinstance(value, str) or not value:
        return parsed
    for chunk in value.split(";"):
        if ":" not in chunk:
            continue
        key, raw = chunk.split(":", 1)
        feature = key.strip()
        if not feature:
            continue
        try:
            parsed[feature] = float(raw)
        except ValueError:
            continue
    return parsed


def collect_top_contributors(contrib_df: pd.DataFrame) -> Set[str]:
    top_set: Set[str] = set()
    for entry in contrib_df.get("top_contributions", pd.Series(dtype=object)):
        mapping = parse_coefficients(str(entry))
        top_set.update(mapping.keys())
    return top_set


def aggregate_scores(contrib_df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    targets = set(contrib_df.get("target", []))
    perf_cfg = (cfg.get("monthly") or {}).get("performance_weighting") or {}
    r2_low = float(perf_cfg.get("test_r2_clip_low", 0.0))
    r2_high = float(perf_cfg.get("test_r2_clip_high", 1.0))
    if r2_high < r2_low:
        r2_low, r2_high = r2_high, r2_low

    feature_bins: Dict[str, Dict[str, Set[str] | List[float]]] = defaultdict(
        lambda: {"contrib": [], "oos": [], "freq": [], "targets": set()}
    )
    for _, row in contrib_df.iterrows():
        coeffs = parse_coefficients(str(row.get("coefficients", "")))
        target = row.get("target")
        test_r2 = row.get("test_r2")
        try:
            test_r2_val = float(test_r2)
        except Exception:
            test_r2_val = 0.0
        if np.isnan(test_r2_val):
            test_r2_val = 0.0
        test_r2_val = float(np.clip(test_r2_val, r2_low, r2_high))
        for feature, value in coeffs.items():
            feature_bins[feature]["contrib"].append(abs(value))
            feature_bins[feature]["oos"].append(abs(value) * test_r2_val)
            if target:
                feature_bins[feature]["targets"].add(target)
        stability_file = row.get("stability_file")
        if isinstance(stability_file, str):
            stability_path = Path(stability_file.replace("\\", "/"))
            if not stability_path.is_absolute():
                stability_path = BASE_DIR / stability_path
            if stability_path.exists():
                stability_df = pd.read_csv(stability_path)
                for _, stab_row in stability_df.iterrows():
                    feature = stab_row.get("feature")
                    freq = stab_row.get("frequency")
                    if isinstance(feature, str) and freq is not None:
                        try:
                            feature_bins[feature]["freq"].append(float(freq))
                        except ValueError:
                            continue
    total_targets = len(targets)
    records = []
    for feature, buckets in feature_bins.items():
        records.append(
            {
                "feature": feature,
                "mean_contribution": float(np.mean(buckets["contrib"])) if buckets["contrib"] else 0.0,
                "mean_oos_contribution": float(np.mean(buckets["oos"])) if buckets["oos"] else 0.0,
                "mean_frequency": float(np.mean(buckets["freq"])) if buckets["freq"] else 0.0,
                "coverage": len(buckets["targets"]) / total_targets if total_targets else 0.0,
            }
        )
    df = pd.DataFrame(records)
    if df.empty:
        return df
    for column in ["mean_contribution", "mean_oos_contribution", "mean_frequency", "coverage"]:
        df[f"{column}_norm"] = normalize_series(df[column])

    # If stability-selection frequencies are missing/empty (e.g. bootstraps skipped),
    # re-normalize the weights so ranking does not collapse to contribution only.
    has_stability = bool((df["mean_frequency"] > 0).any())
    weights = ((cfg.get("monthly") or {}).get("ranking_weights") or {}).copy()
    w_contrib = float(weights.get("contribution", 0.0))
    w_oos = float(weights.get("oos_contribution", 0.0))
    w_stab = float(weights.get("stability", 0.0)) if has_stability else 0.0
    w_cov = float(weights.get("coverage", 0.0))
    w_sum = w_contrib + w_oos + w_stab + w_cov
    if w_sum <= 0:
        w_contrib, w_oos, w_stab, w_cov, w_sum = 1.0, 0.0, 0.0, 0.0, 1.0
    w_contrib /= w_sum
    w_oos /= w_sum
    w_stab /= w_sum
    w_cov /= w_sum

    df["rank_score"] = (
        w_contrib * df["mean_contribution_norm"]
        + w_oos * df["mean_oos_contribution_norm"]
        + w_stab * df["mean_frequency_norm"]
        + w_cov * df["coverage_norm"]
    )
    df["source"] = df["feature"].apply(infer_source)
    return df.sort_values("rank_score", ascending=False)


def infer_source(feature: str) -> str:
    normalized = feature.lower()
    if "pc" in normalized and "fc" not in normalized:
        return "pca"
    if "fci" in normalized:
        return "fci"
    return "lasso"


def load_pca_explained(iso: str) -> Dict[str, float]:
    mapping: Dict[str, float] = {}
    for path in FACTOR_PREP_DIR.glob(f"{iso}_*pca*_explained.csv"):
        df = pd.read_csv(path)
        if "component" not in df.columns or "explained_variance_ratio" not in df.columns:
            continue
        for _, row in df.iterrows():
            component = row["component"]
            try:
                ratio = float(row.get("explained_variance_ratio", 0.0))
            except ValueError:
                continue
            mapping[component] = ratio
    return mapping


def determine_forced_features(df: pd.DataFrame, iso: str, contrib_df: Optional[pd.DataFrame] = None) -> Set[str]:
    forced: Set[str] = set()
    explained = load_pca_explained(iso)
    forced.update({comp for comp, ratio in explained.items() if ratio > 0.10 and comp in df["feature"].values})
    forced.update({feature for feature in df["feature"] if "fci" in feature.lower()})
    if contrib_df is not None and not contrib_df.empty:
        forced.update(collect_top_contributors(contrib_df))
    fci_path = FCI_DIR / f"FCI_{iso}.csv"
    if fci_path.exists():
        forced.update({feature for feature in df["feature"] if "fci" in feature.lower()})
    return forced


def select_shortlist(df: pd.DataFrame, forced: Set[str], cfg: dict) -> List[str]:
    selection_cfg = (cfg.get("monthly") or {}).get("selection") or {}
    min_selection = int(selection_cfg.get("min_features", 8))
    max_selection = int(selection_cfg.get("max_features", 12))
    features = df["feature"].tolist()
    shortlist: List[str] = []
    for feature in features:
        if len(shortlist) >= max_selection:
            break
        shortlist.append(feature)
    for feature in forced:
        if feature not in shortlist:
            shortlist.append(feature)
    if len(shortlist) < min_selection:
        extras = [f for f in features if f not in shortlist]
        shortlist.extend(extras[: min_selection - len(shortlist)])
    unique = []
    for feature in shortlist:
        if feature not in unique:
            unique.append(feature)
    limit = max(min_selection, min(max_selection, len(unique)))
    return unique[:limit]


def load_feature_panel(iso: str) -> pd.DataFrame:
    parts: List[pd.DataFrame] = []
    pca_path = FACTOR_PREP_DIR / f"{iso}_pca_components.csv"
    if pca_path.exists():
        parts.append(pd.read_csv(pca_path, index_col=0, parse_dates=True))
    factors_path = FACTOR_PREP_DIR / f"{iso}_factors.csv"
    if factors_path.exists():
        parts.append(pd.read_csv(factors_path, index_col=0, parse_dates=True))
    fci_path = FCI_DIR / f"FCI_{iso}.csv"
    if fci_path.exists():
        parts.append(pd.read_csv(fci_path, index_col=0, parse_dates=True))
    if not parts:
        return pd.DataFrame()
    panel = pd.concat(parts, axis=1)
    panel = panel.loc[:, ~panel.columns.duplicated()]
    return panel.apply(pd.to_numeric, errors="coerce")


def assemble_matrix(
    features: Sequence[str],
    pca_panel: pd.DataFrame,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    if not pca_panel.empty:
        available = [feature for feature in features if feature in pca_panel.columns]
        if available:
            frames.append(pca_panel[available])
    if not frames:
        return pd.DataFrame()
    matrix = pd.concat(frames, axis=1)
    matrix = matrix.loc[:, ~matrix.columns.duplicated()]
    return matrix


def compute_vif(data: pd.DataFrame) -> Dict[str, float]:
    data = data.dropna()
    if data.shape[1] < 2:
        return {}
    vif_scores: Dict[str, float] = {}
    for idx, column in enumerate(data.columns):
        try:
            vif_scores[column] = float(variance_inflation_factor(data.values, idx))
        except Exception:
            continue
    return vif_scores


def select_drop_candidate(f1: str, f2: str, forced: Set[str], score_map: Dict[str, float]) -> Optional[str]:
    if f1 in forced and f2 not in forced:
        return f2
    if f2 in forced and f1 not in forced:
        return f1
    score1 = score_map.get(f1, 0.0)
    score2 = score_map.get(f2, 0.0)
    if score1 == score2:
        return f2
    return f1 if score2 > score1 else f2


def build_corr_clusters(corr: pd.DataFrame, threshold: float) -> List[Set[str]]:
    cols = list(corr.columns)
    adj: Dict[str, Set[str]] = {c: set() for c in cols}
    for i, c1 in enumerate(cols):
        for c2 in cols[i + 1 :]:
            try:
                val = float(corr.at[c1, c2])
            except Exception:
                continue
            if val >= threshold:
                adj[c1].add(c2)
                adj[c2].add(c1)

    seen: Set[str] = set()
    clusters: List[Set[str]] = []
    for node in cols:
        if node in seen:
            continue
        stack = [node]
        comp: Set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            comp.add(cur)
            stack.extend([n for n in adj[cur] if n not in seen])
        clusters.append(comp)
    return clusters


def choose_cluster_rep(cluster: Set[str], forced: Set[str], score_map: Dict[str, float]) -> str:
    forced_in = [f for f in cluster if f in forced]
    candidates = forced_in if forced_in else list(cluster)
    candidates = sorted(candidates, key=lambda f: score_map.get(f, 0.0), reverse=True)
    return candidates[0]


def prune_collinearity_clustered(
    shortlist: List[str],
    panel: pd.DataFrame,
    forced: Set[str],
    score_map: Dict[str, float],
    cfg: dict,
) -> Dict[str, object]:
    pruning_cfg = (cfg.get("monthly") or {}).get("pruning") or {}
    corr_threshold = float(pruning_cfg.get("correlation_cluster_threshold", 0.85))
    vif_threshold = pruning_cfg.get("vif_threshold", 8.0)
    dropna_for_corr = bool(pruning_cfg.get("dropna_for_corr", True))

    selection_cfg = (cfg.get("monthly") or {}).get("selection") or {}
    min_selection = int(selection_cfg.get("min_features", 8))
    max_selection = int(selection_cfg.get("max_features", 12))

    matrix = assemble_matrix(shortlist, panel)
    missing = [feature for feature in shortlist if feature not in matrix.columns]
    available = [feature for feature in shortlist if feature in matrix.columns]
    if len(available) < 2:
        return {"kept": available, "dropped": {}, "clusters": [], "missing": sorted(set(missing))}

    data = matrix[available]
    if dropna_for_corr:
        data = data.dropna()
    if data.shape[0] < 5:
        return {"kept": available, "dropped": {}, "clusters": [], "missing": sorted(set(missing))}

    corr = data.corr().abs()
    clusters = build_corr_clusters(corr, corr_threshold)

    reps: List[str] = []
    rep_to_cluster: Dict[str, List[str]] = {}
    for cluster in clusters:
        rep = choose_cluster_rep(cluster, forced, score_map)
        reps.append(rep)
        rep_to_cluster[rep] = sorted(cluster)

    # Enforce max size while keeping forced representatives.
    forced_reps = [r for r in reps if r in forced]
    other_reps = [r for r in reps if r not in forced]
    other_reps = sorted(other_reps, key=lambda f: score_map.get(f, 0.0), reverse=True)
    kept = forced_reps + other_reps
    if len(kept) > max_selection:
        kept = kept[:max_selection]

    # If clustering collapses too far, backfill by global rank (documented as such).
    backfilled: List[str] = []
    if len(kept) < min_selection:
        candidates = [f for f in shortlist if f not in kept]
        candidates = sorted(candidates, key=lambda f: score_map.get(f, 0.0), reverse=True)
        for f in candidates:
            if len(kept) >= min_selection:
                break
            kept.append(f)
            backfilled.append(f)

    dropped: Dict[str, Dict[str, str]] = {}
    kept_set = set(kept)
    for rep, members in rep_to_cluster.items():
        for member in members:
            if member == rep:
                continue
            if member in kept_set:
                continue
            dropped[member] = {"reason": "cluster_representative", "relative": rep}

    if backfilled:
        for feature in backfilled:
            dropped.pop(feature, None)

    # Optional VIF pruning after clustering.
    if vif_threshold is not None:
        try:
            vif_threshold_val = float(vif_threshold)
        except Exception:
            vif_threshold_val = 0.0
        if vif_threshold_val > 0 and len(kept) >= 2:
            vif_data = matrix[[f for f in kept if f in matrix.columns]].dropna()
            vif_scores = compute_vif(vif_data) if vif_data.shape[1] >= 2 else {}
            for feature, value in sorted(vif_scores.items(), key=lambda kv: kv[1], reverse=True):
                if len(kept) <= min_selection:
                    break
                if value <= vif_threshold_val:
                    continue
                if feature in forced:
                    continue
                if feature in kept:
                    kept.remove(feature)
                    dropped[feature] = {"reason": "high_vif", "value": f"{value:.2f}"}

    return {
        "kept": kept,
        "dropped": dropped,
        "clusters": [sorted(list(c)) for c in clusters],
        "missing": sorted(set(missing)),
    }


def update_manifest(iso: str, manifest_entry: Dict[str, object]) -> None:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as fp:
            manifest = json.load(fp)
    else:
        manifest = {"ISOs": {}}
    manifest.setdefault("ISOs", {})[iso] = manifest_entry
    with open(MANIFEST_PATH, "w", encoding="utf-8") as fp:
        json.dump(manifest, fp, indent=2)


def save_shortlist(iso: str, shortlist_df: pd.DataFrame, features: Sequence[str], forced: Set[str]) -> None:
    output_path = SHORTLIST_DIR / f"factors_{iso}.csv"
    rows = []
    score_map = shortlist_df.set_index("feature")["rank_score"].to_dict()
    for feature in features:
        row = shortlist_df[shortlist_df["feature"] == feature]
        if row.empty:
            continue
        row = row.squeeze()
        rows.append(
            {
                "feature": feature,
                "rank_score": float(score_map.get(feature, 0.0)),
                "forced": feature in forced,
                "mean_contribution": float(row["mean_contribution"]),
                "mean_frequency": float(row["mean_frequency"]),
                "coverage": float(row["coverage"]),
                "source": row.get("source", "lasso"),
            }
        )
    pd.DataFrame(rows).to_csv(output_path, index=False)


def run_shortlist() -> None:
    args = parse_args()
    cfg, cfg_path = load_config(args.config)
    iso_list = list_isos()
    for iso in iso_list:
        contrib_path = FEATURE_OUTPUT_DIR / f"feature_contributions_{iso}.csv"
        if not contrib_path.exists():
            continue
        contrib_df = pd.read_csv(contrib_path)
        df_scores = aggregate_scores(contrib_df, cfg)
        if df_scores.empty:
            continue
        forced = determine_forced_features(df_scores, iso, contrib_df=contrib_df)
        shortlist = select_shortlist(df_scores, forced, cfg)
        panel_data = load_feature_panel(iso)
        score_map = df_scores.set_index("feature")["rank_score"].to_dict()
        collinearity = prune_collinearity_clustered(shortlist, panel_data, forced, score_map, cfg)
        final_features = collinearity["kept"]
        manifest_entry = {
            "step5_config": str(cfg_path.relative_to(BASE_DIR)) if cfg_path and cfg_path.exists() else None,
            "feature_universe_source": str(contrib_path.relative_to(BASE_DIR)),
            "selected": final_features,
            "forced_inclusions": sorted([feature for feature in final_features if feature in forced]),
            "dropped": [
                {"feature": feature, **details}
                for feature, details in collinearity["dropped"].items()
            ],
            "correlation_clusters": collinearity.get("clusters", []),
            "missing_panel_features": collinearity.get("missing", []),
        }
        save_shortlist(iso, df_scores, final_features, forced)
        update_manifest(iso, manifest_entry)


if __name__ == "__main__":
    run_shortlist()
