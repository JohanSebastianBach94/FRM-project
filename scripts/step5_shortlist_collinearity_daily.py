#!/usr/bin/env python3
"""Daily feature shortlist and collinearity check.

This script aggregates daily Lasso outputs and PCA/FCI components to
construct a daily factor shortlist per ISO, analogous to the monthly
Step 5 implementation.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Set

import numpy as np
import pandas as pd
import yaml

FACTOR_DIR_DAILY = Path("analysis_outputs") / "factor_preparation_daily"
FEATURE_DIR_DAILY = Path("analysis_outputs") / "feature_contributions_daily"
OUTPUT_DIR_DAILY = Path("analysis_outputs") / "factors_daily_shortlist"
DIAG_DIR_DAILY = Path("analysis_outputs") / "diagnostics_daily"
DEFAULT_CONFIG_PATH = Path("config") / "step5_shortlist.yaml"
PIPELINE_CONFIG_PATH = Path("SRESS TEST PIPELINE") / "step5_shortlist.yaml"
BLOCK_DEFINITION_PATH = Path("outputs") / "country_block_definition.json"
CATALOG_PATH = Path("catalog.csv")

# Keep economically essential drivers even if they are low-coverage / low-ranked.
REQUIRED_SERIES = {"V2X", "VIXCLS", "ECBDFR", "DFF", "DCOILBRENTEU"}
FORCE_INCLUDE_PREFIXES = [
    "BIS_LBS_Household_Loans",
    "GC.DOD.TOTL.GD.ZS",
    "NOMINAL_GDP",
    "NPL_PROXY",
    "BTP_Bund_Spread",
    "Bonos_Bund_Spread",
    "OAT_Bund_Spread",
    "DCOILBRENTEU",
    "DCOILWTICO",
    "Brent_Crude_Futures",
    "WTI_Crude_Futures",
    "PALUMUSDM",
    "PCOPPUSDM",
    "PIORECRUSDM",
    "PMAIZMTUSDM",
    "PSOYBUSDQ",
    "PWHEAMTUSDM",
    "TTF_GAS",
    "GBP_USD",
    "USD_CNY",
]

_LAG_RE = re.compile(r"_lag\d+$", re.IGNORECASE)
_LAGNUM_RE = re.compile(r"_lag(\d+)$", re.IGNORECASE)


def _base_feature_name(name: str) -> str:
    return _LAG_RE.sub("", name)


def _lag_num(name: str) -> int | None:
    match = _LAGNUM_RE.search(name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _allowed_in_shock_space(feature: str) -> bool:
    """Only keep base / lag0 features in the ADCC/MC shock universe.

    Lag>0 features are useful for mappings, but they are near-duplicates in the
    correlation layer and can destabilize ADCC estimation.
    """

    lag = _lag_num(feature)
    return lag is None or lag == 0


def drop_redundant_lag0_columns(columns: Iterable[str]) -> list[str]:
    cols = list(columns)
    col_set = set(cols)
    filtered: list[str] = []
    for col in cols:
        lag = _lag_num(col)
        if lag == 0:
            base = _base_feature_name(col)
            # If the base series exists, prefer the base and skip *_lag0.
            if base in col_set and base != col:
                continue
        filtered.append(col)
    return filtered


def load_do_not_use_series() -> set[str]:
    if not CATALOG_PATH.exists():
        return set()
    try:
        df = pd.read_csv(CATALOG_PATH)
    except Exception:
        return set()
    if "series" not in df.columns or "do_not_use" not in df.columns:
        return set()

    def _truthy(v: object) -> bool:
        if v is None:
            return False
        if isinstance(v, float) and np.isnan(v):
            return False
        s = str(v).strip().lower()
        return s in {"1", "true", "yes", "y"}

    blocked = df.loc[df["do_not_use"].apply(_truthy), "series"].astype(str)
    return set(blocked.tolist())


def _feature_implied_block_key(feature: str) -> str | None:
    lowered = feature.lower()
    if "_pc" in lowered:
        prefix = lowered.split("_pc", 1)[0]
        # common naming pattern: "financial_markets_pc1" -> "financial_markets"
        if prefix:
            return prefix
    return None


def load_block_series_for_iso(iso: str) -> dict[str, set[str]]:
    if not BLOCK_DEFINITION_PATH.exists():
        return {}
    try:
        payload = json.loads(BLOCK_DEFINITION_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

    iso_obj = payload.get(iso)
    if not isinstance(iso_obj, dict):
        return {}
    blocks = iso_obj.get("blocks")
    if not isinstance(blocks, list):
        return {}

    result: dict[str, set[str]] = {}
    for block in blocks:
        if not isinstance(block, dict):
            continue
        key = block.get("key")
        series_codes = block.get("series_codes")
        if not isinstance(key, str) or not isinstance(series_codes, list):
            continue
        series_set = {str(s) for s in series_codes if isinstance(s, (str, int, float))}
        if series_set:
            result[key] = series_set
    return result


def pick_best_feature_for_block(
    *,
    block_key: str,
    block_series: set[str],
    columns: Iterable[str],
    score: pd.Series,
    excluded: set[str],
) -> str | None:
    best_name: str | None = None
    best_score = float("-inf")
    for col in columns:
        if col in excluded:
            continue
        base = _base_feature_name(col)
        if base in block_series:
            s = float(score.get(col, 0.0))
            if s > best_score:
                best_score = s
                best_name = col
                continue

        implied = _feature_implied_block_key(col)
        if implied and implied == block_key.lower():
            s = float(score.get(col, 0.0))
            if s > best_score:
                best_score = s
                best_name = col
    return best_name


def ensure_dirs() -> None:
    OUTPUT_DIR_DAILY.mkdir(parents=True, exist_ok=True)
    DIAG_DIR_DAILY.mkdir(parents=True, exist_ok=True)


def resolve_force_candidates(columns: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for prefix in FORCE_INCLUDE_PREFIXES:
        for col in columns:
            if not _allowed_in_shock_space(col):
                continue
            if col.startswith(prefix) and col not in seen:
                seen.add(col)
                result.append(col)
    return result


def load_config(path_override: str | None = None) -> dict:
    config_path: Path | None = None
    if path_override:
        candidate = Path(path_override)
        config_path = candidate if candidate.is_absolute() else Path.cwd() / candidate
    elif PIPELINE_CONFIG_PATH.exists():
        config_path = PIPELINE_CONFIG_PATH
    elif DEFAULT_CONFIG_PATH.exists():
        config_path = DEFAULT_CONFIG_PATH

    if config_path and config_path.exists():
        try:
            loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Failed to parse Step 5 config at {config_path}: {exc}")
        return loaded if isinstance(loaded, dict) else {}
    return {}


def build_corr_clusters(corr: pd.DataFrame, threshold: float) -> list[set[str]]:
    cols = list(corr.columns)
    adj: Dict[str, Set[str]] = {c: set() for c in cols}
    for i, c1 in enumerate(cols):
        for c2 in cols[i + 1 :]:
            val = float(corr.at[c1, c2])
            if val >= threshold:
                adj[c1].add(c2)
                adj[c2].add(c1)

    seen: set[str] = set()
    clusters: list[set[str]] = []
    for node in cols:
        if node in seen:
            continue
        stack = [node]
        comp: set[str] = set()
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            comp.add(cur)
            stack.extend([n for n in adj[cur] if n not in seen])
        clusters.append(comp)
    return clusters


def choose_cluster_rep(cluster: set[str], forced: set[str], score: pd.Series) -> str:
    forced_in = [c for c in cluster if c in forced]
    candidates = forced_in if forced_in else list(cluster)
    candidates = sorted(candidates, key=lambda f: float(score.get(f, 0.0)), reverse=True)
    return candidates[0]


def build_shortlist_for_iso(iso: str, max_features: int = 20) -> None:
    factors_path = FACTOR_DIR_DAILY / f"{iso}_factors_daily.csv"
    if not factors_path.exists():
        print(f"[SKIP] Daily factors missing for {iso}: {factors_path}")
        return

    df = pd.read_csv(factors_path, index_col=0, parse_dates=True)
    keep_cols = drop_redundant_lag0_columns([c for c in df.columns if _allowed_in_shock_space(c)])
    df = df.loc[:, keep_cols]

    do_not_use = load_do_not_use_series()
    if do_not_use:
        df = df.loc[:, [c for c in df.columns if _base_feature_name(c) not in do_not_use]]

    required_present = [s for s in sorted(REQUIRED_SERIES) if s in df.columns]
    if required_present and len(required_present) > max_features:
        max_features = len(required_present)

    cfg = load_config()
    daily_cfg = (cfg.get("daily") or {}) if isinstance(cfg, dict) else {}
    selection_cfg = (daily_cfg.get("selection") or {}) if isinstance(daily_cfg, dict) else {}
    pruning_cfg = (daily_cfg.get("pruning") or {}) if isinstance(daily_cfg, dict) else {}
    corr_cluster_threshold = float(pruning_cfg.get("correlation_cluster_threshold", 0.95))

    block_cov_cfg = (selection_cfg.get("block_coverage") or {}) if isinstance(selection_cfg, dict) else {}
    block_cov_enabled = bool(block_cov_cfg.get("enabled", False))
    priority_blocks = block_cov_cfg.get("priority_blocks")
    if not isinstance(priority_blocks, list):
        priority_blocks = []
    priority_blocks = [str(b) for b in priority_blocks if str(b).strip()]

    # Simple proxy: select features with largest absolute coefficients
    # across available daily Lasso targets, but enforce some basic
    # economic diversity so the shortlist is not a single ultra-collinear
    # block (e.g. only oil series).
    coef_files = list(FEATURE_DIR_DAILY.glob(f"{iso}_*_coeffs_daily.csv"))
    coverage_protected: set[str] = set()
    rank_order: list[str] = []
    if not coef_files:
        print(f"[WARN] No daily Lasso coeffs for {iso}; using basic variance ranking")
        ranked_score = df.var().sort_values(ascending=False)
        rank_order = ranked_score.index.tolist()
        shortlist = ranked_score.head(max_features).index.tolist()
    else:
        ranked_score = pd.Series(0.0, index=df.columns)
        for path in coef_files:
            coefs = pd.read_csv(path, index_col=0)["coefficient"]
            coefs = coefs.reindex(df.columns).fillna(0.0)
            ranked_score += coefs.abs()

        ranked = ranked_score.sort_values(ascending=False)
        rank_order = ranked.index.tolist()

        # Basic diversification: try to force at least one indicator per
        # broad thematic block when available.
        thematic_blocks = {
            "oil": ["DCOIL", "BRENT", "WTI"],
            "credit": ["BAML", "HY", "IG"],
            "equity_vol": ["VIX"],
            "dnss": ["beta0", "beta1", "beta2"],
            "fci": ["FCI", "financial_markets_pc"],
        }

        chosen: list[str] = []
        remaining = ranked.copy()

        # First pass: pick top feature for each block if present
        for _, patterns in thematic_blocks.items():
            mask = remaining.index.to_series().apply(
                lambda x, pats=patterns: any(p.lower() in x.lower() for p in pats)
            )
            if mask.any():
                top = remaining[mask].idxmax()
                chosen.append(top)
                remaining = remaining.drop(top)

        # Second pass: fill the rest purely by rank
        for name in remaining.index:
            if len(chosen) >= max_features:
                break
            chosen.append(name)

        shortlist = chosen[:max_features]

        # Block-aware coverage: ensure the shortlist is not an under-identified
        # proxy set when used for block aggregation / MC.
        if block_cov_enabled and priority_blocks:
            block_series_by_key = load_block_series_for_iso(iso)
            selected_set = set(shortlist)

            protected: list[str] = []
            for block_key in priority_blocks:
                series_set = block_series_by_key.get(block_key)
                if not series_set:
                    continue
                # If the block is already represented, still protect the best
                # representative so later pruning can't remove the block entirely.
                already_in_block = [
                    s for s in selected_set if _base_feature_name(s) in series_set
                ]
                if already_in_block:
                    best_existing = max(
                        already_in_block, key=lambda f: float(ranked_score.get(f, 0.0))
                    )
                    if best_existing not in protected:
                        protected.append(best_existing)
                    continue
                candidate = pick_best_feature_for_block(
                    block_key=block_key,
                    block_series=series_set,
                    columns=ranked.index,
                    score=ranked_score,
                    excluded=selected_set,
                )
                if candidate and candidate in df.columns:
                    protected.append(candidate)
                    selected_set.add(candidate)

            coverage_protected = set(protected)

            if protected:
                # Make room by dropping lowest-scoring non-required items.
                required_set = set(required_present)
                scored = sorted(
                    [c for c in selected_set if c not in required_set],
                    key=lambda f: float(ranked_score.get(f, 0.0)),
                )
                while len(selected_set) > max_features and scored:
                    drop = scored.pop(0)
                    # never drop newly protected coverage picks
                    if drop in protected:
                        continue
                    selected_set.remove(drop)

                # Reconstruct ordered shortlist: required -> protected -> rank fill
                ordered: list[str] = []
                for c in required_present:
                    if c in selected_set and c not in ordered:
                        ordered.append(c)
                for c in protected:
                    if c in selected_set and c not in ordered:
                        ordered.append(c)
                for c in ranked.index:
                    if c in selected_set and c not in ordered:
                        ordered.append(c)
                shortlist = ordered[:max_features]

        # "Forced" prefixes are allowed to influence ordering, but should not
        # crowd out block coverage.
        forced_candidates = resolve_force_candidates(df.columns)
        if forced_candidates:
            ordered: list[str] = []
            for candidate in forced_candidates:
                if candidate in shortlist and candidate not in ordered:
                    ordered.append(candidate)
            for candidate in shortlist:
                if candidate not in ordered:
                    ordered.append(candidate)
            shortlist = ordered[:max_features]

    if required_present:
        shortlist = required_present + [s for s in shortlist if s not in required_present]
        if len(shortlist) > max_features:
            shortlist = required_present + [
                s for s in shortlist if s not in required_present
            ][: max_features - len(required_present)]

    # Cluster-based pruning: keep 1 representative per correlation cluster.
    # Governance intent for daily: NEVER drop required anchors (e.g. vol index, policy rate, oil).
    # Also: avoid `.dropna()` across all columns (too strict when series have different start dates).
    shortlist_df = df[shortlist].copy().apply(pd.to_numeric, errors="coerce")
    required_set = set(required_present)
    required_cols = [c for c in shortlist if c in required_set]
    other_cols = [c for c in shortlist if c not in required_set]

    reps_other: list[str]
    if len(other_cols) >= 2 and shortlist_df.shape[0] >= 5:
        corr = shortlist_df[other_cols].corr().abs().fillna(0.0)
        clusters = build_corr_clusters(corr, corr_cluster_threshold)
        forced_other = set(resolve_force_candidates(df.columns))
        # Protect block-coverage representatives + required anchors from being dropped.
        protected_other = set(other_cols) & (forced_other | required_set | coverage_protected)
        reps_other = []
        for cluster in clusters:
            protected_in_cluster = {c for c in cluster if c in protected_other}
            forced_for_cluster = protected_in_cluster or forced_other
            rep = choose_cluster_rep(cluster, forced_for_cluster, ranked_score)
            if rep not in reps_other:
                reps_other.append(rep)
        reps_other = sorted(reps_other, key=lambda f: float(ranked_score.get(f, 0.0)), reverse=True)
    else:
        reps_other = other_cols

    shortlist = required_cols + [c for c in reps_other if c not in required_set]
    shortlist = shortlist[:max_features]

    # Refill after clustering: cluster pruning can reduce the shortlist below the cap.
    if len(shortlist) < max_features and rank_order:
        protected_all = required_set | coverage_protected
        for candidate in rank_order:
            if len(shortlist) >= max_features:
                break
            if candidate in shortlist:
                continue
            if candidate in protected_all:
                shortlist.append(candidate)
                continue
            if candidate in df.columns:
                shortlist.append(candidate)

    shortlist_df = df[shortlist].copy()

    # Collinearity diagnostics on shortlist
    corr = shortlist_df.corr().abs()
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    high_pairs = upper.stack().reset_index()
    high_pairs = high_pairs[high_pairs[0] > 0.95]

    diag_rows: List[Dict[str, object]] = []
    for _, row in high_pairs.iterrows():
        diag_rows.append(
            {
                "iso": iso,
                "series_1": row["level_0"],
                "series_2": row["level_1"],
                "correlation": float(row[0]),
            }
        )

    if diag_rows:
        pd.DataFrame(diag_rows).to_csv(
            DIAG_DIR_DAILY / f"{iso}_final_collinearity_daily.csv", index=False
        )

    out_path = OUTPUT_DIR_DAILY / f"{iso}_factors_daily_shortlist.csv"
    shortlist_df.to_csv(out_path)
    print(f"[DONE] Daily shortlist for {iso}: {out_path}")


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily factor shortlist per ISO")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO codes")
    parser.add_argument("--max-features", type=int, default=12, help="Max shortlist size")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to Step 5 config YAML (overrides defaults)",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    ensure_dirs()
    args = parse_args(argv)
    if args.config:
        # Override the module-level default config resolution by reloading once.
        # (We keep the rest of the code simple by using the same loader.)
        global load_config
        _orig_loader = load_config

        def _loader_override(path_override: str | None = None) -> dict:  # type: ignore[no-redef]
            return _orig_loader(args.config)

        load_config = _loader_override  # type: ignore[assignment]

    for iso in args.isos:
        build_shortlist_for_iso(iso, max_features=args.max_features)


if __name__ == "__main__":  # pragma: no cover
    main()
