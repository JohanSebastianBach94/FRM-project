#!/usr/bin/env python3
"""
Robust Factor Preparation for CountryBlocks.

- Loads combined stress indicator + NSS panel (monthly-aligned).
- Filters to country block series from config/country_blocks_extended.yaml.
- Enforces coverage and minimum observation thresholds.
- Standardizes, optionally computes block-level PCA (sklearn), builds FCI proxy,
  creates lagged features guarded by minimum history, and writes outputs:

  analysis_outputs/factor_preparation/{ISO}_factors.csv
  analysis_outputs/factor_preparation/{ISO}_pca_components.csv
  analysis_outputs/factor_preparation/{ISO}_pca_explained.csv
  analysis_outputs/factor_preparation/{ISO}_missing_series.json
  analysis_outputs/factor_preparation/factor_preparation_summary.md

Assumes your data loader functions exist:
 - load_stress_indicators() -> dict with key "combined": DataFrame (datetime index)
 - load_nss_betas() -> DataFrame

Run:
    python scripts/prepare_country_factors.py
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple
import sys

import numpy as np
import pandas as pd
import yaml
from sklearn.decomposition import PCA

# Ensure the project root is on sys.path so data_pipeline can be imported
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# Replace these with your real import paths
from data_pipeline import load_nss_betas, load_project_config, load_stress_indicators

# --------------------------
# Configurable parameters
# --------------------------
# Coverage thresholds can differ for drivers vs targets; drivers remain stricter.
# Default is overridden by `analysis_outputs/coverage_threshold_config.json` when present.
COVERAGE_THRESHOLD = 0.62           # fraction non-NaN required for driver series
MIN_OBS = 60                        # minimum total (non-NaN) observations to keep a driver series
MIN_OBS_LAGS = 120                  # minimum non-NaN observations required to create lags
LAG_PERIODS = [1, 2, 3]             # months
FCI_GLOBAL_PROXIES = ["VIXCLS", "BAMLH0A0HYM2", "BAMLC0A4CBBB", "BAMLH0A1HYBB", "DCOILWTICO", "DCOILBRENTEU"]
TARGET_PCA_VARIANCE = 0.90
MAX_PCA_COMPONENTS = 6
OUTPUT_DIR = Path("analysis_outputs") / "factor_preparation"
LITERATURE_OUTPUT_DIR = Path("analysis_outputs") / "literature_factors"
PCA_COMPONENT_FILE_SUFFIX = "_pca_components.csv"
PCA_COMPONENT_METADATA_SUFFIX = "_pca_component_metadata.json"
IMF_CHANNEL_MAP = {
    "macro": "macroeconomic",
    "public_finance": "macroeconomic",
    "financial_markets": "market",
    "banking_system": "funding",
    "real_estate": "credit",
    "external_fx": "market",
}
COUNTRY_BLOCK_YAML = Path("config") / "country_blocks_extended.yaml"
FACTOR_SETTINGS_PATH = Path("config") / "factor_settings.yaml"
FROZEN_BLOCKS_PATH = Path("outputs") / "country_block_definition.json"
THRESHOLD_CONFIG_PATH = Path("analysis_outputs") / "coverage_threshold_config.json"
CATALOG_PATH = Path("catalog.csv")
RESAMPLE_RULE = "M"  # monthly alignment: month end

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class CountryFactorSpec:
    country: str
    iso_code: str
    block_series: Dict[str, List[str]]


def _load_factor_settings(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as fp:
            payload = yaml.safe_load(fp) or {}
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _get_nested(payload: dict, keys: Sequence[str], default):
    node = payload
    for key in keys:
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return node


def _load_series_threshold(path: Path) -> float:
    if not path.exists():
        return COVERAGE_THRESHOLD
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return COVERAGE_THRESHOLD
    try:
        return float(payload.get("series_threshold", COVERAGE_THRESHOLD))
    except Exception:
        return COVERAGE_THRESHOLD


def _load_do_not_use_series(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if "series" not in df.columns:
        return set()

    # Accept either explicit DO NOT USE strings or any truthy marker.
    if "do_not_use" not in df.columns:
        return set()
    blocked = set()
    for _, row in df.iterrows():
        series = str(row.get("series", "")).strip()
        if not series:
            continue
        flag = str(row.get("do_not_use", "")).strip()
        if flag and flag.upper() != "NAN":
            blocked.add(series)
    return blocked


def load_country_specs(path: Path) -> List[CountryFactorSpec]:
    """Load country specs preferring the frozen Step-2 artifact.

    The frozen definition already applies the current coverage cutoff and
    weakest-ISO harmonization, so using it ensures Step 3 uses the actual
    governed driver set.
    """

    if FROZEN_BLOCKS_PATH.exists():
        frozen = json.loads(FROZEN_BLOCKS_PATH.read_text(encoding="utf-8"))
        specs: List[CountryFactorSpec] = []
        for iso, entry in frozen.items():
            blocks = entry.get("blocks", []) or []
            block_series = {block.get("key"): block.get("series_codes", []) for block in blocks if block.get("key")}
            specs.append(CountryFactorSpec(country=str(entry.get("country") or iso), iso_code=str(iso), block_series=block_series))
        return specs

    with path.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp)

    specs = []
    for entry in payload.get("country_blocks", []):
        block_series = {
            block["key"]: block.get("series_codes", [])
            for block in entry.get("blocks", [])
        }
        specs.append(
            CountryFactorSpec(country=entry["country"], iso_code=entry["iso_code"], block_series=block_series)
        )
    return specs


def load_combined_panel() -> pd.DataFrame:
    # Prefer the expanded merged panel when present. This is the same universe
    # used for catalog coverage checks and contains many governed series that
    # are not part of the legacy FRED/Yahoo-only stress indicator bundle.
    expanded_candidates = [
        Path("data") / "stress_indicators_expanded.csv",
        Path("data_pipeline") / "data" / "stress_indicators_expanded.csv",
    ]

    combined: pd.DataFrame | None = None
    for candidate in expanded_candidates:
        if candidate.exists():
            combined = _read_panel_csv(candidate)
            break

    if combined is None:
        data = load_stress_indicators()
        combined = data.get("combined")

    if combined is None or combined.empty:
        raise RuntimeError(
            "Combined panel is unavailable. Ensure stress indicators have been built (preferably stress_indicators_expanded.csv)."
        )

    # Trim to the configured project sample window (matches coverage calculations).
    try:
        cfg = load_project_config()
        date_start = getattr(cfg, "date_start", None)
        date_end = getattr(cfg, "date_end", None)
        if date_start is not None and date_end is not None:
            combined = combined.loc[(combined.index >= date_start) & (combined.index <= date_end)]
    except Exception:
        pass

    # Overlay locally-derived CSV drivers that are tracked in the catalog but
    # may not yet be merged into stress_indicators_expanded.csv.
    derived_dir = PROJECT_ROOT / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
    if derived_dir.exists():
        for csv_path in sorted(derived_dir.glob("Price_to_income_ratio_*.csv")):
            iso = csv_path.stem.replace("Price_to_income_ratio_", "").strip()
            if not iso:
                continue
            series_name = f"Price_to_income_{iso}"
            if series_name in combined.columns:
                continue
            try:
                raw = pd.read_csv(csv_path)
            except Exception:
                continue
            if raw.empty:
                continue
            date_col = "date" if "date" in raw.columns else raw.columns[0]
            value_col = "value" if "value" in raw.columns else None
            if value_col is None:
                numeric_cols = raw.select_dtypes(include="number").columns
                if numeric_cols.empty:
                    continue
                value_col = str(numeric_cols[0])
            s = pd.to_numeric(raw[value_col], errors="coerce")
            idx = pd.to_datetime(raw[date_col], errors="coerce")
            overlay = pd.Series(s.values, index=idx, name=series_name).dropna()
            if overlay.empty:
                continue
            overlay = overlay[~overlay.index.duplicated(keep="first")].sort_index()
            combined = combined.join(overlay.to_frame(), how="outer")

    nss = load_nss_betas()
    if nss is not None and not nss.empty:
        nss = nss.loc[:, ~nss.columns.duplicated()].copy()
        missing_cols = [c for c in nss.columns if c not in combined.columns]
        if missing_cols:
            combined = combined.join(nss[missing_cols], how="outer")

    # align to month end (keep last observation in month)
    try:
        combined.index = pd.to_datetime(combined.index)
    except Exception:
        logger.warning("Index conversion failed; ensure the combined panel has a datetime index.")
    combined = combined.sort_index()
    combined_monthly = combined.resample(RESAMPLE_RULE).last()
    return combined_monthly


def standardize(df: pd.DataFrame) -> pd.DataFrame:
    mean = df.mean(skipna=True)
    std = df.std(skipna=True, ddof=0).replace(0, np.nan)
    return (df - mean) / std


def compute_fci(
    iso_code: str,
    scaled_df: pd.DataFrame,
    method: str = "mean",
    global_proxies: Sequence[str] | None = None,
    min_proxies_for_pca: int = 3,
) -> pd.Series:
    # proxies present = global proxies + country-specific funding/term spread proxies
    proxy_list = list(global_proxies or FCI_GLOBAL_PROXIES)
    proxy_list += [f"TERM_SPREAD_{iso_code}_10Y_2Y", f"FUNDING_STRESS_{iso_code}", f"LIQUIDITY_STRESS_{iso_code}"]
    proxies = [col for col in proxy_list if col in scaled_df.columns]
    if not proxies:
        return pd.Series(dtype=float)
    sub = scaled_df[proxies].dropna(how="all")
    if sub.empty:
        return pd.Series(dtype=float)
    if method == "pca" and sub.shape[1] >= max(2, int(min_proxies_for_pca)) and sub.shape[0] >= 10:
        pca = PCA(n_components=min(sub.shape[1], 3), random_state=0)
        pcs = pca.fit_transform(sub.fillna(0))
        return pd.Series(pcs[:, 0], index=sub.index).reindex(scaled_df.index)
    # default: simple mean of z-scored proxies
    return sub.mean(axis=1).reindex(scaled_df.index)


def needs_pca(block_df: pd.DataFrame, corr_cutoff: float = 0.95) -> bool:
    clean = block_df.dropna()
    if clean.shape[1] < 2 or clean.shape[0] < 10:
        return False
    corr = clean.corr().abs()
    np.fill_diagonal(corr.values, 0.0)
    return (corr.values > corr_cutoff).any()


def compute_block_pca(block_df: pd.DataFrame, block_key: str, target_variance: float = TARGET_PCA_VARIANCE,
                      max_components: int = MAX_PCA_COMPONENTS) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (pca_scores_df, pca_loadings_df, explained_variance_df)
    """
    clean = block_df.dropna()
    if clean.shape[0] < 10 or clean.shape[1] < 2:
        return pd.DataFrame(), pd.DataFrame()
    pca = PCA(n_components=min(clean.shape[1], max_components), random_state=0)
    scaled = (clean - clean.mean()) / clean.std(ddof=0).replace(0, np.nan)
    scaled = scaled.fillna(0.0)
    pca.fit(scaled)
    exp_var = pca.explained_variance_ratio_
    cum = np.cumsum(exp_var)
    n_components = int(np.argmax(cum >= target_variance) + 1) if (cum >= target_variance).any() else len(exp_var)
    n_components = max(1, min(n_components, pca.components_.shape[0]))
    # recompute PCA with desired n_components for scores
    pca_final = PCA(n_components=n_components, random_state=0)
    scores = pca_final.fit_transform(scaled)
    cols = [f"{block_key}_pc{i+1}" for i in range(n_components)]
    scores_df = pd.DataFrame(scores, columns=cols, index=clean.index)
    # metadata: component loadings + explained variance
    loadings = pd.DataFrame(pca_final.components_.T, index=clean.columns, columns=cols)
    explained = pd.DataFrame({"component": cols, "explained_variance_ratio": pca_final.explained_variance_ratio_})
    # reindex to original index (will insert NaNs where missing)
    return scores_df.reindex(block_df.index), loadings, explained


def create_lagged_features(df: pd.DataFrame, columns: Sequence[str], lags: Sequence[int],
                           min_obs_for_lags: int = MIN_OBS_LAGS) -> pd.DataFrame:
    frames = []
    for col in columns:
        column_data = df[col]
        if isinstance(column_data, pd.DataFrame):
            column_series = column_data.iloc[:, 0]
        else:
            column_series = column_data
        non_na = int(column_series.notna().sum())
        if non_na < min_obs_for_lags:
            logger.debug(f"Skipping lags for {col} (non-na {non_na} < threshold {min_obs_for_lags})")
            continue
        for lag in lags:
            lagged = column_series.shift(lag)
            lagged.name = f"{col}_lag{lag}"
            frames.append(lagged)
    if not frames:
        return pd.DataFrame(index=df.index)
    return pd.concat(frames, axis=1)


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _read_panel_csv(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, index_col="Date", parse_dates=True)
    except Exception:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    df = df.sort_index()
    return df


def _resample_panel(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    try:
        out = df.resample(rule).last()
    except Exception:
        # If the index isn't datetime-like, let the caller handle.
        raise
    return out


def dedupe_near_duplicates(
    block_df: pd.DataFrame,
    corr_threshold: float = 0.995,
    min_periods: int = 24,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Drop near-duplicate series within a block.

    Greedy rule: if |corr(i,j)| >= threshold, drop the series with lower
    non-missing coverage (tie-break: drop the second).
    """
    if block_df.shape[1] < 2:
        return block_df, {"dropped": [], "pairs": [], "threshold": corr_threshold}

    clean = block_df.copy()
    # If upstream joins produced duplicate column names, make them unique here.
    # Otherwise coverage/corr lookups can return Series and break scalar logic.
    if clean.columns.has_duplicates:
        clean = clean.loc[:, ~clean.columns.duplicated()].copy()
    # Guard against correlation on tiny overlap.
    corr = clean.corr(min_periods=min_periods).abs()
    if corr.empty:
        return clean, {"dropped": [], "pairs": [], "threshold": corr_threshold}

    dropped: set[str] = set()
    pairs: List[dict] = []
    coverage = clean.notna().mean()

    def _to_scalar(value) -> float:
        if isinstance(value, pd.Series):
            # Duplicate label case: take the best-covered representation.
            return float(value.max()) if not value.empty else 0.0
        try:
            return float(value)
        except Exception:
            return 0.0

    # Sort candidate pairs by descending correlation.
    tri = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
    stacked = tri.stack().sort_values(ascending=False)
    for (a, b), rho in stacked.items():
        if rho < corr_threshold:
            break
        if a in dropped or b in dropped:
            continue
        cov_a = _to_scalar(coverage.get(a, 0.0))
        cov_b = _to_scalar(coverage.get(b, 0.0))
        drop = b if cov_a >= cov_b else a
        keep = a if drop == b else b
        dropped.add(drop)
        pairs.append({"keep": keep, "drop": drop, "abs_corr": float(rho), "cov_keep": cov_a if keep == a else cov_b, "cov_drop": cov_b if drop == b else cov_a})

    kept_cols = [c for c in clean.columns if c not in dropped]
    return clean[kept_cols], {"dropped": sorted(dropped), "pairs": pairs, "threshold": corr_threshold, "min_periods": int(min_periods)}


def _expand_to_daily_step_hold(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    start = df.index.min()
    end = df.index.max()
    business_index = pd.bdate_range(start=start, end=end)
    union_index = business_index.union(df.index).sort_values()
    expanded = df.reindex(union_index).ffill()
    return expanded.reindex(business_index)


def _literature_factor_column(iso: str, block_key: str, idx: int) -> str:
    return f"{iso}_{block_key}_f{idx}".replace(" ", "_")


def prepare_literature_factors_for_country(
    spec: CountryFactorSpec,
    panel: pd.DataFrame,
    settings: dict,
    *,
    resample_rule: str,
    expand_to_daily: bool,
    dedupe_corr_threshold: float,
    max_factors_per_block: int,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Literature mode: represent each block by a small number of factors.

    - Deduplicates near-duplicates within each block.
    - Uses within-block PCA to extract 1..N factors (capped).
    - Blocks with K=1 become a single standardized factor.
    """
    series_threshold = _load_series_threshold(THRESHOLD_CONFIG_PATH)
    do_not_use = _load_do_not_use_series(CATALOG_PATH)

    try:
        panel_rs = _resample_panel(panel, resample_rule)
    except Exception:
        panel_rs = panel.copy()

    # Literature blocks can include quarterly/low-frequency series. After resampling
    # to a monthly grid, those would otherwise appear sparse and get dropped by
    # coverage filtering even when their native-frequency coverage is complete.
    # Forward-filling here step-holds the most recent observation so coverage
    # reflects availability rather than reporting frequency.
    panel_rs = panel_rs.ffill()

    desired = [series for block_series in spec.block_series.values() for series in block_series]
    desired = [s for s in desired if s not in do_not_use]
    panel_cols = set(panel_rs.columns)
    available = [s for s in desired if s in panel_cols]
    missing = sorted(set(desired) - set(available))

    df_country = panel_rs[available].copy()
    coverage_frac = df_country.notna().mean()
    non_na_counts = df_country.notna().sum()
    retained_by_coverage = coverage_frac[coverage_frac >= series_threshold].index.tolist()
    retained_by_minobs = non_na_counts[non_na_counts >= MIN_OBS].index.tolist()
    retained = sorted(list(set(retained_by_coverage).intersection(retained_by_minobs)))
    dropped = sorted(set(df_country.columns) - set(retained))
    df_reduced = df_country[retained].copy()

    if df_reduced.empty:
        summary = {
            "country": spec.country,
            "iso": spec.iso_code,
            "retained": 0,
            "dropped": len(dropped),
            "dropped_list": dropped,
            "missing_series": missing,
            "rows": 0,
            "mode": "literature",
        }
        return pd.DataFrame(), summary

    scaled = standardize(df_reduced)

    monthly_pca = _get_nested(settings, ["pca", "monthly"], {}) or {}
    pca_variance_target = float(monthly_pca.get("variance_target", TARGET_PCA_VARIANCE))
    pca_max_components = int(monthly_pca.get("max_components", MAX_PCA_COMPONENTS))

    factors_by_block: Dict[str, List[str]] = {}
    block_details: Dict[str, Dict[str, object]] = {}
    loadings_rows: List[pd.DataFrame] = []
    explained_rows: List[pd.DataFrame] = []
    dedupe_reports: Dict[str, object] = {}

    # Per-block factor constituent maps for downstream auditing / frequency inference.
    # Keyed by block_key; each value is a long-form DataFrame with columns:
    # iso, block, factor, series, weight, method.
    factor_constituent_maps: Dict[str, pd.DataFrame] = {}

    factor_frames: List[pd.DataFrame] = []
    scaled_cols = set(map(str, scaled.columns))
    for block_key, block_series in spec.block_series.items():
        # Start from the frozen/harmonized spec series list and make coverage/filtering
        # decisions explicit at the per-block level.
        configured = [str(s) for s in (block_series or []) if str(s) not in do_not_use]
        present_in_panel = [s for s in configured if s in panel_cols]
        after_filters = [s for s in configured if s in scaled_cols]
        missing_in_panel = [s for s in configured if s not in panel_cols]
        dropped_by_filters = [s for s in present_in_panel if s not in scaled_cols]

        # Always emit a block_details entry, even if the block ends up empty after
        # coverage/min-obs filtering. This prevents diagnostics from appearing to
        # "lose" governed blocks.
        block_details[block_key] = {
            "n_series_configured": int(len(configured)),
            "n_series_present_in_panel": int(len(present_in_panel)),
            "n_series_available": int(len(after_filters)),
            "n_series_after_dedupe": None,
            "method": None,
            "factors": None,
            # Exact series lists used to construct the block factor(s)
            # (configured -> present in panel -> after filters -> post-dedupe).
            "series_configured": list(configured),
            "series_present_in_panel": list(present_in_panel),
            "series_missing_in_panel": list(missing_in_panel),
            "series_dropped_by_filters": list(dropped_by_filters),
            "series_pre_dedupe": list(after_filters),
            "series_post_dedupe": None,
        }

        cols = after_filters
        if not cols:
            block_details[block_key]["method"] = "empty"
            block_details[block_key]["factors"] = []
            block_details[block_key]["n_series_after_dedupe"] = 0
            block_details[block_key]["series_post_dedupe"] = []
            continue

        block_df = scaled[cols]
        min_periods = max(24, int(0.25 * len(block_df)))
        deduped, report = dedupe_near_duplicates(
            block_df,
            corr_threshold=dedupe_corr_threshold,
            min_periods=min_periods,
        )
        dedupe_reports[block_key] = report
        cols_deduped = list(deduped.columns)
        block_details[block_key]["n_series_after_dedupe"] = int(len(cols_deduped))
        block_details[block_key]["series_post_dedupe"] = list(cols_deduped)
        if len(cols_deduped) == 1:
            col = cols_deduped[0]
            factor_col = _literature_factor_column(spec.iso_code, block_key, 1)
            factor = deduped[col].rename(factor_col).to_frame()
            factors_by_block[block_key] = [factor_col]
            factor_frames.append(factor)
            block_details[block_key]["method"] = "single_series"
            block_details[block_key]["factors"] = [factor_col]

            factor_constituent_maps[block_key] = pd.DataFrame(
                {
                    "iso": [spec.iso_code],
                    "block": [block_key],
                    "factor": [factor_col],
                    "series": [col],
                    "weight": [1.0],
                    "method": ["single_series"],
                }
            )
            continue

        pca_scores, loadings, explained = compute_block_pca(
            deduped,
            block_key,
            target_variance=pca_variance_target,
            max_components=min(pca_max_components, max(2, int(max_factors_per_block))),
        )
        if pca_scores.empty:
            # Fallback: use mean factor if PCA is not feasible.
            factor_col = _literature_factor_column(spec.iso_code, block_key, 1)
            factor = deduped.mean(axis=1).rename(factor_col).to_frame()
            factors_by_block[block_key] = [factor_col]
            factor_frames.append(factor)
            block_details[block_key]["method"] = "mean"
            block_details[block_key]["factors"] = [factor_col]

            w = 1.0 / float(len(cols_deduped)) if cols_deduped else 0.0
            factor_constituent_maps[block_key] = pd.DataFrame(
                {
                    "iso": [spec.iso_code] * int(len(cols_deduped)),
                    "block": [block_key] * int(len(cols_deduped)),
                    "factor": [factor_col] * int(len(cols_deduped)),
                    "series": list(cols_deduped),
                    "weight": [float(w)] * int(len(cols_deduped)),
                    "method": ["mean"] * int(len(cols_deduped)),
                }
            )
            continue

        # Cap number of factors per block.
        pca_scores = pca_scores.iloc[:, : max(1, int(max_factors_per_block))]
        renamed_cols = [_literature_factor_column(spec.iso_code, block_key, i + 1) for i in range(pca_scores.shape[1])]
        pca_scores = pca_scores.rename(columns=dict(zip(pca_scores.columns, renamed_cols)))
        factors_by_block[block_key] = renamed_cols
        factor_frames.append(pca_scores)

        block_details[block_key]["method"] = "pca"
        block_details[block_key]["factors"] = list(renamed_cols)

        if isinstance(loadings, pd.DataFrame) and not loadings.empty:
            local = loadings.iloc[:, : len(renamed_cols)].copy()
            local.columns = renamed_cols
            long = (
                local.stack()
                .rename("weight")
                .reset_index()
                .rename(columns={"level_0": "series", "level_1": "factor"})
            )
            long.insert(0, "iso", spec.iso_code)
            long.insert(1, "block", block_key)
            long["method"] = "pca"
            factor_constituent_maps[block_key] = long

        if isinstance(loadings, pd.DataFrame) and not loadings.empty:
            # Re-map loadings columns to renamed factor columns
            loadings = loadings.iloc[:, : len(renamed_cols)].copy()
            loadings.columns = renamed_cols
            loadings.insert(0, "iso", spec.iso_code)
            loadings.insert(1, "block", block_key)
            loadings_rows.append(loadings)
        if isinstance(explained, pd.DataFrame) and not explained.empty:
            explained = explained.iloc[: len(renamed_cols)].copy()
            explained["component"] = renamed_cols
            explained.insert(0, "iso", spec.iso_code)
            explained.insert(1, "block", block_key)
            explained_rows.append(explained)

    if not factor_frames:
        summary = {
            "country": spec.country,
            "iso": spec.iso_code,
            "retained": len(retained),
            "dropped": len(dropped),
            "dropped_list": dropped,
            "missing_series": missing,
            "rows": 0,
            "mode": "literature",
        }
        return pd.DataFrame(), summary

    factors = pd.concat(factor_frames, axis=1).dropna(how="all")
    if expand_to_daily:
        factors = _expand_to_daily_step_hold(factors)

    summary = {
        "country": spec.country,
        "iso": spec.iso_code,
        "retained": len(retained),
        "dropped": len(dropped),
        "dropped_list": dropped,
        "missing_series": missing,
        "rows": int(factors.shape[0]),
        "mode": "literature",
        "factors": int(factors.shape[1]),
        "blocks": int(len(factors_by_block)),
        "factors_by_block": factors_by_block,
        "block_details": block_details,
        "dedupe": dedupe_reports,
    }

    # Write per-ISO audit JSON
    ensure_output_dir(LITERATURE_OUTPUT_DIR)
    (LITERATURE_OUTPUT_DIR / f"{spec.iso_code}_literature_manifest.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    # Write constituent maps under analysis_outputs/factor_preparation for downstream steps.
    ensure_output_dir(OUTPUT_DIR)
    for block_key, df_map in factor_constituent_maps.items():
        try:
            out_path = OUTPUT_DIR / f"{spec.iso_code}_{block_key}_factor_constituents.csv"
            df_map.to_csv(out_path, index=False)
        except Exception:
            # Best-effort: governance artifact.
            pass

    # Append PCA audit tables (cleared at run start in main()) so the files cover all ISOs.
    if loadings_rows:
        out_path = LITERATURE_OUTPUT_DIR / "literature_pca_loadings.csv"
        df_out = pd.concat(loadings_rows, axis=0)
        write_header = not out_path.exists()
        df_out.to_csv(out_path, mode="a", header=write_header)
    if explained_rows:
        out_path = LITERATURE_OUTPUT_DIR / "literature_pca_explained.csv"
        df_out = pd.concat(explained_rows, axis=0)
        write_header = not out_path.exists()
        df_out.to_csv(out_path, mode="a", header=write_header, index=False)

    return factors, summary


def write_summary(entries: List[Dict[str, object]], path: Path) -> None:
    lines = ["# Factor Preparation Status (Generated by scripts/prepare_country_factors.py)", "", "Run this script to refresh the tables and CSVs below.", ""]
    for entry in entries:
        lines.append(f"## {entry['country']} ({entry['iso']})")
        lines.append(f"- Available series retained: {entry['retained']}")
        lines.append(f"- Dropped due to coverage or min obs: {entry['dropped']} ({', '.join(entry.get('dropped_list', []))[:200]})")
        pca = ", ".join(entry.get('pca_blocks', [])) or "none"
        lines.append(f"- PCA applied for blocks: {pca}")
        lines.append(f"- Missing (configured but not in panel): {', '.join(entry.get('missing_series', []))[:400]}")
        lines.append(f"- Output rows: {entry['rows']} (see `{Path(entry['path']).name}`)")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def prepare_factors_for_country(spec: CountryFactorSpec, panel: pd.DataFrame, settings: dict) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """Prepare driver factors for a country.

    Targets are intentionally handled downstream (e.g. in train_lasso_mappings.py)
    so they are not dropped by the stricter driver coverage filters or included in
    block-level PCA.
    """
    series_threshold = _load_series_threshold(THRESHOLD_CONFIG_PATH)
    do_not_use = _load_do_not_use_series(CATALOG_PATH)

    # gather desired driver series from block definitions (frozen, if present)
    desired = [series for block_series in spec.block_series.values() for series in block_series]
    desired = [s for s in desired if s not in do_not_use]
    panel_cols = set(panel.columns)
    available = [s for s in desired if s in panel_cols]
    missing = sorted(set(desired) - set(available))

    logger.info(f"{spec.iso_code}: desired {len(desired)} series, available {len(available)}, missing {len(missing)}")

    df_country = panel[available].copy()
    # coverage fraction and min obs filter
    coverage_frac = df_country.notna().mean()
    non_na_counts = df_country.notna().sum()
    retained_by_coverage = coverage_frac[coverage_frac >= series_threshold].index.tolist()
    retained_by_minobs = non_na_counts[non_na_counts >= MIN_OBS].index.tolist()
    retained = sorted(list(set(retained_by_coverage).intersection(retained_by_minobs)))

    dropped = sorted(set(df_country.columns) - set(retained))
    df_reduced = df_country[retained].copy()

    if df_reduced.empty:
        logger.warning(f"{spec.iso_code}: no columns survive coverage/min-obs filtering. Writing missing series file and skipping.")
        summary = {
            "country": spec.country,
            "iso": spec.iso_code,
            "retained": 0,
            "dropped": len(dropped),
            "dropped_list": dropped,
            "pca_blocks": [],
            "missing_series": missing,
            "rows": 0,
            "path": str(OUTPUT_DIR / f"{spec.iso_code}_factors.csv")
        }
        # Save missing series to JSON for audit
        (OUTPUT_DIR / f"{spec.iso_code}_missing_series.json").write_text(json.dumps({"missing": missing, "desired": desired}, indent=2))
        return pd.DataFrame(), summary

    # standardize
    scaled = standardize(df_reduced)

    # Load shared Step-3 settings.
    monthly_pca = _get_nested(settings, ["pca", "monthly"], {}) or {}
    monthly_fci = _get_nested(settings, ["fci", "monthly"], {}) or {}

    pca_corr_cutoff = float(monthly_pca.get("corr_cutoff", 0.95))
    pca_variance_target = float(monthly_pca.get("variance_target", TARGET_PCA_VARIANCE))
    pca_max_components = int(monthly_pca.get("max_components", MAX_PCA_COMPONENTS))

    fci_method = str(monthly_fci.get("method", "auto")).lower()
    fci_min_proxies_for_pca = int(monthly_fci.get("min_proxies_for_pca", 3))
    fci_global_proxies = monthly_fci.get("proxies_global")
    if not isinstance(fci_global_proxies, list):
        fci_global_proxies = list(FCI_GLOBAL_PROXIES)

    # compute FCI and attach if present (z-scored proxies are already scaled)
    if fci_method == "auto":
        present = [c for c in fci_global_proxies if c in scaled.columns]
        resolved_method = "pca" if len(present) >= fci_min_proxies_for_pca else "mean"
    else:
        resolved_method = fci_method
    fci = compute_fci(
        spec.iso_code,
        scaled,
        method=resolved_method,
        global_proxies=fci_global_proxies,
        min_proxies_for_pca=fci_min_proxies_for_pca,
    )
    if not fci.empty:
        scaled[f"FCI_{spec.iso_code}"] = fci
        logger.info(f"{spec.iso_code}: FCI constructed with {len(fci.dropna())} non-NA rows")

    # PCA per block where needed (store loadings + explained)
    pca_blocks = []
    pca_loadings_frames = []
    pca_explained_frames = []
    pca_score_frames: List[Tuple[str, pd.DataFrame]] = []
    for key, block_series in spec.block_series.items():
        block_columns = [col for col in block_series if col in scaled.columns]
        if len(block_columns) < 2:
            continue
        block_df = scaled[block_columns]
        if needs_pca(block_df, corr_cutoff=pca_corr_cutoff):
            pca_scores, loadings, explained = compute_block_pca(
                block_df,
                key,
                target_variance=pca_variance_target,
                max_components=pca_max_components,
            )
            if not pca_scores.empty:
                pca_blocks.append(key)
                # persist loadings and explained later
                pca_score_frames.append((key, pca_scores))
                pca_loadings_frames.append((key, loadings))
                pca_explained_frames.append((key, explained))

    # concat PCA scores to scaled features
    if pca_score_frames:
        scaled = pd.concat([scaled] + [frame for _, frame in pca_score_frames], axis=1)

    # build lags only for non-FCI columns BY DEFAULT (so FCI is contemporaneous)
    lag_columns = [col for col in scaled.columns if not col.startswith("FCI_")]
    lagged = create_lagged_features(scaled, lag_columns, LAG_PERIODS, min_obs_for_lags=MIN_OBS_LAGS)
    final = pd.concat([scaled, lagged], axis=1) if not lagged.empty else scaled.copy()
    final = final.dropna(how="all")
    final = final.loc[~(final.isna().all(axis=1))]

    # write PCA artifacts
    for key, loadings in pca_loadings_frames:
        loadings_path = OUTPUT_DIR / f"{spec.iso_code}_{key}_pca_loadings.csv"
        loadings.to_csv(loadings_path)
        logger.info(f"Wrote PCA loadings for {spec.iso_code}/{key} to {loadings_path}")
    for key, explained in pca_explained_frames:
        explained_path = OUTPUT_DIR / f"{spec.iso_code}_{key}_pca_explained.csv"
        explained.to_csv(explained_path, index=False)
        logger.info(f"Wrote PCA explained variance for {spec.iso_code}/{key} to {explained_path}")

    # diagnostics for summary
    summary = {
        "country": spec.country,
        "iso": spec.iso_code,
        "retained": len(retained),
        "dropped": len(dropped),
        "dropped_list": dropped,
        "pca_blocks": pca_blocks,
        "missing_series": missing,
        "rows": final.shape[0],
        "path": str(OUTPUT_DIR / f"{spec.iso_code}_factors.csv")
    }

    # save missing-series JSON for audit
    (OUTPUT_DIR / f"{spec.iso_code}_missing_series.json").write_text(json.dumps({"missing": missing, "desired": desired}, indent=2))

    if pca_score_frames:
        pca_components_df = pd.concat([frame for _, frame in pca_score_frames], axis=1)
        pca_components_df = pca_components_df.reindex(final.index)
        pca_components_path = OUTPUT_DIR / f"{spec.iso_code}{PCA_COMPONENT_FILE_SUFFIX}"
        pca_components_df.to_csv(pca_components_path, index=True, float_format="%.6g")
        logger.info(f"Wrote PCA components for {spec.iso_code} to {pca_components_path}")
        summary["pca_components_path"] = str(pca_components_path)
        # write component metadata (channels, blocks)
        metadata = []
        for block_key, frame in pca_score_frames:
            channel = IMF_CHANNEL_MAP.get(block_key, "other")
            for col in frame.columns:
                metadata.append({"component": col, "block": block_key, "channel": channel})
        metadata_path = OUTPUT_DIR / f"{spec.iso_code}{PCA_COMPONENT_METADATA_SUFFIX}"
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        logger.info(f"Wrote PCA component metadata for {spec.iso_code} to {metadata_path}")
    else:
        summary["pca_components_path"] = None

    return final, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare monthly country factors (Step 3).")
    parser.add_argument("--iso", type=str, default=None, help="Optional ISO filter (e.g., --iso USA)")
    parser.add_argument(
        "--literature",
        action="store_true",
        help="Generate literature-mode block factors and factor-based block definitions under analysis_outputs/literature_factors/.",
    )
    parser.add_argument(
        "--literature-mode",
        type=str,
        default="within_block",
        choices=["within_block", "across_blocks"],
        help="How Step 7 should interpret the literature factors: DCC within each block's factors, or one block across all block factors per ISO.",
    )
    parser.add_argument(
        "--literature-freq",
        type=str,
        default="M",
        help="Resampling rule for literature factors (e.g., M, W-FRI). Default: M.",
    )
    parser.add_argument(
        "--literature-expand-to-daily",
        action="store_true",
        help="Forward-fill literature factors to daily frequency (step-hold) so Step 7 can produce daily Sigma_t.",
    )
    parser.add_argument(
        "--literature-dedupe-corr",
        type=float,
        default=0.995,
        help="Absolute correlation threshold for within-block deduplication (default: 0.995).",
    )
    parser.add_argument(
        "--literature-max-factors-per-block",
        type=int,
        default=2,
        help="Max number of factors per block in literature mode (default: 2).",
    )
    parser.add_argument(
        "--factor-settings",
        type=str,
        default=str(FACTOR_SETTINGS_PATH),
        help="Path to factor_settings.yaml (defaults to config/factor_settings.yaml)",
    )
    args = parser.parse_args()

    ensure_output_dir(OUTPUT_DIR)
    if args.literature:
        ensure_output_dir(LITERATURE_OUTPUT_DIR)
        # Clear global PCA audit files so re-runs don't append duplicates.
        for name in ["literature_pca_loadings.csv", "literature_pca_explained.csv"]:
            path = LITERATURE_OUTPUT_DIR / name
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass
    factor_settings_path = Path(args.factor_settings)
    settings = _load_factor_settings(factor_settings_path)
    panel = load_combined_panel()
    specs = load_country_specs(COUNTRY_BLOCK_YAML)

    if args.iso:
        iso_filter = args.iso.strip().upper()
        specs = [s for s in specs if s.iso_code.upper() == iso_filter]

    summaries = []
    literature_summaries = []
    literature_panels = []
    literature_block_defs: Dict[str, dict] = {}

    for spec in specs:
        logger.info(f"Preparing factors for {spec.iso_code} ({spec.country})...")
        df_factors, summary = prepare_factors_for_country(spec, panel, settings)
        if df_factors.empty:
            logger.warning(f"{spec.iso_code}: no factor file written (no retained series).")
            summaries.append(summary)
        else:
            output_path = Path(summary["path"])
            df_factors.to_csv(output_path, index=True, float_format="%.6g")
            logger.info(f"Saved {df_factors.shape[1]} columns × {df_factors.shape[0]} rows to {output_path}")
            summaries.append(summary)

        if args.literature:
            lit_df, lit_summary = prepare_literature_factors_for_country(
                spec,
                panel,
                settings,
                resample_rule=str(args.literature_freq),
                expand_to_daily=bool(args.literature_expand_to_daily),
                dedupe_corr_threshold=float(args.literature_dedupe_corr),
                max_factors_per_block=int(args.literature_max_factors_per_block),
            )
            if lit_df.empty:
                literature_summaries.append(lit_summary)
            else:
                freq_label = str(args.literature_freq).replace("/", "-")
                suffix = "daily" if args.literature_expand_to_daily else "resampled"
                out_path = LITERATURE_OUTPUT_DIR / f"{spec.iso_code}_block_factors_{freq_label}_{suffix}.csv"
                lit_df.to_csv(out_path, index=True, index_label="date", float_format="%.6g")
                lit_summary["path"] = str(out_path)
                literature_summaries.append(lit_summary)
                literature_panels.append(lit_df)

                # Build a factor-based block definition entry compatible with Step 7.
                factors_by_block = lit_summary.get("factors_by_block", {}) or {}
                blocks = []
                if args.literature_mode == "within_block":
                    for block_key, cols in factors_by_block.items():
                        if not cols:
                            continue
                        blocks.append({"key": block_key, "series_codes": cols})
                else:
                    all_cols = [c for cols in factors_by_block.values() for c in cols]
                    if all_cols:
                        blocks.append({"key": "country_factors", "series_codes": all_cols})

                literature_block_defs[spec.iso_code] = {
                    "country": spec.country,
                    "region": None,
                    "coverage_window": None,
                    "blocks": blocks,
                }

    summary_path = OUTPUT_DIR / "factor_preparation_summary.md"
    write_summary(summaries, summary_path)
    logger.info(f"Summary written to {summary_path}")

    if args.literature:
        # Write block definitions + combined panel for Step 7 consumption.
        mode = str(args.literature_mode)
        defs_path = LITERATURE_OUTPUT_DIR / f"country_block_definition.{mode}.json"
        # If this run is scoped to a single ISO, merge into the existing global
        # definition file so downstream Step 7 sees all ISOs (prevents accidental
        # overwrite when doing targeted reruns).
        if args.iso and defs_path.exists():
            try:
                existing = json.loads(defs_path.read_text(encoding="utf-8"))
                if isinstance(existing, dict):
                    existing.update(literature_block_defs)
                    literature_block_defs = existing
            except Exception:
                pass
        defs_path.write_text(json.dumps(literature_block_defs, indent=2), encoding="utf-8")
        logger.info(f"Literature-mode block definition written to {defs_path}")

        if literature_panels:
            combined = pd.concat(literature_panels, axis=1)
            combined = combined.loc[:, ~combined.columns.duplicated()]
            panel_path = LITERATURE_OUTPUT_DIR / f"block_factors.{mode}.csv"
            # Same rule as above: scoped ISO reruns should update the global
            # combined panel instead of overwriting it with a single-ISO subset.
            if args.iso and panel_path.exists():
                try:
                    existing_panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)
                    existing_panel.index.name = existing_panel.index.name or "date"
                    combined = existing_panel.join(combined, how="outer")
                    combined = combined.loc[:, ~combined.columns.duplicated()]
                    combined = combined.sort_index()
                except Exception:
                    pass
            combined.to_csv(panel_path, index=True, index_label="date", float_format="%.6g")
            logger.info(f"Literature-mode combined factor panel written to {panel_path}")



if __name__ == "__main__":
    main()
