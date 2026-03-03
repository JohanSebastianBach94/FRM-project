"""Country-block regression diagnostics with modernized econometric workflow."""
from __future__ import annotations

import argparse
import logging
from importlib import import_module
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple

import numpy as np
import pandas as pd
import yaml
from sklearn.decomposition import PCA
from sklearn.linear_model import LassoCV
from sklearn.metrics import r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def load_country_blocks(config_path: Path) -> List[Dict[str, List[str]]]:
    with config_path.open("r", encoding="utf-8") as source:
        contents = yaml.safe_load(source)
    return contents.get("blocks", [])


def gather_indicator_metadata() -> Dict[str, Dict[str, object]]:
    mod = import_module("config.stress_indicators_config")
    metadata: Dict[str, Dict[str, object]] = {}
    for name, value in vars(mod).items():
        if name.isupper() and isinstance(value, dict):
            metadata.update(value)
    return metadata


def select_core_codes_for_block(
    block: Dict[str, object], metadata: Dict[str, Dict[str, object]]
) -> Set[str]:
    iso = block.get("iso_code")
    region = block.get("region")
    allowed_regions = {region} if region else set()
    for extra in block.get("regions", []):
        if extra:
            allowed_regions.add(extra)
    categories = {cat for cat in block.get("categories", []) if cat}
    matched: Set[str] = set()
    for code, details in metadata.items():
        if iso and details.get("country") == iso:
            matched.add(code)
            continue
        if allowed_regions and details.get("region") in allowed_regions:
            matched.add(code)
            continue
        if categories and details.get("category") in categories:
            matched.add(code)
    return matched


def discover_dynamic_columns(
    block: Dict[str, object], data_columns: Iterable[str], known_codes: Set[str], target_codes: Set[str]
) -> List[str]:
    keywords = [kw.lower() for kw in block.get("keywords", []) if isinstance(kw, str)]
    country = block.get("country")
    if isinstance(country, str):
        keywords.append(country.lower())
    iso_code = block.get("iso_code")
    if isinstance(iso_code, str):
        keywords.append(iso_code.lower())
    keywords = [w for idx, w in enumerate(dict.fromkeys(keywords)) if w]

    dynamic: List[str] = []
    for col in data_columns:
        if col in known_codes or col in target_codes:
            continue
        lower = col.lower()
        if any(keyword in lower for keyword in keywords):
            dynamic.append(col)
    return dynamic


def prepare_data(data_file: Path, date_column: str, code_column: str, value_column: str) -> pd.DataFrame:
    df = pd.read_csv(data_file, parse_dates=[date_column])
    pivot = df.pivot_table(index=date_column, columns=code_column, values=value_column)
    pivot.sort_index(inplace=True)
    logging.info("Prepared data matrix with %d dates and %d series.", pivot.shape[0], pivot.shape[1])
    return pivot


def transform_series(series: pd.Series, periods: int) -> pd.Series:
    transformed = series.pct_change(periods=periods, fill_method=None)
    return transformed.replace([np.inf, -np.inf], np.nan)


def build_feature_columns(
    block: Dict[str, object],
    metadata: Dict[str, Dict[str, object]],
    data_columns: Sequence[str],
    exclude_codes: Set[str],
) -> List[str]:
    relevant_core = select_core_codes_for_block(block, metadata)
    available_core = [code for code in relevant_core if code in data_columns and code not in exclude_codes]
    extra_columns = discover_dynamic_columns(block, data_columns, set(metadata), exclude_codes)
    extra_columns = [code for code in extra_columns if code not in exclude_codes]
    manual_series = [
        code
        for code in block.get("extra_series", []) or []
        if code in data_columns and code not in exclude_codes
    ]
    missing_manual = [
        code for code in block.get("extra_series", []) or [] if code not in manual_series and code not in exclude_codes
    ]
    for series in missing_manual:
        logging.warning(
            "Extra series %s requested for %s block but missing from dataset.",
            series,
            block.get("country", "unknown"),
        )
    return list(dict.fromkeys(available_core + extra_columns + manual_series))


def chronological_split(X: pd.DataFrame, y: pd.Series, train_ratio: float) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    split_idx = max(int(len(y) * train_ratio), 1)
    split_idx = min(split_idx, len(y) - 1)
    X_train = X.iloc[:split_idx]
    y_train = y.iloc[:split_idx]
    X_test = X.iloc[split_idx:]
    y_test = y.iloc[split_idx:]
    return X_train, X_test, y_train, y_test


def extract_feature_contributions(
    model: Pipeline,
    feature_names: Sequence[str],
) -> List[Tuple[str, float]]:
    scaler: StandardScaler = model.named_steps["scaler"]
    scale = getattr(scaler, "scale_", np.ones(len(feature_names)))
    reg: LassoCV = model.named_steps["regressor"]
    if "pca" in model.named_steps:
        pca: PCA = model.named_steps["pca"]
        coef_standardized = pca.components_.T @ reg.coef_
    else:
        coef_standardized = reg.coef_
    safe_scale = np.where(scale == 0, 1.0, scale)
    coef_original = coef_standardized / safe_scale
    contributions = sorted(
        ((name, float(coef)) for name, coef in zip(feature_names, coef_original)),
        key=lambda item: abs(item[1]),
        reverse=True,
    )
    return contributions[:10]


def fit_penalized_regression(
    X: pd.DataFrame,
    y: pd.Series,
    train_ratio: float,
    pca_components: int,
    cv_folds: int,
) -> Dict[str, object]:
    X_train, X_test, y_train, y_test = chronological_split(X, y, train_ratio)
    steps: List[Tuple[str, object]] = [("scaler", StandardScaler())]
    max_components = min(pca_components, X_train.shape[1], len(X_train) - 1)
    if pca_components and max_components >= 1:
        steps.append(("pca", PCA(n_components=max_components, random_state=42)))
    steps.append(("regressor", LassoCV(cv=cv_folds, random_state=42, max_iter=5000)))
    model = Pipeline(steps)
    model.fit(X_train, y_train)
    train_pred = model.predict(X_train)
    train_r2 = float(r2_score(y_train, train_pred)) if len(y_train) > 1 else float("nan")
    if len(y_test) > 1:
        test_pred = model.predict(X_test)
        test_r2 = float(r2_score(y_test, test_pred))
    else:
        test_r2 = float("nan")
    contributions = extract_feature_contributions(model, X.columns)
    return {
        "model": model,
        "train_r2": train_r2,
        "test_r2": test_r2,
        "train_nobs": int(len(y_train)),
        "test_nobs": int(len(y_test)),
        "contributions": contributions,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Validate CCAR/ECB indicators with differencing, PCA factor reduction, and penalized regressions "
            "using chronological train/test splits."
        )
    )
    parser.add_argument("--data-file", type=Path, required=True, help="Input CSV with columns date, series_code, value")
    parser.add_argument("--output", type=Path, default=Path("results/country_block_regression_diagnostics.csv"))
    parser.add_argument("--pct-change-periods", type=int, default=1, help="Periods for percent-change differencing")
    parser.add_argument("--train-ratio", type=float, default=0.7, help="Chronological train share (0-1)")
    parser.add_argument("--pca-components", type=int, default=5, help="Maximum PCA components for factor reduction")
    parser.add_argument("--cv-folds", type=int, default=5, help="Cross-validation folds for LassoCV")
    parser.add_argument(
        "--min-features",
        type=int,
        default=5,
        help="Minimum number of usable regressors required to run the model",
    )
    args = parser.parse_args()

    data_matrix = prepare_data(args.data_file, "date", "series_code", "value")
    indicator_metadata = gather_indicator_metadata()
    logging.info("Core factor universe contains %d series.", len(indicator_metadata))
    blocks = load_country_blocks(Path("config/country_blocks_extended.yaml"))

    diagnostics: List[Dict[str, object]] = []

    for block in blocks:
        country = block.get("country", "unknown")
        target_codes = set(block.get("series_codes", []))
        for series in target_codes:
            if series not in data_matrix.columns:
                logging.warning("Series %s missing from dataset; skipping.", series)
                continue
            feature_columns = build_feature_columns(block, indicator_metadata, data_matrix.columns, {series})
            if len(feature_columns) < args.min_features:
                logging.warning(
                    "Block %s lacks sufficient regressors (%d found); skipping %s.",
                    country,
                    len(feature_columns),
                    series,
                )
                continue
            feature_matrix = data_matrix[feature_columns].copy()
            target_series = data_matrix[series].copy()
            X_transformed = feature_matrix.apply(transform_series, periods=args.pct_change_periods)
            y_transformed = transform_series(target_series, periods=args.pct_change_periods)
            combined = X_transformed.join(y_transformed.rename("target"), how="inner").dropna()
            if combined.empty or combined.shape[0] <= len(feature_columns):
                logging.warning("Not enough aligned observations for %s (%s); skipping.", series, country)
                continue
            X_final = combined[feature_columns].dropna(axis=1, how="all")
            if X_final.shape[1] < args.min_features:
                logging.warning(
                    "Effective features below threshold for %s (%s); skipping.", series, country
                )
                continue
            y_final = combined["target"].loc[X_final.index]
            try:
                model_stats = fit_penalized_regression(
                    X_final,
                    y_final,
                    train_ratio=args.train_ratio,
                    pca_components=args.pca_components,
                    cv_folds=args.cv_folds,
                )
            except ValueError as exc:
                logging.warning("Regression for %s (%s) failed: %s", series, country, exc)
                continue
            diagnostics.append(
                {
                    "country": country,
                    "series_code": series,
                    "train_r_squared": model_stats["train_r2"],
                    "test_r_squared": model_stats["test_r2"],
                    "train_nobs": model_stats["train_nobs"],
                    "test_nobs": model_stats["test_nobs"],
                    "top_contributions": ";".join(
                        f"{name}:{coef:.4f}" for name, coef in model_stats["contributions"]
                    ),
                }
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(diagnostics).to_csv(args.output, index=False)
    logging.info("Regression diagnostics saved to %s", args.output)


if __name__ == "__main__":
    main()
