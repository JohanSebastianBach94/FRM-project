#!/usr/bin/env python3
"""Generate IMF-style stress scenarios using PCA component + Lasso stack."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List

import joblib
import pandas as pd
import yaml

FACTOR_DIR = Path("analysis_outputs") / "factor_preparation"
MODEL_DIR = Path("models")
OUTPUT_DIR = Path("outputs")
PCA_COMPONENT_FILE_SUFFIX = "_pca_components.csv"
PCA_COMPONENT_METADATA_SUFFIX = "_pca_component_metadata.json"
COUNTRY_BLOCKS_YAML = Path("config") / "country_blocks_extended.yaml"

SCENARIOS: Dict[str, Dict[str, float]] = {
    "baseline": {},
    "adverse": {
        "macroeconomic": -1.5,
        "market": 1.0,
        "funding": 1.5,
        "credit": 0.7,
        "other": 0.2,
    },
    "severe": {
        "macroeconomic": -2.5,
        "market": 1.5,
        "funding": 2.0,
        "credit": 1.2,
        "other": 0.5,
    },
}

IMF_CHANNEL_MAP = {
    "macro": "macroeconomic",
    "public_finance": "macroeconomic",
    "financial_markets": "market",
    "banking_system": "funding",
    "real_estate": "credit",
    "external_fx": "market",
}


def load_country_targets(path: Path) -> Dict[str, List[str]]:
    with path.open("r", encoding="utf-8") as fp:
        payload = yaml.safe_load(fp)
    mapping: Dict[str, List[str]] = {}
    for entry in payload.get("country_blocks", []):
        iso = entry.get("iso_code")
        if not iso:
            continue
        codes = []
        for block in entry.get("blocks", []):
            codes.extend(block.get("series_codes", []))
        mapping[iso] = sorted(set(codes))
    return mapping


def load_pca_components(iso: str) -> pd.DataFrame:
    path = FACTOR_DIR / f"{iso}{PCA_COMPONENT_FILE_SUFFIX}"
    if not path.exists():
        raise FileNotFoundError(f"PCA component file missing for {iso} at {path}")
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    return df


def load_pca_metadata(iso: str) -> Dict[str, Dict[str, str]]:
    path = FACTOR_DIR / f"{iso}{PCA_COMPONENT_METADATA_SUFFIX}"
    if not path.exists():
        raise FileNotFoundError(f"PCA metadata missing for {iso} at {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    return {entry["component"]: entry for entry in data}


def apply_scenario_shock(
    pca_df: pd.DataFrame,
    metadata: Dict[str, Dict[str, str]],
    scenario_shocks: Dict[str, float],
) -> pd.DataFrame:
    baseline = pca_df.iloc[-1:].copy()
    std = pca_df.std()
    for col in baseline.columns:
        channel = metadata.get(col, {}).get("channel", "other")
        shock = scenario_shocks.get(channel, 0.0)
        magnitude = std.get(col, 0.0)
        baseline[col] = baseline[col] + shock * magnitude
    return baseline


def score_models(iso: str, features: pd.DataFrame) -> Dict[str, float]:
    results: Dict[str, float] = {}
    for path in sorted(MODEL_DIR.glob(f"lasso_{iso}_*.joblib")):
        target = path.name[len(f"lasso_{iso}_") : -len(".joblib")]
        pipeline = joblib.load(path)
        predictions = pipeline.predict(features)
        if predictions.size == 0:
            continue
        results[target] = float(predictions[0])
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate IMF-style scenarios via PCA+Lasso stack")
    parser.add_argument("--countries", nargs="*", help="Optional ISO codes to evaluate")
    parser.add_argument("--scenario", choices=list(SCENARIOS), default="adverse", help="Scenario template to apply")
    parser.add_argument("--shock", action="append", help="Override channel shock (format channel:value)")
    args = parser.parse_args()

    targets_map = load_country_targets(COUNTRY_BLOCKS_YAML)
    selected = args.countries or list(targets_map.keys())
    scenario = args.scenario
    scenario_shocks = SCENARIOS[scenario].copy()
    if args.shock:
        for entry in args.shock:
            channel, val = entry.split(":", 1)
            scenario_shocks[channel] = float(val)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for iso in selected:
        if iso not in targets_map:
            continue
        try:
            pca_df = load_pca_components(iso)
            metadata = load_pca_metadata(iso)
        except FileNotFoundError as exc:
            print(exc)
            continue
        clean_pca = pca_df.dropna(how="any")
        if clean_pca.empty:
            print(f"No complete PCA row available for {iso}; skipping scenario generation")
            continue
        baseline_vec = clean_pca.iloc[-1:].copy()
        scenario_vec = apply_scenario_shock(clean_pca, metadata, scenario_shocks)
        baseline_scores = score_models(iso, baseline_vec)
        scenario_scores = score_models(iso, scenario_vec)
        rows = []
        for target in sorted(set(baseline_scores) | set(scenario_scores)):
            rows.append(
                {
                    "iso": iso,
                    "target": target,
                    "scenario": scenario,
                    "baseline": baseline_scores.get(target),
                    "scenario_value": scenario_scores.get(target),
                    "delta": (
                        None
                        if baseline_scores.get(target) is None or scenario_scores.get(target) is None
                        else scenario_scores[target] - baseline_scores[target]
                    ),
                }
            )
        out_path = OUTPUT_DIR / f"imf_scenarios_{iso}_{scenario}.csv"
        pd.DataFrame(rows).to_csv(out_path, index=False)
        print(f"Wrote IMF scenario {scenario} for {iso} to {out_path}")


if __name__ == "__main__":
    main()
