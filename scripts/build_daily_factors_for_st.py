"""Build daily factor panels for the FOR-ST pipeline.

This script reuses the existing full data pipeline output
(`data/stress_indicators.csv`) and extracts a focused subset of
series needed for the daily FOR-ST volatility + correlation engine.

For each ISO, it writes:
    analysis_outputs/daily_factors/{ISO}_daily_factors.csv

Assumptions
-----------
- You have already run `data_pipeline/run_full_pipeline.py` so that
  `data/stress_indicators.csv` exists and contains daily data for
  stress indicators and NSS betas.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd


DEFAULT_ISOS = ["ITA", "ESP", "FRA", "DEU", "USA"]

# Mapping from ISO to factor column names in data/stress_indicators.csv
ISO_FACTORS: Dict[str, List[str]] = {
    "ITA": [
        "NAEXKP01ITQ661S",
        "ITACPIALLMINMEI",
        "LRHUTTTTITM156S",
        "MORTGAGE30US",
        "ITA_beta0",
        "ITA_beta1",
        "ITA_beta2",
        "VIXCLS",
        "BAMLH0A0HYM2",
        "ITA_FCI",
    ],
    "ESP": [
        "NAEXKP01ESQ661S",
        "ESPCPIALLMINMEI",
        "LRHUTTTTESM156S",
        "MORTGAGE30US",
        "ESP_beta0",
        "ESP_beta1",
        "ESP_beta2",
        "VIXCLS",
        "BAMLH0A1HYBB",
        "ESP_FCI",
    ],
    "FRA": [
        "NAEXKP01FRQ661S",
        "FRACPIALLMINMEI",
        "LRHUTTTTFRM156S",
        "MORTGAGE30US",
        "FRA_beta0",
        "FRA_beta1",
        "FRA_beta2",
        "VIXCLS",
        "BAMLH0A1HYBB",
        "FRA_FCI",
    ],
    "DEU": [
        "NAEXKP01DEQ661S",
        "DEUCPIALLMINMEI",
        "LRHUTTTTDEM156S",
        "MORTGAGE30US",
        "DEU_beta0",
        "DEU_beta1",
        "DEU_beta2",
        "VIXCLS",
        "BAMLC0A1CAAAEY",
        "DEU_FCI",
    ],
    "USA": [
        "GDPC1",
        "CPIAUCSL",
        "UNRATE",
        "MORTGAGE30US",
        "USA_beta0",
        "USA_beta1",
        "USA_beta2",
        "VIXCLS",
        "BAMLH0A0HYM2",
        "USA_FCI",
    ],
}


def load_master_panel(base_dir: Path) -> pd.DataFrame:
    """Load the combined daily stress indicators + NSS betas panel."""

    path = base_dir / "data" / "stress_indicators.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Master panel not found at {path}.\n"
            "Please run data_pipeline/run_full_pipeline.py first."
        )

    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index.name = "date"
    df = df.sort_index()
    return df


def build_daily_factors_for_iso(iso: str, master: pd.DataFrame, base_dir: Path) -> pd.DataFrame:
    """Extract and save the daily factor panel for a single ISO."""

    factors = ISO_FACTORS.get(iso, [])
    available = [c for c in factors if c in master.columns]
    if not available:
        raise ValueError(f"No configured factors for ISO {iso} found in master panel")

    sub = master[available].copy()
    sub = sub.sort_index()

    out_dir = base_dir / "analysis_outputs" / "daily_factors"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{iso}_daily_factors.csv"

    sub.to_csv(out_path, index=True, index_label="date")
    return sub


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build daily factor panels for FOR-ST")
    parser.add_argument("--isos", nargs="*", default=DEFAULT_ISOS, help="ISO codes to process")
    parser.add_argument("--base-dir", type=str, default=".", help="Project root directory")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_dir = Path(args.base_dir).resolve()

    master = load_master_panel(base_dir)

    for iso in args.isos:
        _ = build_daily_factors_for_iso(iso, master, base_dir)


if __name__ == "__main__":  # pragma: no cover
    main()
