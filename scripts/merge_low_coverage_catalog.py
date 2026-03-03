"""Produce low_coverage_with_catalog.csv by joining diagnostics to catalog metadata."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
LOW_COVERAGE_PATH = BASE_DIR / "analysis_outputs" / "low_coverage_prioritized_from_recompute.csv"
CATALOG_PATH = BASE_DIR / "catalog.csv"
OUTPUT_PATH = BASE_DIR / "analysis_outputs" / "low_coverage_with_catalog.csv"
METADATA_COLUMNS = ["series", "source", "provider", "fetch_method", "storage_path"]


def main() -> None:
    if not LOW_COVERAGE_PATH.exists():
        raise FileNotFoundError(f"Missing low-coverage diagnostics at {LOW_COVERAGE_PATH}")
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"Missing catalog at {CATALOG_PATH}")

    low_cov = pd.read_csv(LOW_COVERAGE_PATH)
    catalog = pd.read_csv(CATALOG_PATH, usecols=METADATA_COLUMNS)

    merged = low_cov.merge(catalog, on="series", how="left", indicator=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote {len(merged)} rows to {OUTPUT_PATH} (merge status counts: {merged._merge.value_counts().to_dict()})")


if __name__ == "__main__":
    main()
