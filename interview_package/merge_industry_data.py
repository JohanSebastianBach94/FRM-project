"""
Merge Industry Data with Existing Pipeline
Combines stress_indicators.csv (72 series) + industry_data_raw.csv (52 series)
Output: stress_indicators_expanded.csv (124 series)
"""

import pandas as pd
import numpy as np
from datetime import datetime

print("=" * 80)
print("MERGING INDUSTRY DATA WITH EXISTING PIPELINE")
print("=" * 80)

# Load existing stress indicators
print("\n[1/3] Loading existing stress_indicators.csv...")
try:
    df_existing = pd.read_csv("data/stress_indicators.csv", index_col=0, parse_dates=True)
    print(f"  Loaded: {df_existing.shape[0]} dates x {df_existing.shape[1]} series")
    print(f"  Date range: {df_existing.index.min()} to {df_existing.index.max()}")
except Exception as e:
    print(f"  [ERROR] {e}")
    print("  [FALLBACK] Trying alternative path...")
    df_existing = pd.read_csv("stress_indicators.csv", index_col=0, parse_dates=True)
    print(f"  Loaded: {df_existing.shape[0]} dates x {df_existing.shape[1]} series")

# Load new industry data
print("\n[2/3] Loading industry_data_raw.csv...")
df_industry = pd.read_csv("industry_data_raw.csv", index_col=0, parse_dates=True)
print(f"  Loaded: {df_industry.shape[0]} dates x {df_industry.shape[1]} series")
print(f"  Date range: {df_industry.index.min()} to {df_industry.index.max()}")

# Check for overlapping column names
overlapping = set(df_existing.columns) & set(df_industry.columns)
if overlapping:
    print(f"  [WARNING] {len(overlapping)} overlapping series names:")
    for col in list(overlapping)[:5]:
        print(f"    - {col}")
    print("  These will be taken from existing data only")
    # Drop overlapping from industry data
    df_industry = df_industry.drop(columns=list(overlapping))

# Merge datasets (outer join to preserve all dates)
print("\n[3/3] Merging datasets...")
df_merged = pd.concat([df_existing, df_industry], axis=1, join="outer")
df_merged = df_merged.sort_index()

print(f"  Result: {df_merged.shape[0]} dates x {df_merged.shape[1]} series")
print(f"  Date range: {df_merged.index.min()} to {df_merged.index.max()}")

# Check data quality
print("\n" + "=" * 80)
print("DATA QUALITY CHECK")
print("=" * 80)

missing_pct = (df_merged.isna().sum() / len(df_merged) * 100).sort_values(ascending=False)
print(f"\nSeries with >50% missing data:")
high_missing = missing_pct[missing_pct > 50]
if len(high_missing) > 0:
    for series, pct in high_missing.head(10).items():
        print(f"  {series:30} {pct:5.1f}% missing")
    print(f"  ... and {len(high_missing) - 10} more" if len(high_missing) > 10 else "")
else:
    print("  None! All series have <50% missing data")

# Series breakdown
print(f"\n{'=' * 80}")
print("SERIES BREAKDOWN")
print(f"{'=' * 80}")
print(f"Existing series:     {len(df_existing.columns)}")
print(f"New industry series: {len(df_industry.columns)}")
print(f"Total series:        {len(df_merged.columns)}")
print(f"Correlation pairs:   {len(df_merged.columns) * (len(df_merged.columns) - 1) // 2:,}")

# Save merged dataset
output_file = "stress_indicators_expanded.csv"
df_merged.to_csv(output_file, index_label='Date')
print(f"\n[SAVED] {output_file}")

# Also save to data/ directory if it exists
import os
if os.path.exists("data"):
    df_merged.to_csv("data/stress_indicators_expanded.csv", index_label='Date')
    print("[SAVED] data/stress_indicators_expanded.csv")

# Generate merge report
merge_report = {
    "merge_date": datetime.now().isoformat(),
    "input_files": {
        "existing": "stress_indicators.csv",
        "industry": "industry_data_raw.csv"
    },
    "output_file": output_file,
    "series_counts": {
        "existing": len(df_existing.columns),
        "industry_new": len(df_industry.columns),
        "total": len(df_merged.columns),
        "overlapping_removed": len(overlapping)
    },
    "date_range": {
        "start": str(df_merged.index.min()),
        "end": str(df_merged.index.max()),
        "n_dates": len(df_merged)
    },
    "correlation_pairs": len(df_merged.columns) * (len(df_merged.columns) - 1) // 2,
    "data_quality": {
        "series_with_high_missing": len(high_missing),
        "threshold": "50%"
    }
}

import json
with open("merge_report.json", "w") as f:
    json.dump(merge_report, f, indent=2)

print("[SAVED] merge_report.json")

print("\n" + "=" * 80)
print("MERGE COMPLETE!")
print("=" * 80)
print(f"Next step: Run DCC-GARCH on {len(df_merged.columns)} series")
print(f"Expected runtime: 10-15 minutes")
print(f"Expected output: {len(df_merged.columns) * (len(df_merged.columns) - 1) // 2:,} correlation pairs")
