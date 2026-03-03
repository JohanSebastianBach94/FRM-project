#!/usr/bin/env python3
"""Understand the join behavior in Step 4."""

import pandas as pd
from pathlib import Path

FACTOR_DIR = Path("analysis_outputs") / "factor_preparation_daily"
RT_DIR = Path("analysis_outputs") / "diag_corr_daily"
iso = "ESP"

factors_path = FACTOR_DIR / f"{iso}_factors_daily.csv"
rt_path = RT_DIR / f"{iso}_Rt_daily.csv"

print(f"=== Analyzing Step 4 Join for {iso} ===\n")

X_df = pd.read_csv(factors_path, index_col=0, parse_dates=True).sort_index()
y_df = pd.read_csv(rt_path, index_col=0, parse_dates=True).sort_index()

print(f"Factor file:")
print(f"  Rows: {len(X_df)}")
print(f"  Date range: {X_df.index[0].date()} to {X_df.index[-1].date()}")
print(f"  Columns: {len(X_df.columns)}")

# Check PALUMUSDM
palumusdm_cols = [c for c in X_df.columns if 'PALUMUSDM' in c]
if palumusdm_cols:
    col = palumusdm_cols[0]
    print(f"\n  {col}:")
    print(f"    Coverage: {X_df[col].notna().mean():.1%}")
    print(f"    Non-null: {X_df[col].notna().sum()} rows")

print(f"\nRt file:")
print(f"  Rows: {len(y_df)}")
print(f"  Date range: {y_df.index[0].date()} to {y_df.index[-1].date()}")
print(f"  Columns: {y_df.columns.tolist()}")

y_series = y_df.iloc[:, 0]

print(f"\n=== JOIN BEHAVIOR ===")
print(f"\nStep 1: Inner join (align dates)")
joined = X_df.join(y_series, how="inner")
print(f"  Result: {len(joined)} rows (from {len(X_df)} factors + {len(y_df)} Rt)")

if palumusdm_cols:
    print(f"  {col} after join: {joined[col].notna().sum()} non-null")

print(f"\nStep 2: dropna(how='any') - removes rows with ANY missing value")
panel = joined.dropna(how="any")
print(f"  Result: {len(panel)} rows")

if palumusdm_cols and col in panel.columns:
    print(f"  {col} after dropna: {panel[col].notna().sum()} non-null")

print(f"\nColumns that survive dropna: {len(panel.columns)} columns")
print(f"Missing from factor file: {set(X_df.columns) - set(panel.columns)}")

# Check which columns have NaN after join
print(f"\n=== Columns with NaN after join (before dropna) ===")
na_counts = joined.isna().sum().sort_values(ascending=False)
na_cols = na_counts[na_counts > 0]
if len(na_cols) > 0:
    print(f"Top 20 columns with missing values:")
    for col, count in na_cols.head(20).items():
        pct = count / len(joined) * 100
        print(f"  {col:50s}: {count:5d} missing ({pct:5.1f}%)")
else:
    print("  No columns with NaN!")
