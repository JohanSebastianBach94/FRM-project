"""
Full Data Pipeline Execution
Collects all 52 stress indicators + 20 NSS betas = 72 total series
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, "C:/Users/frank/Documents/FRM project")

from data_pipeline.stress_indicators import load_stress_indicators
from data_pipeline.nss_betas import load_nss_betas
import pandas as pd
from datetime import datetime

print("=" * 80)
print("FULL DATA PIPELINE EXECUTION")
print("=" * 80)
print(f"Start time: {datetime.now()}")

# Step 1: Load stress indicators
print("\n[STEP 1/3] Loading stress indicators...")
print("  - FRED series (economic data)")
print("  - Yahoo Finance (equity indices, VIX)")
print("  - Calculated spreads (BTP-Bund, OAT-Bund, etc.)")
print("  - Upsampling monthly/quarterly data to daily frequency")

try:
    stress_data = load_stress_indicators(
        upsample_to_daily=True,  # Convert all to daily
        fill_limit_days=92       # Forward-fill up to 1 quarter
    )
    
    if 'combined' in stress_data:
        df_stress = stress_data['combined']
        print(f"  [SUCCESS] {df_stress.shape[1]} stress indicators loaded")
        print(f"  Date range: {df_stress.index.min()} to {df_stress.index.max()}")
        print(f"  Total observations: {len(df_stress)}")
    else:
        print(f"  [WARNING] No combined dataset")
        print(f"  Available keys: {list(stress_data.keys())}")
        # Try to manually combine
        all_frames = []
        for key, df in stress_data.items():
            if isinstance(df, pd.DataFrame):
                all_frames.append(df)
                print(f"    - {key}: {df.shape[1]} series")
        if all_frames:
            df_stress = pd.concat(all_frames, axis=1, join='outer')
            print(f"  [COMBINED] {df_stress.shape[1]} total series")
        else:
            raise ValueError("No valid DataFrames in stress_data")
        
except Exception as e:
    print(f"  [ERROR] {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 2: Load NSS betas
print("\n[STEP 2/3] Loading NSS yield curve betas...")
print("  - ITA, ESP, FRA, DEU, USA")
print("  - 4 betas per country (beta0, beta1, beta2, beta3)")

try:
    nss_data = load_nss_betas()
    
    if isinstance(nss_data, pd.DataFrame):
        df_nss = nss_data
        print(f"  [SUCCESS] {df_nss.shape[1]} NSS beta series loaded")
        print(f"  Date range: {df_nss.index.min()} to {df_nss.index.max()}")
    else:
        print(f"  [WARNING] NSS data type: {type(nss_data)}")
        if isinstance(nss_data, dict):
            # Try to combine dictionary
            nss_frames = []
            for key, df in nss_data.items():
                if isinstance(df, pd.DataFrame):
                    nss_frames.append(df)
                    print(f"    - {key}: {df.shape}")
            if nss_frames:
                df_nss = pd.concat(nss_frames, axis=1, join='outer')
                print(f"  [COMBINED] {df_nss.shape[1]} NSS series")
            else:
                df_nss = pd.DataFrame()
        else:
            df_nss = pd.DataFrame()
        
except Exception as e:
    print(f"  [WARNING] Could not load NSS betas: {e}")
    import traceback
    traceback.print_exc()
    print("  Continuing without NSS betas...")
    df_nss = pd.DataFrame()

# Step 3: Combine all data
print("\n[STEP 3/3] Combining stress indicators + NSS betas...")

if not df_nss.empty:
    # Merge on date index
    df_combined = pd.concat([df_stress, df_nss], axis=1, join='outer')
    df_combined = df_combined.sort_index()
    print(f"  Combined dataset: {df_combined.shape[1]} series")
else:
    df_combined = df_stress
    print(f"  Using stress indicators only: {df_combined.shape[1]} series")

print(f"  Date range: {df_combined.index.min()} to {df_combined.index.max()}")
print(f"  Total dates: {len(df_combined)}")

# Save to CSV
output_path = "data/stress_indicators.csv"
df_combined.to_csv(output_path)
print(f"\n[SAVED] {output_path}")

# Data quality check
missing_pct = (df_combined.isna().sum() / len(df_combined) * 100).sort_values(ascending=False)
high_missing = missing_pct[missing_pct > 50]

print("\n" + "=" * 80)
print("DATA QUALITY REPORT")
print("=" * 80)
print(f"Total series: {len(df_combined.columns)}")
print(f"Total dates: {len(df_combined)}")
print(f"Date range: {df_combined.index.min()} to {df_combined.index.max()}")
print(f"\nSeries with >50% missing data: {len(high_missing)}")
if len(high_missing) > 0:
    print("\nHigh missing series (top 10):")
    for series, pct in high_missing.head(10).items():
        print(f"  {series:40} {pct:5.1f}%")

print("\n" + "=" * 80)
print("DATA PIPELINE COMPLETE!")
print("=" * 80)
print(f"End time: {datetime.now()}")
print(f"Output: {output_path}")
print(f"Series count: {len(df_combined.columns)}")
print(f"Next step: Merge with industry_data_raw.csv (51 series)")
print(f"Expected total: {len(df_combined.columns)} + 51 = {len(df_combined.columns) + 51} series")
