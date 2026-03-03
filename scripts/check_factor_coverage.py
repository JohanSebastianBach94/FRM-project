#!/usr/bin/env python3
"""Check coverage of flagged factors in daily factor files."""

import pandas as pd
from pathlib import Path

FACTOR_DIR = Path("analysis_outputs") / "factor_preparation_daily"
ISOS = ["DEU", "ESP", "FRA", "ITA", "USA"]

# Factors from risk_factor_holes_feed.csv
FLAGGED = [
    "PALUMUSDM", "PCOPPUSDM", "PIORECRUSDM", "PMAIZMTUSDM", "PSOYBUSDQ", "PWHEAMTUSDM",
    "TTF_GAS", "BTP_Bund_Spread", "Bonos_Bund_Spread", "OAT_Bund_Spread",
    "BIS_LBS_Household_Loans", "GC.DOD.TOTL.GD.ZS", "NPL_PROXY"
]

print("=== Factor Coverage in Daily Factor Files ===\n")

for iso in ISOS:
    path = FACTOR_DIR / f"{iso}_factors_daily.csv"
    if not path.exists():
        print(f"{iso}: file not found")
        continue
    
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    print(f"\n{iso} ({len(df)} rows):")
    
    for factor in FLAGGED:
        # Check base factor and lag0
        candidates = [c for c in df.columns if c == factor or c == f"{factor}_lag0" or c.startswith(f"{factor}_{iso}")]
        
        if not candidates:
            # Check country-specific variant
            country_factor = f"{factor}_{iso}"
            candidates = [c for c in df.columns if c == country_factor or c == f"{country_factor}_lag0"]
        
        if candidates:
            col = candidates[0]
            coverage = df[col].notna().mean()
            count = df[col].notna().sum()
            print(f"  {factor:30s} -> {col:40s}: {coverage:6.1%} ({count:5d} rows)")
        else:
            print(f"  {factor:30s} -> NOT IN FILE")

print("\n=== Checking Rt files ===\n")
RT_DIR = Path("analysis_outputs") / "diag_corr_daily"
for iso in ISOS:
    rt_path = RT_DIR / f"{iso}_Rt_daily.csv"
    if rt_path.exists():
        rt_df = pd.read_csv(rt_path, index_col=0, parse_dates=True)
        print(f"{iso}_Rt_daily: {len(rt_df)} rows, from {rt_df.index[0].date()} to {rt_df.index[-1].date()}")
