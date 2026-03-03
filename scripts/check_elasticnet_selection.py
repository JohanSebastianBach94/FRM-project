#!/usr/bin/env python3
"""Check ElasticNet coefficients for flagged factors."""

import pandas as pd
from pathlib import Path

COEFF_DIR = Path("analysis_outputs") / "feature_contributions_daily"
ISOS = ["DEU", "ESP", "FRA", "ITA", "USA"]

FLAGGED = [
    "PALUMUSDM", "PCOPPUSDM", "PIORECRUSDM", "PMAIZMTUSDM", "PSOYBUSDQ", "PWHEAMTUSDM",
    "TTF_GAS", "BTP_Bund_Spread", "Bonos_Bund_Spread", "OAT_Bund_Spread",
    "BIS_LBS_Household_Loans", "GC.DOD.TOTL.GD.ZS"
]

print("=== ElasticNet Coefficients for Flagged Factors ===\n")

for iso in ISOS:
    coeff_path = COEFF_DIR / f"{iso}_Rt_daily_coeffs_daily.csv"
    if not coeff_path.exists():
        print(f"{iso}: coefficients file not found")
        continue
    
    df = pd.read_csv(coeff_path, index_col=0)
    total = len(df)
    nonzero = (df['coefficient'].abs() > 1e-10).sum()
    
    print(f"\n{iso}: {nonzero}/{total} nonzero coefficients")
    
    for factor in FLAGGED:
        # Find all lags of this factor
        if factor.startswith("BIS_LBS_Household_Loans") or factor.startswith("GC.DOD"):
            # Country-specific
            matches = [idx for idx in df.index if f"{factor}_{iso}" in idx]
        else:
            matches = [idx for idx in df.index if factor in idx]
        
        if matches:
            max_coef = df.loc[matches, 'coefficient'].abs().max()
            any_nonzero = (df.loc[matches, 'coefficient'].abs() > 1e-10).any()
            status = "✓ SELECTED" if any_nonzero else "✗ ALL ZERO"
            print(f"  {factor:30s}: {len(matches):2d} lags, max|coef|={max_coef:8.6f} {status}")
        else:
            print(f"  {factor:30s}: NOT FOUND")
