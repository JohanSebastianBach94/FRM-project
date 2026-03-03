"""
Quick diagnostics health check for GARCH/FIGARCH conditional vols and Ljung-Box residuals
"""
import pandas as pd
import numpy as np
from pathlib import Path

print("=" * 80)
print("VOLATILITY MODEL DIAGNOSTICS - HEALTH CHECK")
print("=" * 80)

isos = ['USA', 'ITA', 'ESP', 'FRA', 'DEU']

# 1. Ljung-Box p-value check (standardized residuals should be uncorrelated)
print("\n1. LJUNG-BOX P-VALUE DISTRIBUTION (residual autocorrelation)")
print("-" * 80)
for iso in isos:
    df = pd.read_csv(f"analysis_outputs/diagnostics/garch_diagnostics_{iso}.csv")
    total = len(df)
    p_lt_05 = sum(df["ljung_box_pvalue"] < 0.05)
    p_severe = sum(df["ljung_box_pvalue"] < 1e-10)
    pct_fail = 100 * p_lt_05 / total
    print(f"{iso:3s}: {p_lt_05:3d}/{total} ({pct_fail:5.1f}%) fail (p<0.05); {p_severe:3d} severe (p~0)")

# 2. Conditional volatility stability (check for NaN, Inf, extreme values)
print("\n2. CONDITIONAL VOLATILITY STABILITY")
print("-" * 80)
for iso in isos:
    vol_file = Path(f"analysis_outputs/diagnostics/garch_conditional_vols_{iso}.csv")
    if not vol_file.exists():
        print(f"{iso}: MISSING conditional vols CSV")
        continue
    
    vols = pd.read_csv(vol_file).iloc[:, 1:]  # drop date column
    
    # Check for NaN/Inf
    nan_count = vols.isna().sum().sum()
    inf_count = np.isinf(vols.select_dtypes(include=[np.number])).sum().sum()
    
    # Summary stats
    stats = vols.describe().loc[['mean', 'std', 'min', 'max']]
    mean_range = (stats.loc['mean'].min(), stats.loc['mean'].max())
    max_vol = stats.loc['max'].max()
    min_vol = stats.loc['min'].min()
    
    print(f"{iso:3s}: NaN={nan_count:4d}, Inf={inf_count:4d} | "
          f"Mean vol ∈ [{mean_range[0]:6.3f}, {mean_range[1]:6.3f}], "
          f"Max={max_vol:7.3f}, Min={min_vol:6.3f}")

# 3. Model choice breakdown
print("\n3. VOLATILITY MODEL SELECTION (GARCH vs FIGARCH vs HAR)")
print("-" * 80)
for iso in isos:
    df = pd.read_csv(f"analysis_outputs/diagnostics/garch_diagnostics_{iso}.csv")
    model_counts = df['volatility_model'].value_counts()
    total = len(df)
    breakdown = ', '.join([f"{m}={c} ({100*c/total:.1f}%)" for m, c in model_counts.items()])
    print(f"{iso:3s}: {breakdown}")

# 4. Persistence distribution (check for near-unit-root issues)
print("\n4. PERSISTENCE DISTRIBUTION (α + β)")
print("-" * 80)
for iso in isos:
    df = pd.read_csv(f"analysis_outputs/diagnostics/garch_diagnostics_{iso}.csv")
    # FIGARCH doesn't return persistence in same way; filter GARCH only
    garch_df = df[df['volatility_model'] == 'GARCH'].copy()
    if len(garch_df) == 0:
        print(f"{iso}: No GARCH models fitted")
        continue
    
    pers = garch_df['persistence']
    near_unit = sum((pers > 0.99) & (pers <= 1.0))
    integrated = sum(pers >= 1.0)
    
    print(f"{iso:3s}: mean={pers.mean():.4f}, "
          f">0.99: {near_unit}/{len(garch_df)}, "
          f"≥1.0: {integrated}/{len(garch_df)}")

print("\n" + "=" * 80)
print("SUMMARY RECOMMENDATIONS")
print("=" * 80)
print("✓ If Ljung-Box p<0.05 is widespread: residuals still autocorrelated → DCC may capture")
print("  spurious dynamics. Consider longer lags or AR-augmented GARCH.")
print("✓ If conditional vols contain NaN/Inf: numerical instability detected. Check FIGARCH")
print("  convergence and consider trimming extreme observations.")
print("✓ If persistence ≥1.0 is common: IGARCH behavior → Σₜ will inherit non-stationarity.")
print("  Confirm you want integrated vol for stress testing, or switch to stationary specs.")
print("=" * 80)
