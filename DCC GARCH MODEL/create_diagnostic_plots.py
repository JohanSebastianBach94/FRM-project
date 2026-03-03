"""
Diagnostic Plots Suite - DCC-GARCH Model Validation
====================================================

Comprehensive visualization of:
1. Beta series (Kalman smoother vs monthly interpolation)
2. Conditional volatilities and standardized residuals
3. Correlation heatmaps during critical dates
4. Rolling alpha+beta persistence

Purpose: Visual validation of model quality and crisis behavior
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

print("="*80)
print("DCC-GARCH DIAGNOSTIC PLOTS")
print("="*80)
print(f"Started: {datetime.now()}\n")

# Setup
plt.style.use('seaborn-v0_8-whitegrid')
COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#6A994E']
OUTPUT_DIR = Path(__file__).parent / "diagnostic_plots"
OUTPUT_DIR.mkdir(exist_ok=True)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RT_PARAMS_DIR = PROJECT_ROOT / "Output" / "nss_parameters"
RT_OVERLAY_FILE = RT_PARAMS_DIR / "Rt_strategy_overlay.pkl"
RT_DRIVER_FILE = RT_PARAMS_DIR / "Rt_driver_contrib.pkl"

def load_rt_pickle(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"⚠️ Rt diagnostics missing: {path}")
        return pd.DataFrame()
    df = pd.read_pickle(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    return df.sort_index()

# =============================================================================
# Load Data
# =============================================================================
print("Loading data...")

# Kalman daily betas
kalman_path = Path("../Output/trial data folder/nss_parameters/nss_parameters_fred_kalman_daily.pkl")
if kalman_path.exists():
    kalman_data = pd.read_pickle(kalman_path)
    print(f"✅ Kalman daily: {list(kalman_data.keys())}")
else:
    print("❌ Kalman data not found")
    sys.exit(1)

# Monthly betas (for comparison)
monthly_path = Path("../Output/trial data folder/nss_parameters/nss_parameters_fred_monthly.pkl")
if monthly_path.exists():
    monthly_data = pd.read_pickle(monthly_path)
    print(f"✅ Monthly data loaded")
    has_monthly = True
else:
    print("⚠️ Monthly data not found (comparison skipped)")
    has_monthly = False

# DCC-GARCH results
results_dir = Path("../DCC GARCH MODEL/results")
cond_vols = pd.read_csv(results_dir / "conditional_volatilities.csv", index_col=0, parse_dates=True)
garch_params = pd.read_csv(results_dir / "dcc_garch_parameters.csv", index_col=0)

print(f"✅ Conditional volatilities: {cond_vols.shape}")
print(f"✅ GARCH parameters: {garch_params.shape}")

rt_overlay_df = load_rt_pickle(RT_OVERLAY_FILE)
rt_driver_df = load_rt_pickle(RT_DRIVER_FILE)

# Try to load correlation matrices
try:
    import pickle
    with open(results_dir / "dcc_correlation_matrices.pkl", 'rb') as f:
        corr_matrices = pickle.load(f)
    print(f"✅ Correlation matrices: {len(corr_matrices)} time points")
    has_corr_matrices = True
except:
    print("⚠️ Correlation matrices not found (heatmaps skipped)")
    has_corr_matrices = False

# Crisis periods
CRISIS_PERIODS = {
    '2008 Lehman': ('2008-09-01', '2008-12-31', 'red'),
    '2011 Eurozone': ('2011-08-01', '2011-12-31', 'orange'),
    '2020 COVID': ('2020-02-20', '2020-05-31', 'purple')
}

CRITICAL_DATES = {
    'Lehman Bankruptcy': '2008-09-15',
    'Bear Stearns': '2008-03-16',
    'Eurozone Peak': '2011-11-25',
    'COVID Crash': '2020-03-16',
    'COVID Bottom': '2020-03-23'
}


# =============================================================================
# PLOT 1: Beta Series Comparison (Kalman vs Monthly)
# =============================================================================
print("\n" + "="*80)
print("PLOT 1: BETA SERIES - KALMAN vs MONTHLY")
print("="*80)

countries = ['USA', 'ITA', 'ESP']  # Focus on key countries
beta_names = ['beta0', 'beta1', 'beta2', 'beta3']

fig, axes = plt.subplots(len(countries), 4, figsize=(20, 12))
fig.suptitle('NSS Beta Dynamics: Kalman Daily vs Monthly\n(Crisis periods highlighted)', 
             fontsize=16, fontweight='bold')

for i, country in enumerate(countries):
    for j, beta_name in enumerate(beta_names):
        ax = axes[i, j]
        
        # Kalman daily
        kalman_beta = kalman_data[country][beta_name]
        ax.plot(kalman_beta.index, kalman_beta.values, 
                linewidth=1, color=COLORS[0], alpha=0.8, label='Kalman Daily')
        
        # Monthly (if available)
        if has_monthly and country in monthly_data:
            monthly_beta = monthly_data[country][beta_name]
            # Interpolate to daily for comparison
            monthly_beta_daily = monthly_beta.reindex(kalman_beta.index, method='ffill')
            ax.plot(monthly_beta_daily.index, monthly_beta_daily.values,
                    linewidth=1, color=COLORS[1], alpha=0.6, linestyle='--', label='Monthly (interp)')
        
        # Shade crisis periods
        for crisis_name, (start, end, color) in CRISIS_PERIODS.items():
            ax.axvspan(start, end, alpha=0.15, color=color)
        
        # Mark critical dates
        for event, date in CRITICAL_DATES.items():
            date_ts = pd.Timestamp(date)
            if date_ts in kalman_beta.index:
                ax.axvline(date_ts, color='red', linestyle=':', linewidth=1, alpha=0.5)
        
        ax.set_title(f'{country} - {beta_name}', fontsize=11, fontweight='bold')
        ax.grid(alpha=0.3)
        
        if i == 0 and j == 0:
            ax.legend(fontsize=8, loc='upper left')
        
        if i == len(countries) - 1:
            ax.set_xlabel('Date', fontsize=9)
        if j == 0:
            ax.set_ylabel('Beta Value', fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "1_beta_series_comparison.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: {OUTPUT_DIR / '1_beta_series_comparison.png'}")
plt.close()


# =============================================================================
# PLOT 2: Beta First Differences (Check Variation)
# =============================================================================
print("\n" + "="*80)
print("PLOT 2: BETA FIRST DIFFERENCES - VARIATION CHECK")
print("="*80)

fig, axes = plt.subplots(len(countries), 4, figsize=(20, 12))
fig.suptitle('NSS Beta First Differences: Δβ_t\n(Check for interpolation smoothness)', 
             fontsize=16, fontweight='bold')

for i, country in enumerate(countries):
    for j, beta_name in enumerate(beta_names):
        ax = axes[i, j]
        
        # Kalman daily differences
        kalman_beta = kalman_data[country][beta_name]
        kalman_diff = kalman_beta.diff()
        
        ax.plot(kalman_diff.index, kalman_diff.values, 
                linewidth=0.5, color=COLORS[0], alpha=0.6)
        ax.axhline(0, color='black', linewidth=0.8)
        
        # Shade crisis periods
        for crisis_name, (start, end, color) in CRISIS_PERIODS.items():
            ax.axvspan(start, end, alpha=0.15, color=color)
        
        # Statistics
        std_diff = kalman_diff.std()
        ax.text(0.02, 0.98, f'σ(Δβ) = {std_diff:.4f}', 
                transform=ax.transAxes, fontsize=8, 
                verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        ax.set_title(f'{country} - Δ{beta_name}', fontsize=11, fontweight='bold')
        ax.grid(alpha=0.3)
        
        if i == len(countries) - 1:
            ax.set_xlabel('Date', fontsize=9)
        if j == 0:
            ax.set_ylabel('First Difference', fontsize=9)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "2_beta_first_differences.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: {OUTPUT_DIR / '2_beta_first_differences.png'}")
plt.close()


# =============================================================================
# PLOT 3: Conditional Volatility (σ_t)
# =============================================================================
print("\n" + "="*80)
print("PLOT 3: CONDITIONAL VOLATILITY - σ_t")
print("="*80)

# Focus on representative betas
test_series = ['USA_beta0', 'USA_beta1', 'ITA_beta0', 'ESP_beta2']

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Conditional Volatility: σ_t from GARCH(1,1)\n(Should spike during crises)', 
             fontsize=16, fontweight='bold')

for idx, series in enumerate(test_series):
    ax = axes[idx // 2, idx % 2]
    
    if series in cond_vols.columns:
        vol = cond_vols[series]
        ax.plot(vol.index, vol.values, linewidth=1, color=COLORS[0], alpha=0.8)
        
        # Shade crisis periods
        for crisis_name, (start, end, color) in CRISIS_PERIODS.items():
            ax.axvspan(start, end, alpha=0.15, color=color)
        
        # Statistics
        mean_vol = vol.mean()
        max_vol = vol.max()
        max_date = vol.idxmax()
        
        ax.axhline(mean_vol, color='green', linestyle='--', linewidth=1, 
                   label=f'Mean: {mean_vol:.4f}')
        ax.axhline(mean_vol + 2*vol.std(), color='red', linestyle='--', linewidth=1,
                   label=f'Mean + 2σ: {mean_vol + 2*vol.std():.4f}')
        
        ax.set_title(f'{series}\nMax σ: {max_vol:.4f} on {max_date.strftime("%Y-%m-%d")}', 
                     fontsize=11, fontweight='bold')
        ax.set_xlabel('Date', fontsize=10)
        ax.set_ylabel('Conditional Volatility', fontsize=10)
        ax.legend(fontsize=8)
        ax.grid(alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "3_conditional_volatility.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: {OUTPUT_DIR / '3_conditional_volatility.png'}")
plt.close()


# =============================================================================
# PLOT 3B: Rt Strategy Overlay & Signal
# =============================================================================
if not rt_overlay_df.empty:
    print("\n" + "="*80)
    print("PLOT 3B: RT STRATEGY OVERLAY")
    print("="*80)
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(rt_overlay_df.index, rt_overlay_df["overlay"], label="Rt Strategy Overlay", color=COLORS[2], linewidth=2)
    ax.plot(rt_overlay_df.index, rt_overlay_df["benchmark"], label="Benchmark", color="#999999", linewidth=1, linestyle="--")
    ax.axhline(0, color="black", linestyle="--", linewidth=1, alpha=0.7)

    positive = rt_overlay_df["overlay"] > 0
    negative = rt_overlay_df["overlay"] < 0
    ax.fill_between(rt_overlay_df.index, 0, rt_overlay_df["overlay"], where=positive, interpolate=True, color="#6A994E", alpha=0.15, label="Risk-on")
    ax.fill_between(rt_overlay_df.index, 0, rt_overlay_df["overlay"], where=negative, interpolate=True, color="#C73E1D", alpha=0.15, label="Risk-off")

    for crisis_name, (start, end, color) in CRISIS_PERIODS.items():
        ax.axvspan(start, end, alpha=0.08, color=color)

    latest_overlay = rt_overlay_df["overlay"].iloc[-1]
    ax.text(0.99, 0.02, f"Latest overlay: {latest_overlay:+.2f}", transform=ax.transAxes, ha="right", fontsize=9, bbox=dict(facecolor="white", alpha=0.7, edgecolor="none"))

    ax.set_title("Rt Regime Overlay vs Benchmark", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date", fontsize=10)
    ax.set_ylabel("Overlay Signal", fontsize=10)
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(alpha=0.3)

    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "3b_rt_strategy_overlay.png", dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR / '3b_rt_strategy_overlay.png'}")
    plt.close()
else:
    print("⚠️ Rt overlay data missing; skipping Rt overlay plot")

if not rt_driver_df.empty:
    print("\n" + "="*80)
    print("PLOT 3C: RT DRIVER CONTRIBUTIONS")
    print("="*80)
    fig, ax = plt.subplots(figsize=(14, 6))
    rt_driver_df.plot.area(ax=ax, alpha=0.6, linewidth=0, stacked=False)
    ax.set_title("Rt Driver Contributions", fontsize=14, fontweight="bold")
    ax.set_xlabel("Date", fontsize=10)
    ax.set_ylabel("Contribution", fontsize=10)
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "3c_rt_driver_contributions.png", dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR / '3c_rt_driver_contributions.png'}")
    plt.close()
else:
    print("⚠️ Rt driver contributions missing; skipping driver plot")


# =============================================================================
# PLOT 4: Standardized Residuals (z_t) - Check Whiteness
# =============================================================================
print("\n" + "="*80)
print("PLOT 4: STANDARDIZED RESIDUALS - z_t")
print("="*80)

# Need to load original data to compute residuals
try:
    from data_pipeline import load_stress_indicators
    from data_pipeline.nss_betas import load_nss_betas
    
    stress_data = load_stress_indicators()
    nss_betas = load_nss_betas()
    combined_data = pd.concat([stress_data, nss_betas], axis=1)
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('Standardized Residuals: z_t = r_t / σ_t\n(Should be white noise)', 
                 fontsize=16, fontweight='bold')
    
    for idx, series in enumerate(test_series):
        ax = axes[idx // 2, idx % 2]
        
        if series in cond_vols.columns and series in combined_data.columns:
            returns = combined_data[series].dropna()
            vol = cond_vols[series]
            
            # Align dates
            common_dates = returns.index.intersection(vol.index)
            returns_aligned = returns.loc[common_dates]
            vol_aligned = vol.loc[common_dates]
            
            # Compute standardized residuals
            std_resid = returns_aligned / vol_aligned
            
            # Plot
            ax.plot(std_resid.index, std_resid.values, 
                    linewidth=0.5, color=COLORS[0], alpha=0.6)
            ax.axhline(0, color='black', linewidth=0.8)
            ax.axhline(2, color='red', linestyle='--', linewidth=1, alpha=0.5)
            ax.axhline(-2, color='red', linestyle='--', linewidth=1, alpha=0.5)
            
            # Shade crisis periods
            for crisis_name, (start, end, color) in CRISIS_PERIODS.items():
                ax.axvspan(start, end, alpha=0.15, color=color)
            
            # Statistics
            mean_z = std_resid.mean()
            std_z = std_resid.std()
            skew_z = std_resid.skew()
            kurt_z = std_resid.kurtosis()
            
            stats_text = f'μ={mean_z:.3f}, σ={std_z:.3f}\nSkew={skew_z:.3f}, Kurt={kurt_z:.3f}'
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
                    fontsize=8, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            ax.set_title(f'{series}', fontsize=11, fontweight='bold')
            ax.set_xlabel('Date', fontsize=10)
            ax.set_ylabel('Standardized Residual', fontsize=10)
            ax.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "4_standardized_residuals.png", dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR / '4_standardized_residuals.png'}")
    plt.close()
    
except Exception as e:
    print(f"⚠️ Could not compute standardized residuals: {e}")


# =============================================================================
# PLOT 5: Correlation Heatmaps at Critical Dates
# =============================================================================
print("\n" + "="*80)
print("PLOT 5: CORRELATION HEATMAPS - CRITICAL DATES")
print("="*80)

if has_corr_matrices:
    # Find correlation matrices at critical dates
    corr_dates = pd.DatetimeIndex([pd.Timestamp(d) for d in corr_matrices.keys()])
    
    critical_corrs = {}
    for event, date in CRITICAL_DATES.items():
        target_date = pd.Timestamp(date)
        # Find nearest available date
        nearest_idx = (corr_dates - target_date).abs().argmin()
        nearest_date = corr_dates[nearest_idx]
        critical_corrs[f"{event}\n{nearest_date.strftime('%Y-%m-%d')}"] = corr_matrices[nearest_date]
    
    # Plot heatmaps
    n_dates = len(critical_corrs)
    fig, axes = plt.subplots(2, 3, figsize=(20, 12))
    axes = axes.flatten()
    
    fig.suptitle('Dynamic Correlation Matrices at Critical Dates\n(DCC-GARCH R_t)', 
                 fontsize=16, fontweight='bold')
    
    for idx, (event, R_t) in enumerate(critical_corrs.items()):
        if idx >= 6:  # Max 6 plots
            break
        
        ax = axes[idx]
        
        # Focus on NSS betas subset for readability
        beta_cols = [col for col in R_t.columns if 'beta' in col][:20]
        R_subset = R_t.loc[beta_cols, beta_cols]
        
        # Plot
        sns.heatmap(R_subset, cmap='RdYlGn', center=0, vmin=-1, vmax=1,
                    square=True, ax=ax, cbar_kws={'label': 'Correlation'},
                    xticklabels=True, yticklabels=True)
        
        ax.set_title(event, fontsize=11, fontweight='bold')
        ax.set_xlabel('')
        ax.set_ylabel('')
        
        # Rotate labels
        ax.set_xticklabels(ax.get_xticklabels(), rotation=90, fontsize=7)
        ax.set_yticklabels(ax.get_yticklabels(), rotation=0, fontsize=7)
    
    # Hide unused subplots
    for idx in range(len(critical_corrs), 6):
        axes[idx].axis('off')
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "5_correlation_heatmaps.png", dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR / '5_correlation_heatmaps.png'}")
    plt.close()
else:
    print("⚠️ Skipping correlation heatmaps (matrices not loaded)")


# =============================================================================
# PLOT 6: Rolling Alpha + Beta (Persistence)
# =============================================================================
print("\n" + "="*80)
print("PLOT 6: ROLLING PERSISTENCE - α+β")
print("="*80)

# This requires re-estimating GARCH on rolling windows (computationally expensive)
# For now, show static alpha+beta from full-sample estimation

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('GARCH Persistence: α + β by Series Type\n(Static from full-sample estimation)', 
             fontsize=16, fontweight='bold')

# Categorize series
beta_series = garch_params[garch_params.index.str.contains('beta')]
credit_series = garch_params[garch_params.index.str.contains('BAML')]
vix_series = garch_params[garch_params.index.str.contains('VIX')]
equity_series = garch_params[garch_params.index.str.contains('SP500|DJIA')]

categories = [
    ('NSS Betas', beta_series),
    ('Credit Spreads', credit_series),
    ('VIX Volatility', vix_series),
    ('Equity Indices', equity_series)
]

for idx, (cat_name, cat_data) in enumerate(categories):
    if len(cat_data) == 0:
        continue
    
    ax = axes[idx // 2, idx % 2]
    
    alpha_beta = cat_data['alpha_beta_sum'].dropna()
    
    # Histogram
    ax.hist(alpha_beta, bins=20, color=COLORS[idx % len(COLORS)], 
            alpha=0.7, edgecolor='black')
    
    # Mark unit root
    ax.axvline(1.0, color='red', linestyle='--', linewidth=2, label='Unit Root')
    ax.axvline(alpha_beta.mean(), color='green', linestyle='--', linewidth=2,
               label=f'Mean: {alpha_beta.mean():.3f}')
    
    # Stats
    stationary = (alpha_beta < 0.99).sum()
    borderline = ((alpha_beta >= 0.99) & (alpha_beta <= 1.01)).sum()
    
    ax.set_title(f'{cat_name}\nStationary: {stationary}/{len(alpha_beta)}, Borderline: {borderline}/{len(alpha_beta)}',
                 fontsize=11, fontweight='bold')
    ax.set_xlabel('α + β', fontsize=10)
    ax.set_ylabel('Frequency', fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR / "6_rolling_persistence.png", dpi=300, bbox_inches='tight')
print(f"✅ Saved: {OUTPUT_DIR / '6_rolling_persistence.png'}")
plt.close()


# =============================================================================
# SUMMARY REPORT
# =============================================================================
print("\n" + "="*80)
print("DIAGNOSTIC PLOTS SUMMARY")
print("="*80)

print(f"\nPlots saved to: {OUTPUT_DIR}")
print("\nGenerated plots:")
print("  1. Beta series comparison (Kalman vs Monthly)")
print("  2. Beta first differences (variation check)")
print("  3. Conditional volatility (crisis spikes)")
print("  4. Standardized residuals (whiteness check)")
print("  5. Correlation heatmaps (critical dates)")
print("  6. Persistence distribution (α+β by category)")

print("\n" + "="*80)
print(f"Diagnostic analysis complete: {datetime.now()}")
print("="*80)
