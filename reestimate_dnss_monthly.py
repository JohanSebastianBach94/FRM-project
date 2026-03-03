"""
Re-estimate DNSS Parameters at MONTHLY Frequency
=================================================

Changes from quarterly to monthly estimation for better stress testing granularity.

Original: 101 quarterly observations (1990-2025)
New: ~420 monthly observations (1990-2025)

Runtime: ~2-4 hours for 5 countries
"""

import pandas as pd
import numpy as np
from pathlib import Path
from scipy.optimize import minimize
import pickle
from datetime import datetime
import sys

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))
from nss_models.core import nelson_siegel_svensson_institutional

print("="*80)
print("DNSS MONTHLY RE-ESTIMATION")
print("="*80)
print(f"Started: {datetime.now()}")
print("\nChanging frequency: Quarterly → Monthly")
print("Expected runtime: 2-4 hours (5 countries × ~420 months × 6 parameters)")
print("="*80)

# Configuration
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output" / "trial data folder"
EXPORT_DIR = OUTPUT_DIR / "nss_parameters"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

ZCB_STRIP_DIR = BASE_DIR / "data" / "ZCB STRIPS"
_ZCB_STRIP_FILES = {
    'ITA': 'ita_historical_strips_20251014_140448.csv',
    'FRA': 'fra_historical_strips_20251014_140448.csv',
    'DEU': 'deu_historical_strips_20251014_140448.csv',
    'ESP': 'esp_historical_strips_20251014_140448.csv',
    'USA': 'usa_historical_strips_20251014_140448.csv',
}
ZCB_STRIP_PATHS = {
    iso: ZCB_STRIP_DIR / filename
    for iso, filename in _ZCB_STRIP_FILES.items()
}

COUNTRIES = list(ZCB_STRIP_PATHS.keys())
INVESTING_COUNTRIES = ['ITA', 'FRA', 'USA']

# ✅ CHANGE: Quarterly → Monthly
SAMPLE_FREQ = 'ME'  # Month-End (was 'QE' for Quarter-End)

START_DATE = pd.Timestamp('1990-02-01')
END_DATE = pd.Timestamp('2025-10-22')

# NSS parameter bounds (institutional standards)
BOUNDS = [
    (-0.5, 0.5),   # beta0 (level)
    (-0.5, 0.5),   # beta1 (slope)
    (-0.5, 0.5),   # beta2 (curvature)
    (-0.5, 0.5),   # beta3 (second curvature)
    (0.2, 2.0),    # lambda1
    (0.1, 1.5),    # lambda2
]

print(f"\n📋 Configuration:")
print(f"  Countries: {COUNTRIES}")
print(f"  Frequency: {SAMPLE_FREQ} (Monthly)")
print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
print(f"  Output: {EXPORT_DIR}")


def nss_objective(params, maturities, yields):
    """NSS objective function for optimization"""
    beta0, beta1, beta2, beta3, lambda1, lambda2 = params
    
    # Predict yields
    predicted = nelson_siegel_svensson_institutional(
        maturities, beta0, beta1, beta2, beta3, lambda1, lambda2
    )
    
    # Sum of squared errors
    sse = np.sum((yields - predicted) ** 2)
    return sse


def fit_nss_for_date(yields_series, maturities, initial_guess=None):
    """
    Fit NSS parameters for a single date
    
    Parameters:
    -----------
    yields_series : pd.Series
        Yields for different maturities at one date
    maturities : array-like
        Maturity points in years
    initial_guess : array, optional
        Initial parameter guess
        
    Returns:
    --------
    dict with beta0, beta1, beta2, beta3, lambda1, lambda2, r_squared, rmse
    """
    # Remove NaN values
    valid_mask = ~np.isnan(yields_series)
    valid_yields = yields_series[valid_mask].values
    valid_maturities = np.array(maturities)[valid_mask]
    
    if len(valid_yields) < 4:  # Need at least 4 points
        return None
    
    # Initial guess
    if initial_guess is None:
        initial_guess = [
            valid_yields.mean(),  # beta0 ~ level
            valid_yields[0] - valid_yields[-1],  # beta1 ~ slope
            0.0,  # beta2
            0.0,  # beta3
            0.5,  # lambda1
            0.3,  # lambda2
        ]
    
    # Optimize
    result = minimize(
        nss_objective,
        initial_guess,
        args=(valid_maturities, valid_yields),
        method='L-BFGS-B',
        bounds=BOUNDS,
        options={'maxiter': 1000}
    )
    
    if not result.success:
        # Try differential evolution as fallback
        from scipy.optimize import differential_evolution
        result = differential_evolution(
            nss_objective,
            BOUNDS,
            args=(valid_maturities, valid_yields),
            maxiter=500,
            seed=42
        )
    
    # Extract parameters
    beta0, beta1, beta2, beta3, lambda1, lambda2 = result.x
    
    # Calculate fit statistics
    predicted = nelson_siegel_svensson_institutional(
        valid_maturities, beta0, beta1, beta2, beta3, lambda1, lambda2
    )
    
    ss_res = np.sum((valid_yields - predicted) ** 2)
    ss_tot = np.sum((valid_yields - valid_yields.mean()) ** 2)
    r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
    rmse = np.sqrt(ss_res / len(valid_yields))
    
    return {
        'beta0': beta0,
        'beta1': beta1,
        'beta2': beta2,
        'beta3': beta3,
        'lambda1': lambda1,
        'lambda2': lambda2,
        'r_squared': r_squared,
        'rmse': rmse
    }


def load_yield_data():
    """Load yield curve data from the approved ZCB strip files"""
    print("\n📥 Loading yield data...")

    data = {'fred': {}, 'investing': {}}

    print("  Allowed ZCB strip files:")
    for iso, path in ZCB_STRIP_PATHS.items():
        print(f"    {iso}: {path.name}")

    for country in COUNTRIES:
        strip_path = ZCB_STRIP_PATHS[country]
        if not strip_path.exists():
            print(f"  ⚠️ Missing ZCB file for {country}: {strip_path.name}")
            continue

        df = pd.read_csv(strip_path, parse_dates=['date'])
        df = df.set_index('date')
        df = df[(df.index >= START_DATE) & (df.index <= END_DATE)]
        country_df = df[df['country'] == country].copy()
        if country_df.empty:
            print(f"  ⚠️ No {country} records found in {strip_path.name}")
            continue

        data['fred'][country] = country_df
        print(f"    ✅ {country}: {len(country_df)} records ({strip_path.name})")

    # Load Investing.com data
    master_investing = BASE_DIR / "data" / "Investing bond" / "master_investing_bonds_20241014_140403.csv"

    if master_investing.exists():
        print(f"  Loading Investing.com: {master_investing.name}")
        df_inv = pd.read_csv(master_investing, parse_dates=['date'])
        df_inv = df_inv.set_index('date')
        df_inv = df_inv[(df_inv.index >= START_DATE) & (df_inv.index <= END_DATE)]

        for country in INVESTING_COUNTRIES:
            country_df = df_inv[df_inv['country'] == country].copy()
            if len(country_df) > 0:
                data['investing'][country] = country_df
                print(f"    ✅ {country}: {len(country_df)} records")
    else:
        print(f"  ⚠️ Investing data not found: {master_investing}")

    return data


def estimate_nss_parameters(data_dict, source_name):
    """
    Estimate NSS parameters at monthly frequency
    
    Parameters:
    -----------
    data_dict : dict
        Dictionary of country → DataFrame with yield data
    source_name : str
        'fred' or 'investing'
        
    Returns:
    --------
    dict : country → DataFrame with monthly NSS parameters
    """
    print(f"\n{'='*80}")
    print(f"ESTIMATING NSS PARAMETERS: {source_name.upper()}")
    print(f"{'='*80}")
    
    results = {}
    
    for country, df in data_dict.items():
        print(f"\n📊 Processing {country}...")
        
        # Get unique maturities
        maturities = sorted(df['maturity_years'].unique())
        print(f"  Maturities: {maturities}")
        
        # Pivot to get yields by maturity
        yield_pivot = df.pivot_table(
            index='date',
            columns='maturity_years',
            values='yield_percent',  # Column name in master file
            aggfunc='mean'
        )
        
        # Resample to monthly frequency (✅ KEY CHANGE)
        print(f"  Original observations: {len(yield_pivot)}")
        yield_monthly = yield_pivot.resample(SAMPLE_FREQ).last()
        print(f"  Monthly observations: {len(yield_monthly)}")
        
        # Estimate NSS for each month
        params_list = []
        successful = 0
        failed = 0
        
        last_params = None  # Use previous month as initial guess
        
        for idx, (date, row) in enumerate(yield_monthly.iterrows()):
            if idx % 50 == 0:
                print(f"  Progress: {idx}/{len(yield_monthly)} ({idx/len(yield_monthly)*100:.1f}%)")
            
            # Fit NSS
            params = fit_nss_for_date(row, maturities, initial_guess=last_params)
            
            if params is not None:
                params['date'] = date
                params_list.append(params)
                successful += 1
                
                # Update initial guess for next iteration
                last_params = [
                    params['beta0'], params['beta1'], params['beta2'],
                    params['beta3'], params['lambda1'], params['lambda2']
                ]
            else:
                failed += 1
        
        print(f"\n  ✅ Completed: {successful} successful, {failed} failed")
        print(f"  Success rate: {successful/(successful+failed)*100:.1f}%")
        
        # Convert to DataFrame
        params_df = pd.DataFrame(params_list)
        params_df = params_df.set_index('date')
        params_df = params_df.sort_index()
        
        results[country] = params_df
        
        # Show summary statistics
        print(f"\n  Summary statistics:")
        print(f"    beta0 (level): {params_df['beta0'].mean():.4f} ± {params_df['beta0'].std():.4f}")
        print(f"    beta1 (slope): {params_df['beta1'].mean():.4f} ± {params_df['beta1'].std():.4f}")
        print(f"    R²: {params_df['r_squared'].mean():.4f}")
        print(f"    RMSE: {params_df['rmse'].mean():.6f}")
    
    return results


def save_parameters(fred_params, investing_params):
    """Save parameters to pickle files (main location + backup)"""
    print(f"\n{'='*80}")
    print("SAVING PARAMETERS")
    print(f"{'='*80}")
    
    # Create backup directory
    backup_dir = BASE_DIR / "backups" / "dnss_monthly_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save FRED parameters
    if fred_params:
        # Main location
        fred_path = EXPORT_DIR / 'nss_parameters_fred_monthly.pkl'
        with open(fred_path, 'wb') as f:
            pickle.dump(fred_params, f)
        print(f"✅ Saved FRED parameters (main): {fred_path}")
        
        # Backup location
        fred_backup = backup_dir / f'nss_parameters_fred_monthly_{timestamp}.pkl'
        with open(fred_backup, 'wb') as f:
            pickle.dump(fred_params, f)
        print(f"✅ Saved FRED parameters (backup): {fred_backup}")
        
        print(f"   Countries: {list(fred_params.keys())}")
        print(f"   Total observations: {sum(len(df) for df in fred_params.values())}")
    
    # Save Investing parameters
    if investing_params:
        # Main location
        inv_path = EXPORT_DIR / 'nss_parameters_investing_monthly.pkl'
        with open(inv_path, 'wb') as f:
            pickle.dump(investing_params, f)
        print(f"✅ Saved Investing parameters (main): {inv_path}")
        
        # Backup location
        inv_backup = backup_dir / f'nss_parameters_investing_monthly_{timestamp}.pkl'
        with open(inv_backup, 'wb') as f:
            pickle.dump(investing_params, f)
        print(f"✅ Saved Investing parameters (backup): {inv_backup}")
        
        print(f"   Countries: {list(investing_params.keys())}")
        print(f"   Total observations: {sum(len(df) for df in investing_params.values())}")
    
    # Also save a comparison with quarterly
    print(f"\n📊 Quarterly vs Monthly Comparison:")
    
    try:
        quarterly_path = EXPORT_DIR / 'nss_parameters_fred.pkl'
        if quarterly_path.exists():
            with open(quarterly_path, 'rb') as f:
                quarterly_data = pickle.load(f)
            
            print(f"\n  Quarterly (old):")
            for country, df in quarterly_data.items():
                print(f"    {country}: {len(df)} observations")
            
            print(f"\n  Monthly (new):")
            for country, df in fred_params.items():
                if country in quarterly_data:
                    old_count = len(quarterly_data[country])
                    new_count = len(df)
                    improvement = (new_count / old_count - 1) * 100
                    print(f"    {country}: {new_count} observations (+{improvement:.1f}%)")
    except Exception as e:
        print(f"  ⚠️ Could not load quarterly data for comparison: {e}")


def main():
    """Main execution"""
    
    # Load data
    data = load_yield_data()
    
    if not data['fred'] and not data['investing']:
        print("\n❌ ERROR: No data loaded!")
        print("   Check that master data files exist in:")
        print(f"   - {BASE_DIR / 'data' / 'ZCB STRIPS'}")
        print(f"   - {BASE_DIR / 'data' / 'Investing bond'}")
        return
    
    # Estimate FRED parameters
    fred_params = {}
    if data['fred']:
        fred_params = estimate_nss_parameters(data['fred'], 'fred')
    
    # Estimate Investing parameters
    investing_params = {}
    if data['investing']:
        investing_params = estimate_nss_parameters(data['investing'], 'investing')
    
    # Save results
    save_parameters(fred_params, investing_params)
    
    print(f"\n{'='*80}")
    print("MONTHLY DNSS RE-ESTIMATION COMPLETE")
    print(f"{'='*80}")
    print(f"Finished: {datetime.now()}")
    print("\nNext steps:")
    print("1. Update data_pipeline/nss_betas.py to load monthly files")
    print("2. Re-run DCC-GARCH model")
    print("3. Compare results with quarterly baseline")


if __name__ == '__main__':
    main()
