"""
Kalman-Filtered Daily DNSS Parameter Estimation
================================================

State-space model for daily estimation of Nelson-Siegel-Svensson parameters:

Measurement equation: y_t(τ) = NSS(β_t, τ, λ) + v_t
State equation: β_t = β_{t-1} + η_t  (random walk)

Where:
  - y_t(τ) = observed yields at maturity τ
  - β_t = [β0_t, β1_t, β2_t, β3_t]' = time-varying NSS parameters
  - λ = [λ1, λ2]' = fixed shape parameters (Svensson defaults)
  - v_t ~ N(0, R) = measurement noise
  - η_t ~ N(0, Q) = process noise (controls smoothness)

Implementation: Kalman Filter + Rauch-Tung-Striebel (RTS) Smoother

Runtime: ~5-10 minutes for 5 countries × 13,000 daily observations

References:
  - Diebold & Li (2006): "Forecasting the Term Structure"
  - Kim & Wright (2005): "Arbitrage-Free Nelson-Siegel Model"
  - Christensen et al. (2011): "Dynamic Nelson-Siegel Model"
"""

import pandas as pd
import numpy as np
from pathlib import Path
from scipy.optimize import minimize
import pickle
from datetime import datetime
import sys
import warnings

# Kalman filter library
try:
    from filterpy.kalman import KalmanFilter
    from filterpy.common import Q_discrete_white_noise
    FILTERPY_AVAILABLE = True
except ImportError:
    FILTERPY_AVAILABLE = False
    print("⚠️ filterpy not available. Install with: pip install filterpy")

# Add project to path
sys.path.insert(0, str(Path(__file__).parent))
from nss_models.core import nelson_siegel_svensson_institutional

print("="*80)
print("KALMAN-FILTERED DAILY DNSS ESTIMATION")
print("="*80)
print(f"Started: {datetime.now()}")
print("\nMethod: State-Space Model with Kalman Filter + RTS Smoother")
print("Frequency: DAILY (no interpolation needed!)")
print("Expected runtime: 5-10 minutes (5 countries × ~13,000 days)")
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

START_DATE = pd.Timestamp('1990-02-01')
END_DATE = pd.Timestamp('2025-10-22')

# NSS shape parameters (FIXED for stability)
LAMBDA1_FIXED = 0.0609  # Svensson (1994) default
LAMBDA2_FIXED = 0.0609  # Svensson (1994) default

# Kalman filter hyperparameters
PROCESS_NOISE_STD = 0.01  # σ(η) - controls smoothness (tune this!)
MEASUREMENT_NOISE_STD = 0.05  # σ(v) - measurement error (tune this!)

print(f"\n📋 Configuration:")
print(f"  Countries: {COUNTRIES}")
print(f"  Frequency: DAILY")
print(f"  Date range: {START_DATE.date()} to {END_DATE.date()}")
print(f"  λ₁ (fixed): {LAMBDA1_FIXED}")
print(f"  λ₂ (fixed): {LAMBDA2_FIXED}")
print(f"  Process noise σ(η): {PROCESS_NOISE_STD}")
print(f"  Measurement noise σ(v): {MEASUREMENT_NOISE_STD}")
print(f"  Output: {EXPORT_DIR}")


class KalmanDNSS:
    """
    Kalman filter for daily DNSS parameter estimation
    """
    
    def __init__(self, maturities, lambda1=LAMBDA1_FIXED, lambda2=LAMBDA2_FIXED,
                 process_noise_std=PROCESS_NOISE_STD, measurement_noise_std=MEASUREMENT_NOISE_STD):
        """
        Initialize Kalman filter for DNSS
        
        Parameters:
        -----------
        maturities : array-like
            Yield maturities in years (e.g., [1, 2, 3, 5, 7, 10, 20, 30])
        lambda1, lambda2 : float
            Fixed NSS shape parameters
        process_noise_std : float
            Process noise standard deviation (controls smoothness)
        measurement_noise_std : float
            Measurement noise standard deviation
        """
        self.maturities = np.array(maturities)
        self.lambda1 = lambda1
        self.lambda2 = lambda2
        self.n_maturities = len(maturities)
        
        # State dimension: 4 betas (β0, β1, β2, β3)
        self.n_state = 4
        
        # Process noise covariance Q (4x4)
        self.Q = np.eye(self.n_state) * (process_noise_std ** 2)
        
        # Measurement noise covariance R (n_maturities x n_maturities)
        self.R = np.eye(self.n_maturities) * (measurement_noise_std ** 2)
        
        # Compute NSS factor loadings (constant for fixed λ)
        self.H = self._compute_nss_loadings()
        
    def _compute_nss_loadings(self):
        """
        Compute NSS factor loadings H matrix
        
        For each maturity τ:
          f0(τ) = 1
          f1(τ) = (1 - exp(-λ₁τ))/(λ₁τ)
          f2(τ) = f1(τ) - exp(-λ₁τ)
          f3(τ) = (1 - exp(-λ₂τ))/(λ₂τ) - exp(-λ₂τ)
        
        Returns:
        --------
        H : array (n_maturities × 4)
            Factor loading matrix
        """
        tau = self.maturities
        lambda1 = self.lambda1
        lambda2 = self.lambda2
        
        # Factor 0: Level (constant)
        f0 = np.ones_like(tau)
        
        # Factor 1: Slope
        f1 = (1 - np.exp(-lambda1 * tau)) / (lambda1 * tau)
        
        # Factor 2: Curvature
        f2 = f1 - np.exp(-lambda1 * tau)
        
        # Factor 3: Second curvature
        f3 = (1 - np.exp(-lambda2 * tau)) / (lambda2 * tau) - np.exp(-lambda2 * tau)
        
        # Stack into H matrix (n_maturities × 4)
        H = np.column_stack([f0, f1, f2, f3])
        
        return H
    
    def initialize_state(self, initial_yields):
        """
        Initialize state from first observation using OLS
        
        Parameters:
        -----------
        initial_yields : array
            First yield observation
            
        Returns:
        --------
        beta0 : array (4,)
            Initial state estimate
        P0 : array (4, 4)
            Initial state covariance
        """
        # Remove NaN
        valid_mask = ~np.isnan(initial_yields)
        
        if valid_mask.sum() < 4:
            # Not enough data, use simple heuristics
            beta0 = np.array([
                np.nanmean(initial_yields),  # β0 ~ average level
                np.nanmax(initial_yields) - np.nanmin(initial_yields),  # β1 ~ range
                0.0,  # β2
                0.0   # β3
            ])
        else:
            # OLS: y = H * β + ε
            H_valid = self.H[valid_mask, :]
            y_valid = initial_yields[valid_mask]
            
            try:
                beta0 = np.linalg.lstsq(H_valid, y_valid, rcond=None)[0]
            except:
                # Fallback
                beta0 = np.array([
                    np.nanmean(initial_yields),
                    0.0, 0.0, 0.0
                ])
        
        # Initial covariance (moderate uncertainty)
        P0 = np.eye(self.n_state) * 0.01
        
        return beta0, P0
    
    def fit(self, yields_daily):
        """
        Fit Kalman filter to daily yield observations
        
        Parameters:
        -----------
        yields_daily : pd.DataFrame
            Daily yields with maturities as columns
            Shape: (n_days, n_maturities)
            
        Returns:
        --------
        results : dict
            - beta_filtered: (n_days, 4) filtered state estimates
            - beta_smoothed: (n_days, 4) smoothed state estimates (USE THIS!)
            - P_filtered: (n_days, 4, 4) filtered covariances
            - P_smoothed: (n_days, 4, 4) smoothed covariances
            - innovations: (n_days, n_maturities) measurement innovations
            - dates: DatetimeIndex
        """
        n_days = len(yields_daily)
        dates = yields_daily.index
        
        # Storage for forward pass (filtering)
        beta_filtered = np.zeros((n_days, self.n_state))
        P_filtered = np.zeros((n_days, self.n_state, self.n_state))
        innovations = np.zeros((n_days, self.n_maturities))
        
        # Initialize state
        beta_t, P_t = self.initialize_state(yields_daily.iloc[0].values)
        
        print(f"    Initial state: β0={beta_t[0]:.4f}, β1={beta_t[1]:.4f}, β2={beta_t[2]:.4f}, β3={beta_t[3]:.4f}")
        
        # Forward pass (Kalman filter)
        for t in range(n_days):
            if t % 1000 == 0:
                print(f"      Filtering: {t}/{n_days} ({t/n_days*100:.1f}%)")
            
            y_t = yields_daily.iloc[t].values
            
            # Prediction step
            beta_pred = beta_t  # Random walk: β_t|t-1 = β_t-1
            P_pred = P_t + self.Q  # P_t|t-1 = P_t-1 + Q
            
            # Handle missing observations
            valid_mask = ~np.isnan(y_t)
            
            if valid_mask.sum() > 0:
                # Update step (only for valid observations)
                H_t = self.H[valid_mask, :]
                y_t_valid = y_t[valid_mask]
                R_t = self.R[np.ix_(valid_mask, valid_mask)]
                
                # Innovation
                y_pred = H_t @ beta_pred
                innov = y_t_valid - y_pred
                
                # Innovation covariance
                S = H_t @ P_pred @ H_t.T + R_t
                
                # Kalman gain
                K = P_pred @ H_t.T @ np.linalg.inv(S)
                
                # Update state
                beta_t = beta_pred + K @ innov
                P_t = (np.eye(self.n_state) - K @ H_t) @ P_pred
                
                # Store innovation
                innovations[t, valid_mask] = innov
            else:
                # No valid observations, just propagate
                beta_t = beta_pred
                P_t = P_pred
            
            # Store filtered estimates
            beta_filtered[t, :] = beta_t
            P_filtered[t, :, :] = P_t
        
        print(f"      Filtering complete!")
        
        # Backward pass (RTS smoother)
        print(f"      Smoothing...")
        beta_smoothed = np.zeros((n_days, self.n_state))
        P_smoothed = np.zeros((n_days, self.n_state, self.n_state))
        
        # Initialize with last filtered estimate
        beta_smoothed[-1, :] = beta_filtered[-1, :]
        P_smoothed[-1, :, :] = P_filtered[-1, :, :]
        
        # Backward pass
        for t in range(n_days - 2, -1, -1):
            if t % 1000 == 0:
                print(f"      Smoothing: {n_days-t}/{n_days} ({(n_days-t)/n_days*100:.1f}%)")
            
            # Prediction for next step
            P_pred = P_filtered[t, :, :] + self.Q
            
            # Smoother gain
            J = P_filtered[t, :, :] @ np.linalg.inv(P_pred)
            
            # Smoothed estimates
            beta_smoothed[t, :] = beta_filtered[t, :] + J @ (beta_smoothed[t+1, :] - beta_filtered[t, :])
            P_smoothed[t, :, :] = P_filtered[t, :, :] + J @ (P_smoothed[t+1, :, :] - P_pred) @ J.T
        
        print(f"      Smoothing complete!")
        
        # Return results
        results = {
            'beta_filtered': beta_filtered,
            'beta_smoothed': beta_smoothed,  # ← USE THIS for DCC-GARCH!
            'P_filtered': P_filtered,
            'P_smoothed': P_smoothed,
            'innovations': innovations,
            'dates': dates
        }
        
        return results


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


def estimate_kalman_dnss(data_dict, source_name):
    """
    Estimate daily DNSS parameters using Kalman filter
    
    Parameters:
    -----------
    data_dict : dict
        Dictionary of country → DataFrame with yield data
    source_name : str
        'fred' or 'investing'
        
    Returns:
    --------
    dict : country → DataFrame with DAILY NSS parameters
    """
    print(f"\n{'='*80}")
    print(f"KALMAN FILTER ESTIMATION: {source_name.upper()}")
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
            values='yield_percent',
            aggfunc='mean'
        )
        
        # Create business day index (fills weekends/holidays with NaN)
        full_dates = pd.date_range(START_DATE, END_DATE, freq='B')  # Business days
        yield_daily = yield_pivot.reindex(full_dates)
        
        print(f"  Daily observations: {len(yield_daily)}")
        print(f"  Missing data: {yield_daily.isna().sum().sum()} / {yield_daily.size} ({yield_daily.isna().sum().sum()/yield_daily.size*100:.1f}%)")
        
        # Initialize Kalman filter
        kf = KalmanDNSS(
            maturities=maturities,
            lambda1=LAMBDA1_FIXED,
            lambda2=LAMBDA2_FIXED,
            process_noise_std=PROCESS_NOISE_STD,
            measurement_noise_std=MEASUREMENT_NOISE_STD
        )
        
        # Fit Kalman filter
        print(f"  Running Kalman filter...")
        kalman_results = kf.fit(yield_daily)
        
        # Extract smoothed betas (USE THESE!)
        beta_smoothed = kalman_results['beta_smoothed']
        dates = kalman_results['dates']
        
        # Create DataFrame
        params_df = pd.DataFrame({
            'beta0': beta_smoothed[:, 0],
            'beta1': beta_smoothed[:, 1],
            'beta2': beta_smoothed[:, 2],
            'beta3': beta_smoothed[:, 3],
            'lambda1': LAMBDA1_FIXED,
            'lambda2': LAMBDA2_FIXED
        }, index=dates)
        
        # Compute fit statistics (on non-missing data)
        print(f"  Computing fit statistics...")
        r_squared_list = []
        rmse_list = []
        
        for t in range(0, len(yield_daily), 100):  # Sample every 100 days for speed
            y_t = yield_daily.iloc[t].values
            valid_mask = ~np.isnan(y_t)
            
            if valid_mask.sum() >= 4:
                y_pred = nelson_siegel_svensson_institutional(
                    np.array(maturities)[valid_mask],
                    params_df.iloc[t]['beta0'],
                    params_df.iloc[t]['beta1'],
                    params_df.iloc[t]['beta2'],
                    params_df.iloc[t]['beta3'],
                    LAMBDA1_FIXED,
                    LAMBDA2_FIXED
                )
                
                y_valid = y_t[valid_mask]
                ss_res = np.sum((y_valid - y_pred) ** 2)
                ss_tot = np.sum((y_valid - y_valid.mean()) ** 2)
                r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
                rmse = np.sqrt(ss_res / len(y_valid))
                
                r_squared_list.append(r_squared)
                rmse_list.append(rmse)
        
        avg_r2 = np.mean(r_squared_list)
        avg_rmse = np.mean(rmse_list)
        
        params_df['r_squared'] = avg_r2
        params_df['rmse'] = avg_rmse
        
        results[country] = params_df
        
        # Summary statistics
        print(f"\n  ✅ Completed: {len(params_df)} daily observations")
        print(f"  Summary statistics:")
        print(f"    beta0 (level): {params_df['beta0'].mean():.4f} ± {params_df['beta0'].std():.4f}")
        print(f"    beta1 (slope): {params_df['beta1'].mean():.4f} ± {params_df['beta1'].std():.4f}")
        print(f"    beta2 (curve): {params_df['beta2'].mean():.4f} ± {params_df['beta2'].std():.4f}")
        print(f"    beta3 (curve2): {params_df['beta3'].mean():.4f} ± {params_df['beta3'].std():.4f}")
        print(f"    Avg R²: {avg_r2:.4f}")
        print(f"    Avg RMSE: {avg_rmse:.6f}")
    
    return results


def save_parameters(fred_params, investing_params):
    """Save parameters to pickle files (main location + backup)"""
    print(f"\n{'='*80}")
    print("SAVING PARAMETERS")
    print(f"{'='*80}")
    
    # Create backup directory
    backup_dir = BASE_DIR / "backups" / "dnss_kalman_daily_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save FRED parameters
    if fred_params:
        # Main location
        fred_path = EXPORT_DIR / 'nss_parameters_fred_kalman_daily.pkl'
        with open(fred_path, 'wb') as f:
            pickle.dump(fred_params, f)
        print(f"✅ Saved FRED parameters (main): {fred_path}")
        
        # Backup location
        fred_backup = backup_dir / f'nss_parameters_fred_kalman_daily_{timestamp}.pkl'
        with open(fred_backup, 'wb') as f:
            pickle.dump(fred_params, f)
        print(f"✅ Saved FRED parameters (backup): {fred_backup}")
        
        print(f"   Countries: {list(fred_params.keys())}")
        print(f"   Total observations: {sum(len(df) for df in fred_params.values())}")
    
    # Save Investing parameters
    if investing_params:
        # Main location
        inv_path = EXPORT_DIR / 'nss_parameters_investing_kalman_daily.pkl'
        with open(inv_path, 'wb') as f:
            pickle.dump(investing_params, f)
        print(f"✅ Saved Investing parameters (main): {inv_path}")
        
        # Backup location
        inv_backup = backup_dir / f'nss_parameters_investing_kalman_daily_{timestamp}.pkl'
        with open(inv_backup, 'wb') as f:
            pickle.dump(investing_params, f)
        print(f"✅ Saved Investing parameters (backup): {inv_backup}")
        
        print(f"   Countries: {list(investing_params.keys())}")
        print(f"   Total observations: {sum(len(df) for df in investing_params.values())}")
    
    # Comparison with monthly
    print(f"\n📊 Monthly vs Daily Kalman Comparison:")
    
    try:
        monthly_path = EXPORT_DIR / 'nss_parameters_fred_monthly.pkl'
        if monthly_path.exists():
            with open(monthly_path, 'rb') as f:
                monthly_data = pickle.load(f)
            
            print(f"\n  Monthly (interpolated):")
            for country, df in monthly_data.items():
                print(f"    {country}: {len(df)} observations")
            
            print(f"\n  Daily Kalman (true daily):")
            for country, df in fred_params.items():
                if country in monthly_data:
                    monthly_count = len(monthly_data[country])
                    daily_count = len(df)
                    improvement = (daily_count / monthly_count - 1) * 100
                    print(f"    {country}: {daily_count} observations (+{improvement:.1f}%)")
    except Exception as e:
        print(f"  ⚠️ Could not load monthly data for comparison: {e}")


def main():
    """Main execution"""
    
    if not FILTERPY_AVAILABLE:
        print("\n❌ ERROR: filterpy not installed!")
        print("   Install with: pip install filterpy")
        return
    
    # Load data
    data = load_yield_data()
    
    if not data['fred'] and not data['investing']:
        print("\n❌ ERROR: No data loaded!")
        print("   Check that master data files exist")
        return
    
    # Estimate FRED parameters
    fred_params = {}
    if data['fred']:
        fred_params = estimate_kalman_dnss(data['fred'], 'fred')
    
    # Estimate Investing parameters
    investing_params = {}
    if data['investing']:
        investing_params = estimate_kalman_dnss(data['investing'], 'investing')
    
    # Save results
    save_parameters(fred_params, investing_params)
    
    print(f"\n{'='*80}")
    print("KALMAN DAILY DNSS ESTIMATION COMPLETE")
    print(f"{'='*80}")
    print(f"Finished: {datetime.now()}")
    print("\nNext steps:")
    print("1. Update data_pipeline/nss_betas.py to load Kalman daily files")
    print("2. Re-run DCC-GARCH model with daily betas (no interpolation!)")
    print("3. Check that GARCH converges for all 20 betas")
    print("4. Compare with monthly baseline")


if __name__ == '__main__':
    main()
