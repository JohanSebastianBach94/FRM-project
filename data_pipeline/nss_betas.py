"""
NSS Beta Extraction Module
Extracts Nelson-Siegel-Svensson beta parameters from sovereign yield curve data

For each country and date, fits NSS model:
    y(τ) = β₀ + β₁·f₁(τ) + β₂·f₂(τ) + β₃·f₃(τ)

Where:
    β₀ = Level (long-term rate)
    β₁ = Slope (short vs long rates)
    β₂ = Curvature (medium-term hump)
    β₃ = Second curvature (twist)

Output: Time series of betas for each country (risk factors for DCC-GARCH)
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
from scipy.optimize import minimize, differential_evolution

warnings.filterwarnings('ignore')


@dataclass
class NSSBetasConfig:
    """Configuration for NSS beta extraction"""
    
    base_dir: Path
    output_dir: Path
    date_start: pd.Timestamp
    date_end: pd.Timestamp
    
    # Countries to process
    countries: List[str] = None
    
    # Maturities to use (years)
    maturities: List[float] = None
    
    # NSS parameters
    lambda1_fixed: float = 2.5  # First decay parameter
    lambda2_fixed: float = 5.0  # Second decay parameter
    
    def __post_init__(self):
        if self.countries is None:
            self.countries = ['France', 'Italy', 'United']  # United Kingdom
        
        if self.maturities is None:
            self.maturities = [1, 2, 3, 5, 7, 10, 15, 20, 30]


class NSSBetaExtractor:
    """
    Extracts NSS beta parameters from yield curve data
    
    Steps:
    1. Load yield data for each country and maturity
    2. For each date, fit NSS model to observed yields
    3. Extract (β₀, β₁, β₂, β₃) time series
    4. Validate and clean beta series
    5. Output as risk factors for DCC-GARCH
    """
    
    def __init__(self, config: NSSBetasConfig):
        self.config = config
        self._yield_data: Optional[Dict[str, pd.DataFrame]] = None
    
    def load_yield_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load yield curve data for all countries
        
        Returns:
            Dict mapping country name to DataFrame with columns = maturities
        """
        if self._yield_data is not None:
            return self._yield_data
        
        print("\n" + "="*70)
        print("LOADING YIELD CURVE DATA")
        print("="*70)
        
        yield_data = {}
        
        for country in self.config.countries:
            print(f"\nLoading {country}...")
            country_yields = {}
            
            for maturity in self.config.maturities:
                # File naming: BOND_Country_10Y.csv
                file_name = f"BOND_{country}_{int(maturity)}Y.csv"
                file_path = self.config.base_dir / "output" / file_name
                
                if not file_path.exists():
                    print(f"  [SKIP] {maturity}Y - file not found")
                    continue
                
                try:
                    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    
                    # Extract yield column (typically 'Yield' or country name)
                    if 'Yield' in df.columns:
                        yields = df['Yield']
                    elif country in df.columns:
                        yields = df[country]
                    else:
                        # Take first numeric column
                        numeric_cols = df.select_dtypes(include=[np.number]).columns
                        if len(numeric_cols) > 0:
                            yields = df[numeric_cols[0]]
                        else:
                            print(f"  [SKIP] {maturity}Y - no yield column found")
                            continue
                    
                    country_yields[maturity] = yields
                    print(f"  [OK] {maturity}Y - {len(yields)} observations")
                    
                except Exception as e:
                    print(f"  [ERROR] {maturity}Y - {str(e)[:50]}")
                    continue
            
            if not country_yields:
                print(f"  [WARNING] No data loaded for {country}")
                continue
            
            # Combine into DataFrame
            country_df = pd.DataFrame(country_yields)
            country_df.columns = [f'{int(m)}Y' for m in country_df.columns]
            
            # Filter date range
            if self.config.date_start:
                country_df = country_df[country_df.index >= self.config.date_start]
            if self.config.date_end:
                country_df = country_df[country_df.index <= self.config.date_end]
            
            # Clean data
            country_df = country_df.dropna(how='all')
            
            yield_data[country] = country_df
            print(f"  Combined: {country_df.shape[0]} dates, {country_df.shape[1]} maturities")
        
        self._yield_data = yield_data
        
        print(f"\nTotal countries loaded: {len(yield_data)}")
        return yield_data
    
    @staticmethod
    def nss_loading_factors(tau: float, lambda1: float, lambda2: float) -> Tuple[float, float, float]:
        """
        Calculate NSS loading factors for maturity tau
        
        Returns:
            (f1, f2, f3) loadings
        """
        # Avoid division by zero
        if tau < 1e-6:
            return 1.0, 0.0, 0.0
        
        # First factor: slope decay
        exp1 = np.exp(-tau / lambda1)
        f1 = (1 - exp1) / (tau / lambda1)
        
        # Second factor: curvature hump
        f2 = f1 - exp1
        
        # Third factor: second curvature
        exp2 = np.exp(-tau / lambda2)
        f3 = (1 - exp2) / (tau / lambda2) - exp2
        
        return f1, f2, f3
    
    @staticmethod
    def nss_yield(tau: float, beta0: float, beta1: float, beta2: float, beta3: float,
                  lambda1: float = 2.5, lambda2: float = 5.0) -> float:
        """
        Calculate NSS yield for maturity tau
        """
        f1, f2, f3 = NSSBetaExtractor.nss_loading_factors(tau, lambda1, lambda2)
        return beta0 + beta1 * f1 + beta2 * f2 + beta3 * f3
    
    def fit_nss_single_date(self, yields: pd.Series, maturities: List[float]) -> Tuple[float, float, float, float, float]:
        """
        Fit NSS model to yields on a single date
        
        Parameters:
        -----------
        yields : pd.Series
            Observed yields (with maturity as index or values)
        maturities : List[float]
            Corresponding maturities in years
        
        Returns:
        --------
        (beta0, beta1, beta2, beta3, fit_error)
        """
        # Clean data
        valid_idx = ~yields.isna()
        y_clean = yields[valid_idx].values
        tau_clean = np.array([maturities[i] for i, v in enumerate(valid_idx) if v])
        
        if len(y_clean) < 4:
            # Not enough points for 4-parameter fit
            return np.nan, np.nan, np.nan, np.nan, np.inf
        
        lambda1 = self.config.lambda1_fixed
        lambda2 = self.config.lambda2_fixed
        
        # Objective function: sum of squared errors
        def objective(params):
            beta0, beta1, beta2, beta3 = params
            y_fitted = np.array([
                self.nss_yield(tau, beta0, beta1, beta2, beta3, lambda1, lambda2)
                for tau in tau_clean
            ])
            return np.sum((y_clean - y_fitted) ** 2)
        
        # Initial guess
        beta0_init = y_clean[-1] if len(y_clean) > 0 else 2.0  # Long rate
        beta1_init = y_clean[0] - y_clean[-1] if len(y_clean) > 1 else 0.0  # Slope
        beta2_init = 0.0
        beta3_init = 0.0
        
        x0 = [beta0_init, beta1_init, beta2_init, beta3_init]
        
        # Bounds: reasonable ranges for betas
        bounds = [
            (-5, 15),   # beta0: level (can be negative in rare cases)
            (-10, 10),  # beta1: slope
            (-10, 10),  # beta2: curvature
            (-10, 10)   # beta3: second curvature
        ]
        
        try:
            # Try local optimization first
            result = minimize(objective, x0, method='L-BFGS-B', bounds=bounds)
            
            if result.success and result.fun < 10.0:  # Good fit
                beta0, beta1, beta2, beta3 = result.x
                fit_error = np.sqrt(result.fun / len(y_clean))  # RMSE
                return beta0, beta1, beta2, beta3, fit_error
            else:
                # Fall back to global optimization
                result = differential_evolution(objective, bounds, maxiter=100, seed=42)
                beta0, beta1, beta2, beta3 = result.x
                fit_error = np.sqrt(result.fun / len(y_clean))
                return beta0, beta1, beta2, beta3, fit_error
                
        except Exception as e:
            # Optimization failed
            return np.nan, np.nan, np.nan, np.nan, np.inf
    
    def extract_betas_for_country(self, country: str) -> pd.DataFrame:
        """
        Extract NSS beta time series for one country
        
        Returns:
            DataFrame with columns: beta0, beta1, beta2, beta3, fit_error
        """
        print(f"\n{'='*70}")
        print(f"EXTRACTING NSS BETAS: {country}")
        print(f"{'='*70}")
        
        if self._yield_data is None:
            self.load_yield_data()
        
        if country not in self._yield_data:
            raise ValueError(f"No yield data loaded for {country}")
        
        country_yields = self._yield_data[country]
        
        # Extract maturities from column names (e.g., '10Y' -> 10)
        maturities = []
        for col in country_yields.columns:
            try:
                mat = float(col.replace('Y', ''))
                maturities.append(mat)
            except:
                print(f"[WARNING] Could not parse maturity from column: {col}")
        
        print(f"Maturities: {maturities}")
        print(f"Date range: {country_yields.index[0]} to {country_yields.index[-1]}")
        print(f"Observations: {len(country_yields)}")
        
        # Fit NSS for each date
        results = []
        failed_count = 0
        
        print("\nFitting NSS models...")
        for date, yields in country_yields.iterrows():
            beta0, beta1, beta2, beta3, fit_error = self.fit_nss_single_date(yields, maturities)
            
            results.append({
                'date': date,
                'beta0': beta0,
                'beta1': beta1,
                'beta2': beta2,
                'beta3': beta3,
                'fit_error': fit_error
            })
            
            if np.isnan(beta0):
                failed_count += 1
        
        # Create DataFrame
        betas_df = pd.DataFrame(results)
        betas_df.set_index('date', inplace=True)
        
        # Statistics
        success_rate = (1 - failed_count / len(betas_df)) * 100
        print(f"\nFitting Results:")
        print(f"  Success: {len(betas_df) - failed_count}/{len(betas_df)} ({success_rate:.1f}%)")
        print(f"  Failed: {failed_count}")
        
        if success_rate > 0:
            print(f"\nBeta Statistics (successful fits):")
            print(betas_df[['beta0', 'beta1', 'beta2', 'beta3']].describe())
            
            # Check for outliers
            for col in ['beta0', 'beta1', 'beta2', 'beta3']:
                q1 = betas_df[col].quantile(0.01)
                q99 = betas_df[col].quantile(0.99)
                outliers = ((betas_df[col] < q1) | (betas_df[col] > q99)).sum()
                if outliers > 0:
                    print(f"  [WARN] {col}: {outliers} outliers (outside 1%-99% range)")
        
        return betas_df
    
    def extract_all_betas(self) -> Dict[str, pd.DataFrame]:
        """
        Extract NSS betas for all countries
        
        Returns:
            Dict mapping country to beta DataFrame
        """
        if self._yield_data is None:
            self.load_yield_data()
        
        all_betas = {}
        
        for country in self._yield_data.keys():
            try:
                betas = self.extract_betas_for_country(country)
                all_betas[country] = betas
            except Exception as e:
                print(f"\n[ERROR] Failed to extract betas for {country}: {str(e)}")
                continue
        
        return all_betas
    
    def create_risk_factors_dataframe(self) -> pd.DataFrame:
        """
        Create combined DataFrame of NSS betas as risk factors
        
        Output columns: Country_beta0, Country_beta1, Country_beta2, Country_beta3
        """
        print(f"\n{'='*70}")
        print("CREATING RISK FACTORS DATAFRAME")
        print(f"{'='*70}")
        
        all_betas = self.extract_all_betas()
        
        if not all_betas:
            raise ValueError("No betas extracted for any country")
        
        # Combine all countries
        risk_factors = pd.DataFrame()
        
        for country, betas_df in all_betas.items():
            # Select only beta columns (exclude fit_error)
            beta_cols = ['beta0', 'beta1', 'beta2', 'beta3']
            country_betas = betas_df[beta_cols]
            
            # Rename with country prefix
            country_betas.columns = [f'{country}_{col}' for col in beta_cols]
            
            # Merge into combined DataFrame
            if risk_factors.empty:
                risk_factors = country_betas
            else:
                risk_factors = risk_factors.join(country_betas, how='outer')
        
        # Sort by date
        risk_factors = risk_factors.sort_index()
        
        print(f"\nRisk Factors Summary:")
        print(f"  Shape: {risk_factors.shape}")
        print(f"  Date range: {risk_factors.index[0]} to {risk_factors.index[-1]}")
        print(f"  Countries: {len(all_betas)}")
        print(f"  Total betas: {risk_factors.shape[1]} (4 per country)")
        
        # Data quality check
        missing_pct = risk_factors.isna().sum() / len(risk_factors) * 100
        print(f"\nMissing Data:")
        for col, pct in missing_pct.items():
            status = "OK" if pct < 5 else "WARN" if pct < 20 else "FAIL"
            print(f"  [{status}] {col}: {pct:.1f}%")
        
        return risk_factors
    
    def save_betas(self, output_dir: Optional[Path] = None):
        """
        Save NSS betas to CSV files
        """
        if output_dir is None:
            output_dir = self.config.output_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*70}")
        print("SAVING NSS BETAS")
        print(f"{'='*70}")
        
        # Extract and save
        risk_factors = self.create_risk_factors_dataframe()
        
        # Save combined risk factors
        rf_path = output_dir / 'nss_beta_risk_factors.csv'
        risk_factors.to_csv(rf_path)
        print(f"[OK] Risk factors: {rf_path}")
        
        # Save individual country betas
        all_betas = self.extract_all_betas()
        for country, betas_df in all_betas.items():
            country_path = output_dir / f'nss_betas_{country.lower()}.csv'
            betas_df.to_csv(country_path)
            print(f"[OK] {country} betas: {country_path}")
        
        print(f"\nAll files saved to: {output_dir}")
        
        return risk_factors


def load_nss_betas(base_dir: Optional[Path] = None, use_existing_dnss: bool = True, 
                   interpolate_to_daily: bool = True, use_monthly: bool = True) -> pd.DataFrame:
    """
    Load NSS beta risk factors from existing DNSS model output
    
    Loads pre-computed NSS parameters from:
    - nss_parameters_fred_monthly.pkl (preferred - monthly estimates)
    - nss_parameters_fred.pkl (fallback - quarterly estimates)
    
    Parameters:
    -----------
    base_dir : Path, optional
        Base directory of project
    use_existing_dnss : bool, default=True
        If True, load from existing DNSS model output
        If False, load from extracted CSV
    interpolate_to_daily : bool, default=True
        If True, interpolate monthly/quarterly data to daily frequency
        Needed to match daily stress indicators
    use_monthly : bool, default=True
        If True, try to load monthly estimates first (nss_parameters_fred_monthly.pkl)
        Falls back to quarterly if monthly not available
    
    Returns:
    --------
    DataFrame with columns: Country_beta0, Country_beta1, Country_beta2, Country_beta3
    
    Usage:
        from data_pipeline.nss_betas import load_nss_betas
        nss_betas = load_nss_betas()  # Tries monthly first, daily interpolated
    """
    import pickle
    
    if base_dir is None:
        base_dir = Path(__file__).parent.parent
    
    if use_existing_dnss:
        # Load from existing DNSS model output
        nss_params_dir = base_dir / "output" / "trial data folder" / "nss_parameters"
        
        # Try Kalman daily first, then monthly, fallback to quarterly
        fred_path = None
        freq_label = "Unknown"
        
        # Priority 1: Kalman-filtered daily (BEST - no interpolation!)
        kalman_daily_path = nss_params_dir / 'nss_parameters_fred_kalman_daily.pkl'
        if kalman_daily_path.exists():
            fred_path = kalman_daily_path
            freq_label = "Kalman Daily"
            interpolate_to_daily = False  # Already daily, no interpolation needed!
            print(f"[OK] Found Kalman daily DNSS parameters: {kalman_daily_path.name}")
            print("     TRUE daily betas (no interpolation) [OK]")
        
        # Priority 2: Monthly estimates (needs interpolation)
        if fred_path is None and use_monthly:
            monthly_path = nss_params_dir / 'nss_parameters_fred_monthly.pkl'
            if monthly_path.exists():
                fred_path = monthly_path
                freq_label = "Monthly"
                print(f"[OK] Found monthly DNSS parameters: {monthly_path.name}")
        
        # Priority 3: Quarterly estimates (needs interpolation)
        if fred_path is None:
            quarterly_path = nss_params_dir / 'nss_parameters_fred.pkl'
            if quarterly_path.exists():
                fred_path = quarterly_path
                freq_label = "Quarterly"
                print(f"[INFO] Using quarterly DNSS parameters: {quarterly_path.name}")
                print(f"       (Run estimate_dnss_kalman_daily.py for best results)")
        
        if fred_path is None:
            raise FileNotFoundError(
                f"No DNSS parameters found in {nss_params_dir}\n"
                f"Expected files:\n"
                f"  - nss_parameters_fred_monthly.pkl (preferred)\n"
                f"  - nss_parameters_fred.pkl (fallback)"
            )
        
        all_betas = {}
        
        # Load FRED data
        with open(fred_path, 'rb') as f:
            fred_data = pickle.load(f)
            
            for country, params_df in fred_data.items():
                # Extract only beta columns
                beta_cols = ['beta0', 'beta1', 'beta2', 'beta3']
                betas = params_df[beta_cols].copy()
                
                # Rename columns with country prefix
                betas.columns = [f'{country}_{col}' for col in beta_cols]
                
                all_betas[country] = betas
        
        # Combine all countries
        if not all_betas:
            raise FileNotFoundError(
                f"No DNSS parameters found in {nss_params_dir}\n"
                f"Expected files: nss_parameters_fred.pkl or nss_parameters_investing.pkl"
            )
        
        # Merge into single DataFrame
        result = pd.DataFrame()
        for country, betas_df in all_betas.items():
            if result.empty:
                result = betas_df
            else:
                result = result.join(betas_df, how='outer')
        
        result = result.sort_index()
        
        print(f"[OK] Loaded DNSS betas from existing model:")
        print(f"  Countries: {list(all_betas.keys())}")
        print(f"  Original frequency: {freq_label}")
        print(f"  Date range: {result.index[0]} to {result.index[-1]}")
        print(f"  {freq_label} observations: {len(result)}")
        print(f"  Total betas: {result.shape[1]} ({len(all_betas)} countries × 4 betas)")
        
        # Interpolate to daily frequency if requested
        if interpolate_to_daily:
            print(f"\n[INFO] Interpolating to daily frequency...")
            
            start = result.index[0]
            end = result.index[-1]
            business_index = pd.bdate_range(start=start, end=end)
            union_index = business_index.union(result.index).sort_values()

            # Reindex and interpolate on a calendar that preserves weekend-dated releases.
            result_daily = result.reindex(union_index)
            result_daily = result_daily.interpolate(method='linear', limit_direction='forward')
            
            # Forward fill any remaining gaps at the start
            result_daily = result_daily.ffill()

            # Final output is strictly business days.
            result_daily = result_daily.reindex(business_index)
            
            print(f"  Daily observations: {len(result_daily)}")
            print(f"  Interpolation method: Linear")
            print(f"  Max gap between actual estimates: {30 if freq_label == 'Monthly' else 92} days")
            
            return result_daily
        
        return result
    
    else:
        # Fallback: Load from extracted CSV
        output_dir = base_dir / "output" / "trial data folder" / "stress_indicators"
        file_path = output_dir / 'nss_beta_risk_factors.csv'
        
        if not file_path.exists():
            raise FileNotFoundError(
                f"NSS beta risk factors not found: {file_path}\n"
                f"Run NSSBetaExtractor.save_betas() first to generate the file."
            )
        
        df = pd.read_csv(file_path, index_col=0, parse_dates=True)
        return df


if __name__ == "__main__":
    """
    Test extraction
    """
    from pathlib import Path
    
    # Setup
    base_dir = Path(__file__).parent.parent
    output_dir = base_dir / "output" / "trial data folder" / "stress_indicators"
    
    config = NSSBetasConfig(
        base_dir=base_dir,
        output_dir=output_dir,
        date_start=pd.Timestamp('1999-01-01'),
        date_end=pd.Timestamp('2025-10-28'),
        countries=['France', 'Italy', 'United'],
        lambda1_fixed=2.5,
        lambda2_fixed=5.0
    )
    
    # Extract
    extractor = NSSBetaExtractor(config)
    risk_factors = extractor.save_betas()
    
    print("\n" + "="*70)
    print("NSS BETA EXTRACTION COMPLETE")
    print("="*70)
    print(f"\nExtracted {risk_factors.shape[1]} beta risk factors")
    print(f"Date range: {risk_factors.index[0]} to {risk_factors.index[-1]}")
    print(f"Total observations: {len(risk_factors)}")
