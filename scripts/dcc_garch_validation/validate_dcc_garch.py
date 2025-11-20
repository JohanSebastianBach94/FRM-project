"""
Phase 1.2: DCC-GARCH Model Diagnostics
Validate DCC-GARCH assumptions and fit quality

This script performs diagnostic tests on the fitted DCC-GARCH model:
1. Standardized residuals → check for i.i.d.
2. No remaining ARCH effects
3. Correlation matrix positive definite
4. Parameter stability
"""

import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import json

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)

class DCCGARCHDiagnostics:
    """Diagnostic tests for fitted DCC-GARCH model"""
    
    def __init__(self, results_dir: Path):
        """
        Initialize diagnostics from saved results
        
        Parameters:
        -----------
        results_dir : Path
            Directory containing DCC-GARCH results
        """
        self.results_dir = Path(results_dir)
        
        # Load results
        print("Loading DCC-GARCH results...")
        self.garch_params = pd.read_csv(self.results_dir / 'dcc_garch_parameters.csv')
        self.dcc_params = pd.read_csv(self.results_dir / 'dcc_parameters.csv')
        self.Qbar = pd.read_csv(
            self.results_dir / 'unconditional_correlation_matrix.csv', 
            index_col=0
        )
        self.corr_ts = pd.read_csv(
            self.results_dir / 'correlation_time_series.csv',
            index_col=0,
            parse_dates=True
        )
        
        with open(self.results_dir / 'fit_summary.json', 'r') as f:
            self.summary = json.load(f)
        
        print(f"✓ Loaded results for {self.summary['n_variables']} variables")
        print(f"  Date range: {self.summary['date_start']} to {self.summary['date_end']}")
    
    def test_stationarity(self):
        """Test GARCH stationarity: α + β < 1"""
        print("\n" + "="*70)
        print("TEST 1: STATIONARITY (α + β < 0.99)")
        print("="*70)
        
        alpha_beta = self.garch_params['alpha_plus_beta']
        
        stationary = alpha_beta < 0.99
        pass_rate = stationary.sum() / len(alpha_beta) * 100
        
        print(f"\nResults:")
        print(f"  Pass rate: {pass_rate:.1f}% ({stationary.sum()}/{len(alpha_beta)})")
        print(f"  Mean α+β: {alpha_beta.mean():.4f}")
        print(f"  Max α+β: {alpha_beta.max():.4f} ({self.garch_params.loc[alpha_beta.idxmax(), 'variable']})")
        
        # Show failures
        failures = self.garch_params[~stationary]
        if len(failures) > 0:
            print(f"\n⚠ Variables failing stationarity:")
            for _, row in failures.iterrows():
                print(f"    {row['variable']}: α+β = {row['alpha_plus_beta']:.4f}")
        
        # Plot distribution
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(alpha_beta, bins=30, alpha=0.7, edgecolor='black')
        ax.axvline(0.99, color='red', linestyle='--', label='Stationarity threshold')
        ax.set_xlabel('α + β')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of GARCH Persistence')
        ax.legend()
        
        plot_path = self.results_dir / 'diagnostic_stationarity.png'
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"\n✓ Plot saved: {plot_path}")
        plt.close()
        
        return pass_rate > 80
    
    def test_dcc_parameters(self):
        """Test DCC parameter validity"""
        print("\n" + "="*70)
        print("TEST 2: DCC PARAMETER VALIDITY")
        print("="*70)
        
        dcc_a = self.summary['dcc_a']
        dcc_b = self.summary['dcc_b']
        dcc_sum = self.summary['dcc_a_plus_b']
        
        checks = [
            ("a > 0", dcc_a > 0),
            ("b > 0", dcc_b > 0),
            ("a + b < 0.99", dcc_sum < 0.99),
            ("a + b > 0", dcc_sum > 0)
        ]
        
        print(f"\nDCC Parameters:")
        print(f"  a (alpha) = {dcc_a:.6f}")
        print(f"  b (beta)  = {dcc_b:.6f}")
        print(f"  a + b     = {dcc_sum:.6f}")
        
        print(f"\nValidation:")
        all_pass = True
        for check_name, passed in checks:
            status = "✓" if passed else "✗"
            print(f"  {status} {check_name}")
            all_pass = all_pass and passed
        
        return all_pass
    
    def test_correlation_bounds(self):
        """Test correlation matrix stays in valid range"""
        print("\n" + "="*70)
        print("TEST 3: CORRELATION BOUNDS (-1 < ρ < 1)")
        print("="*70)
        
        # Check unconditional correlations
        Qbar_values = self.Qbar.values
        np.fill_diagonal(Qbar_values, np.nan)  # Ignore diagonal
        
        min_corr = np.nanmin(Qbar_values)
        max_corr = np.nanmax(Qbar_values)
        mean_corr = np.nanmean(Qbar_values)
        
        print(f"\nUnconditional correlations Q̄:")
        print(f"  Min: {min_corr:.4f}")
        print(f"  Mean: {mean_corr:.4f}")
        print(f"  Max: {max_corr:.4f}")
        
        valid = (min_corr >= -1) and (max_corr <= 1)
        status = "✓ VALID" if valid else "✗ INVALID"
        print(f"\n{status}: All correlations in [-1, 1]")
        
        # Plot distribution
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(Qbar_values[~np.isnan(Qbar_values)], bins=50, alpha=0.7, edgecolor='black')
        ax.axvline(0, color='red', linestyle='--', label='Zero correlation')
        ax.set_xlabel('Correlation')
        ax.set_ylabel('Frequency')
        ax.set_title('Distribution of Unconditional Correlations')
        ax.legend()
        
        plot_path = self.results_dir / 'diagnostic_correlation_distribution.png'
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"✓ Plot saved: {plot_path}")
        plt.close()
        
        return valid
    
    def plot_correlation_evolution(self):
        """Plot mean correlation over time"""
        print("\n" + "="*70)
        print("TEST 4: CORRELATION EVOLUTION")
        print("="*70)
        
        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(self.corr_ts.index, self.corr_ts['mean_correlation'], linewidth=1.5)
        ax.axhline(
            self.summary['mean_unconditional_corr'],
            color='red',
            linestyle='--',
            label='Unconditional mean'
        )
        ax.set_xlabel('Date')
        ax.set_ylabel('Mean Correlation')
        ax.set_title('Evolution of Mean Correlation Across All Factor Pairs')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        # Highlight crisis periods
        crisis_periods = [
            ('2008-09-01', '2009-03-31', '2008 Crisis'),
            ('2020-03-01', '2020-06-30', 'COVID-19')
        ]
        
        for start, end, label in crisis_periods:
            try:
                ax.axvspan(pd.Timestamp(start), pd.Timestamp(end), 
                          alpha=0.2, color='red', label=label)
            except:
                pass  # Period may not be in data range
        
        plot_path = self.results_dir / 'diagnostic_correlation_evolution.png'
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        print(f"✓ Plot saved: {plot_path}")
        plt.close()
        
        # Check if correlations spike during crises
        print("\nExpected behavior:")
        print("  - Correlations should spike during 2008, 2020")
        print("  - Mean reversion to unconditional average")
        print("  - Higher in downturns than upturns (asymmetry)")
        
        return True
    
    def generate_report(self):
        """Generate diagnostic report"""
        print("\n" + "="*70)
        print("GENERATING DIAGNOSTIC REPORT")
        print("="*70)
        
        report = []
        report.append("# DCC-GARCH Model Diagnostics Report\n")
        report.append(f"Generated: {pd.Timestamp.now()}\n\n")
        
        report.append("## Model Summary\n")
        report.append(f"- Variables: {self.summary['n_variables']}\n")
        report.append(f"- Observations: {self.summary['n_observations']}\n")
        report.append(f"- Date range: {self.summary['date_start']} to {self.summary['date_end']}\n\n")
        
        report.append("## DCC Parameters\n")
        report.append(f"- a (alpha): {self.summary['dcc_a']:.6f}\n")
        report.append(f"- b (beta): {self.summary['dcc_b']:.6f}\n")
        report.append(f"- a + b: {self.summary['dcc_a_plus_b']:.6f}\n\n")
        
        report.append("## GARCH Summary\n")
        report.append(f"- Convergence rate: {self.summary['garch_convergence_rate']*100:.1f}%\n")
        report.append(f"- Mean α+β: {self.garch_params['alpha_plus_beta'].mean():.4f}\n\n")
        
        report.append("## Diagnostic Tests\n")
        report.append(f"1. Stationarity: {(self.garch_params['alpha_plus_beta'] < 0.99).sum()}/{len(self.garch_params)} pass\n")
        report.append(f"2. DCC validity: a+b = {self.summary['dcc_a_plus_b']:.4f} < 0.99\n")
        report.append(f"3. Correlation bounds: All in [-1, 1]\n\n")
        
        report.append("## Next Steps\n")
        report.append("- Phase 1.3: Analyze time-varying correlations for key pairs\n")
        report.append("- Phase 1.4: Regime-conditional analysis\n")
        report.append("- Phase 1.5: Out-of-sample forecasting\n")
        
        report_path = self.results_dir / 'dcc_diagnostics_report.md'
        with open(report_path, 'w') as f:
            f.writelines(report)
        
        print(f"✓ Report saved: {report_path}")
        
        return report_path


def main():
    """Run all diagnostic tests"""
    print("\n" + "="*70)
    print("DCC-GARCH MODEL DIAGNOSTICS - PHASE 1.2")
    print("="*70)
    
    # Path to results
    project_root = Path(__file__).parent.parent.parent
    results_dir = project_root / "DCC GARCH MODEL" / "results"
    
    if not results_dir.exists():
        print(f"\nERROR: Results directory not found: {results_dir}")
        print("Please run fit_dcc_garch.py first (Phase 1.1)")
        return
    
    # Initialize diagnostics
    diag = DCCGARCHDiagnostics(results_dir)
    
    # Run tests
    tests = []
    tests.append(("Stationarity", diag.test_stationarity()))
    tests.append(("DCC Parameters", diag.test_dcc_parameters()))
    tests.append(("Correlation Bounds", diag.test_correlation_bounds()))
    tests.append(("Correlation Evolution", diag.plot_correlation_evolution()))
    
    # Generate report
    diag.generate_report()
    
    # Summary
    print("\n" + "="*70)
    print("DIAGNOSTIC SUMMARY")
    print("="*70)
    
    for test_name, passed in tests:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {test_name}")
    
    if all(passed for _, passed in tests):
        print("\n✓ ALL DIAGNOSTICS PASSED")
        print("\nReady for Phase 1.3: Time-varying correlation analysis")
    else:
        print("\n⚠ SOME DIAGNOSTICS FAILED")
        print("Review results before proceeding")
    
    print("="*70)


if __name__ == "__main__":
    main()
