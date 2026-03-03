# -*- coding: utf-8 -*-
"""
DCC-GARCH Model Fitting - Phase 1.1
Dynamic Conditional Correlation GARCH Model for 52 Stress Indicators

THREE-TIER RISK AGGREGATION STRATEGY:
- < 95th percentile: Linear aggregation (simple summation)
- 95-99th percentile: DCC-GARCH conditional correlations (THIS MODULE)
- > 99th percentile: Copula models for extreme tails (future)

This script implements a two-step DCC-GARCH estimation:
1. Univariate GARCH(1,1) models for each series
2. DCC model for time-varying correlations

The output (Sigma_t) will be used for risk aggregation in the 95-99% quantile range
where correlation dynamics and volatility clustering matter most.

Author: FRM Project
Date: October 2025
"""

import sys
import sys
import os
import warnings
from pathlib import Path
import re
import argparse
import json
from datetime import datetime

# Ensure project root is on sys.path so we can import config.*
PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.block_alias_map import apply_aliases
import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import mstats
from scipy.optimize import minimize
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import re

from config.block_alias_map import apply_aliases

warnings.filterwarnings('ignore')

# Try to import arch package
try:
    from arch import arch_model
    ARCH_AVAILABLE = True
    print("arch package available - using GARCH estimation")
except ImportError:
    ARCH_AVAILABLE = False
    print("WARNING: 'arch' package not installed. Install with: pip install arch")
    print("Continuing with fallback methods...")

try:
    import yaml
except ImportError:
    yaml = None


class DCCGARCHFitter:
    """
    DCC-GARCH Model Fitter
    
    Implements two-step estimation:
    1. Univariate GARCH(1,1) for each series
    2. DCC model for dynamic correlations
    """
    
    def __init__(self, data, config=None):
        """
        Initialize the DCC-GARCH fitter
        
        Parameters:
        -----------
        data : pd.DataFrame
            Time series data with datetime index
        config : dict
            Configuration parameters
        """
        self.data = data.copy()
        self.config = config or {}
        self.n_series = data.shape[1]
        self.n_obs = data.shape[0]
        
        # Storage for results
        self.garch_results = {}
        self.conditional_volatilities = None
        self.standardized_residuals = None
        self.cleaned_residuals = None
        self.dcc_params = {}
        self.unconditional_correlation = None
        self.dynamic_correlations = None
        self.dcc_update_stats = {}
        self.adcc_params = {}
        self.adcc_dynamic_correlations = None
        self.adcc_correlation_time_series = None
        self.adcc_update_stats = {}
        self.dcc_grid_best_ll = -np.inf
        self.adcc_grid_best_ll = -np.inf

        # Optional: if the selected (a,b) implies very high persistence (often flagged
        # as overfitting), we can apply a small shrinkage towards unconditional
        # correlation inside the recursion for additional robustness.
        self._dcc_overfit_shrinkage = 0.0

        # ADCC guardrail: record whether we forced gamma back down.
        self._adcc_guardrail = {
            'triggered': False,
            'gamma_threshold': float((self.config or {}).get('adcc_gamma_guardrail_threshold', 0.7)),
            'min_ll_improvement_per_obs': float((self.config or {}).get('adcc_min_ll_improvement_per_obs', 1e-4)),
            'll_improvement_per_obs': None,
            'reason': None,
        }

        # Explicit quality gate: prevent low-quality GARCH residuals from silently
        # contaminating correlation estimation.
        self.garch_quality_gate = {
            'triggered': False,
            'convergence_rate': np.nan,
            'min_convergence_rate': float((self.config or {}).get('garch_quality_gate_min_convergence_rate', 0.5)),
            'shrinkage': float((self.config or {}).get('garch_quality_gate_shrinkage', 0.2)),
            'reason': None,
        }
        
        print(f"\nInitialized DCC-GARCH fitter:")
        print(f"  Series: {self.n_series}")
        print(f"  Observations: {self.n_obs}")
        print(f"  Period: {data.index[0]} to {data.index[-1]}")

    @staticmethod
    def _nearest_spd_correlation(R: np.ndarray, eps: float = 1e-10) -> np.ndarray:
        """Project a symmetric matrix to a valid SPD correlation matrix."""
        R = np.asarray(R, dtype=float)
        R = (R + R.T) / 2
        w, V = np.linalg.eigh(R)
        w = np.maximum(w, eps)
        R_spd = (V * w) @ V.T

        d = np.sqrt(np.clip(np.diag(R_spd), eps, None))
        Dinv = np.diag(1.0 / d)
        R_corr = Dinv @ R_spd @ Dinv
        R_corr = (R_corr + R_corr.T) / 2
        np.fill_diagonal(R_corr, 1.0)

        # If any off-diagonal is too close to 1 in magnitude, shrink globally.
        off = R_corr.copy()
        np.fill_diagonal(off, 0.0)
        max_off = float(np.max(np.abs(off))) if off.size else 0.0
        if max_off >= 0.999:
            shrink = 1.0 - (0.999 / max_off)
            shrink = float(np.clip(shrink, 0.0, 0.25))
            I = np.eye(R_corr.shape[0])
            R_corr = (1.0 - shrink) * R_corr + shrink * I
            R_corr = (R_corr + R_corr.T) / 2
            np.fill_diagonal(R_corr, 1.0)

        return R_corr

    def _ensure_valid_correlation(self, R: np.ndarray, eps: float = 1e-10) -> tuple[np.ndarray, bool]:
        """Ensure correlation matrix is SPD and reasonably conditioned.

        Returns (R_valid, projected_flag).
        """
        R = np.asarray(R, dtype=float)
        R = (R + R.T) / 2
        np.fill_diagonal(R, 1.0)
        try:
            min_eig = float(np.min(np.linalg.eigvalsh(R)))
        except np.linalg.LinAlgError:
            return self._nearest_spd_correlation(R, eps=eps), True
        if not np.isfinite(min_eig) or min_eig <= eps:
            return self._nearest_spd_correlation(R, eps=eps), True
        return R, False

    @staticmethod
    def _shrink_correlation_to_min_eigen(
        R: np.ndarray,
        min_eigen_target: float,
    ) -> tuple[np.ndarray, float, float, float]:
        """Ensure a symmetric correlation matrix has min eigenvalue >= target.

        Uses minimal linear shrinkage towards identity: R' = (1-w)R + wI.

        Returns: (R_new, w, min_eig_before, min_eig_after)
        """
        R = np.asarray(R, dtype=float)
        if R.ndim != 2 or R.shape[0] != R.shape[1] or R.shape[0] == 0:
            return R, 0.0, float('nan'), float('nan')

        R = (R + R.T) / 2
        np.fill_diagonal(R, 1.0)

        if R.shape[0] == 1:
            return R, 0.0, 1.0, 1.0

        try:
            eig = np.linalg.eigvalsh(R)
        except np.linalg.LinAlgError:
            eig = None
        min_before = float(np.min(eig)) if eig is not None and eig.size else float('nan')

        target = float(min_eigen_target)
        if not np.isfinite(target) or target <= 0.0:
            return R, 0.0, min_before, min_before

        if np.isfinite(min_before) and min_before >= target:
            return R, 0.0, min_before, min_before

        # Minimal shrinkage weight so that min eigen meets the target.
        # For eigenvalues, shrinkage gives: eig' = (1-w)*eig + w.
        # Solve for w using min eigen.
        if not np.isfinite(min_before):
            w = 0.05
        else:
            denom = 1.0 - min_before
            if denom <= 1e-12:
                w = 0.0
            else:
                w = (target - min_before) / denom
        w = float(np.clip(w, 0.0, 0.5))

        if w <= 0.0:
            return R, 0.0, min_before, min_before

        I = np.eye(R.shape[0])
        R2 = (1.0 - w) * R + w * I
        R2 = (R2 + R2.T) / 2
        np.fill_diagonal(R2, 1.0)
        try:
            min_after = float(np.min(np.linalg.eigvalsh(R2)))
        except np.linalg.LinAlgError:
            min_after = float('nan')

        return R2, w, min_before, min_after

    def _finalize_unconditional_correlation(self, uncorr: pd.DataFrame) -> pd.DataFrame:
        """Finalize the unconditional correlation matrix before use/export."""
        unc = uncorr.copy()
        unc = unc.clip(lower=-0.999, upper=0.999)
        np.fill_diagonal(unc.values, 1.0)
        unc = (unc + unc.T) / 2

        floor = float((self.config or {}).get('unconditional_corr_min_eigen', 0.01))
        R2, w, before, after = self._shrink_correlation_to_min_eigen(unc.values, min_eigen_target=floor)
        if w > 0.0:
            print(
                f"[INFO] Unconditional corr shrinkage applied: w={w:.4f} "
                f"(min_eig {before:.6f} -> {after:.6f}, target={floor:.6f})"
            )
        unc = pd.DataFrame(R2, index=unc.index, columns=unc.columns)
        unc = unc.clip(lower=-0.999, upper=0.999)
        np.fill_diagonal(unc.values, 1.0)
        unc = (unc + unc.T) / 2
        return unc
    
    def step1_univariate_garch(self, p=1, q=1, distribution='StudentT'):
        """
        Step 1: Fit univariate GARCH models to each series
        
        Parameters:
        -----------
        p : int
            GARCH lag order
        q : int
            ARCH lag order
        distribution : str
            'Normal' or 'StudentT'
        """
        print(f"\n{'='*70}")
        print("STEP 1: Univariate GARCH({},{}) Estimation".format(p, q))
        print(f"{'='*70}")
        print(f"Distribution: {distribution}")
        print(f"Mean model: Zero (data pre-demeaned)")
        
        volatilities = pd.DataFrame(index=self.data.index)
        residuals = pd.DataFrame(index=self.data.index)
        
        converged = 0
        failed = 0
        
        for col in self.data.columns:
            try:
                series = self.data[col].dropna()
                
                if len(series) < 100:
                    print(f"  [{col}] SKIP - insufficient data ({len(series)} obs)")
                    failed += 1
                    continue
                
                if ARCH_AVAILABLE:
                    # Use arch package
                    # arch package uses 't' for Student-t distribution
                    dist_name = 't' if distribution == 'StudentT' else distribution.lower()
                    
                    model = arch_model(
                        series,
                        mean='Zero',  # Data is pre-demeaned
                        vol='GARCH',
                        p=p,
                        q=q,
                        dist=dist_name
                    )
                    
                    # PROFESSIONAL APPROACH: Apply constraints to ensure stationarity
                    # Many practitioners impose α + β < 0.998 to avoid boundary issues
                    # This is standard in industry (JPMorgan, ECB, BoE)
                    # Note: arch package uses optimization constraints internally
                    
                    # SPEED OPTIMIZATION: Use typical starting values to avoid long search
                    # Standard GARCH parameters from literature: omega=0.01, alpha=0.1, beta=0.85
                    starting_values = np.array([0.01, 0.1, 0.85])
                    if dist_name == 't':
                        starting_values = np.append(starting_values, 8.0)  # nu parameter
                    
                    result = model.fit(
                        disp='off', 
                        show_warning=False,
                        starting_values=starting_values,
                        options={'ftol': 1e-6, 'maxiter': 100}  # Faster convergence
                    )
                    
                    # Extract parameters
                    omega = result.params.get('omega', np.nan)
                    alpha = result.params.get('alpha[1]', np.nan)
                    beta = result.params.get('beta[1]', np.nan)
                    
                    # Check stationarity
                    alpha_beta_sum = alpha + beta
                    is_converged = bool(getattr(result, "convergence_flag", 1) == 0)
                    status = "OK" if (is_converged and alpha_beta_sum < 0.99) else "WARN"
                    
                    # Store results
                    self.garch_results[col] = {
                        'omega': omega,
                        'alpha': alpha,
                        'beta': beta,
                        'alpha_beta_sum': alpha_beta_sum,
                        'converged': is_converged,
                        'loglikelihood': result.loglikelihood
                    }
                    
                    if distribution == 'StudentT':
                        nu = result.params.get('nu', np.nan)
                        self.garch_results[col]['nu'] = nu
                    
                    # Extract conditional volatility and standardized residuals
                    volatilities[col] = result.conditional_volatility
                    residuals[col] = result.std_resid
                    
                    if is_converged:
                        converged += 1
                    else:
                        failed += 1
                    print(f"  [{col}] {status} a+b={alpha_beta_sum:.4f} ll={result.loglikelihood:.1f}")
                    
                else:
                    # Fallback: rolling volatility
                    print(f"  [{col}] Using rolling volatility (fallback)")
                    vol = series.rolling(window=21, min_periods=10).std()
                    volatilities[col] = vol
                    residuals[col] = series / vol.replace(0, np.nan)
                    
                    self.garch_results[col] = {
                        'omega': np.nan,
                        'alpha': np.nan,
                        'beta': np.nan,
                        'alpha_beta_sum': np.nan,
                        'converged': False,
                        'method': 'rolling'
                    }
                    failed += 1
                    
            except Exception as e:
                print(f"  [{col}] FAILED - {str(e)[:50]}")
                failed += 1
                continue
        
        self.conditional_volatilities = volatilities
        self.standardized_residuals = residuals
        
        print(f"\nStep 1 Summary:")
        print(f"  Converged: {converged}/{self.n_series}")
        print(f"  Failed: {failed}/{self.n_series}")

        total = max(1, (converged + failed))
        conv_rate = converged / total
        self.garch_quality_gate['convergence_rate'] = float(conv_rate)
        if self.garch_quality_gate['min_convergence_rate'] and conv_rate < self.garch_quality_gate['min_convergence_rate']:
            self.garch_quality_gate['triggered'] = True
            self.garch_quality_gate['reason'] = 'low_garch_convergence'
            print(
                f"  [QUALITY GATE] Low GARCH convergence ({converged}/{total} = {conv_rate:.1%}) "
                f"< {self.garch_quality_gate['min_convergence_rate']:.1%}; will force shrinkage correlation."
            )
        
        return converged >= 0.5 * self.n_series  # Success if >50% converged

    def _compute_shrinkage_correlation_series(self, resids: pd.DataFrame, shrinkage: float) -> None:
        """Build a static shrinkage correlation series as a safe fallback."""
        shrinkage = float(np.clip(shrinkage, 0.0, 1.0))
        if resids.shape[1] < 2:
            raise ValueError("Need at least 2 series for correlation")

        corr = resids.corr().fillna(0.0)
        corr = corr.clip(lower=-0.999, upper=0.999)
        np.fill_diagonal(corr.values, 1.0)
        corr = (corr + corr.T) / 2

        I = np.eye(corr.shape[0])
        corr_shr = (1.0 - shrinkage) * corr.values + shrinkage * I
        corr_shr = self._nearest_spd_correlation(corr_shr, eps=1e-10)

        T = len(resids)
        K = resids.shape[1]
        dynamic_corr = np.repeat(corr_shr.reshape(1, K, K), T, axis=0)
        self.dynamic_correlations = dynamic_corr
        self.dcc_update_stats = {
            'successful_updates': T,
            'skipped_dates': 0,
            'outlier_dates': 0,
            'spd_projection_dates': 1,
            'total_updates': T,
            'fallback_reason': 'garch_quality_gate',
            'shrinkage': shrinkage,
        }

        corr_ts = pd.DataFrame(index=resids.index)
        cols = list(resids.columns)
        for i in range(K):
            for j in range(i + 1, K):
                corr_ts[f'{cols[i]}__{cols[j]}'] = corr_shr[i, j]
        self.correlation_time_series = corr_ts

    def _prepare_standardized_residuals(self):
        """Clean standardized residuals for correlation estimation"""
        if self.standardized_residuals is None:
            raise ValueError("Run step1_univariate_garch first")

        resids = self.standardized_residuals.dropna(how='all')
        resids = resids.ffill(limit=5)

        row_missing_pct = resids.isna().sum(axis=1) / resids.shape[1] * 100
        resids = resids[row_missing_pct < 50]
        resids = resids.dropna(axis=1, how='all')

        self.cleaned_residuals = resids
        print(f"Standardized residuals (cleaned): {resids.shape}")
        return resids
    
    def step2_dcc_estimation(self, a_grid=None, b_grid=None):
        """
        Step 2: Estimate DCC parameters via grid search
        
        The DCC model:
        Q_t = (1-a-b)*Qbar + a*(eps_{t-1} * eps_{t-1}') + b*Q_{t-1}
        R_t = diag(Q_t)^{-1/2} * Q_t * diag(Q_t)^{-1/2}
        
        Parameters:
        -----------
        a_grid : list
            Grid for parameter a (DCC ARCH effect)
        b_grid : list
            Grid for parameter b (DCC GARCH effect)
        """
        print(f"\n{'='*70}")
        print("STEP 2: DCC Parameter Estimation")
        print(f"{'='*70}")
        
        resids = self._prepare_standardized_residuals()

        # Quality gate: if GARCH convergence was too low, force shrinkage correlation
        # and skip parameter estimation entirely.
        if bool(self.garch_quality_gate.get('triggered')):
            shrinkage = float(self.garch_quality_gate.get('shrinkage', 0.2))
            print(
                f"\n[QUALITY GATE] Forcing shrinkage correlation (shrinkage={shrinkage:.2f}) "
                f"due to {self.garch_quality_gate.get('reason')}; skipping DCC fit."
            )
            self.dcc_params = {'a': np.nan, 'b': np.nan}
            self.dcc_grid_best_ll = -np.inf
            uncorr = resids.corr().fillna(0.0)
            self.unconditional_correlation = self._finalize_unconditional_correlation(uncorr)
            self._compute_shrinkage_correlation_series(resids, shrinkage=shrinkage)
            return False

        # Compute unconditional correlation matrix and keep values strictly inside (-1, 1)
        uncorr = resids.corr().fillna(0.0)
        self.unconditional_correlation = self._finalize_unconditional_correlation(uncorr)
        print(f"Unconditional correlation matrix: {self.unconditional_correlation.shape}")
        
        fit_method = (self.config.get('dcc_fit_method') or 'opt').lower()

        # Grid search for DCC parameters
        config_a_grid = self.config.get('dcc_a_grid')
        config_b_grid = self.config.get('dcc_b_grid')
        a_grid = a_grid or config_a_grid or [0.01, 0.02, 0.03, 0.04, 0.05]
        b_grid = b_grid or config_b_grid or [0.80, 0.85, 0.88, 0.90, 0.92]
        max_sum = self.config.get('dcc_max_a_plus_b', 0.97)
        
        print(f"\nGrid search:")
        print(f"  a values: {a_grid}")
        print(f"  b values: {b_grid}")
        print(f"  max (a+b) allowed: {max_sum:.2f}")

        reg_penalty = self.config.get('dcc_reg_penalty', 5.0)
        stationarity_target = self.config.get('dcc_stationarity_target', 0.93)
        shrinkage_target = self.config.get('dcc_shrinkage_target', 0.90)
        shrinkage_strength = self.config.get('dcc_shrinkage_strength', 30.0)

        best_score = -np.inf
        best_ll = -np.inf
        best_penalty = 0.0
        best_params = None

        def score_dcc(a_val: float, b_val: float) -> tuple[float, float, float]:
            sum_ab = a_val + b_val
            if sum_ab >= max_sum:
                return -np.inf, -np.inf, np.inf

            max_a = float(self.config.get('dcc_max_a', max_sum - 1e-6))
            max_a = min(max_a, max_sum - 1e-6)
            if a_val > max_a:
                return -np.inf, -np.inf, np.inf

            ll_val = self._compute_dcc_likelihood(resids, a_val, b_val)
            if not np.isfinite(ll_val):
                return -np.inf, -np.inf, np.inf
            excess = max(0.0, sum_ab - stationarity_target)
            penalty_term = reg_penalty * excess ** 2
            shrinkage_penalty = shrinkage_strength * max(0.0, sum_ab - shrinkage_target) ** 2

            large_a_lambda = float(self.config.get('dcc_large_a_penalty_lambda', 0.0))
            large_a_threshold = float(self.config.get('dcc_large_a_penalty_threshold', 0.2))
            large_a_penalty = (
                large_a_lambda * max(0.0, a_val - large_a_threshold) ** 2
                if large_a_lambda > 0
                else 0.0
            )

            score_val = ll_val - penalty_term - shrinkage_penalty - large_a_penalty
            return float(score_val), float(ll_val), float(penalty_term)

        # Always run the coarse grid as a robust initializer.
        try:
            for a in a_grid:
                for b in b_grid:
                    score, ll, penalty = score_dcc(a, b)
                    if score > best_score:
                        best_score = score
                        best_ll = ll
                        best_penalty = penalty
                        best_params = {'a': a, 'b': b}
        except Exception as e:
            print(f"  Grid search error: {str(e)}")

        if best_params is None:
            print("  Grid search failed, using conservative defaults")
            best_params = {'a': 0.03, 'b': 0.90}
            best_penalty = 0.0
            best_ll = -np.inf

        # Optional: constrained optimizer refinement (consistent with Step 6).
        if fit_method == 'opt':
            try:
                def objective(x: np.ndarray) -> float:
                    a_val, b_val = float(x[0]), float(x[1])
                    score, ll, _ = score_dcc(a_val, b_val)
                    if not np.isfinite(score):
                        return 1e9
                    return -score

                x0 = np.array([best_params['a'], best_params['b']], dtype=float)
                max_a = float(self.config.get('dcc_max_a', max_sum - 1e-6))
                max_a = min(max_a, max_sum - 1e-6)
                bounds = [(1e-6, max_a), (1e-6, max_sum - 1e-6)]
                constraints = [
                    {'type': 'ineq', 'fun': lambda x: (max_sum - 1e-6) - (x[0] + x[1])},
                ]

                result = minimize(
                    objective,
                    x0=x0,
                    method='SLSQP',
                    bounds=bounds,
                    constraints=constraints,
                    options={'maxiter': int(self.config.get('dcc_opt_maxiter', 200)), 'ftol': 1e-8},
                )
                if result.success:
                    a_opt, b_opt = float(result.x[0]), float(result.x[1])
                    score_opt, ll_opt, penalty_opt = score_dcc(a_opt, b_opt)
                    if np.isfinite(score_opt) and score_opt >= best_score:
                        best_score = score_opt
                        best_ll = ll_opt
                        best_penalty = penalty_opt
                        best_params = {'a': a_opt, 'b': b_opt}
                else:
                    print(f"  [WARN] DCC optimizer failed: {result.message}")
            except Exception as e:
                print(f"  [WARN] DCC optimizer error: {e}")

        if reg_penalty > 0:
            print(f"  Stationarity penalty: lambda={reg_penalty}, target sum={stationarity_target}")
            print(f"  Best penalty contribution: {best_penalty:.4f}")
        if np.isfinite(best_ll):
            print(f"  Best log-likelihood (pre-penalty): {best_ll:.2f}")

        self.dcc_params = best_params
        self.dcc_grid_best_ll = best_ll

        # Overfitting action: if persistence is extremely high, apply a small
        # shrinkage towards unconditional correlation during the recursion.
        try:
            overfit_threshold = float(self.config.get('dcc_overfit_threshold', 0.98))
            overfit_shrinkage = float(self.config.get('dcc_overfit_shrinkage', 0.05))
        except Exception:
            overfit_threshold = 0.98
            overfit_shrinkage = 0.05
        sum_ab = float(best_params['a'] + best_params['b'])
        if np.isfinite(sum_ab) and sum_ab >= overfit_threshold and overfit_shrinkage > 0:
            self._dcc_overfit_shrinkage = float(np.clip(overfit_shrinkage, 0.0, 0.25))
        else:
            self._dcc_overfit_shrinkage = 0.0
        print(f"\nOptimal DCC parameters:")
        print(f"  a (DCC-ARCH): {best_params['a']:.4f}")
        print(f"  b (DCC-GARCH): {best_params['b']:.4f}")
        print(f"  a + b: {(best_params['a'] + best_params['b']):.4f}")
        
        # Compute dynamic correlations
        self._compute_dynamic_correlations(resids)
        
        return True
    
    def _compute_dcc_likelihood(self, resids, a, b):
        """
        Compute log-likelihood for DCC model with parameters a, b
        
        Simplified likelihood calculation
        """
        T = len(resids)
        K = resids.shape[1]
        
        Qbar = self.unconditional_correlation.values
        Qt = Qbar.copy()
        
        log_likelihood = 0
        
        window = int(self.config.get('dcc_ll_window', 100))
        window = max(2, min(window, T))

        # DCC recursion uses eps_{t-1} to update Q_t and evaluates density using eps_t.
        for t in range(1, window):
            eps_prev = resids.iloc[t - 1].values.reshape(-1, 1)
            eps_t = resids.iloc[t].values.reshape(-1, 1)

            # Update Q_t
            Qt = (1 - a - b) * Qbar + a * (eps_prev @ eps_prev.T) + b * Qt

            # Compute R_t
            Qt_diag_inv_sqrt = np.diag(1.0 / np.sqrt(np.diag(Qt)))
            Rt = Qt_diag_inv_sqrt @ Qt @ Qt_diag_inv_sqrt

            Rt, projected = self._ensure_valid_correlation(Rt, eps=1e-10)
            try:
                sign, logdet = np.linalg.slogdet(Rt)
            except np.linalg.LinAlgError:
                return -np.inf

            if sign <= 0 or not np.isfinite(logdet):
                if projected:
                    return -np.inf
                Rt = self._nearest_spd_correlation(Rt, eps=1e-10)
                try:
                    sign, logdet = np.linalg.slogdet(Rt)
                except np.linalg.LinAlgError:
                    return -np.inf
                if sign <= 0 or not np.isfinite(logdet):
                    return -np.inf

            try:
                quad = float(eps_t.T @ np.linalg.solve(Rt, eps_t))
            except np.linalg.LinAlgError:
                Rt = self._nearest_spd_correlation(Rt, eps=1e-10)
                try:
                    quad = float(eps_t.T @ np.linalg.solve(Rt, eps_t))
                    sign, logdet = np.linalg.slogdet(Rt)
                except np.linalg.LinAlgError:
                    return -np.inf

            log_likelihood += -0.5 * (K * np.log(2 * np.pi) + float(logdet) + quad)
        
        return float(log_likelihood)
    
    def _compute_dynamic_correlations(self, resids):
        """
        Compute time-varying correlation matrices using estimated DCC parameters
        """
        print("\nComputing dynamic correlations...")
        
        T = len(resids)
        K = resids.shape[1]
        a = self.dcc_params['a']
        b = self.dcc_params['b']
        
        Qbar = self.unconditional_correlation.values
        
        # CRITICAL FIX: Fill NaNs in Qbar (from missing data in pairwise correlations)
        # Strategy: For NaN pairs, use 0 correlation (neutral assumption)
        # This prevents NaN propagation through the entire DCC recursion
        nan_count = np.isnan(Qbar).sum()
        if nan_count > 0:
            print(f"  [WARNING] Found {nan_count} NaN values in unconditional correlation matrix")
            print(f"  Filling with 0 (neutral correlation assumption)")
            Qbar = np.nan_to_num(Qbar, nan=0.0)
            # Ensure diagonal is 1 and symmetry
            np.fill_diagonal(Qbar, 1.0)
            Qbar = (Qbar + Qbar.T) / 2
        
        # Initialize storage
        dynamic_corr = np.zeros((T, K, K))
        Qt = Qbar.copy()
        
        print(f"  Starting DCC recursion: T={T}, K={K}")
        print(f"  DCC parameters: a={a:.4f}, b={b:.4f}")
        
        # ROBUST DCC RECURSION - Proper handling of missing data and outliers
        successful_updates = 0
        skipped_dates = 0
        outlier_dates = 0
        spd_projection_dates = 0
        spd_projection_dates = 0
        
        for t in range(T):
            try:
                if t == 0:
                    # Initialize with unconditional correlation
                    dynamic_corr[t] = Qbar
                    successful_updates += 1
                else:
                    # Get previous residuals - KEEP NaN (don't fill!)
                    eps_t_raw = resids.iloc[t-1].values
                    
                    # Check data quality: need at least 50% non-NaN for reliable update
                    # (Lowered from 80% - with 72 series, some dates have partial data)
                    non_nan_mask = ~np.isnan(eps_t_raw)
                    non_nan_pct = non_nan_mask.sum() / K
                    
                    if non_nan_pct < 0.5:
                        # Insufficient data - use previous Qt, don't update
                        dynamic_corr[t] = dynamic_corr[t-1]
                        skipped_dates += 1
                        continue
                    
                    # For partial data: use only complete observations
                    # Create masked outer product
                    eps_t = eps_t_raw.copy()
                    eps_t[~non_nan_mask] = 0.0  # Zero out NaN for outer product
                    eps_t = eps_t.reshape(-1, 1)
                    outer_prod = eps_t @ eps_t.T
                    
                    # Apply mask to outer product (zero out rows/cols with NaN)
                    mask_2d = non_nan_mask.reshape(-1, 1) @ non_nan_mask.reshape(1, -1)
                    outer_prod = outer_prod * mask_2d
                    
                    # Outlier detection: check if outer product has extreme values
                    max_outer = np.abs(outer_prod[mask_2d]).max() if mask_2d.sum() > 0 else 0
                    if max_outer > 100:  # Extreme shock (>10 std devs)
                        # Downweight extreme observations
                        outer_prod = outer_prod * (100 / max_outer)
                        outlier_dates += 1
                    
                    # Update Q_t with robust recursion
                    Qt_new = (1 - a - b) * Qbar + a * outer_prod + b * Qt
                    
                    # Validate Qt
                    if np.any(np.isnan(Qt_new)) or np.any(np.isinf(Qt_new)):
                        # Invalid update - keep previous
                        dynamic_corr[t] = dynamic_corr[t-1]
                        skipped_dates += 1
                        continue
                    
                    # Check diagonal positivity
                    Qt_diag = np.diag(Qt_new)
                    if np.any(Qt_diag <= 1e-8):
                        # Numerical issue - keep previous
                        dynamic_corr[t] = dynamic_corr[t-1]
                        skipped_dates += 1
                        continue
                    
                    # Update Qt for next iteration
                    Qt = Qt_new
                    
                    # Compute correlation matrix R_t
                    Qt_diag_inv_sqrt = np.diag(1.0 / np.sqrt(Qt_diag))
                    Rt = Qt_diag_inv_sqrt @ Qt @ Qt_diag_inv_sqrt

                    Rt, projected = self._ensure_valid_correlation(Rt, eps=1e-10)
                    if projected:
                        spd_projection_dates += 1

                    # Optional robustness: shrink towards unconditional correlation
                    # when the fitted persistence is extremely high.
                    if getattr(self, '_dcc_overfit_shrinkage', 0.0) and np.isfinite(getattr(self, '_dcc_overfit_shrinkage', 0.0)):
                        s = float(getattr(self, '_dcc_overfit_shrinkage', 0.0))
                        if s > 0:
                            Rt = (1.0 - s) * Rt + s * Qbar
                            Rt, _ = self._ensure_valid_correlation(Rt, eps=1e-10)
                    
                    dynamic_corr[t] = Rt
                    successful_updates += 1
                    
            except Exception as e:
                print(f"  [ERROR] DCC recursion failed at t={t}: {e}")
                # Use previous correlation for failed dates
                if t > 0:
                    dynamic_corr[t] = dynamic_corr[t-1]
                else:
                    dynamic_corr[t] = Qbar
                skipped_dates += 1
        
        self.dynamic_correlations = dynamic_corr
        print(f"\n  DCC Recursion Summary:")
        print(f"    Successful updates: {successful_updates}/{T} ({100*successful_updates/T:.1f}%)")
        print(f"    Skipped (insufficient data): {skipped_dates}")
        print(f"    Outlier-adjusted: {outlier_dates}")
        print(f"    SPD projections applied: {spd_projection_dates}")
        
        if successful_updates < T * 0.9:
            print(f"  [WARNING] Only {successful_updates}/{T} matrices computed successfully!")
            print(f"  This may indicate severe data quality issues")
        
        self.dcc_update_stats = {
            'successful_updates': successful_updates,
            'skipped_dates': skipped_dates,
            'outlier_dates': outlier_dates,
            'spd_projection_dates': spd_projection_dates,
            'total_updates': T
        }

        # Save time series of ALL correlations (BUG FIX #2)
        corr_ts = pd.DataFrame(index=resids.index)
        
        # Save all unique pairs (upper triangle)
        for i in range(K):
            for j in range(i+1, K):
                col_i = resids.columns[i]
                col_j = resids.columns[j]
                corr_ts[f'{col_i}__{col_j}'] = dynamic_corr[:, i, j]
        
        self.correlation_time_series = corr_ts
        print(f"  Saved {len(corr_ts.columns)} correlation pairs")

    def step3_adcc_estimation(self, a_grid=None, b_grid=None, gamma_grid=None):
        """Estimate ADCC parameters and compute asymmetric correlations"""
        print(f"\n{'='*70}")
        print("STEP 3: ADCC PARAMETER ESTIMATION")
        print(f"{'='*70}")

        # If the quality gate tripped, do not run ADCC.
        if bool(self.garch_quality_gate.get('triggered')):
            print("\n[QUALITY GATE] Skipping ADCC: residuals deemed unreliable (low GARCH convergence).")
            self.adcc_params = {'a': np.nan, 'b': np.nan, 'gamma': np.nan}
            self.adcc_dynamic_correlations = None
            self.adcc_correlation_time_series = None
            self.adcc_update_stats = {
                'fallback_reason': 'garch_quality_gate',
                'convergence_rate': float(self.garch_quality_gate.get('convergence_rate', np.nan)),
                'min_convergence_rate': float(self.garch_quality_gate.get('min_convergence_rate', np.nan)),
            }
            return False

        resids = self.cleaned_residuals if self.cleaned_residuals is not None else self._prepare_standardized_residuals()

        if self.unconditional_correlation is None:
            raise ValueError("Run step2_dcc_estimation before ADCC")

        fit_method = (self.config.get('adcc_fit_method') or self.config.get('dcc_fit_method') or 'opt').lower()

        config_a_grid = self.config.get('adcc_a_grid')
        config_b_grid = self.config.get('adcc_b_grid')
        config_gamma_grid = self.config.get('adcc_gamma_grid')
        a_grid = a_grid or config_a_grid or [0.01, 0.02, 0.03]
        b_grid = b_grid or config_b_grid or [0.80, 0.85, 0.88]
        gamma_grid = gamma_grid or config_gamma_grid or [0.0, 0.01, 0.02, 0.03]
        max_sum = self.config.get('adcc_max_sum', 0.95)
        reg_penalty = self.config.get('adcc_reg_penalty', 10.0)
        stationarity_target = self.config.get('adcc_stationarity_target', 0.92)
        cv_splits = self.config.get('adcc_cv_splits', 3)
        cv_min_obs = self.config.get('adcc_cv_min_obs', 40)

        split_windows = []
        if cv_splits > 1:
            split_bounds = np.linspace(0, len(resids), cv_splits + 1, dtype=int)
            for start, end in zip(split_bounds[:-1], split_bounds[1:]):
                subset = resids.iloc[start:end]
                if len(subset) >= cv_min_obs:
                    split_windows.append(subset)
        if not split_windows:
            split_windows = [resids]

        print(f"\nADCC grid search:")
        print(f"  a values: {a_grid}")
        print(f"  b values: {b_grid}")
        print(f"  gamma values: {gamma_grid}")
        print(f"  max (a + b + gamma): {max_sum:.3f}")

        best_score = -np.inf
        best_ll = -np.inf
        best_penalty = 0.0
        best_params = None

        def score_adcc(a_val: float, b_val: float, g_val: float) -> tuple[float, float, float]:
            sum_abg = a_val + b_val + g_val
            if sum_abg >= max_sum:
                return -np.inf, -np.inf, np.inf

            max_a = float(self.config.get('adcc_max_a', max_sum - 1e-6))
            max_a = min(max_a, max_sum - 1e-6)
            if a_val > max_a:
                return -np.inf, -np.inf, np.inf

            ll_values = []
            for split in split_windows:
                split_ll = self._compute_adcc_likelihood(split, a_val, b_val, g_val)
                if not np.isfinite(split_ll):
                    ll_values = []
                    break
                ll_values.append(split_ll)
            if not ll_values:
                return -np.inf, -np.inf, np.inf
            avg_ll = float(np.mean(ll_values))
            excess = max(0.0, sum_abg - stationarity_target)
            penalty_term = float(reg_penalty * excess ** 2)

            large_a_lambda = float(self.config.get('adcc_large_a_penalty_lambda', 0.0))
            large_a_threshold = float(self.config.get('adcc_large_a_penalty_threshold', 0.2))
            large_a_penalty = (
                large_a_lambda * max(0.0, a_val - large_a_threshold) ** 2
                if large_a_lambda > 0
                else 0.0
            )

            score_val = avg_ll - penalty_term - large_a_penalty
            return float(score_val), float(avg_ll), penalty_term

        # Coarse grid for robust initialization.
        try:
            for a in a_grid:
                for b in b_grid:
                    for gamma in gamma_grid:
                        score, ll, penalty = score_adcc(a, b, gamma)
                        if score > best_score:
                            best_score = score
                            best_ll = ll
                            best_penalty = penalty
                            best_params = {'a': a, 'b': b, 'gamma': gamma}
        except Exception as e:
            print(f"  ADCC grid search error: {e}")

        if best_params is None:
            print("  ADCC grid search failed, using DCC defaults with gamma=0")
            best_params = {'a': self.dcc_params['a'], 'b': self.dcc_params['b'], 'gamma': 0.0}
            best_penalty = 0.0
            best_ll = -np.inf

        # Optional: constrained optimizer refinement (consistent with Step 6).
        if fit_method == 'opt':
            try:
                def objective(x: np.ndarray) -> float:
                    a_val, b_val, g_val = float(x[0]), float(x[1]), float(x[2])
                    score, ll, _ = score_adcc(a_val, b_val, g_val)
                    if not np.isfinite(score):
                        return 1e9
                    return -score

                x0 = np.array([best_params['a'], best_params['b'], best_params['gamma']], dtype=float)
                max_a = float(self.config.get('adcc_max_a', max_sum - 1e-6))
                max_a = min(max_a, max_sum - 1e-6)
                bounds = [(1e-6, max_a), (1e-6, max_sum - 1e-6), (0.0, max_sum - 1e-6)]
                constraints = [
                    {'type': 'ineq', 'fun': lambda x: (max_sum - 1e-6) - (x[0] + x[1] + x[2])},
                ]
                result = minimize(
                    objective,
                    x0=x0,
                    method='SLSQP',
                    bounds=bounds,
                    constraints=constraints,
                    options={'maxiter': int(self.config.get('adcc_opt_maxiter', 200)), 'ftol': 1e-8},
                )
                if result.success:
                    a_opt, b_opt, g_opt = float(result.x[0]), float(result.x[1]), float(result.x[2])
                    score_opt, ll_opt, penalty_opt = score_adcc(a_opt, b_opt, g_opt)
                    if np.isfinite(score_opt) and score_opt >= best_score:
                        best_score = score_opt
                        best_ll = ll_opt
                        best_penalty = penalty_opt
                        best_params = {'a': a_opt, 'b': b_opt, 'gamma': g_opt}
                else:
                    print(f"  [WARN] ADCC optimizer failed: {result.message}")
            except Exception as e:
                print(f"  [WARN] ADCC optimizer error: {e}")

        self.adcc_params = best_params
        self.adcc_grid_best_ll = best_ll

        # Guardrail: if gamma is extreme but ADCC offers negligible improvement
        # over the symmetric DCC likelihood, force gamma -> 0 (symmetric) to avoid
        # unstable asymmetric stress behavior.
        try:
            gamma_threshold = float(self._adcc_guardrail.get('gamma_threshold', 0.7))
            min_imp_per_obs = float(self._adcc_guardrail.get('min_ll_improvement_per_obs', 1e-4))
        except Exception:
            gamma_threshold = 0.7
            min_imp_per_obs = 1e-4

        try:
            window = int(self.config.get('adcc_ll_window', 100))
            window = max(2, min(window, len(resids)))
        except Exception:
            window = 100

        base_ll = float(getattr(self, 'dcc_grid_best_ll', -np.inf))
        ll_improvement = float(best_ll - base_ll) if (np.isfinite(best_ll) and np.isfinite(base_ll)) else 0.0
        ll_improvement_per_obs = float(ll_improvement / max(1, window))
        self._adcc_guardrail['ll_improvement_per_obs'] = ll_improvement_per_obs

        if (
            best_params.get('gamma') is not None
            and float(best_params.get('gamma')) >= gamma_threshold
            and ll_improvement_per_obs < min_imp_per_obs
        ):
            self._adcc_guardrail['triggered'] = True
            self._adcc_guardrail['reason'] = 'high_gamma_low_ll_gain'
            self.adcc_params = {
                'a': float(self.dcc_params.get('a')) if self.dcc_params else float(best_params.get('a')),
                'b': float(self.dcc_params.get('b')) if self.dcc_params else float(best_params.get('b')),
                'gamma': 0.0,
            }
            print(
                f"  [GUARDRAIL] Forcing gamma->0 due to high gamma (>= {gamma_threshold}) "
                f"with negligible ll improvement per obs ({ll_improvement_per_obs:.2e} < {min_imp_per_obs:.2e})."
            )

        if reg_penalty > 0:
            print(f"  Stationarity penalty: lambda={reg_penalty}, target sum={stationarity_target}")
            print(f"  Best penalty contribution: {best_penalty:.4f}")
        if np.isfinite(best_ll):
            print(f"  Best log-likelihood (pre-penalty): {best_ll:.2f}")

        # Log the parameters that will actually be used downstream (post-guardrail).
        params_for_use = self.adcc_params or best_params
        print(
            f"\nSelected ADCC parameters: a={params_for_use['a']:.4f}, b={params_for_use['b']:.4f}, gamma={params_for_use['gamma']:.4f}"
        )
        print(f"  Sum a + b + gamma: {(params_for_use['a'] + params_for_use['b'] + params_for_use['gamma']):.4f}")

        self._compute_adcc_correlations(resids)
        return True

    def _compute_adcc_likelihood(self, resids, a, b, gamma):
        resid_values = resids.values if hasattr(resids, 'values') else np.asarray(resids)
        resid_values = np.asarray(resid_values, dtype=float)
        T = resid_values.shape[0]
        K = resid_values.shape[1]
        Qbar = self.unconditional_correlation.values
        Qt = Qbar.copy()
        log_likelihood = 0

        window = int(self.config.get('adcc_ll_window', 100))
        window = max(2, min(window, T))

        neg_mask_values = (resid_values < 0.0).astype(float)

        # ADCC recursion uses eps_{t-1} (and its negative part) to update Q_t and evaluates density using eps_t.
        for t in range(1, window):
            eps_prev = resid_values[t - 1].reshape(-1, 1)
            eps_t = resid_values[t].reshape(-1, 1)
            neg_eps = eps_prev * neg_mask_values[t - 1].reshape(-1, 1)
            neg_outer = neg_eps @ neg_eps.T

            Qt = (1 - a - b - gamma) * Qbar + a * (eps_prev @ eps_prev.T) + b * Qt + gamma * neg_outer

            Qt_diag = np.diag(Qt)
            if np.any(Qt_diag <= 0):
                return -np.inf

            Qt_diag_inv_sqrt = np.diag(1.0 / np.sqrt(Qt_diag))
            Rt = Qt_diag_inv_sqrt @ Qt @ Qt_diag_inv_sqrt

            Rt, projected = self._ensure_valid_correlation(Rt, eps=1e-10)
            try:
                sign, logdet = np.linalg.slogdet(Rt)
            except np.linalg.LinAlgError:
                return -np.inf
            if sign <= 0 or not np.isfinite(logdet):
                if projected:
                    return -np.inf
                Rt = self._nearest_spd_correlation(Rt, eps=1e-10)
                try:
                    sign, logdet = np.linalg.slogdet(Rt)
                except np.linalg.LinAlgError:
                    return -np.inf
                if sign <= 0 or not np.isfinite(logdet):
                    return -np.inf

            try:
                quad = float(eps_t.T @ np.linalg.solve(Rt, eps_t))
            except np.linalg.LinAlgError:
                Rt = self._nearest_spd_correlation(Rt, eps=1e-10)
                try:
                    quad = float(eps_t.T @ np.linalg.solve(Rt, eps_t))
                    sign, logdet = np.linalg.slogdet(Rt)
                except np.linalg.LinAlgError:
                    return -np.inf

            log_likelihood += -0.5 * (K * np.log(2 * np.pi) + float(logdet) + quad)

        return float(log_likelihood)

    def _compute_adcc_correlations(self, resids):
        print("\nComputing ADCC dynamic correlations...")
        T = len(resids)
        K = resids.shape[1]
        params = self.adcc_params
        a = params['a']
        b = params['b']
        gamma = params['gamma']
        Qbar = self.unconditional_correlation.values
        Qt = Qbar.copy()

        dynamic_corr = np.zeros((T, K, K))
        successful_updates = 0
        skipped_dates = 0
        outlier_dates = 0
        spd_projection_dates = 0

        for t in range(T):
            if t == 0:
                dynamic_corr[t] = Qbar
                successful_updates += 1
                continue

            eps_t_raw = resids.iloc[t-1].values
            non_nan_mask = ~np.isnan(eps_t_raw)
            non_nan_pct = non_nan_mask.sum() / K

            if non_nan_pct < 0.5:
                dynamic_corr[t] = dynamic_corr[t-1]
                skipped_dates += 1
                continue

            eps_t = eps_t_raw.copy()
            eps_t[~non_nan_mask] = 0.0
            eps_t = eps_t.reshape(-1, 1)
            outer_prod = eps_t @ eps_t.T
            mask_2d = non_nan_mask.reshape(-1, 1) @ non_nan_mask.reshape(1, -1)
            outer_prod = outer_prod * mask_2d

            neg_eps = np.zeros_like(eps_t)
            neg_mask = (eps_t_raw < 0) & non_nan_mask
            neg_eps[neg_mask, 0] = eps_t_raw[neg_mask]
            neg_outer = neg_eps @ neg_eps.T

            max_outer = np.abs(outer_prod[mask_2d]).max() if mask_2d.sum() > 0 else 0
            if max_outer > 100:
                outer_prod = outer_prod * (100 / max_outer)
                neg_outer = neg_outer * (100 / max_outer)
                outlier_dates += 1

            Qt_new = (1 - a - b - gamma) * Qbar + a * outer_prod + b * Qt + gamma * neg_outer

            if np.any(np.isnan(Qt_new)) or np.any(np.isinf(Qt_new)):
                dynamic_corr[t] = dynamic_corr[t-1]
                skipped_dates += 1
                continue

            Qt_diag = np.diag(Qt_new)
            if np.any(Qt_diag <= 1e-8):
                dynamic_corr[t] = dynamic_corr[t-1]
                skipped_dates += 1
                continue

            Qt = Qt_new
            Qt_diag_inv_sqrt = np.diag(1.0 / np.sqrt(Qt_diag))
            Rt = Qt_diag_inv_sqrt @ Qt @ Qt_diag_inv_sqrt

            Rt, projected = self._ensure_valid_correlation(Rt, eps=1e-10)
            if projected:
                spd_projection_dates += 1
            dynamic_corr[t] = Rt
            successful_updates += 1

        self.adcc_dynamic_correlations = dynamic_corr

        self.adcc_update_stats = {
            'successful_updates': successful_updates,
            'skipped_dates': skipped_dates,
            'outlier_dates': outlier_dates,
            'spd_projection_dates': spd_projection_dates,
            'total_updates': T
        }

        corr_ts = pd.DataFrame(index=resids.index)
        for i in range(K):
            for j in range(i+1, K):
                col_i = resids.columns[i]
                col_j = resids.columns[j]
                corr_ts[f'{col_i}__{col_j}'] = dynamic_corr[:, i, j]

        self.adcc_correlation_time_series = corr_ts
        print(f"  ADCC saved {len(corr_ts.columns)} correlation pairs")
    
    def get_covariance_matrix(self, t):
        """
        Get time-varying covariance matrix at time t
        
        Sigma_t = D_t * R_t * D_t
        
        where D_t is diagonal matrix of conditional volatilities
        """
        if self.conditional_volatilities is None or self.dynamic_correlations is None:
            raise ValueError("Run estimation steps first")
        
        # Get volatilities at time t
        vols = self.conditional_volatilities.iloc[t].values
        D_t = np.diag(vols)
        
        # Get correlation at time t
        R_t = self.dynamic_correlations[t]
        
        # Compute covariance
        Sigma_t = D_t @ R_t @ D_t
        
        return Sigma_t
    
    def save_results(self, output_dir):
        """
        Save all results to files
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        print(f"\n{'='*70}")
        print("Saving Results")
        print(f"{'='*70}")
        
        # 1. GARCH parameters
        garch_params_df = pd.DataFrame(self.garch_results).T
        garch_params_path = output_path / 'dcc_garch_parameters.csv'
        garch_params_df.to_csv(garch_params_path)
        print(f"[OK] GARCH parameters: {garch_params_path}")
        
        # 2. DCC parameters
        dcc_params_df = pd.DataFrame([self.dcc_params or {}])
        dcc_params_path = output_path / 'dcc_parameters.csv'
        dcc_params_df.to_csv(dcc_params_path, index=False)
        print(f"[OK] DCC parameters: {dcc_params_path}")
        
        # 3. Unconditional correlation matrix
        if self.unconditional_correlation is None:
            cols = list(getattr(self, 'data', pd.DataFrame()).columns)
            if cols:
                self.unconditional_correlation = pd.DataFrame(np.eye(len(cols)), index=cols, columns=cols)
                print("[INFO] Unconditional correlation missing; exporting identity matrix")
        if self.unconditional_correlation is not None:
            uncond_corr_path = output_path / 'unconditional_correlation_matrix.csv'
            self.unconditional_correlation.to_csv(uncond_corr_path)
            print(f"[OK] Unconditional correlation: {uncond_corr_path}")
        else:
            print("[INFO] Unconditional correlation not available; skipping export")
        
        # 4. Correlation time series (sample pairs)
        if hasattr(self, 'correlation_time_series'):
            corr_ts_path = output_path / 'correlation_time_series.csv'
            self.correlation_time_series.to_csv(corr_ts_path)
            print(f"[OK] Correlation time series: {corr_ts_path}")
        
        # 5. Conditional volatilities
        if self.conditional_volatilities is not None:
            vol_path = output_path / 'conditional_volatilities.csv'
            self.conditional_volatilities.to_csv(vol_path)
            print(f"[OK] Conditional volatilities: {vol_path}")
        else:
            print("[INFO] Conditional volatilities not available; skipping export")
        
        # 5b. Standardized residuals (BUG FIX #1)
        if self.standardized_residuals is not None:
            resid_path = output_path / 'standardized_residuals.csv'
            self.standardized_residuals.to_csv(resid_path)
            print(f"[OK] Standardized residuals: {resid_path}")
        
        # 6. ADCC parameters/time series (if computed)
        if self.adcc_params:
            adcc_params_df = pd.DataFrame([self.adcc_params])
            adcc_params_path = output_path / 'adcc_parameters.csv'
            adcc_params_df.to_csv(adcc_params_path, index=False)
            print(f"[OK] ADCC parameters: {adcc_params_path}")

            if self.adcc_correlation_time_series is not None:
                skip_export = os.environ.get('DCC_SKIP_ADCC_CORR_EXPORT') == '1'
                if skip_export:
                    print("[INFO] Skipping ADCC correlation time series export (tradeoff mode)")
                else:
                    adcc_corr_ts_path = output_path / 'adcc_correlation_time_series.csv'
                    self.adcc_correlation_time_series.to_csv(adcc_corr_ts_path)
                    print(f"[OK] ADCC correlation series: {adcc_corr_ts_path}")

        # 6. Summary JSON
        summary = {
            'timestamp': datetime.now().isoformat(),
            'n_series': self.n_series,
            'n_observations': self.n_obs,
            'date_range': {
                'start': str(self.data.index[0]),
                'end': str(self.data.index[-1])
            },
            'dcc_parameters': self.dcc_params,
            'adcc_parameters': self.adcc_params if self.adcc_params else None,
            'garch_quality_gate': self.garch_quality_gate,
            'garch_convergence': {
                'converged': sum(1 for r in self.garch_results.values() if r.get('converged', False)),
                'failed': sum(1 for r in self.garch_results.values() if not r.get('converged', False))
            }
        }
        
        summary_path = output_path / 'fit_summary.json'
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"[OK] Summary: {summary_path}")
        
        print(f"\nAll results saved to: {output_path}")


    # Block helper definitions moved to module level


def execute_dcc_pipeline(df, output_dir, dcc_config, label='global', refit_mode=False):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"\n{'='*70}")
    print(f"PIPELINE [{label}] | Series: {df.shape[1]} | Obs: {df.shape[0]}")
    print(f"{'='*70}")

    fitter = DCCGARCHFitter(df, config=dcc_config)

    refit_paths = {
        'residuals': output_path / 'standardized_residuals.csv',
        'params': output_path / 'dcc_garch_parameters.csv',
        'vols': output_path / 'conditional_volatilities.csv'
    }

    step1_success = False
    if refit_mode and all(p.exists() for p in refit_paths.values()):
        fitter.standardized_residuals = pd.read_csv(refit_paths['residuals'], index_col=0, parse_dates=True)
        fitter.conditional_volatilities = pd.read_csv(refit_paths['vols'], index_col=0, parse_dates=True)
        garch_params_df = pd.read_csv(refit_paths['params'], index_col=0)
        fitter.garch_results = garch_params_df.to_dict(orient='index')
        print(f"[INFO] Loaded saved GARCH state for {label}")
        step1_success = True
    else:
        distribution = 'StudentT'
        step1_success = fitter.step1_univariate_garch(p=1, q=1, distribution=distribution)
        if not step1_success:
            print(f"\n[WARNING] {label} Step 1 had many failures. Results may be unreliable.")

    fitter.step2_dcc_estimation()
    if bool((dcc_config or {}).get('skip_adcc', False)):
        print("[INFO] Skipping ADCC estimation (skip_adcc=True)")
    else:
        fitter.step3_adcc_estimation()
    fitter.save_results(output_path)

    print(f"\n{'='*70}")
    print(f"VALIDATION CHECKS [{label}]")
    print(f"{'='*70}")

    checks_passed = 0
    checks_total = 0

    # Check 1: Stationarity
    checks_total += 1
    a_val = fitter.dcc_params.get('a')
    b_val = fitter.dcc_params.get('b')
    a_plus_b = (a_val + b_val) if (a_val is not None and b_val is not None) else float('nan')
    if np.isfinite(a_plus_b):
        if a_plus_b < 0.99:
            print(f"[OK] DCC parameters stationary (a+b = {a_plus_b:.4f})")
            checks_passed += 1
        else:
            print(f"[WARN] DCC may be non-stationary (a+b = {a_plus_b:.4f})")
    else:
        print("[INFO] DCC stationarity check skipped (quality gate / missing params)")

    # Check 2: GARCH convergence
    checks_total += 1
    converged = sum(1 for r in fitter.garch_results.values() if r.get('converged', False))
    total = len(fitter.garch_results) or 1
    conv_rate = converged / total
    if conv_rate > 0.8:
        print(f"[OK] GARCH convergence rate: {converged}/{total} ({conv_rate*100:.1f}%)")
        checks_passed += 1
    else:
        print(f"[WARN] Low GARCH convergence: {converged}/{total} ({conv_rate*100:.1f}%)")

    # Check 3: Correlation bounds
    checks_total += 1
    corr_vals = fitter.unconditional_correlation.values[np.triu_indices_from(fitter.unconditional_correlation.values, k=1)]
    if (corr_vals >= -1).all() and (corr_vals <= 1).all():
        print("[OK] Correlations within [-1, 1]")
        checks_passed += 1
    else:
        print("[WARN] Some correlations out of bounds")

    print(f"\nValidation: {checks_passed}/{checks_total} checks passed")
    if checks_passed == checks_total:
        print("[OK] All validation checks passed")
    else:
        print("[WARNING] Some validation checks failed")

    corr_ts = getattr(fitter, 'correlation_time_series', None)
    smoothness = None
    smoothness_q75 = None
    if corr_ts is not None and not corr_ts.empty:
        diffs = corr_ts.diff().abs()
        stack = diffs.stack().dropna()
        if not stack.empty:
            smoothness = float(stack.mean())
            smoothness_q75 = float(stack.quantile(0.75))

    update_stats = getattr(fitter, 'dcc_update_stats', {})
    updates_total = update_stats.get('total_updates', 0)
    updates_success = update_stats.get('successful_updates', 0)
    update_rate = float(updates_success / updates_total) if updates_total else None

    correlation_model_used = 'shrinkage' if bool(getattr(fitter, 'garch_quality_gate', {}).get('triggered')) else 'dcc'
    cfg = dcc_config or {}
    metrics = {
        'label': label,
        'n_series': fitter.n_series,
        'n_observations': fitter.n_obs,
        'dcc_a': fitter.dcc_params.get('a'),
        'dcc_b': fitter.dcc_params.get('b'),
        'a_plus_b': a_plus_b,
        'adcc_gamma': fitter.adcc_params.get('gamma'),
        'correlation_model_used': correlation_model_used,
        'garch_quality_gate_triggered': bool(getattr(fitter, 'garch_quality_gate', {}).get('triggered')),
        'garch_quality_gate_min_convergence_rate': float(getattr(fitter, 'garch_quality_gate', {}).get('min_convergence_rate', 0.0)),
        'garch_quality_gate_shrinkage': float(getattr(fitter, 'garch_quality_gate', {}).get('shrinkage', 0.2)),
        'dcc_max_a': float(cfg.get('dcc_max_a', cfg.get('dcc_max_a_plus_b', 0.97) - 1e-6)),
        'dcc_a_at_upper_bound': bool(
            fitter.dcc_params.get('a') is not None
            and fitter.dcc_params.get('a') >= float(cfg.get('dcc_max_a', cfg.get('dcc_max_a_plus_b', 0.97) - 1e-6)) - 1e-6
        ),
        'dcc_large_a_penalty_lambda': float(cfg.get('dcc_large_a_penalty_lambda', 0.0)),
        'dcc_large_a_penalty_threshold': float(cfg.get('dcc_large_a_penalty_threshold', 0.2)),
        'adcc_max_a': float(cfg.get('adcc_max_a', cfg.get('adcc_max_sum', 0.95) - 1e-6)),
        'adcc_a_at_upper_bound': bool(
            fitter.adcc_params.get('a') is not None
            and fitter.adcc_params.get('a') >= float(cfg.get('adcc_max_a', cfg.get('adcc_max_sum', 0.95) - 1e-6)) - 1e-6
        ),
        'adcc_large_a_penalty_lambda': float(cfg.get('adcc_large_a_penalty_lambda', 0.0)),
        'adcc_large_a_penalty_threshold': float(cfg.get('adcc_large_a_penalty_threshold', 0.2)),
        'adcc_guardrail_triggered': bool(getattr(fitter, '_adcc_guardrail', {}).get('triggered', False)),
        'adcc_ll_improvement_per_obs': getattr(fitter, '_adcc_guardrail', {}).get('ll_improvement_per_obs'),
        'update_success_rate': update_rate,
        'smoothness_mean': smoothness,
        'smoothness_75pct': smoothness_q75,
        'successful_updates': updates_success,
        'total_updates': updates_total,
        'spd_projection_dates': update_stats.get('spd_projection_dates'),
        'garch_converged': converged,
        'garch_total': total,
        'garch_convergence_rate': conv_rate,
        'validation_checks_passed': checks_passed,
        'validation_checks_total': checks_total,
        'overfitting_flag': bool(np.isfinite(a_plus_b) and a_plus_b >= 0.98),
        'status': 'completed'
    }

    metrics_path = output_path / 'fit_metrics.json'
    with open(metrics_path, 'w') as fmetrics:
        json.dump(metrics, fmetrics, indent=2)
    print(f"Fit metrics saved: {metrics_path}")

    return fitter, metrics


def execute_univariate_pipeline(df, output_dir, dcc_config, label='univariate', refit_mode=False):
    """Run only the univariate GARCH step.

    This is used for K=1 blocks where DCC/ADCC is not defined.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    print(f"\n{'='*70}")
    print(f"PIPELINE [{label}] | Series: {df.shape[1]} | Obs: {df.shape[0]} (UNIVARIATE-ONLY)")
    print(f"{'='*70}")

    fitter = DCCGARCHFitter(df, config=dcc_config)

    refit_paths = {
        'residuals': output_path / 'standardized_residuals.csv',
        'params': output_path / 'dcc_garch_parameters.csv',
        'vols': output_path / 'conditional_volatilities.csv'
    }

    if refit_mode and all(p.exists() for p in refit_paths.values()):
        fitter.standardized_residuals = pd.read_csv(refit_paths['residuals'], index_col=0, parse_dates=True)
        fitter.conditional_volatilities = pd.read_csv(refit_paths['vols'], index_col=0, parse_dates=True)
        garch_params_df = pd.read_csv(refit_paths['params'], index_col=0)
        fitter.garch_results = garch_params_df.to_dict(orient='index')
        print(f"[INFO] Loaded saved GARCH state for {label}")
    else:
        distribution = 'StudentT'
        step1_success = fitter.step1_univariate_garch(p=1, q=1, distribution=distribution)
        if not step1_success:
            print(f"\n[WARNING] {label} Step 1 had failures. Univariate outputs may be unreliable.")

    cols = list(df.columns)
    fitter.unconditional_correlation = pd.DataFrame(np.eye(len(cols)), index=cols, columns=cols)
    try:
        T = len(df.index)
        fitter.dynamic_correlations = np.ones((T, len(cols), len(cols)))
    except Exception:
        fitter.dynamic_correlations = None

    fitter.dcc_params = {'a': None, 'b': None}
    fitter.adcc_params = None
    fitter.adcc_correlation_time_series = None
    fitter.correlation_time_series = pd.DataFrame(index=df.index)

    fitter.save_results(output_path)

    metrics = {
        'label': label,
        'n_series': fitter.n_series,
        'n_observations': fitter.n_obs,
        'dcc_a': None,
        'dcc_b': None,
        'a_plus_b': None,
        'adcc_gamma': None,
        'correlation_model_used': 'univariate_only',
        'garch_quality_gate_triggered': bool(getattr(fitter, 'garch_quality_gate', {}).get('triggered')),
        'status': 'completed'
    }

    metrics_path = output_path / 'fit_metrics.json'
    with open(metrics_path, 'w') as fmetrics:
        json.dump(metrics, fmetrics, indent=2)
    print(f"Fit metrics saved: {metrics_path}")

    return fitter, metrics


_BLOCK_ID_RE = re.compile(r'[^a-z0-9]+')


def slugify_block_id(iso_code, block_key):
    cleaned = f"{iso_code}_{block_key or 'block'}".lower()
    cleaned = _BLOCK_ID_RE.sub('_', cleaned)
    return cleaned.strip('_')


def load_block_definitions(project_root, override_path=None):
    config_path = Path(project_root) / 'config' / 'country_blocks_extended.yaml'
    frozen_blocks_path = Path(project_root) / 'outputs' / 'country_block_definition.json'
    payload = None

    if override_path:
        override_path = Path(override_path)
        if override_path.exists():
            try:
                frozen = json.loads(override_path.read_text(encoding='utf-8'))
                countries = []
                for iso, entry in frozen.items():
                    countries.append({
                        'country': entry.get('country'),
                        'iso_code': iso,
                        'region': entry.get('region'),
                        'coverage_window': entry.get('coverage_window'),
                        'blocks': entry.get('blocks', []),
                    })
                payload = {'country_blocks': countries}
                print(f"Loaded overridden country blocks from {override_path}")
            except Exception as exc:
                print(f"WARNING: Failed to read overridden block definition ({override_path}): {exc}")

    if payload is None and frozen_blocks_path.exists():
        try:
            frozen = json.loads(frozen_blocks_path.read_text(encoding='utf-8'))
            countries = []
            for iso, entry in frozen.items():
                countries.append({
                    'country': entry.get('country'),
                    'iso_code': iso,
                    'region': entry.get('region'),
                    'coverage_window': entry.get('coverage_window'),
                    'blocks': entry.get('blocks', []),
                })
            payload = {'country_blocks': countries}
            print(f"Loaded frozen country blocks from {frozen_blocks_path}")
        except Exception as exc:
            print(f"WARNING: Failed to read frozen block definition ({frozen_blocks_path}): {exc}")

    if payload is None:
        if not config_path.exists():
            print("[WARN] Block configuration not found; falling back to full-model run")
            return []
        if yaml is None:
            print("[WARN] PyYAML not installed; skipping block-aware configuration")
            return []
        try:
            payload = yaml.safe_load(config_path.read_text())
            print(f"Loaded country blocks from {config_path}")
        except Exception as exc:
            print(f"ERROR: Failed to read country_blocks_extended.yaml: {exc}")
        return []

    blocks = []
    for country_block in payload.get('country_blocks', []):
        country = country_block.get('country') or country_block.get('iso_code') or 'unknown'
        iso = country_block.get('iso_code') or country[:3].upper()
        for block in country_block.get('blocks', []):
            block_key = block.get('key', 'block')
            block_id = slugify_block_id(iso, block_key)

            raw_series = list(dict.fromkeys(block.get('series_codes', [])))
            resolved_series = apply_aliases(raw_series)

            blocks.append({
                'block_id': block_id,
                'country': country,
                'iso_code': iso,
                'block_key': block_key,
                'description': block.get('coverage') or block.get('status_notes') or '',
                'series_codes': resolved_series,
                'raw_series_codes': raw_series,
            })
    return blocks


def preprocess_data(df, config=None):
    """
    Preprocess data for DCC-GARCH estimation
    
    Steps:
    1. Check stationarity (convert levels to returns if needed)
    2. De-mean series
    3. Handle outliers (winsorization)
    4. Optional: PCA for dimensionality reduction
    5. Final data quality checks
    """
    print(f"\n{'='*70}")
    print("PREPROCESSING DATA")
    print(f"{'='*70}")
    
    config = config or {}
    df_processed = df.copy()

    # Optional explicit exclusions (artifact-prone series, governance overrides)
    exclude_series = config.get('exclude_series') or []
    try:
        exclude_series = [str(x).strip() for x in exclude_series if str(x).strip()]
    except Exception:
        exclude_series = []
    if exclude_series:
        present = [c for c in exclude_series if c in df_processed.columns]
        if present:
            print(f"\n  [INFO] Excluding {len(present)} series via config.exclude_series")
            df_processed = df_processed.drop(columns=present)
    
    print(f"\nInput data: {df_processed.shape}")
    print(f"  Date range: {df_processed.index[0]} to {df_processed.index[-1]}")
    
    # Step 1: Stationarity check and conversion
    print("\n[STEP 1] Stationarity Check - Literature-Based Approach")
    print("-" * 50)
    print("  Following Ang & Chen (2002), Forbes & Rigobon (2002):")
    print("  - Stock indices: RETURNS (pct_change)")
    print("  - VIX, Spreads, Rates: LEVELS (already stationary)")
    print("  - NSS betas: First difference (I(1) process)")
    print()
    
    auto_convert = config.get('auto_convert_to_returns', True)
    price_threshold = config.get('price_threshold', 50)

    # Frequency-aware guardrail: do NOT convert low-frequency series to daily returns.
    # This implements the triage ladder: fix resampling/transform upstream rather than
    # winsorizing artifacts downstream.
    force_level_low_freq = bool(config.get('force_level_if_low_frequency', True))
    catalog_meta = {}
    if force_level_low_freq:
        try:
            catalog_path = config.get('catalog_path')
            if catalog_path:
                catalog_path = Path(str(catalog_path))
                if not catalog_path.is_absolute():
                    catalog_path = Path(__file__).resolve().parents[1] / catalog_path
            else:
                catalog_path = Path(__file__).resolve().parents[1] / 'catalog.csv'

            if catalog_path.exists():
                cat = pd.read_csv(catalog_path)
                if 'series' in cat.columns:
                    cat['series'] = cat['series'].astype(str).str.strip()
                    for _, row in cat.iterrows():
                        key = str(row.get('series') or '').strip()
                        if not key:
                            continue
                        catalog_meta[key] = {
                            'frequency_label': str(row.get('frequency_label') or '').strip().lower(),
                            'median_gap_days': row.get('median_gap_days'),
                        }
                print(f"\n  [INFO] Loaded catalog metadata for {len(catalog_meta)} series from {catalog_path}")
            else:
                print(f"\n  [WARN] catalog.csv not found at {catalog_path}; low-frequency guardrail disabled")
                force_level_low_freq = False
        except Exception as exc:
            print(f"\n  [WARN] Failed to load catalog.csv for frequency guardrail: {exc}")
            force_level_low_freq = False
    
    # Track which series are kept as LEVELS (don't demean these later!)
    level_series = []
    return_series = []
    
    if auto_convert:
        for col in df_processed.columns:
            series = df_processed[col].dropna()
            
            if len(series) < 10:
                continue

            # Guardrail: if series is not truly daily (or has large median gaps), do not convert
            # to daily pct_change/diff. Keep as level/step and standardize later.
            if force_level_low_freq:
                meta = catalog_meta.get(col) or {}
                freq = str(meta.get('frequency_label') or '')
                try:
                    gap = float(meta.get('median_gap_days'))
                except Exception:
                    gap = float('nan')
                is_low_freq = (freq and freq not in {'daily', 'business_daily', 'bday'}) or (np.isfinite(gap) and gap > 3.0)
                if is_low_freq:
                    extra = []
                    if freq:
                        extra.append(f"freq={freq}")
                    if np.isfinite(gap):
                        extra.append(f"median_gap_days={gap:.1f}")
                    suffix = ("; " + ", ".join(extra)) if extra else ""
                    print(f"  [{col}] LEVEL (low-frequency guardrail{suffix})")
                    level_series.append(col)
                    continue
            
            # VIX and volatility: KEEP AS LEVELS (literature standard)
            if any(x in col for x in ['VIX', 'VOLW']):
                print(f"  [{col}] LEVEL (volatility index)")
                level_series.append(col)
                # No transformation - keep original values
            
            # Spreads (credit/sovereign/etc): KEEP AS LEVELS.
            # These are typically mean-reverting in levels; pct_change can explode
            # (esp. when spreads are near 0) and cause GARCH non-convergence.
            elif 'SPREAD' in col.upper():
                print(f"  [{col}] LEVEL (spread)")
                level_series.append(col)
                # No transformation - keep original values

            # Credit spreads (BAML): KEEP AS LEVELS (basis points)
            elif any(x in col for x in ['BAML', 'BAMH']):
                print(f"  [{col}] LEVEL (credit spread in bps)")
                level_series.append(col)
                # No transformation - keep original values

            # Sovereign spreads: KEEP AS LEVELS (stationary, mean-reverting)
            elif any(x in col for x in ['BTP_Bund', 'Bonos_Bund', 'OAT_Bund', '_Spread']):
                print(f"  [{col}] LEVEL (sovereign spread)")
                level_series.append(col)
                # No transformation - keep original values
            
            # Interest rates: KEEP AS LEVELS (policy rates are I(0) around target)
            elif any(x in col for x in ['DFF', 'ECBDFR', 'TEDRATE']):
                print(f"  [{col}] LEVEL (policy/money market rate)")
                level_series.append(col)
                # No transformation - keep original values
            
            # Long-term rates: First difference (I(1) with trend)
            elif any(x in col for x in ['IRLTLT', 'DSWP', 'MORTGAGE']):
                print(f"  [{col}] First difference (long-term rate)")
                df_processed[col] = series.diff()
                return_series.append(col)
            
            # NSS betas: First difference (persistent/I(1))
            elif 'beta' in col.lower():
                print(f"  [{col}] First difference (NSS beta)")
                df_processed[col] = series.diff()
                return_series.append(col)
            
            # Unemployment: First difference (I(1))
            elif any(x in col for x in ['UNRATE', 'LRHUTTTT']):
                print(f"  [{col}] First difference (unemployment rate)")
                df_processed[col] = series.diff()
                return_series.append(col)
            
            # Stock indices, commodities: RETURNS (pct_change)
            elif any(x in col for x in ['SP500', 'DAX', 'IBEX', 'Gold', 'WTI', 'Brent', '_Futures']):
                print(f"  [{col}] Returns (equity/commodity)")
                df_processed[col] = series.pct_change() * 100
                return_series.append(col)
            
            # Prices/CPI/GDP: RETURNS
            elif series.abs().mean() > price_threshold:
                print(f"  [{col}] Returns (price index, mean={series.mean():.1f})")
                df_processed[col] = series.pct_change() * 100
                return_series.append(col)
            
            # Exchange rates: RETURNS
            elif any(x in col for x in ['EUR_', '_USD', '_GBP', '_CHF', '_JPY']):
                print(f"  [{col}] Returns (FX rate)")
                df_processed[col] = series.pct_change() * 100
                return_series.append(col)
            
            # Default: Use as-is if small values (likely already stationary)
            else:
                if series.abs().mean() < 5:
                    print(f"  [{col}] LEVEL (appears stationary)")
                    level_series.append(col)
                else:
                    print(f"  [{col}] Returns (default)")
                    df_processed[col] = series.pct_change() * 100
                    return_series.append(col)

    # Hard guard: pct_change/diff can produce inf/-inf (e.g., divide-by-zero).
    # Replace those with NaN so downstream models can safely drop/ffill.
    inf_count = int(np.isinf(df_processed.to_numpy()).sum())
    if inf_count > 0:
        print(f"\n  [WARN] Found {inf_count} inf/-inf values after transforms; replacing with NaN")
        df_processed = df_processed.replace([np.inf, -np.inf], np.nan)
    
    print(f"\n  Summary: {len(level_series)} LEVEL series, {len(return_series)} RETURN/CHANGE series")
    
    # Step 2: Standardize/demean series based on type
    print("\n[STEP 2] Standardization Strategy")
    print("-" * 50)
    
    demean = config.get('demean', True)
    if demean:
        # For LEVEL series: Full standardization (z-score)
        # This makes VIX, spreads, rates comparable with returns
        if level_series:
            print(f"\n  LEVEL series (VIX, spreads, rates): Z-SCORE standardization")
            for col in level_series:
                if col in df_processed.columns:
                    series = df_processed[col]
                    mean = series.mean()
                    std = series.std()
                    if std is None or np.isnan(std) or np.isclose(std, 0.0):
                        df_processed[col] = series - mean
                        print(f"    [{col}] mean={mean:.2f}, std≈0 -> demean only")
                    else:
                        df_processed[col] = (series - mean) / std
                        print(f"    [{col}] mean={mean:.2f}, std={std:.2f} -> z-score")
        
        # For RETURN/CHANGE series: Simple demeaning (already unit-comparable)
        if return_series:
            print(f"\n  RETURN/CHANGE series: Simple demeaning")
            means = df_processed[return_series].mean()
            df_processed[return_series] = df_processed[return_series] - means
            print(f"    Demeaned {len(return_series)} series")
            print(f"    Mean range: [{means.min():.4f}, {means.max():.4f}]")
    else:
        print(f"  Skipped standardization/demeaning")
    
    # Step 3: Outlier treatment
    print("\n[STEP 3] Outlier Treatment")
    print("-" * 50)
    
    winsorize = config.get('winsorize', True)
    if winsorize:
        lower_pct = config.get('winsorize_lower', 0.5)
        upper_pct = config.get('winsorize_upper', 99.5)
        
        for col in df_processed.columns:
            series = df_processed[col].dropna()
            
            if len(series) < 10:
                continue
            
            # Winsorize using scipy
            limits = (lower_pct/100, (100-upper_pct)/100)
            winsorized = mstats.winsorize(series, limits=limits)
            df_processed.loc[series.index, col] = winsorized
        
        print(f"  Winsorized at {lower_pct}% / {upper_pct}% percentiles")
    
    # Step 4: Dimensionality check
    print("\n[STEP 4] Dimensionality Check")
    print("-" * 50)
    
    use_pca = config.get('use_pca', False)
    n_components = config.get('pca_components', 8)
    
    n_vars = df_processed.shape[1]
    print(f"  Current dimensions: {n_vars} variables")
    
    if n_vars > 20:
        print(f"  [WARNING] {n_vars} variables may be too many for DCC-GARCH")
        print(f"  Consider using PCA to reduce dimensionality")
    
    if use_pca and n_vars > n_components:
        print(f"  Applying PCA to reduce to {n_components} components")
        
        # Standardize before PCA
        scaler = StandardScaler()
        df_scaled = pd.DataFrame(
            scaler.fit_transform(df_processed.dropna()),
            index=df_processed.dropna().index,
            columns=df_processed.columns
        )
        
        # Apply PCA
        pca = PCA(n_components=n_components)
        pca_result = pca.fit_transform(df_scaled)
        
        # Create new dataframe with PCs
        pc_names = [f'PC{i+1}' for i in range(n_components)]
        df_processed = pd.DataFrame(
            pca_result,
            index=df_scaled.index,
            columns=pc_names
        )
        
        print(f"  Explained variance: {pca.explained_variance_ratio_.sum()*100:.1f}%")
        for i, var in enumerate(pca.explained_variance_ratio_):
            print(f"    PC{i+1}: {var*100:.1f}%")
    
    # Step 5: Final summary
    print("\n[STEP 5] Final Data Summary")
    print("-" * 50)
    
    print(f"  Output shape: {df_processed.shape}")
    print(f"  Missing values: {df_processed.isna().sum().sum()}")
    print(f"  Mean: {df_processed.mean().mean():.6f}")
    print(f"  Std: {df_processed.std().mean():.4f}")
    
    # Check correlations
    corr_matrix = df_processed.corr()
    high_corrs = (corr_matrix.abs() > 0.99) & (corr_matrix != 1.0)
    
    if high_corrs.sum().sum() > 0:
        print(f"  [WARNING] Found {high_corrs.sum().sum()//2} pairs with correlation > 0.99")
        print(f"  This may indicate multicollinearity issues")
    
    print("\nPreprocessing complete!")
    
    return df_processed


def main(config_overrides=None, iso_filter=None, block_id_filter=None):
    """
    Main execution function
    
    Fits DCC-GARCH model for THREE-TIER risk aggregation:
    - This module: 95-99th percentile (stress conditions)
    - Time-varying correlations for tail risk
    - Output: Sigma_t matrices for conditional aggregation
    """
    print("="*70)
    print("DCC-GARCH MODEL FITTING - PHASE 1.1")
    print("THREE-TIER STRATEGY: Tier 2 (95-99% Quantile)")
    print("Using REAL Risk Factors + NSS Betas")
    print("="*70)
    print(f"Started at: {datetime.now()}")
    print("\nPurpose: Estimate time-varying correlations for stress periods")
    print("Quantile Range: 95th - 99th percentile")
    print("Method: DCC-GARCH (conditional correlations)")
    
    # Configuration overrides allow experimentation
    config_overrides = config_overrides or {}

    # Paths
    project_root = Path(__file__).parent.parent
    results_dir = Path(__file__).parent / 'results'
    # Optional override so experiments (e.g., sensitivity runs) can write to a
    # separate folder without clobbering the baseline outputs.
    # Supported override locations:
    # - config_overrides['paths']['results_dir']
    # - config_overrides['dcc']['results_dir']
    # - env var DCC_RESULTS_DIR
    try:
        paths_section = config_overrides.get('paths', {}) if isinstance(config_overrides, dict) else {}
        dcc_section = config_overrides.get('dcc', {}) if isinstance(config_overrides, dict) else {}
        override = None
        if isinstance(dcc_section, dict):
            override = dcc_section.get('results_dir')
        if override is None and isinstance(paths_section, dict):
            override = paths_section.get('results_dir')
        if override is None:
            override = os.environ.get('DCC_RESULTS_DIR')

        if override:
            override_path = Path(str(override))
            # Treat relative paths as project-root relative for convenience.
            results_dir = override_path if override_path.is_absolute() else (project_root / override_path)
    except Exception:
        pass

    results_dir.mkdir(parents=True, exist_ok=True)

    # Support both styles:
    # - config_overrides['literature']
    # - config_overrides['dcc']['literature']
    literature_cfg = {}
    dcc_section = config_overrides.get('dcc', {})
    if isinstance(dcc_section, dict) and isinstance(dcc_section.get('literature'), dict):
        literature_cfg.update(dcc_section.get('literature') or {})
    top_level_lit = config_overrides.get('literature')
    if isinstance(top_level_lit, dict):
        literature_cfg.update(top_level_lit)
    literature_enabled = bool(literature_cfg.get('enabled', False)) or (os.environ.get('DCC_LITERATURE', '0') == '1')
    literature_panel_path = literature_cfg.get('panel_path')
    literature_blocks_path = literature_cfg.get('block_definitions_path')
    literature_skip_nss = bool(literature_cfg.get('skip_nss_join', True))
    if literature_enabled and not literature_panel_path:
        # Default to the Step-3 literature artifact.
        literature_panel_path = project_root / 'analysis_outputs' / 'literature_factors' / 'block_factors.within_block.csv'
    if literature_enabled and not literature_blocks_path:
        literature_blocks_path = project_root / 'analysis_outputs' / 'literature_factors' / 'country_block_definition.within_block.json'

    df_stress = None
    if literature_enabled:
        panel_path = Path(literature_panel_path)
        print("\n[INFO] Literature mode enabled")
        print(f"[INFO] Loading literature factor panel: {panel_path}")
        if not panel_path.exists():
            raise FileNotFoundError(f"Literature factor panel not found: {panel_path}")
        df_stress = pd.read_csv(panel_path, index_col=0, parse_dates=True).sort_index()
        df_stress = df_stress.dropna(axis=1, how='all')
        print(f"[OK] Loaded literature panel: {df_stress.shape[0]} obs, {df_stress.shape[1]} vars")
    
    # Load the canonical merged stress indicator panel.
    # In literature mode, we should *not* overwrite the literature factor panel.
    if df_stress is None:
        print("\nLoading canonical stress indicator panel...")
        canonical_candidates = [
            project_root / "stress_indicators_expanded.csv",
            project_root / "data" / "stress_indicators_expanded.csv",
        ]
        for candidate in canonical_candidates:
            if not candidate.exists():
                continue
            try:
                tmp = pd.read_csv(candidate, index_col="Date", parse_dates=True)
            except Exception:
                tmp = pd.read_csv(candidate, index_col=0, parse_dates=True)
            tmp = tmp.sort_index()
            # Drop any fully-empty columns to avoid downstream surprises.
            tmp = tmp.dropna(axis=1, how="all")
            df_stress = tmp
            print(f"[OK] Loaded canonical panel: {candidate} ({df_stress.shape[0]} obs, {df_stress.shape[1]} vars)")
            break

    # Fallback: legacy data_pipeline loader (kept for backwards compatibility).
    if df_stress is None:
        print("\n[WARN] Canonical panel not found; falling back to data_pipeline loader...")
        try:
            sys.path.insert(0, str(project_root))
            from data_pipeline import load_stress_indicators
            # NOTE: NSS betas loaded from extracted CSV file (line ~711) to bypass Unicode error

            print("Loading stress indicators from integrated pipeline...")
            datasets = load_stress_indicators(upsample_to_daily=True, fill_limit_days=92)

            df_stress = datasets['combined']
            print(f"[OK] Loaded stress indicators: {df_stress.shape[0]} observations, {df_stress.shape[1]} variables")

            if 'fred' in datasets:
                print(f"  - FRED indicators: {datasets['fred'].shape[1]} series")
            if 'yahoo' in datasets:
                print(f"  - Yahoo market data: {datasets['yahoo'].shape[1]} series")
            if 'spreads' in datasets:
                print(f"  - Sovereign spreads: {datasets['spreads'].shape[1]} series")
        except Exception as e:
            df_stress = None
            print(f"[WARNING] Could not load from data_pipeline: {e}")
            
    if df_stress is None:
        print("\nTrying fallback: direct file loading...")
        
        # Fallback: Try to load directly from files
        stress_data_dir = project_root / "output" / "trial data folder" / "stress_indicators"
        
        fred_path = stress_data_dir / "fred_stress_indicators.csv"
        yahoo_path = stress_data_dir / "yahoo_market_data.csv"
        spreads_path = stress_data_dir / "sovereign_spreads.csv"
        
        dfs = []
        if fred_path.exists():
            fred_df = pd.read_csv(fred_path, index_col='date', parse_dates=True)
            dfs.append(fred_df)
            print(f"[OK] Loaded FRED data: {fred_df.shape}")
        
        if yahoo_path.exists():
            yahoo_df = pd.read_csv(yahoo_path, index_col=0, parse_dates=True)
            dfs.append(yahoo_df)
            print(f"[OK] Loaded Yahoo data: {yahoo_df.shape}")
        
        if spreads_path.exists():
            spreads_df = pd.read_csv(spreads_path, index_col=0, parse_dates=True)
            dfs.append(spreads_df)
            print(f"[OK] Loaded spreads data: {spreads_df.shape}")
        
        if not dfs:
            raise FileNotFoundError("Could not load any data files")
        
        df = pd.concat(dfs, axis=1)
        print(f"\n[OK] Combined data: {df.shape}")

        df_stress = df

    if df_stress is None:
        raise FileNotFoundError("No stress indicator dataset could be loaded (canonical panel and fallbacks all failed).")

    if not (literature_enabled and literature_skip_nss):
        # Load NSS betas (20 factors: 4 betas × 5 countries)
        print("\nLoading NSS beta risk factors...")
        # HOTFIX: Load from extracted file instead of data_pipeline (Unicode error bypass)
        nss_file = Path(__file__).parent / "nss_betas_extracted.csv"
        if not nss_file.exists():
            raise FileNotFoundError(f"NSS betas file not found: {nss_file}")
        df_nss = pd.read_csv(nss_file, index_col=0, parse_dates=True)
        print(f"[OK] Loaded NSS betas: {df_nss.shape[0]} observations, {df_nss.shape[1]} variables")
        print(f"  - Countries: {df_nss.shape[1] // 4} (4 betas each)")

        # Combine stress indicators with NSS betas
        print("\nCombining stress indicators + NSS betas...")
        common_index = df_stress.index.intersection(df_nss.index)
        if common_index.empty:
            raise ValueError("No overlapping dates between stress indicator panel and NSS betas")

        overlap_cols = set(df_stress.columns).intersection(df_nss.columns)
        if overlap_cols:
            missing_beta_cols = [c for c in df_nss.columns if c not in df_stress.columns]
            if missing_beta_cols:
                df_stress = df_stress.join(df_nss[missing_beta_cols], how="left")
            df = df_stress.loc[common_index].copy()
            print(
                f"[INFO] Canonical panel already contains {len(overlap_cols)} NSS beta columns; using canonical values and aligning on common dates"
            )
        else:
            df = df_stress.loc[common_index].join(df_nss.loc[common_index], how="inner")

        print(f"[OK] Combined dataset: {df.shape[0]} observations, {df.shape[1]} variables")
        print(f"  - Stress indicators: {df_stress.shape[1]}")
        print(f"  - NSS betas: {df_nss.shape[1]}")
        print(f"  - Total risk factors: {df.shape[1]}")
    else:
        df = df_stress.copy()
        print("\n[INFO] Literature mode: skipping NSS beta join")
        print(f"[OK] Using input panel: {df.shape[0]} observations, {df.shape[1]} variables")
    
    # Data cleaning
    cleaning_config = config_overrides.get('cleaning', {})
    ffill_primary = cleaning_config.get('ffill_limit', 5)
    ffill_secondary = cleaning_config.get('second_ffill_limit', 10)
    row_missing_threshold = cleaning_config.get('row_missing_threshold', 50)
    # Important for upsampled monthly/quarterly series: compute missingness after
    # applying a step-hold fill, otherwise sparse series will look "95% missing"
    # and get dropped before they ever get a chance to be forward-filled.
    prefill_limit = cleaning_config.get('prefill_limit_for_missing_pct', None)
    try:
        prefill_limit = int(prefill_limit) if prefill_limit is not None else None
    except Exception:
        prefill_limit = None
    if prefill_limit is None:
        prefill_limit = max(int(ffill_primary), int(ffill_secondary), 92)

    print("\nData cleaning...")
    print(f"  Initial shape: {df.shape}")
    
    # Remove columns with too much missing data (after prefill)
    df_prefill = df.ffill(limit=prefill_limit)
    missing_pct = df_prefill.isna().sum() / len(df_prefill) * 100
    print("\nMissing data summary:")
    print(f"  Variables with <5% missing: {(missing_pct < 5).sum()}")
    print(f"  Variables with 5-20% missing: {((missing_pct >= 5) & (missing_pct < 20)).sum()}")
    print(f"  Variables with 20-50% missing: {((missing_pct >= 20) & (missing_pct < 50)).sum()}")
    print(f"  Variables with 50-95% missing: {((missing_pct >= 50) & (missing_pct < 95)).sum()}")
    print(f"  Variables with >95% missing: {(missing_pct >= 95).sum()}")
    
    # ✅ CHANGED: Increased threshold from 50% to 95%
    # Since we now upsample monthly/quarterly data to daily with forward-fill,
    # we should have much less missing data. Keep only truly empty series.
    missing_threshold = 95  # Only remove if >95% missing (essentially empty series)
    good_vars = missing_pct < missing_threshold
    df_filtered = df_prefill.loc[:, good_vars]
    
    if df_filtered.shape[1] < df.shape[1]:
        removed_count = df.shape[1] - df_filtered.shape[1]
        print(f"\n[INFO] Removed {removed_count} variables with >{missing_threshold}% missing data")
        print(f"  Remaining: {df_filtered.shape[1]} variables")
        if removed_count > 0:
            removed_vars = df.columns[~good_vars].tolist()
            print(f"  Removed variables: {removed_vars}")
    else:
        print(f"\n[INFO] All {df.shape[1]} variables retained")
    
    # Forward fill remaining gaps (limited)
    df_filtered = df_filtered.ffill(limit=ffill_primary)
    
    # Instead of dropping all rows with ANY missing value,
    # only drop rows where ALL values are missing
    df_filtered = df_filtered.dropna(how='all')
    
    # Then forward-fill again to handle any remaining small gaps
    df_filtered = df_filtered.ffill(limit=ffill_secondary)
    
    # Finally, only drop rows that STILL have >50% missing across all variables
    row_missing_pct = df_filtered.isna().sum(axis=1) / df_filtered.shape[1] * 100
    df_filtered = df_filtered[row_missing_pct < row_missing_threshold]
    
    if len(df_filtered) < 500:
        print(f"\n[WARNING] Only {len(df_filtered)} observations after cleaning. Check data quality.")
    else:
        print(f"\n[OK] {len(df_filtered)} observations retained after cleaning")
    
    print(f"  Final shape: {df_filtered.shape}")
    
    # Preprocessing configuration
    if literature_enabled:
        # Literature factors are already standardized; default to no extra transforms.
        preprocess_config = {
            'auto_convert_to_returns': False,
            'price_threshold': 50,
            'demean': False,
            'winsorize': False,
            'winsorize_lower': 0.5,
            'winsorize_upper': 99.5,
            'use_pca': False,
            'pca_components': 8
        }
    else:
        preprocess_config = {
            'auto_convert_to_returns': True,
            'price_threshold': 50,
            'demean': True,
            'winsorize': True,
            'winsorize_lower': 0.5,
            'winsorize_upper': 99.5,
            'use_pca': False,  # Set to True if too many variables
            'pca_components': 8
        }
    preprocess_config.update(config_overrides.get('preprocess', {}))
    
    # Preprocess data
    df_preprocessed = preprocess_data(df_filtered, preprocess_config)

    # Optional: restrict the global universe to the series present in the current
    # post-threshold + post-harmonization country blocks.
    refit_mode = os.environ.get('DCC_REFIT_ONLY', '0') == '1'
    block_definitions = load_block_definitions(project_root, override_path=(literature_blocks_path if literature_enabled else None))

    # Optional: restrict block fitting to specific ISOs / blocks for faster reruns.
    if block_definitions and (iso_filter or block_id_filter):
        before_n = len(block_definitions)
        filtered = []
        for block in block_definitions:
            if iso_filter and str(block.get('iso_code') or '').upper() not in iso_filter:
                continue
            if block_id_filter and str(block.get('block_id') or '') not in block_id_filter:
                continue
            filtered.append(block)
        block_definitions = filtered
        print(f"\n[INFO] Filtered block_definitions: {before_n} -> {len(block_definitions)}")

    # Reconcile block series names with the *actual* columns present in the loaded panel.
    # Some block configs use Yahoo tickers (e.g. ^GDAXI, ^IBEX) that exist in
    # stress_indicators_expanded.csv, but aliasing maps them to friendlier names
    # (e.g. DAX, IBEX_35) which may not be present in this panel. Prefer the raw
    # series code when the aliased name is absent but the raw code exists.
    if block_definitions:
        available_cols = set(df_preprocessed.columns)
        for block in block_definitions:
            raw_series = block.get('raw_series_codes') or []
            resolved_series = block.get('series_codes') or []
            if not raw_series or not resolved_series:
                continue
            if len(raw_series) != len(resolved_series):
                continue

            effective = []
            changed = False
            for raw_name, resolved_name in zip(raw_series, resolved_series):
                if resolved_name in available_cols:
                    effective.append(resolved_name)
                elif raw_name in available_cols:
                    effective.append(raw_name)
                    changed = True
                else:
                    effective.append(resolved_name)

            if changed:
                # Preserve order + drop duplicates
                block['series_codes'] = list(dict.fromkeys(effective))
    block_series_pool = set()
    if block_definitions:
        block_series_pool = {
            series
            for block in block_definitions
            for series in block['series_codes']
        }

    if block_series_pool:
        original_cols = list(df_preprocessed.columns)
        allowed_cols = [c for c in original_cols if c in block_series_pool]
        dropped_cols = [c for c in original_cols if c not in block_series_pool]
        if not allowed_cols:
            raise ValueError("No preprocessed series match the defined country blocks; check configuration.")
        if dropped_cols:
            print(f"\n[INFO] Removing {len(dropped_cols)} preprocessed series not covered by country blocks: {dropped_cols}")
        if len(allowed_cols) < len(original_cols):
            print(
                f"[INFO] Retaining {len(allowed_cols)} block-series from {len(original_cols)} preprocessed variables"
            )
        df_preprocessed = df_preprocessed.loc[:, allowed_cols]
    
    # Initialize DCC-GARCH fitter with regularization hints
    dcc_config = {
        'dcc_reg_penalty': 1000.0,
        'dcc_stationarity_target': 0.90,
        'dcc_a_grid': [0.01, 0.02, 0.03, 0.04],
        'dcc_b_grid': [0.80, 0.85, 0.88, 0.90, 0.92],
        'dcc_max_a_plus_b': 0.98,
        'dcc_max_a': 0.20,
        # Encourage solutions away from the hard upper bound on a.
        # This reduces the observed "a pins at max" behavior without increasing the cap.
        'dcc_large_a_penalty_lambda': 10.0,
        'dcc_large_a_penalty_threshold': 0.15,
        # Overfit action: if a+b is extremely high, shrink dynamic correlations a bit
        # towards unconditional correlation for robustness.
        'dcc_overfit_threshold': 0.975,
        'dcc_overfit_shrinkage': 0.05,
        'adcc_a_grid': [0.01, 0.02, 0.03],
        'adcc_b_grid': [0.80, 0.85, 0.88],
        'adcc_gamma_grid': [0.0, 0.01, 0.02, 0.03],
        # ADCC optimization can be very slow for many blocks; default to robust grid-search.
        # Override via config overrides if you want refinement.
        'adcc_fit_method': 'grid',
        'adcc_max_sum': 0.99,
        'adcc_max_a': 0.20,
        'adcc_large_a_penalty_lambda': 0.0,
        'adcc_large_a_penalty_threshold': 0.20,
        # ADCC guardrail: if gamma is extreme but fit improvement is negligible,
        # force symmetric behavior (gamma->0).
        'adcc_gamma_guardrail_threshold': 0.7,
        'adcc_min_ll_improvement_per_obs': 1e-4,
        'adcc_stationarity_target': 0.92,
        'adcc_reg_penalty': 500.0,
        # K=1 blocks: run univariate GARCH only, mark block completed.
        'allow_k1_blocks': True,
        # Some blocks intentionally reuse country drivers already present in earlier
        # blocks (e.g., sovereign spread used in both public_finance and systemic_stress).
        # Without this, later blocks can collapse to univariate_only.
        'allow_reuse_in_block_keys': ['systemic_stress', 'market_stress'],
    }
    dcc_config.update(config_overrides.get('dcc', {}))

    if block_definitions:
        blocks_root = results_dir / 'blocks'
        blocks_root.mkdir(exist_ok=True)
        # IMPORTANT: assignment should be per-country, not global.
        # Many blocks intentionally share global factors (commodities, EUR crosses,
        # VIX-like indices). A global "already_assigned" pool would cause later
        # countries to lose drivers and blocks to be skipped even when the data
        # exists. We therefore keep assignment pools per ISO.
        unassigned_by_iso = {iso: set(df_preprocessed.columns) for iso in sorted({b.get('iso_code') for b in block_definitions if b.get('iso_code')})}
        block_results = []
        global_garch_results = {}
        global_vol_parts = []
        global_resid_parts = []
        global_corr_parts = []
        global_adcc_corr_parts = []
        dcc_param_rows = []
        adcc_param_rows = []
        global_uncorr = pd.DataFrame(np.nan, index=df_preprocessed.columns, columns=df_preprocessed.columns)

        def run_block(block_meta, series_list):
            block_id = block_meta['block_id']
            block_label = f"{block_id} ({block_meta['country']}:{block_meta['block_key']})"
            block_output_dir = blocks_root / block_id
            block_df = df_preprocessed[series_list].copy()

            # Optional per-block dimensionality control.
            # If a block is large, DCC/ADCC estimation can become unstable / slow.
            max_vars = dcc_config.get('max_block_variables')
            if max_vars is not None:
                try:
                    max_vars = int(max_vars)
                except (TypeError, ValueError):
                    max_vars = None
            if max_vars and block_df.shape[1] > max_vars:
                n_components = dcc_config.get('block_pca_components', max_vars)
                try:
                    n_components = int(n_components)
                except (TypeError, ValueError):
                    n_components = max_vars
                n_components = max(2, min(n_components, block_df.shape[1]))

                fit_df = block_df.dropna(how='any')
                min_rows = dcc_config.get('block_pca_min_rows', 250)
                try:
                    min_rows = int(min_rows)
                except (TypeError, ValueError):
                    min_rows = 250

                if len(fit_df) < min_rows:
                    print(
                        f"  [WARN] Block {block_id} has {block_df.shape[1]} vars but only {len(fit_df)} complete rows (<{min_rows}); skipping PCA reduction"
                    )
                else:
                    print(
                        f"  [INFO] Block {block_id} has {block_df.shape[1]} vars; applying PCA -> {n_components} comps (per-block reduction)"
                    )
                    scaler = StandardScaler()
                    scaled = scaler.fit_transform(fit_df)
                    pca = PCA(n_components=n_components)
                    pcs = pca.fit_transform(scaled)
                    pc_names = [f'PC{i+1}' for i in range(n_components)]
                    reduced = pd.DataFrame(pcs, index=fit_df.index, columns=pc_names)
                    block_output_dir.mkdir(parents=True, exist_ok=True)
                    loadings = pd.DataFrame(
                        pca.components_.T,
                        index=fit_df.columns,
                        columns=pc_names,
                    )
                    loadings.to_csv(block_output_dir / 'pca_loadings.csv', index=True)
                    explained = pd.DataFrame({
                        'component': pc_names,
                        'explained_variance_ratio': pca.explained_variance_ratio_,
                    })
                    explained.to_csv(block_output_dir / 'pca_explained_variance.csv', index=False)
                    meta = {
                        'mode': 'pca',
                        'original_variables': list(fit_df.columns),
                        'n_original': int(fit_df.shape[1]),
                        'n_components': int(n_components),
                        'explained_variance_ratio_sum': float(pca.explained_variance_ratio_.sum()),
                        'min_complete_rows': int(min_rows),
                        'complete_rows_used': int(len(fit_df)),
                    }
                    (block_output_dir / 'dimensionality_control.json').write_text(
                        json.dumps(meta, indent=2),
                        encoding='utf-8',
                    )
                    block_df = reduced

            if block_df.shape[1] < 2:
                fitter, metrics = execute_univariate_pipeline(block_df, block_output_dir, dcc_config, block_label, refit_mode)
            else:
                fitter, metrics = execute_dcc_pipeline(block_df, block_output_dir, dcc_config, block_label, refit_mode)
            global_garch_results.update(fitter.garch_results)
            if fitter.conditional_volatilities is not None:
                global_vol_parts.append(fitter.conditional_volatilities)
            if fitter.standardized_residuals is not None:
                global_resid_parts.append(fitter.standardized_residuals)
            if getattr(fitter, 'correlation_time_series', None) is not None:
                renamed = fitter.correlation_time_series.rename(columns=lambda c: f"{block_id}__{c}")
                global_corr_parts.append(renamed)
            if getattr(fitter, 'adcc_correlation_time_series', None) is not None:
                renamed_adcc = fitter.adcc_correlation_time_series.rename(columns=lambda c: f"{block_id}__{c}")
                global_adcc_corr_parts.append(renamed_adcc)

            dcc_params = fitter.dcc_params or {}
            adcc_params = getattr(fitter, 'adcc_params', None) or {}
            dcc_param_rows.append({
                'block_id': block_id,
                'label': block_label,
                'a': dcc_params.get('a'),
                'b': dcc_params.get('b'),
                'a_plus_b': (dcc_params.get('a') or 0.0) + (dcc_params.get('b') or 0.0)
            })
            adcc_param_rows.append({
                'block_id': block_id,
                'label': block_label,
                'gamma': adcc_params.get('gamma', 0.0)
            })
            uncorr = fitter.unconditional_correlation
            if uncorr is not None:
                valid_series = [s for s in series_list if s in uncorr.index]
                if valid_series:
                    overlap = uncorr.loc[valid_series, valid_series]
                    global_uncorr.loc[valid_series, valid_series] = overlap.values
            metrics.update({
                'block_label': block_label,
                'status': 'completed'
            })
            return metrics

        assigned_global = set()

        for block in block_definitions:
            block_id = block['block_id']
            block_label = f"{block_id} ({block['country']}:{block['block_key']})"
            iso = str(block.get('iso_code') or '')
            if iso not in unassigned_by_iso:
                unassigned_by_iso[iso] = set(df_preprocessed.columns)
            iso_unassigned = unassigned_by_iso[iso]
            existing = [s for s in block['series_codes'] if s in df_preprocessed.columns]

            reuse_keys = set(dcc_config.get('allow_reuse_in_block_keys') or [])
            reuse_allowed = block.get('block_key') in reuse_keys
            if reuse_allowed:
                # Keep full existing set for this block, even if already used earlier
                # for the same ISO. This is intentional for certain block semantics.
                assigned = list(existing)
                duplicates = []
            else:
                assigned = [s for s in existing if s in iso_unassigned]
                duplicates = [s for s in existing if s not in iso_unassigned]
            missing = [s for s in block['series_codes'] if s not in df_preprocessed.columns]
            print(f"\nPreparing block {block_id}: {block['country']} - {block['block_key']}")
            if missing:
                print(f"  Missing {len(missing)} variables (not ingested): {missing}")
            if duplicates:
                print(f"  {len(duplicates)} variables already assigned to earlier blocks: {duplicates}")
            allow_k1 = bool(dcc_config.get('allow_k1_blocks', True))
            if len(assigned) < 2 and not (allow_k1 and len(assigned) == 1):
                print(f"  Skipping block {block_id}: need at least 2 new series for DCC")
                block_results.append({
                    'block_id': block_id,
                    'label': block_label,
                    'country': block['country'],
                    'block_key': block['block_key'],
                    'status': 'skipped',
                    'reason': 'insufficient_new_series',
                    'min_required_new_series': 2,
                    'n_series_existing': len(existing),
                    'n_series_new': len(assigned),
                    'n_series_duplicates': len(duplicates),
                    'n_series_missing': len(missing),
                    'missing_series': missing,
                    'already_assigned': duplicates,
                    'series_covered': len(assigned)
                })
                continue
            iso_unassigned -= set(assigned)
            assigned_global |= set(assigned)
            metrics = run_block(block, assigned)
            metrics.update({
                'block_id': block_id,
                'country': block['country'],
                'block_key': block['block_key'],
                'series_covered': len(assigned),
                'missing_series': missing,
                'already_assigned': duplicates
            })
            block_results.append(metrics)

        never_assigned = set(df_preprocessed.columns) - assigned_global
        if never_assigned:
            print(
                f"[WARN] {len(never_assigned)} preprocessed series were never used in any fitted block: {sorted(never_assigned)}"
            )

        summary_rows = []
        for row in block_results:
            row_copy = dict(row)
            missing = row_copy.get('missing_series') or []
            row_copy['missing_series'] = ';'.join(missing)
            dupes = row_copy.get('already_assigned') or []
            row_copy['already_assigned'] = ';'.join(dupes)
            summary_rows.append(row_copy)

        summary_df = pd.DataFrame(summary_rows)
        summary_csv = results_dir / 'block_fit_summary.csv'
        summary_df.to_csv(summary_csv, index=False)
        summary_json = results_dir / 'block_fit_metrics.json'
        with open(summary_json, 'w') as fjson:
            json.dump({'blocks': block_results}, fjson, indent=2)

        if global_garch_results:
            pd.DataFrame(global_garch_results).T.to_csv(results_dir / 'dcc_garch_parameters.csv')
        if global_vol_parts:
            global_vol = pd.concat(global_vol_parts, axis=1)
            global_vol = global_vol.loc[:, [c for c in df_preprocessed.columns if c in global_vol.columns]]
            global_vol.to_csv(results_dir / 'conditional_volatilities.csv')
        if global_resid_parts:
            global_resid = pd.concat(global_resid_parts, axis=1)
            global_resid = global_resid.loc[:, [c for c in df_preprocessed.columns if c in global_resid.columns]]
            global_resid.to_csv(results_dir / 'standardized_residuals.csv')
        if global_corr_parts:
            # Global correlation time series can become extremely wide (sum of all
            # pairwise correlations across many blocks). Writing this as CSV can
            # be very slow and is not required for Step 11 (per-block outputs are
            # already saved under results/blocks/<block_id>/).
            try:
                total_cols = int(sum(int(getattr(p, 'shape', (0, 0))[1]) for p in global_corr_parts))
            except Exception:
                total_cols = 0
            if total_cols and total_cols > 2000:
                print(f"[WARN] Skipping global correlation_time_series.csv (too wide: {total_cols} columns)")
            else:
                pd.concat(global_corr_parts, axis=1).to_csv(results_dir / 'correlation_time_series.csv')
        if global_adcc_corr_parts:
            try:
                total_cols = int(sum(int(getattr(p, 'shape', (0, 0))[1]) for p in global_adcc_corr_parts))
            except Exception:
                total_cols = 0
            if total_cols and total_cols > 2000:
                print(f"[WARN] Skipping global adcc_correlation_time_series.csv (too wide: {total_cols} columns)")
            else:
                pd.concat(global_adcc_corr_parts, axis=1).to_csv(results_dir / 'adcc_correlation_time_series.csv')
        if not global_uncorr.isna().all().all():
            np.fill_diagonal(global_uncorr.values, 1.0)
            global_uncorr = global_uncorr.fillna(0.0)
            global_uncorr.to_csv(results_dir / 'unconditional_correlation_matrix.csv')
        if dcc_param_rows:
            pd.DataFrame(dcc_param_rows).to_csv(results_dir / 'dcc_parameters.csv', index=False)
        if adcc_param_rows:
            pd.DataFrame(adcc_param_rows).to_csv(results_dir / 'adcc_parameters.csv', index=False)

        completed_blocks = [b for b in block_results if b.get('status') == 'completed']
        def mean_ignore_none(values):
            vals = [float(v) for v in values if v is not None]
            return sum(vals) / len(vals) if vals else None
        def sanitize_json(obj):
            if isinstance(obj, np.generic):
                return obj.item()
            if isinstance(obj, dict):
                return {k: sanitize_json(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitize_json(v) for v in obj]
            return obj
        total_series_modeled = sum(b.get('series_covered', 0) for b in completed_blocks)
        total_updates = sum(b.get('total_updates', 0) or 0 for b in completed_blocks)
        successful_updates = sum(b.get('successful_updates', 0) or 0 for b in completed_blocks)
        agg_metrics = {
            'a': mean_ignore_none([b.get('dcc_a') for b in completed_blocks]),
            'b': mean_ignore_none([b.get('dcc_b') for b in completed_blocks]),
            'a_plus_b': mean_ignore_none([b.get('a_plus_b') for b in completed_blocks]),
            'update_success_rate': (successful_updates / total_updates) if total_updates else None,
            'smoothness_mean': mean_ignore_none([b.get('smoothness_mean') for b in completed_blocks]),
            'smoothness_75pct': mean_ignore_none([b.get('smoothness_75pct') for b in completed_blocks]),
            'successful_updates': successful_updates,
            'total_updates': total_updates,
            'blocks_modeled': len(completed_blocks),
            'series_modeled': total_series_modeled,
            'overfitting_blocks': [b['block_id'] for b in completed_blocks if b.get('overfitting_flag')]
        }
        agg_metrics_path = results_dir / 'fit_metrics.json'
        with open(agg_metrics_path, 'w') as fh:
            json.dump(sanitize_json(agg_metrics), fh, indent=2)

        summary = {
            'timestamp': datetime.now().isoformat(),
            'blocks_modeled': len(completed_blocks),
            'series_modeled': total_series_modeled,
            'block_summary': summary_rows
        }
        with open(results_dir / 'fit_summary.json', 'w') as fh:
            json.dump(sanitize_json(summary), fh, indent=2)

        print(f"\nBlock-level results saved: {summary_csv}, {summary_json}")
        if agg_metrics['overfitting_blocks']:
            print(f"Overfitting warning: {len(agg_metrics['overfitting_blocks'])} block(s) have a+b >= 0.98")
        else:
            print("No block exceeds a+b >= 0.98; overfitting flag cleared")

        return None

    fitter, _ = execute_dcc_pipeline(df_preprocessed, results_dir, dcc_config, 'global', refit_mode)
    return fitter


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fit the DCC-GARCH/ADCC pipeline with optional override settings"
    )
    parser.add_argument(
        "--config-overrides",
        type=str,
        help="Path to a JSON file that overrides preprocess/cleaning/DCC settings",
        default=None
    )
    parser.add_argument(
        "--skip-adcc",
        action="store_true",
        help="Skip ADCC estimation (runs DCC only). Useful for faster/stabler reruns when ADCC is not required.",
    )
    parser.add_argument(
        "--isos",
        type=str,
        default=None,
        help="Optional comma-separated ISO country codes to fit (e.g. 'USA,DEU').",
    )
    parser.add_argument(
        "--block-ids",
        type=str,
        default=None,
        help="Optional comma-separated block_ids to fit (e.g. 'usa_systemic_stress,esp_financial_markets').",
    )
    args = parser.parse_args()
    overrides = {}
    if args.config_overrides:
        with open(args.config_overrides, 'r', encoding='utf-8') as f:
            overrides = json.load(f)

    if args.skip_adcc:
        overrides.setdefault('dcc', {})
        overrides['dcc']['skip_adcc'] = True

    iso_filter = None
    if args.isos:
        iso_filter = {x.strip().upper() for x in str(args.isos).split(',') if x.strip()}
    block_id_filter = None
    if args.block_ids:
        block_id_filter = {x.strip() for x in str(args.block_ids).split(',') if x.strip()}

    fitter = main(config_overrides=overrides, iso_filter=iso_filter, block_id_filter=block_id_filter)
