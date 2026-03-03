"""Run the Phase 3 ISO-level GARCH + ADCC diagnostics pipeline.

This script follows the implementation plan: it loads the standardized factor shortlists, fits
univariate GARCH/GJR models per driver, captures diagnostics (AIC/BIC, Ljung-Box, ARCH-LM,
persistence), saves the standardized residuals and conditional volatilities, and feeds the
residuals into an ADCC/DCC estimation to build Σₜ = Dₜ Rₜ Dₜ.

Outputs settle under `analysis_outputs/diagnostics` and `analysis_outputs/diag_corr` so the
scenario engine can pick up the diagnostics, correlations, and covariances per country/ISO.
"""

from __future__ import annotations

import argparse
import json
import warnings
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import joblib
import numpy as np
import pandas as pd
from arch import arch_model
from joblib import dump
from scipy.linalg import eigh
from statsmodels.stats.diagnostic import acorr_ljungbox, het_arch

warnings.filterwarnings("ignore")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FACTOR_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation"
DIAG_DIR = PROJECT_ROOT / "analysis_outputs" / "diagnostics"
DIAG_CORR_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr"
MODELS_DIR = PROJECT_ROOT / "models"
GARCH_MODELS_DIR = MODELS_DIR / "garch"
ADCC_MODELS_DIR = MODELS_DIR / "adcc"
VOL_SEL_DIR = (
    PROJECT_ROOT
    / "Volatility_MeanReversion"
    / "outputs"
    / "volatility_selection"
)

REQUIRED_SERIES = {"SOFR_3m", "V2X"}

for directory in (DIAG_DIR, DIAG_CORR_DIR, GARCH_MODELS_DIR, ADCC_MODELS_DIR):
    directory.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fit ISO-level GARCH and ADCC diagnostics"
    )

    parser.add_argument(
        "--isos",
        default="ITA,DEU,FRA,ESP,USA",
        help="Comma-separated ISO codes to process (default: ITA,DEU,FRA,ESP,USA)",
    )
    parser.add_argument(
        "--model",
        choices=["adcc", "dcc"],
        default="adcc",
        help="Correlation specification to estimate",
    )
    parser.add_argument(
        "--use-gjr",
        action="store_true",
        help="Estimate GJR-GARCH (leverage term) instead of vanilla GARCH",
    )
    parser.add_argument(
        "--clip",
        type=float,
        default=10.0,
        help="Clip standardized residuals to avoid explosive updates",
    )
    parser.add_argument(
        "--grid-a",
        nargs="+",
        type=float,
        help="Grid of 'a' values for correlation estimation",
    )
    parser.add_argument(
        "--grid-b",
        nargs="+",
        type=float,
        help="Grid of 'b' values for correlation estimation",
    )
    parser.add_argument(
        "--grid-g",
        nargs="+",
        type=float,
        help="Grid of 'g' values (ADCC asymmetry parameter)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing diagnostics/correlation outputs",
    )

    parser.add_argument(
        "--coverage-threshold",
        type=float,
        default=0.5,
        help="Minimum coverage (fraction of non-missing dates) a series must have to be included",
    )
    parser.add_argument(
        "--min-residual-rows",
        type=int,
        default=40,
        help="Minimum number of standardized-residual rows before ADCC runs (shrinkage fallback otherwise)",
    )
    parser.add_argument(
        "--vol-models",
        nargs="+",
        default=["GARCH", "FIGARCH"],
        help="Volatility models to try on each series (GARCH, FIGARCH, HARCH, etc.)",
    )
    parser.add_argument(
        "--garch-p",
        type=int,
        default=1,
        help="ARCH order p for GARCH/FIGARCH fits (default: 1)",
    )
    parser.add_argument(
        "--garch-q",
        type=int,
        default=1,
        help="GARCH order q for GARCH/FIGARCH fits (default: 1)",
    )
    parser.add_argument(
        "--mean-lags",
        type=int,
        choices=[0, 1, 2],
        default=0,
        help="Number of AR lags in the mean equation (0 = zero mean, 1/2 = AR(1)/AR(2))",
    )
    parser.add_argument(
        "--shrinkage",
        type=float,
        default=0.2,
        help="Shrinkage weight applied if ADCC cannot run (0=no shrinkage, 1=identity)",
    )
    parser.add_argument(
        "--low-acorr-pvalue",
        type=float,
        default=0.0,
        help="Ljung-Box p-value threshold for the low-autocorrelation subset (0 disables)",
    )
    parser.add_argument(
        "--use-filtered-factors",
        action="store_true",
        help="Use USA_factors_filtered.csv (GDPC1/BAMLC0A4CBBB removed) instead of USA_factors.csv",
    )

    args = parser.parse_args()
    args.vol_models = [model.upper() for model in args.vol_models]
    return args


def load_factor_data(iso: str, use_filtered: bool = False) -> pd.DataFrame:
    # Use filtered CSV if flag is set and we're processing USA
    if use_filtered and iso == "USA":
        path = FACTOR_DIR / "USA_factors_filtered.csv"
    else:
        path = FACTOR_DIR / f"{iso}_factors.csv"
    
    if not path.exists():
        raise FileNotFoundError(f"Factor matrix missing for {iso}: {path}")

    header = pd.read_csv(path, nrows=0)
    has_date_col = "date" in header.columns
    if has_date_col:
        df = pd.read_csv(path, index_col="date", parse_dates=["date"])
    else:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
    if "date" not in df.index.names:
        df.index.name = "date"
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.dropna(how="all")
    drop_unused = [col for col in df.columns if col.startswith("Unnamed:")]
    if drop_unused:
        df = df.drop(columns=drop_unused)
    return df


def drop_low_coverage_series(data: pd.DataFrame, threshold: float) -> pd.DataFrame:
    if threshold <= 0:
        return data
    threshold = min(max(threshold, 0.0), 1.0)
    coverage = data.notna().mean()
    low_coverage = coverage[coverage < threshold]
    optional_dropped = [col for col in low_coverage.index if col not in REQUIRED_SERIES]
    dropped = [col for col in optional_dropped]
    required_kept = [col for col in data.columns if col in REQUIRED_SERIES]
    if required_kept:
        print(f"  Keeping required series regardless of coverage: {', '.join(required_kept)}")
    if dropped:
        print(
            f"  Dropping {len(dropped)} low-coverage factors (<{threshold:.2f}): "
            f"{', '.join(dropped)}"
        )
    kept = [col for col in coverage.index if coverage[col] >= threshold or col in REQUIRED_SERIES]
    if len(kept) < 2:
        raise ValueError("Not enough high-coverage series to run diagnostics")
    return data[kept]


def trim_residuals_for_min_rows(
    residuals: pd.DataFrame, cond_vols: pd.DataFrame, min_rows: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, List[str]]:
    trimmed_resid = residuals.copy()
    trimmed_vol = cond_vols.reindex(columns=trimmed_resid.columns)
    cleaned = trimmed_resid.dropna(how="any")
    dropped: List[str] = []
    while cleaned.shape[0] < min_rows:
        if trimmed_resid.shape[1] <= 2:
            break
        nan_counts = trimmed_resid.isna().sum()
        if nan_counts.max() == 0:
            break
        worst = nan_counts.idxmax()
        trimmed_resid = trimmed_resid.drop(columns=[worst])
        trimmed_vol = trimmed_vol.drop(columns=[worst], errors="ignore")
        dropped.append(worst)
        cleaned = trimmed_resid.dropna(how="any")
    return trimmed_resid, trimmed_vol, cleaned, dropped


def _get_low_acorr_series(diag_df: pd.DataFrame, cleaned: pd.DataFrame, threshold: float) -> List[str]:
    if threshold <= 0 or cleaned.empty:
        return []
    selected: List[str] = []
    for column in cleaned.columns:
        matches = diag_df.loc[diag_df.get("series") == column]
        if matches.empty:
            continue
        lb_values = matches["ljung_box_pvalue"].dropna()
        if lb_values.empty:
            continue
        if float(lb_values.iloc[-1]) >= threshold:
            selected.append(column)
    return selected


def evaluate_low_autocorr_subset(
    cleaned: pd.DataFrame,
    diag_df: pd.DataFrame,
    threshold: float,
    model_type: str,
    grid_a: Optional[Sequence[float]],
    grid_b: Optional[Sequence[float]],
    grid_g: Optional[Sequence[float]],
) -> Optional[Dict[str, Any]]:
    columns = _get_low_acorr_series(diag_df, cleaned, threshold)
    if len(columns) < 2:
        return None
    subset = cleaned[columns]
    params, loglik = grid_search_adcc(subset, model_type, grid_a, grid_b, grid_g)
    persistence = params[0] + params[1] + 0.5 * params[2]
    return {
        "series": columns,
        "params": params,
        "loglik": loglik,
        "persistence": persistence,
    }


def build_shrinkage_correlation_series(
    cleaned: pd.DataFrame, shrinkage: float
) -> pd.DataFrame:
    if cleaned.empty:
        raise ValueError("No residuals available for shrinkage correlation")
    shrinkage = min(max(shrinkage, 0.0), 1.0)
    cov = cleaned.cov()
    diag_vals = np.diag(cov.to_numpy())
    avg_var = float(np.nanmean(diag_vals)) if not np.isnan(np.nanmean(diag_vals)) else 1.0
    target = np.eye(len(cov)) * (avg_var if not np.isnan(avg_var) else 1.0)
    shrink_cov = shrinkage * target + (1 - shrinkage) * cov.to_numpy()
    diag_sq = np.sqrt(np.diag(shrink_cov))
    diag_sq = np.where(diag_sq < 1e-8, 1e-8, diag_sq)
    corr_matrix = shrink_cov / np.outer(diag_sq, diag_sq)
    corr_matrix = np.clip(corr_matrix, -0.999, 0.999)
    np.fill_diagonal(corr_matrix, 1.0)
    columns = cleaned.columns
    pair_labels = [f"{columns[i]}_{columns[j]}" for i, j in combinations(range(len(columns)), 2)]
    base_values = [corr_matrix[i, j] for i, j in combinations(range(len(columns)), 2)]
    corr_array = np.tile(base_values, (len(cleaned), 1))
    return pd.DataFrame(corr_array, index=cleaned.index, columns=pair_labels)


def fit_series_garch(
    series: pd.Series,
    vol_models: Sequence[str],
    use_gjr: bool,
    clip_resid: float,
    garch_p: int,
    garch_q: int,
    mean_lags: int,
) -> Tuple[pd.Series, pd.Series, Dict[str, Any], Optional[Any], str]:
    scaled = series.dropna() * 100
    if len(scaled) < 40:
        raise ValueError("Too few observations for reliable GARCH")

    best_result = None
    best_vol_model = "GARCH"
    best_bic = np.inf
    mean_option = "Zero" if mean_lags == 0 else "AR"
    base_kwargs: Dict[str, Any] = dict(p=garch_p, q=garch_q, mean=mean_option, dist="StudentsT")
    if mean_option == "AR":
        base_kwargs["lags"] = mean_lags
    else:
        base_kwargs.pop("lags", None)
    for vol_model in vol_models:
        vol_key = vol_model.upper()
        kwargs = dict(base_kwargs)
        kwargs["vol"] = vol_key
        if vol_key == "GARCH":
            kwargs["o"] = 1 if use_gjr else 0
        else:
            kwargs.pop("o", None)

        try:
            model = arch_model(scaled, **kwargs)
            result = model.fit(disp="off", show_warning=False)
        except Exception:
            continue

        bic = getattr(result, "bic", np.inf)
        if bic < best_bic:
            best_bic = bic
            best_result = result
            best_vol_model = vol_key

    if best_result is None:
        raise ValueError("No volatility model converged for this series")

    std_resid = best_result.std_resid
    if clip_resid is not None:
        std_resid = np.clip(std_resid, -clip_resid, clip_resid)
    cond_vol = best_result.conditional_volatility / 100

    params = best_result.params.to_dict()
    diagnostics = dict(
        aic=best_result.aic,
        bic=best_result.bic,
        loglik=best_result.loglikelihood,
        converged=getattr(best_result, "convergence", True),
        nu=params.get("nu", np.nan),
        volatility_model=best_vol_model,
    )

    diagnostics["omega"] = params.get("omega", np.nan)
    alpha_values = [safe_float(value) for key, value in params.items() if key.lower().startswith("alpha")]
    alpha_values = [value for value in alpha_values if not np.isnan(value)]
    beta_values = [safe_float(value) for key, value in params.items() if key.lower().startswith("beta")]
    beta_values = [value for value in beta_values if not np.isnan(value)]
    gamma_candidates = [
        safe_float(value) for key, value in params.items() if key.lower().startswith("gamma")
    ]
    gamma_value = gamma_candidates[0] if gamma_candidates else 0.0
    alpha_sum = sum(alpha_values) if alpha_values else 0.0
    beta_sum = sum(beta_values) if beta_values else 0.0
    diagnostics["alpha_sum"] = alpha_sum
    diagnostics["beta_sum"] = beta_sum
    diagnostics["alpha"] = safe_float(params.get("alpha[1]", np.nan))
    diagnostics["beta"] = safe_float(params.get("beta[1]", np.nan))
    diagnostics["gamma"] = gamma_value
    diagnostics["garch_p"] = garch_p
    diagnostics["garch_q"] = garch_q
    diagnostics["mean_lags"] = mean_lags
    gamma_adj = gamma_value / (2 if use_gjr and best_vol_model == "GARCH" else 1)
    diagnostics["persistence"] = alpha_sum + beta_sum + gamma_adj

    resid_trimmed = pd.Series(std_resid, index=std_resid.index)
    valid_resid = resid_trimmed.dropna()
    if len(valid_resid) >= 12:
        lb = acorr_ljungbox(valid_resid, lags=[12], return_df=True)
        diagnostics["ljung_box_pvalue"] = lb["lb_pvalue"].iloc[-1]
        arch_lm = het_arch(valid_resid, nlags=12)
        diagnostics["arch_lm_pvalue"] = arch_lm[3]
    else:
        diagnostics["ljung_box_pvalue"] = np.nan
        diagnostics["arch_lm_pvalue"] = np.nan

    return (
        pd.Series(std_resid, index=series.index),
        pd.Series(cond_vol, index=series.index),
        diagnostics,
        best_result,
        best_vol_model,
    )


def run_garch_diagnostics(
    iso: str,
    data: pd.DataFrame,
    use_gjr: bool,
    clip: float,
    overwrite: bool,
    vol_models: Sequence[str],
    garch_p: int,
    garch_q: int,
    mean_lags: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    residuals = pd.DataFrame(index=data.index)
    cond_vols = pd.DataFrame(index=data.index)
    diagnostics: List[Dict[str, Any]] = []

    for column in data.columns:
        print(f"[{iso}] GARCH -> {column}")
        col_path = GARCH_MODELS_DIR / f"{iso}_{column}.pkl"
        if col_path.exists() and not overwrite:
            try:
                model = joblib.load(col_path)
                diagnostics.append(
                    dict(
                        iso=iso,
                        series=column,
                        status="loaded",
                        **{
                            k: model["diagnostic"].get(k)
                            for k in [
                                "aic",
                                "bic",
                                "loglik",
                                "persistence",
                                "ljung_box_pvalue",
                                "arch_lm_pvalue",
                                "volatility_model",
                                "model_type",
                                "garch_p",
                                "garch_q",
                                "mean_lags",
                            ]
                        },
                    )
                )
                residuals[column] = model["residuals"].reindex(data.index)
                cond_vols[column] = model["volatility"].reindex(data.index)
                continue
            except Exception:  # pragma: no cover - fallback to re-fitting
                print(f"  Reload failed; re-fitting {column}")

        try:
            std_resid, vol, diagnostics_row, result, selected_vol = fit_series_garch(
                data[column],
                vol_models,
                use_gjr,
                clip,
                garch_p,
                garch_q,
                mean_lags,
            )
            diagnostics_row.update(
                dict(
                    iso=iso,
                    series=column,
                    model_type=("gjr" if selected_vol == "GARCH" and use_gjr else selected_vol.lower()),
                    volatility_model=selected_vol,
                )
            )
            diagnostics.append(diagnostics_row)
            residuals[column] = std_resid
            cond_vols[column] = vol

            dump(
                {
                    "diagnostic": diagnostics_row,
                    "model": result,
                    "residuals": std_resid,
                    "volatility": vol,
                },
                col_path,
            )
        except Exception as exc:
            print(f"  SKIP {column}: {exc}")
            diagnostics.append(
                dict(
                    iso=iso,
                    series=column,
                    status="failed",
                    error=str(exc),
                )
            )

    diag_df = pd.DataFrame(diagnostics)
    residuals = residuals.dropna(how="all")
    cond_vols = cond_vols.loc[residuals.index]
    return diag_df, residuals, cond_vols


def dcc_loglikelihood(
    params: Sequence[float],
    residuals: pd.DataFrame,
    R_bar: pd.DataFrame,
    model_type: str,
) -> float:
    a, b = params[0], params[1]
    g = params[2] if model_type == "adcc" else 0.0
    if a <= 0 or b <= 0 or g < 0:
        return -np.inf
    if 1 - a - b - 0.5 * g <= 0:
        return -np.inf

    Q = R_bar.values.copy()
    T, K = residuals.shape
    loglik = 0.0
    for t in range(1, T):
        u_t = residuals.iloc[t].values.reshape(-1, 1)
        u_tm1 = residuals.iloc[t - 1].values.reshape(-1, 1)
        eta_term = np.zeros_like(Q)
        if model_type == "adcc" and g > 0:
            eta_tm1 = np.minimum(u_tm1, 0.0)
            eta_term = g * (eta_tm1 @ eta_tm1.T)

        Q = (
            (1 - a - b - 0.5 * g) * R_bar.values
            + a * (u_tm1 @ u_tm1.T)
            + b * Q
            + eta_term
        )
        Q = (Q + Q.T) / 2
        diag_inv = np.diag(1 / np.sqrt(np.maximum(np.diag(Q), 1e-8)))
        R_t = diag_inv @ Q @ diag_inv
        R_t = np.clip(R_t, -0.999, 0.999)
        np.fill_diagonal(R_t, 1.0)
        try:
            sign, logdet = np.linalg.slogdet(R_t)
            if sign <= 0:
                return -np.inf
            R_t_inv = np.linalg.inv(R_t)
            quad = u_t.T @ R_t_inv @ u_t
            loglik += -0.5 * (logdet + quad[0, 0])
        except np.linalg.LinAlgError:
            return -np.inf
    return loglik


def grid_search_adcc(
    residuals: pd.DataFrame,
    model_type: str,
    grid_a: Optional[Sequence[float]],
    grid_b: Optional[Sequence[float]],
    grid_g: Optional[Sequence[float]],
) -> Tuple[Tuple[float, float, float], float]:
    a_values = list(grid_a or [0.01, 0.03, 0.05, 0.07, 0.10, 0.15])
    b_values = list(grid_b or [0.60, 0.70, 0.80, 0.85, 0.90, 0.94])
    g_values = list(grid_g or ([0.0, 0.05, 0.10, 0.15, 0.20] if model_type == "adcc" else [0.0]))

    best_loglik = -np.inf
    best_params = (0.05, 0.94, 0.0)
    total_combinations = len(a_values) * len(b_values) * len(g_values)
    combo_counter = 0
    log_interval = max(1, total_combinations // 20)
    print(f"  ADCC grid search: {total_combinations} combinations (model={model_type})")
    for a in a_values:
        for b in b_values:
            for g in g_values:
                combo_counter += 1
                if combo_counter % log_interval == 0 or combo_counter == total_combinations:
                    print(
                        f"    combo {combo_counter}/{total_combinations} "
                        f"(a={a:.3f}, b={b:.3f}, g={g:.3f})",
                        flush=True,
                    )
                params = (a, b, g)
                loglik = dcc_loglikelihood(params, residuals, residuals.corr(), model_type)
                if loglik > best_loglik:
                    best_loglik = loglik
                    best_params = params
    return best_params, best_loglik


def compute_dynamic_correlations(
    residuals: pd.DataFrame,
    model_type: str,
    params: Tuple[float, float, float],
) -> Tuple[pd.DataFrame, float]:
    a, b, g = params
    R_bar = residuals.corr()
    Q = R_bar.values.copy()
    T = len(residuals)
    columns = residuals.columns
    pair_labels = [f"{i}_{j}" for i, j in combinations(columns, 2)]
    corr_array = np.full((T, len(pair_labels)), np.nan)
    corr_array[0] = [R_bar.iloc[idx_i, idx_j] for idx_i, idx_j in combinations(range(len(columns)), 2)]

    for t in range(1, T):
        u_t = residuals.iloc[t].values.reshape(-1, 1)
        u_tm1 = residuals.iloc[t - 1].values.reshape(-1, 1)
        eta_term = np.zeros_like(Q)
        if model_type == "adcc" and g > 0:
            eta_tm1 = np.minimum(u_tm1, 0.0)
            eta_term = g * (eta_tm1 @ eta_tm1.T)

        Q = (
            (1 - a - b - 0.5 * g) * R_bar.values
            + a * (u_tm1 @ u_tm1.T)
            + b * Q
            + eta_term
        )
        Q = (Q + Q.T) / 2
        inv_sqrt = np.diag(1 / np.sqrt(np.maximum(np.diag(Q), 1e-8)))
        R_t = inv_sqrt @ Q @ inv_sqrt
        R_t = np.clip(R_t, -0.999, 0.999)
        np.fill_diagonal(R_t, 1.0)
        corr_array[t] = [R_t[idx_i, idx_j] for idx_i, idx_j in combinations(range(len(columns)), 2)]

    corr_df = pd.DataFrame(corr_array, index=residuals.index, columns=pair_labels)
    persistence = a + b + 0.5 * g
    return corr_df, persistence


def build_covariance_series(
    corr_df: pd.DataFrame,
    cond_vols: pd.DataFrame,
    residuals: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    columns = residuals.columns
    pair_labels = corr_df.columns
    cov_array = np.full_like(corr_df.values, np.nan, dtype=float)

    diag_vols = cond_vols.loc[residuals.index]
    diag_values = diag_vols.to_numpy()

    for lo_idx, (i, j) in enumerate(combinations(range(len(columns)), 2)):
        cov_array[:, lo_idx] = (
            diag_values[:, i] * diag_values[:, j] * corr_df.iloc[:, lo_idx].to_numpy()
        )

    cov_df = pd.DataFrame(cov_array, index=corr_df.index, columns=pair_labels)

    eigen_summary = []
    for idx, date in enumerate(residuals.index):
        vol_diag = np.diag(diag_values[idx])
        corr_matrix = make_correlation_matrix(corr_df.iloc[idx], columns)
        sigma = vol_diag @ corr_matrix @ vol_diag
        eigvals = eigh(sigma, eigvals_only=True)
        eigen_summary.append(
            dict(
                date=date.strftime("%Y-%m-%d"),
                min_eigen=float(np.min(eigvals)),
                max_eigen=float(np.max(eigvals)),
            )
        )
    eigen_df = pd.DataFrame(eigen_summary)
    eigen_df["date"] = pd.to_datetime(eigen_df["date"])
    eigen_df = eigen_df.set_index("date")
    return cov_df, eigen_df


def make_correlation_matrix(pair_series: pd.Series, columns: Sequence[str]) -> np.ndarray:
    matrix = np.eye(len(columns))
    for (i, j), value in zip(combinations(range(len(columns)), 2), pair_series.values):
        matrix[i, j] = value
        matrix[j, i] = value
    return matrix


def save_iso_outputs(
    iso: str,
    diag_df: pd.DataFrame,
    residuals: pd.DataFrame,
    cond_vols: pd.DataFrame,
    corr_df: pd.DataFrame,
    cov_df: pd.DataFrame,
    eigen_df: pd.DataFrame,
    persistence: float,
    params: Tuple[float, float, float],
    loglik: float,
    model_type: str,
    subset_info: Optional[Dict[str, Any]],
) -> None:
    diag_path = DIAG_DIR / f"garch_diagnostics_{iso}.csv"
    residuals_path = DIAG_DIR / f"garch_standardized_residuals_{iso}.csv"
    vol_path = DIAG_DIR / f"garch_conditional_vols_{iso}.csv"
    corr_path = DIAG_CORR_DIR / f"{iso}_Rt.csv"
    cov_path = DIAG_CORR_DIR / f"{iso}_Sigma_pairs.csv"
    eigen_path = DIAG_CORR_DIR / f"{iso}_Sigma_eigenvalues.csv"
    metadata_path = ADCC_MODELS_DIR / f"{iso}_adcc.json"

    diag_df.to_csv(diag_path, index=False)
    residuals.to_csv(residuals_path)
    cond_vols.to_csv(vol_path)
    corr_df.to_csv(corr_path)
    cov_df.to_csv(cov_path)
    eigen_df.to_csv(eigen_path)

    metadata = {
        "iso": iso,
        "model": model_type.upper(),
        "params": {"a": params[0], "b": params[1], "g": params[2]},
        "persistence": persistence,
        "loglikelihood": loglik,
        "min_eigen": float(eigen_df["min_eigen"].min()),
        "residuals_shape": residuals.shape,
        "pair_count": len(corr_df.columns),
    }
    if subset_info:
        metadata["low_autocorr_subset"] = {
            "series": subset_info["series"],
            "params": [float(x) for x in subset_info["params"]],
            "loglikelihood": float(subset_info["loglik"]),
            "persistence": float(subset_info["persistence"]),
        }
    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(metadata, handle, indent=2)


def main() -> None:
    args = parse_arguments()
    isos = [iso.strip().upper() for iso in args.isos.split(",") if iso.strip()]

    for iso in isos:
        print(f"\n=== Processing {iso} ===")
        data = load_factor_data(iso, args.use_filtered_factors)

        try:
            data = drop_low_coverage_series(data, args.coverage_threshold)
        except ValueError as exc:
            print(f"  {exc}; skip diagnostics for {iso}")
            continue

        # Optional: load precomputed volatility selection (HAR / FIGARCH / GARCH)
        # If present, this provides the conditional volatilities per factor; otherwise
        # we fall back to the internal GARCH diagnostics.
        vol_sel_path = VOL_SEL_DIR / f"vol_selection_{iso}.csv"
        if vol_sel_path.exists():
            try:
                sel_df = pd.read_csv(vol_sel_path, index_col=0, parse_dates=True)
                sel_df = sel_df.reindex(data.index).dropna(how="all")
                if sel_df.empty:
                    raise ValueError("empty selection frame after alignment")

                # Use provided conditional vols and neutral residuals (scaled returns);
                # Σ_t construction only needs D_t from these vols, R_t from ADCC uses
                # standardized residuals below.
                cond_vols = sel_df.copy()
                diag_df = pd.DataFrame(
                    {
                        "iso": iso,
                        "series": list(cond_vols.columns),
                        "status": "external_vol_selection",
                    }
                )

                # Build simple standardized residuals compatible with ADCC;
                # clip is applied as in GARCH path.
                scaled = data.loc[cond_vols.index]
                residuals = scaled.divide(cond_vols).replace([np.inf, -np.inf], np.nan)
                residuals = residuals.dropna(how="any")
                residuals = residuals.clip(-args.clip, args.clip)
            except Exception:
                # Fallback to built-in GARCH diagnostics path on any issue.
                diag_df, residuals, cond_vols = run_garch_diagnostics(
                    iso,
                    data,
                    args.use_gjr,
                    args.clip,
                    args.overwrite,
                    args.vol_models,
                    args.garch_p,
                    args.garch_q,
                    args.mean_lags,
                )
        else:
            diag_df, residuals, cond_vols = run_garch_diagnostics(
                iso,
                data,
                args.use_gjr,
                args.clip,
                args.overwrite,
                args.vol_models,
                args.garch_p,
                args.garch_q,
                args.mean_lags,
            )

        trimmed_resid, trimmed_vol, cleaned, dropped_columns = trim_residuals_for_min_rows(
            residuals, cond_vols, args.min_residual_rows
        )
        if dropped_columns:
            print(
                f"  Dropped {len(dropped_columns)} series to reach "
                f"{args.min_residual_rows} rows: {', '.join(dropped_columns)}"
            )

        if trimmed_resid.shape[1] < 2 or cleaned.empty:
            print(
                f"  Not enough series or rows after trimming "
                f"({trimmed_resid.shape[1]} series, {len(cleaned)} rows); skip ADCC"
            )
            continue

        subset_info: Optional[Dict[str, Any]] = None
        fallback = cleaned.shape[0] < args.min_residual_rows
        if fallback:
            print(f"  Shrinkage fallback (only {len(cleaned)} rows available)")
            corr_df = build_shrinkage_correlation_series(cleaned, args.shrinkage)
            persistence = np.nan
            params = (np.nan, np.nan, np.nan)
            loglik = np.nan
            model_label = "shrinkage"
        else:
            params, loglik = grid_search_adcc(
                cleaned,
                args.model,
                args.grid_a,
                args.grid_b,
                args.grid_g,
            )
            corr_df, persistence = compute_dynamic_correlations(cleaned, args.model, params)
            model_label = args.model
            subset_info = evaluate_low_autocorr_subset(
                cleaned,
                diag_df,
                args.low_acorr_pvalue,
                args.model,
                args.grid_a,
                args.grid_b,
                args.grid_g,
            )
            if subset_info:
                subset_log = subset_info["loglik"]
                subset_persistence = subset_info["persistence"]
                print(
                    f"  Low-autocorr subset DCC ({len(subset_info['series'])} series) loglik={subset_log:.2f}, persistence={subset_persistence:.4f}"
                )
                if np.isfinite(loglik) and subset_log > loglik:
                    print("    Subset loglik exceeds full-set loglik; review filtered correlations.")

        cov_df, eigen_df = build_covariance_series(corr_df, trimmed_vol.loc[cleaned.index], cleaned)
        save_iso_outputs(
            iso,
            diag_df,
            trimmed_resid.loc[cleaned.index],
            trimmed_vol.loc[cleaned.index],
            corr_df,
            cov_df,
            eigen_df,
            persistence,
            params,
            loglik,
            model_label,
            subset_info,
        )
        persistence_label = f"{persistence:.4f}" if np.isfinite(persistence) else "nan"
        loglik_label = f"{loglik:.2f}" if np.isfinite(loglik) else "nan"
        print(
            f"{iso} {model_label.upper()} done (persistence={persistence_label}, loglik={loglik_label})"
        )


if __name__ == "__main__":
    main()
