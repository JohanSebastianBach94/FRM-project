"""
Inspect autocorrelation quality of the filtered USA dataset (GDPC1/BAMLC0A4CBBB removed).
Compare full-set vs low-autocorr subset residuals.
"""
import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import acf

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DIAG_DIR = PROJECT_ROOT / "analysis_outputs" / "diagnostics"
MODELS_DIR = PROJECT_ROOT / "models" / "adcc"


def main():
    # Load diagnostics and residuals
    diag_path = DIAG_DIR / "garch_diagnostics_USA.csv"
    resid_path = DIAG_DIR / "garch_standardized_residuals_USA.csv"
    meta_path = MODELS_DIR / "USA_adcc.json"
    
    diag_df = pd.read_csv(diag_path)
    resid_df = pd.read_csv(resid_path, index_col=0, parse_dates=True)
    
    with open(meta_path) as f:
        metadata = json.load(f)
    
    subset_series = metadata["low_autocorr_subset"]["series"]
    
    print(f"Total series: {len(resid_df.columns)}")
    print(f"Subset series: {len(subset_series)}")
    print(f"Full-set persistence: {metadata['persistence']:.4f}")
    print(f"Subset persistence: {metadata['low_autocorr_subset']['persistence']:.4f}")
    print(f"Full-set loglik: {metadata['loglikelihood']:.2f}")
    print(f"Subset loglik: {metadata['low_autocorr_subset']['loglikelihood']:.2f}")
    
    # Compute ACF for each series
    subset_acf = []
    others_acf = []
    
    for col in resid_df.columns:
        series_clean = resid_df[col].dropna()
        if len(series_clean) < 50:
            continue
        
        acf_vals = acf(series_clean, nlags=12, fft=True)
        
        if col in subset_series:
            subset_acf.append(acf_vals)
        else:
            others_acf.append(acf_vals)
    
    subset_avg = np.mean(subset_acf, axis=0) if subset_acf else np.zeros(13)
    others_avg = np.mean(others_acf, axis=0) if others_acf else np.zeros(13)
    
    print(f"\nSubset avg ACF (lags 1-6): {subset_avg[1:7]}")
    print(f"Others avg ACF (lags 1-6): {others_avg[1:7]}")
    
    # Check Ljung-Box stats
    subset_ljung = diag_df[diag_df["series"].isin(subset_series)]
    others_ljung = diag_df[~diag_df["series"].isin(subset_series)]
    
    print(f"\nSubset Ljung-Box failures (p<0.05): {(subset_ljung['ljung_box_pvalue'] < 0.05).sum()} / {len(subset_ljung)}")
    print(f"Others Ljung-Box failures (p<0.05): {(others_ljung['ljung_box_pvalue'] < 0.05).sum()} / {len(others_ljung)}")
    
    print(f"\nSubset avg persistence: {subset_ljung['persistence'].mean():.4f}")
    print(f"Others avg persistence: {others_ljung['persistence'].mean():.4f}")
    
    # Identify worst offenders in "others"
    worst = others_ljung.nsmallest(5, "ljung_box_pvalue")[["series", "ljung_box_pvalue", "persistence"]]
    print(f"\nWorst offenders (excluded from subset):")
    print(worst.to_string(index=False))
    
    # Plot ACF comparison
    fig, ax = plt.subplots(figsize=(10, 6))
    lags = np.arange(13)
    ax.plot(lags, subset_avg, marker="o", label=f"Subset ({len(subset_series)} series)", linewidth=2)
    ax.plot(lags, others_avg, marker="s", label=f"Others ({len(resid_df.columns) - len(subset_series)} series)", linewidth=2)
    ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax.axhline(0.05, color="red", linestyle=":", linewidth=0.8, alpha=0.5, label="±0.05 threshold")
    ax.axhline(-0.05, color="red", linestyle=":", linewidth=0.8, alpha=0.5)
    ax.set_xlabel("Lag", fontsize=12)
    ax.set_ylabel("Average ACF", fontsize=12)
    ax.set_title("Filtered Dataset: Subset vs Others Autocorrelation", fontsize=14, fontweight="bold")
    ax.legend(fontsize=10)
    ax.grid(alpha=0.3)
    
    plot_path = DIAG_DIR / "filtered_subset_acf_comparison.png"
    fig.savefig(plot_path, dpi=150, bbox_inches="tight")
    print(f"\nPlot saved to: {plot_path}")
    plt.close()


if __name__ == "__main__":
    main()
