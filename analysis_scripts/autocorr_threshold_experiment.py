"""Produce summaries/comparisons of standardized residual autocorrelation and low-acorr subsets."""
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import acf as sm_acf

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))
import scripts.phase3_iso_adcc as phase3  # noqa: E402


def main() -> None:
    diag_root = PROJECT_ROOT / "analysis_outputs" / "diagnostics"
    runs = [("AR0", 0), ("AR1", 1), ("AR2", 2)]
    thresholds = [0.001, 0.01, 0.05]
    max_lag = 12
    summary_rows = []
    threshold_rows = []
    acf_profiles = {}
    grid_a = [0.02, 0.05]
    grid_b = [0.65, 0.85]
    grid_g = [0.0, 0.1]

    for label, mean_lag in runs:
        diag_path = diag_root / f"garch_diagnostics_USA_meanlag{mean_lag}.csv"
        resid_path = diag_root / f"garch_standardized_residuals_USA_meanlag{mean_lag}.csv"
        diag = pd.read_csv(diag_path)
        resid = pd.read_csv(resid_path, index_col=0, parse_dates=True)

        lb = diag["ljung_box_pvalue"].dropna()
        summary_rows.append(
            {
                "run": label,
                "mean_lag": mean_lag,
                "series": len(lb),
                "fails_0.05": int((lb < 0.05).sum()),
                "fails_1e-4": int((lb < 1e-4).sum()),
                "lb_mean": float(lb.mean()),
                "lb_median": float(lb.median()),
                "persistence": float(diag["persistence"].mean()),
            }
        )

        cleaned = resid.dropna(how="any")
        acf_matrix = []
        for column in cleaned.columns:
            series = cleaned[column].dropna()
            if len(series) < max_lag + 2:
                continue
            vals = sm_acf(series, nlags=max_lag, fft=False, missing="drop")
            acf_matrix.append(vals[1:])
        acf_profiles[label] = np.nanmean(acf_matrix, axis=0) if acf_matrix else np.full(max_lag, np.nan)

        for threshold in thresholds:
            subset = phase3.evaluate_low_autocorr_subset(
                cleaned,
                diag,
                threshold,
                "adcc",
                grid_a,
                grid_b,
                grid_g,
            )
            if subset:
                threshold_rows.append(
                    {
                        "run": label,
                        "mean_lag": mean_lag,
                        "threshold": threshold,
                        "series": len(subset["series"]),
                        "loglik": subset["loglik"],
                        "persistence": subset["persistence"],
                    }
                )
            else:
                threshold_rows.append(
                    {
                        "run": label,
                        "mean_lag": mean_lag,
                        "threshold": threshold,
                        "series": 0,
                        "loglik": np.nan,
                        "persistence": np.nan,
                    }
                )

    summary_df = pd.DataFrame(summary_rows)
    threshold_df = pd.DataFrame(threshold_rows)
    summary_out = diag_root / "autocorr_threshold_summary.csv"
    threshold_out = diag_root / "autocorr_threshold_detail.csv"
    summary_df.to_csv(summary_out, index=False)
    threshold_df.to_csv(threshold_out, index=False)

    plt.style.use("default")
    fig, ax = plt.subplots(figsize=(9, 5))
    lags = np.arange(1, max_lag + 1)
    for label, values in acf_profiles.items():
        ax.plot(lags, values, marker="o", label=label)
    ax.set_xlabel("Lag")
    ax.set_ylabel("Average ACF")
    ax.set_title("Standardized residual autocorrelation")
    ax.legend()
    ax.set_xticks(lags)
    fig.tight_layout()
    plot_out = diag_root / "autocorr_comparison.png"
    fig.savefig(plot_out)

    print("Saved summary:", summary_out)
    print("Saved detail:", threshold_out)
    print("Saved plot:", plot_out)


if __name__ == "__main__":
    main()
