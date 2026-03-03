"""Compare subset vs full-set residual autocorrelations."""
from pathlib import Path
import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import acf as sm_acf

ROOT = Path(__file__).resolve().parents[1]
resid = pd.read_csv(
    ROOT / "analysis_outputs" / "diagnostics" / "garch_standardized_residuals_USA_meanlag1.csv",
    index_col=0,
    parse_dates=True,
).dropna(how="any")
subset_series = {
    "MORTGAGE30US",
    "VIXCLS",
    "BAMLC0A3CAEY",
    "DCOILBRENTEU",
    "MORTGAGE30US_lag0",
    "MORTGAGE30US_lag1",
    "MORTGAGE30US_lag2",
    "MORTGAGE30US_lag3",
    "VIXCLS_lag0",
    "VIXCLS_lag1",
    "VIXCLS_lag2",
    "VIXCLS_lag3",
    "BAMLC0A3CAEY_lag0",
    "BAMLC0A3CAEY_lag1",
    "BAMLC0A3CAEY_lag2",
    "BAMLC0A3CAEY_lag3",
    "DCOILBRENTEU_lag0",
    "DCOILBRENTEU_lag1",
    "DCOILBRENTEU_lag2",
    "DCOILBRENTEU_lag3",
    "USA_FCI",
}
max_lag = 12
acf_matrix = {}
for col in resid.columns:
    acf_matrix[col] = sm_acf(resid[col], nlags=max_lag, fft=False, missing="drop")[1:]
subset_acf = np.nanmean([acf_matrix[col] for col in subset_series if col in acf_matrix], axis=0)
other_series = [col for col in resid.columns if col not in subset_series]
other_acf = np.nanmean([acf_matrix[col] for col in other_series], axis=0)
print("subset avg acf:", np.round(subset_acf, 3))
print("others avg acf:", np.round(other_acf, 3))
lag1 = {col: vals[0] for col, vals in acf_matrix.items()}
worst = sorted(lag1.items(), key=lambda x: -abs(x[1]))[:6]
print("top abs(lag1):")
for col, val in worst:
    print(f" {col} {val:.3f}")
over_05 = [col for col, val in lag1.items() if abs(val) > 0.5]
print(f"series with |lag1|>0.5: {over_05}")
