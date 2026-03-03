"""HAR-RV Model implementation."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import statsmodels.api as sm
import warnings

# Suppress known numpy runtime warnings triggered by empty cov calculations.
warnings.filterwarnings(
    "ignore",
    category=RuntimeWarning,
    module=r"numpy\.lib\._function_base_impl",
)


try:
    from sklearn.linear_model import Ridge
except Exception:  # pragma: no cover - sklearn optional at import time
    Ridge = None


@dataclass
class HARResult:
    params: pd.Series
    tvalues: pd.Series
    aic: float
    bic: float


class HARModel:
    """Heterogeneous Auto-Regressive model for realised volatility.

    Supports OLS (statsmodels) and Ridge (scikit-learn) regression backends.
    Use `regression='ols'` or `regression='ridge'` and set `alpha` for ridge.
    """

    def __init__(
        self,
        features: pd.DataFrame,
        target_column: str = "rv_target",
        regression: str = "ols",
        alpha: float = 0.0,
    ) -> None:
        if target_column not in features.columns:
            raise KeyError(f"Target column '{target_column}' not found in features")
        self.features = features.copy()
        self.target_column = target_column
        self.regression = (regression or "ols").lower()
        self.alpha = float(alpha)
        self.model = None
        self.result = None
        # storage for ridge coefficients when used
        self._ridge_coef: Optional[pd.Series] = None

    def fit(self) -> HARResult:
        y = self.features[self.target_column]
        X = self.features.drop(columns=[self.target_column])
        X_const = sm.add_constant(X)

        if self.regression == "ols" or (self.regression == "ridge" and self.alpha == 0.0):
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Degrees of freedom <= 0 for slice")
                warnings.filterwarnings("ignore", message="divide by zero encountered in divide")
                warnings.filterwarnings("ignore", message="invalid value encountered in multiply")
                with np.errstate(divide="ignore", invalid="ignore"):
                    model = sm.OLS(y, X_const)
                    self.model = model
                    fitted = model.fit()
            self.result = fitted
            return HARResult(
                params=fitted.params,
                tvalues=fitted.tvalues,
                aic=fitted.aic,
                bic=fitted.bic,
            )

        # Ridge regression path
        if self.regression == "ridge":
            if Ridge is None:
                raise ImportError("scikit-learn is required for ridge regression but is not available")
            # sklearn Ridge does not include constant by default if fit_intercept=False.
            # We will fit intercept separately by working with numpy arrays and using fit_intercept=True.
            ridge = Ridge(alpha=self.alpha, fit_intercept=True)
            ridge.fit(X.values, y.values)
            coef = pd.Series(ridge.coef_, index=X.columns)
            intercept = float(ridge.intercept_)
            params = pd.concat([pd.Series({"const": intercept}), coef])
            self._ridge_coef = params
            # store a simple result-like container for parity where possible
            self.result = None
            # AIC/BIC are not computed for Ridge; return None for those fields
            return HARResult(params=params, tvalues=pd.Series(dtype=float), aic=None, bic=None)

        raise ValueError(f"Unknown regression backend: {self.regression}")

    def forecast(self, latest_features: pd.Series) -> float:
        if self.result is None and self._ridge_coef is None:
            raise RuntimeError("Model must be fitted before forecasting")

        if self._ridge_coef is not None:
            params = self._ridge_coef
            aligned = latest_features.reindex(params.index, fill_value=1.0)
            if np.isnan(aligned).any():
                raise ValueError("Forecast features contain NaNs")
            return float(np.dot(params.values, aligned.values))

        # OLS path
        if "const" not in self.result.params.index:
            raise RuntimeError("Fitted model missing constant term")
        aligned = latest_features.reindex(self.result.params.index, fill_value=1.0)
        if np.isnan(aligned).any():
            raise ValueError("Forecast features contain NaNs")
        return float(np.dot(self.result.params.values, aligned.values))

    def predict(self, feature_frame: pd.DataFrame) -> pd.Series:
        if self.result is None and self._ridge_coef is None:
            raise RuntimeError("Model must be fitted before prediction")

        X = feature_frame.copy()
        X = sm.add_constant(X, has_constant="add")

        if self._ridge_coef is not None:
            params = self._ridge_coef
            design = X.reindex(columns=params.index, fill_value=0.0)
            if "const" in params.index:
                design["const"] = 1.0
            preds = design.values.dot(params.values)
            return pd.Series(preds, index=feature_frame.index, name="har_forecast")

        exog_names = list(self.result.model.exog_names)
        # ensure constant column present when expected
        if "const" in exog_names and "const" not in X.columns:
            X["const"] = 1.0
        X_aligned = X.reindex(columns=exog_names, fill_value=0.0)
        predictions = self.result.predict(X_aligned)
        return pd.Series(predictions, index=feature_frame.index, name="har_forecast")

    def summary(self) -> Optional[str]:
        # For OLS we can return the statsmodels summary. For Ridge, no rich summary available.
        if self._ridge_coef is not None:
            return None
        if self.result is None:
            return None
        return self.result.summary().as_text()
