"""Model selection helper for volatility models (GARCH, FIGARCH, HAR-RV).

This module provides utilities to fit multiple volatility models on a single
return / realised volatility series and select the best specification based
on a common scoring interface.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

from .figarch import FIGARCHModel
from .garch import GARCHModel
from .har_rv import HARModel


@dataclass
class VolModelResult:
    """Container for a single volatility model fit.

    Attributes
    ----------
    name: str
        Identifier for the model family (e.g. "garch", "figarch", "har").
    variance: pd.Series
        Conditional variance (or realised volatility) path aligned to the
        original index.
    score: float
        Main scalar score used for selection (lower is better), typically
        out-of-sample RMSE.
    aic: Optional[float]
        Information criterion when available.
    bic: Optional[float]
        Information criterion when available.
    meta: Dict
        Free-form diagnostics and parameters.
    """

    name: str
    variance: pd.Series
    score: float
    aic: Optional[float]
    bic: Optional[float]
    meta: Dict


def _rmse(actual: pd.Series, forecast: pd.Series) -> float:
    aligned = pd.concat([actual, forecast], axis=1).dropna()
    if aligned.empty:
        return float("inf")
    diff = aligned.iloc[:, 0] - aligned.iloc[:, 1]
    return float(np.sqrt(np.mean(np.square(diff))))


def fit_all_models(
    returns: pd.Series,
    rv_target: pd.Series,
    cfg: Dict,
) -> Dict[str, VolModelResult]:
    """Fit GARCH, FIGARCH and HAR-RV on the provided series.

    Parameters
    ----------
    returns:
        Raw return series (e.g. daily log returns).
    rv_target:
        Realised variance / volatility series on the same index.
    cfg:
        Configuration dictionary controlling splits and model options.

    Returns
    -------
    Dict[str, VolModelResult]
        Mapping from model key to fit result.
    """

    results: Dict[str, VolModelResult] = {}
    idx = returns.index

    # GARCH using existing wrapper
    try:
        garch_model = GARCHModel(returns)
        garch_res = garch_model.fit()
        garch_var = garch_model.conditional_variance()
        garch_score = _rmse(rv_target, garch_var.reindex(idx))
        results["garch"] = VolModelResult(
            name="garch",
            variance=garch_var.reindex(idx),
            score=garch_score,
            aic=getattr(garch_res, "aic", None),
            bic=getattr(garch_res, "bic", None),
            meta={"params": getattr(garch_res, "params", None)},
        )
    except Exception as exc:  # pragma: no cover - defensive
        results["garch"] = VolModelResult(
            name="garch",
            variance=pd.Series(index=idx, dtype=float),
            score=float("inf"),
            aic=None,
            bic=None,
            meta={"error": str(exc)},
        )

    # FIGARCH
    try:
        order = tuple(cfg.get("figarch", {}).get("order", (1, 0.4, 1)))
        figarch_model = FIGARCHModel(returns, order=order)  # type: ignore[arg-type]
        figarch_res = figarch_model.fit(disp=False)
        figarch_var = figarch_model.forecast_series(index=idx)
        figarch_score = _rmse(rv_target, figarch_var.reindex(idx))
        results["figarch"] = VolModelResult(
            name="figarch",
            variance=figarch_var.reindex(idx),
            score=figarch_score,
            aic=figarch_res.aic,
            bic=figarch_res.bic,
            meta={"params": figarch_res.params.to_dict()},
        )
    except Exception as exc:  # pragma: no cover - defensive
        results["figarch"] = VolModelResult(
            name="figarch",
            variance=pd.Series(index=idx, dtype=float),
            score=float("inf"),
            aic=None,
            bic=None,
            meta={"error": str(exc)},
        )

    # HAR-RV
    try:
        har_cfg = cfg.get("har", {})
        regression = har_cfg.get("regression", "ols")
        alpha = float(har_cfg.get("alpha", 0.0))
        features = pd.DataFrame({"rv_target": rv_target})
        har_model = HARModel(features, regression=regression, alpha=alpha)
        har_res = har_model.fit()
        har_forecast = har_model.predict(features.drop(columns=["rv_target"]))
        har_score = _rmse(rv_target, har_forecast.reindex(idx))
        results["har"] = VolModelResult(
            name="har",
            variance=har_forecast.reindex(idx),
            score=har_score,
            aic=har_res.aic,
            bic=har_res.bic,
            meta={"params": har_res.params.to_dict()},
        )
    except Exception as exc:  # pragma: no cover - defensive
        results["har"] = VolModelResult(
            name="har",
            variance=pd.Series(index=idx, dtype=float),
            score=float("inf"),
            aic=None,
            bic=None,
            meta={"error": str(exc)},
        )

    return results


def score_models(results: Dict[str, VolModelResult]) -> Tuple[str, pd.Series]:
    """Return the key and variance series of the best model.

    Selection is based primarily on the RMSE score (lower is better).
    Ties or near-ties can be broken by AIC/BIC where available.
    """

    if not results:
        raise ValueError("No model results supplied")

    # First pick by RMSE
    best_key = min(results.keys(), key=lambda k: results[k].score)
    best = results[best_key]

    # If multiple close scores exist, optionally refine via AIC/BIC
    # (keep it simple for now and just return the RMSE winner).
    return best_key, best.variance
