"""ADCC-based correlation models for stress-testing (FOR ST).

This module takes standardized residuals (or simple returns) and
constructs a time series of pairwise correlations in the same
wide-pair format used by the monthly Rt diagnostics.

For now, we implement a simple rolling-window correlation as a
placeholder; this can later be swapped for a full ADCC
implementation using the project's correlation_models utilities.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd


@dataclass
class CorrelationConfig:
    """Configuration for FOR-ST correlation estimation.

    Attributes
    ----------
    iso: Country ISO code (e.g. "ITA").
    factors: List of factor column names whose correlations
        should be tracked.
    window: Integer size of the rolling window (in days) used as a
        proxy for dynamic correlation.
    model_name: Label for the correlation model, e.g. "adcc".
    """

    iso: str
    factors: List[str]
    window: int = 60
    model_name: str = "adcc"


def load_standardised_residuals(path: Path) -> pd.DataFrame:
    """Load residuals or returns to be used for correlation estimation.

    The input is expected to be a CSV with a ``date`` column and
    one column per factor in ``CorrelationConfig.factors``.
    """

    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
    else:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
    return df


def _rolling_corr(a: pd.Series, b: pd.Series, window: int) -> pd.Series:
    return a.rolling(window=window, min_periods=window).corr(b)


def build_rt_panel_for_st(
    residuals_path: Path,
    config: CorrelationConfig,
    out_path: Path,
) -> pd.DataFrame:
    """Construct a daily Rt-style correlation panel for FOR-ST use.

    Parameters
    ----------
    residuals_path:
        CSV with daily standardized residuals (or returns) for all
        factors in ``config.factors``.
    config:
        CorrelationConfig describing ISO, factor list and window.
    out_path:
        Destination CSV for the wide pairwise correlation panel.

    Returns
    -------
    DataFrame of shape (T, n_pairs) where each column is named
    ``A_B`` for unordered factor pairs (A, B).
    """

    df = load_standardised_residuals(residuals_path)

    # Ensure we only keep the factors of interest and sort columns
    cols = [c for c in config.factors if c in df.columns]
    df = df[cols].astype(float)

    rt_df = pd.DataFrame(index=df.index)

    for a, b in combinations(cols, 2):
        col_name = f"{a}_{b}"
        rt_df[col_name] = _rolling_corr(df[a], df[b], config.window)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    rt_df.to_csv(out_path, index=True, index_label="date")
    return rt_df


__all__ = ["CorrelationConfig", "load_standardised_residuals", "build_rt_panel_for_st"]
