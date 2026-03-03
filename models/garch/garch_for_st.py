"""GARCH-based volatility models for stress-testing (FOR ST).

This module provides thin wrappers around the existing project
infrastructure to estimate and store daily conditional volatilities
for a set of factors for a given country ISO.

The actual model specification should mirror the production
mean-reversion setup (GARCH/FIGARCH/HAR), but is deliberately
kept minimal here so it can be wired into the stress-testing
pipeline incrementally.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

import pandas as pd


@dataclass
class VolatilityConfig:
    """Configuration for FOR-ST volatility estimation.

    Attributes
    ----------
    iso: Country ISO code (e.g. "ITA").
    factors: List of factor column names to model (matching daily panel).
    model_name: Name of the volatility model family (e.g. "garch").
    """

    iso: str
    factors: List[str]
    model_name: str = "garch"


def load_daily_factors(path: Path) -> pd.DataFrame:
    """Load the daily factor panel used as input for FOR-ST models.

    Parameters
    ----------
    path: Path to a CSV with a Date column and factor columns.

    Returns
    -------
    DataFrame indexed by date.
    """

    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
    else:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
    return df


def build_vol_panel_for_st(
    daily_factors_path: Path,
    config: VolatilityConfig,
    out_path: Path,
) -> pd.DataFrame:
    """Create a placeholder daily volatility panel for FOR-ST pipeline.

    This function is intentionally simple for now: it copies the
    absolute value of demeaned returns as a crude volatility proxy.
    It is meant to be replaced by calls into the project's existing
    mean-reversion / GARCH infrastructure once those utilities are
    factored into importable functions.

    Parameters
    ----------
    daily_factors_path:
        CSV file with daily factor values.
    config:
        VolatilityConfig describing ISO and factor list.
    out_path:
        Destination CSV for the volatility panel.

    Returns
    -------
    DataFrame with columns named ``vol_{factor}``.
    """

    df = load_daily_factors(daily_factors_path)

    vol_df = pd.DataFrame(index=df.index)
    for col in config.factors:
        if col not in df.columns:
            continue
        series = df[col].astype(float).copy()
        # Simple proxy: absolute demeaned series.
        demeaned = series - series.mean()
        vol_df[f"vol_{col}"] = demeaned.abs()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    vol_df.to_csv(out_path, index=True, index_label="date")
    return vol_df


__all__ = ["VolatilityConfig", "load_daily_factors", "build_vol_panel_for_st"]
