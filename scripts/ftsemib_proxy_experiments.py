from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import numpy as np
import pandas as pd

import matplotlib

# Use a non-interactive backend so the script can run headless
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import yfinance as yf


OUTPUT_DIR = Path("analysis_outputs/ftsemib_proxy")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def download_series(ticker: str, start: str, end: str, interval: str = "1d") -> pd.Series:
    """Download an adjusted close series and drop empty results."""

    def _sanitize(frame: pd.DataFrame) -> pd.Series:
        if frame.empty:
            return pd.Series(dtype=float, name=ticker)
        if "Adj Close" in frame:
            series = frame["Adj Close"].rename(ticker)
        elif "Close" in frame:
            series = frame["Close"].rename(ticker)
        else:
            return pd.Series(dtype=float, name=ticker)
        series = series.dropna()
        if getattr(series.index, "tz", None) is not None:
            series = series.tz_localize(None)
        return series

    try:
        data = yf.download(
            ticker,
            start=start,
            end=end,
            progress=False,
            auto_adjust=True,
            interval=interval,
        )
        series = _sanitize(data)
        if not series.empty:
            return series
    except Exception:
        pass

    try:
        history = yf.Ticker(ticker).history(
            start=start,
            end=end,
            interval=interval,
            auto_adjust=True,
        )
        series = _sanitize(history)
        if not series.empty:
            return series
    except Exception:
        pass

    if ticker.upper() in {"FTSEMIB.MI", "FTSE_MIB_OFFICIAL"}:
        local_path = Path("data/EQUITY_Italy_FTSEMIB_MI.csv")
        if local_path.exists():
            df = pd.read_csv(local_path, parse_dates=[0], index_col=0)
            series = df.iloc[:, 0].rename("FTSEMIB.MI")
            if getattr(series.index, "tz", None) is not None:
                series = series.tz_localize(None)
            mask = (series.index >= pd.Timestamp(start)) & (series.index <= pd.Timestamp(end))
            return series.loc[mask]

    return pd.Series(dtype=float, name=ticker)


def build_equal_weight_proxy(tickers: Iterable[str], start: str, end: str) -> pd.Series:
    """Construct an equal-weighted price index using adjusted close series."""

    series_list: list[pd.Series] = []
    for ticker in tickers:
        series = download_series(ticker, start=start, end=end)
        if series.empty:
            continue
        series_list.append(series)

    if not series_list:
        return pd.Series(dtype=float, name="eq_weight_proxy")

    prices = pd.concat(series_list, axis=1).sort_index().ffill().dropna(how="all")
    prices = prices.dropna(axis=1, how="all")
    if prices.empty:
        return pd.Series(dtype=float, name="eq_weight_proxy")

    weights = pd.DataFrame(1.0 / len(prices.columns), index=prices.index, columns=prices.columns)
    returns = prices.pct_change().fillna(0.0)
    portfolio_returns = (returns * weights).sum(axis=1)
    index = (1.0 + portfolio_returns).cumprod() * 100.0
    index.name = "eq_weight_proxy"
    return index


def extend_with_official(proxy: pd.Series, official: pd.Series) -> pd.Series:
    """Scale proxy to official index at first overlap and concatenate."""

    proxy = proxy.sort_index()
    official = official.sort_index()
    overlap = proxy.index.intersection(official.index)
    if overlap.empty:
        anchor_proxy_date = proxy.index[-1]
        anchor_official_date = official.index[0]
        proxy_value = proxy.iloc[-1]
        official_value = official.iloc[0]
    else:
        anchor_proxy_date = overlap[-1]
        anchor_official_date = overlap[-1]
        proxy_value = proxy.loc[anchor_proxy_date]
        official_value = official.loc[anchor_official_date]

    scale = 1.0 if proxy_value == 0 else official_value / proxy_value

    scaled_proxy = proxy * scale
    combined = pd.concat([scaled_proxy, official])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.name = "FTSE_MIB_proxy"
    return combined


def evaluate_proxies(proxies: Dict[str, pd.Series], official: pd.Series) -> pd.DataFrame:
    """Compute overlap metrics (return correlation, RMSE) for each proxy."""

    stats: list[Dict[str, float]] = []

    for name, series in proxies.items():
        aligned = series.dropna().sort_index()
        overlap = aligned.index.intersection(official.index)
        if len(overlap) < 30:
            continue

        proxy_overlap = aligned.loc[overlap]
        official_overlap = official.loc[overlap]

        proxy_returns = proxy_overlap.pct_change().dropna()
        official_returns = official_overlap.pct_change().dropna()
        common = proxy_returns.index.intersection(official_returns.index)
        if common.empty:
            continue

        corr = proxy_returns.loc[common].corr(official_returns.loc[common])
        rmse = np.sqrt(np.mean((proxy_overlap.loc[common] - official_overlap.loc[common]) ** 2))
        stats.append(
            {
                "proxy": name,
                "overlap_days": float(len(common)),
                "return_corr": float(corr),
                "rmse": float(rmse),
            }
        )

    if not stats:
        return pd.DataFrame(columns=["proxy", "overlap_days", "return_corr", "rmse"])

    return pd.DataFrame(stats).sort_values(by="return_corr", ascending=False)


def main() -> None:
    tickers = [
        "ENI.MI",
        "ENEL.MI",
        "EXO.MI",
        "TIT.MI",
        "UCG.MI",
        "ISP.MI",
        "G.MI",
        "PIRC.MI",
        "STM.MI",
        "BMPS.MI",
        "MB.MI",
    ]

    eq_proxy = build_equal_weight_proxy(tickers, start="1990-01-01", end="1997-12-31")

    alternative_candidates = {
        "MSCI_Italy": "MIBI.MI",  # may require data vendor access
        "STOXX_Italy": "I269.MI",  # placeholder, will be skipped if unavailable
        "COMIT_Index": "COMIT.MI",
    }

    alternative_proxies: Dict[str, pd.Series] = {}
    for name, ticker in alternative_candidates.items():
        series = download_series(ticker, start="1990-01-01", end="2025-10-31")
        if not series.empty:
            alternative_proxies[name] = series

    official = download_series("FTSEMIB.MI", start="1997-01-01", end="2025-10-31")
    if official.empty:
        raise RuntimeError("Unable to download official FTSE MIB series.")

    proxies: Dict[str, pd.Series] = {}
    if not eq_proxy.empty:
        proxies["eq_weight_proxy"] = extend_with_official(eq_proxy, official)

    for name, series in alternative_proxies.items():
        proxies[name] = series

    for name, series in proxies.items():
        series.to_csv(OUTPUT_DIR / f"{name}_1990_2025.csv", header=[name])

    evaluation = evaluate_proxies(proxies, official)
    evaluation.to_csv(OUTPUT_DIR / "proxy_evaluation.csv", index=False)

    plt.figure(figsize=(12, 6))
    for name, series in proxies.items():
        plt.plot(series.index, series.values, label=name)

    plt.plot(official.index, official.values, label="FTSE_MIB_official", linewidth=2.0, linestyle="--")
    plt.title("FTSE MIB Proxy Candidates vs Official Index")
    plt.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "FTSE_MIB_proxy_comparison.png", dpi=200)
    plt.close()

    print("✅ Proxy comparison complete. Outputs saved to", OUTPUT_DIR)
    if not evaluation.empty:
        print(evaluation)
    else:
        print("⚠️ No proxies with sufficient overlap for evaluation.")


if __name__ == "__main__":
    main()