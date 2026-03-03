"""Fetch selected market tickers from Yahoo Finance (via yfinance).

Writes CSVs compatible with merge_industry_data.py loader:
  data_repository/raw/market_data/<FILE_STEM>.csv with columns Date,Value

Usage:
  python scripts/fetch_yahoo_market_tickers.py
  python scripts/fetch_yahoo_market_tickers.py 1990-01-01

Notes:
- Uses daily history; Yahoo coverage varies by ticker.
- Keep the mapping explicit so we know exactly what enters the panel.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    import yfinance as yf
except Exception as exc:  # pragma: no cover
    raise SystemExit("yfinance is required. Install with: pip install yfinance") from exc


BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data_repository" / "raw" / "market_data"


TICKERS = {
    # ticker: (series_name, file_stem)
    "EMLC": ("EMLC", "ETF_EMLC"),
}


def _download_one(ticker: str, start: str) -> pd.DataFrame:
    df = yf.download(
        tickers=ticker,
        start=start,
        end=datetime.utcnow().strftime("%Y-%m-%d"),
        interval="1d",
        auto_adjust=False,
        progress=False,
        group_by="column",
        threads=False,
    )

    if df is None or df.empty:
        raise ValueError("empty history")

    if isinstance(df.columns, pd.MultiIndex):
        if ("Close", ticker) in df.columns:
            s = df[("Close", ticker)]
        elif ("Adj Close", ticker) in df.columns:
            s = df[("Adj Close", ticker)]
        else:
            raise ValueError(f"unexpected multiindex columns: {df.columns.tolist()[:5]}")
    else:
        if "Close" in df.columns:
            s = df["Close"]
        elif "Adj Close" in df.columns:
            s = df["Adj Close"]
        else:
            raise ValueError(f"unexpected columns: {list(df.columns)}")

    out = pd.DataFrame(
        {
            "Date": pd.to_datetime(pd.Index(s.index).to_numpy(), errors="coerce"),
            "Value": pd.to_numeric(pd.Series(s).to_numpy(), errors="coerce"),
        }
    )
    out = out.dropna(subset=["Date", "Value"]).sort_values("Date")
    return out


def main(argv: list[str]) -> int:
    start = argv[0].strip() if argv and argv[0].strip() else "1990-01-01"

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for ticker, (series_name, file_stem) in TICKERS.items():
        try:
            df = _download_one(ticker, start=start)
        except Exception as exc:
            print(f"[WARN] {ticker} ({series_name}): {exc}")
            continue

        out_path = OUT_DIR / f"{file_stem}.csv"
        df.to_csv(out_path, index=False)
        print(f"[OK] {ticker} -> {out_path} ({len(df):,} rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
