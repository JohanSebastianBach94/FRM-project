"""Fetch selected EUR/* FX crosses from Yahoo Finance (via yfinance).

Outputs CSVs compatible with the pipeline loader:
  data_repository/raw/market_data/FX_<PAIR>_<TICKER>.csv with columns Date,Value

Example:
  EURTRY=X -> FX_EURTRY_EURTRY_X.csv
"""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
import pandas as pd

try:
    import yfinance as yf
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "yfinance is required. Install with: pip install yfinance"
    ) from exc


BASE_DIR = Path(__file__).resolve().parents[1]
OUT_DIR = BASE_DIR / "data_repository" / "raw" / "market_data"


PAIRS = {
    "EURTRY=X": "EURTRY",
    "EURPLN=X": "EURPLN",
    "EURCZK=X": "EURCZK",
}


def _download_close(ticker: str, start: str = "1990-01-01") -> pd.Series:
    df = yf.download(
        ticker,
        start=start,
        progress=False,
        auto_adjust=False,
        actions=False,
        threads=False,
    )
    if df is None or df.empty:
        raise ValueError(f"yfinance returned empty data for {ticker}")

    col = "Adj Close" if ("Adj Close" in df.columns or (isinstance(df.columns, pd.MultiIndex) and "Adj Close" in df.columns.get_level_values(0))) else "Close"

    if isinstance(df.columns, pd.MultiIndex):
        if col not in df.columns.get_level_values(0):
            raise ValueError(f"yfinance missing {col} for {ticker}: {df.columns}")
        block = df[col]
        if isinstance(block, pd.DataFrame):
            if ticker in block.columns:
                s = block[ticker]
            elif block.shape[1] == 1:
                s = block.iloc[:, 0]
            else:
                raise ValueError(f"Ambiguous {col} block columns for {ticker}: {list(block.columns)}")
        else:
            s = block
    else:
        if col not in df.columns:
            raise ValueError(f"yfinance missing {col} for {ticker}: {list(df.columns)}")
        s = df[col]

    s = pd.to_numeric(s, errors="coerce").dropna()
    s.index = pd.to_datetime(s.index, errors="coerce").tz_localize(None)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if s.empty:
        raise ValueError(f"No valid close values for {ticker}")
    return s


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Output dir: {OUT_DIR}")
    print(f"Pairs: {', '.join(PAIRS.keys())}")

    for ticker, pair_stem in PAIRS.items():
        try:
            s = _download_close(ticker)
        except Exception as exc:
            print(f"[WARN] {ticker}: {exc}")
            continue

        out_name = f"FX_{pair_stem}_{pair_stem}_X.csv"
        out_path = OUT_DIR / out_name
        out_df = pd.DataFrame({"Date": s.index.strftime("%Y-%m-%d"), "Value": s.to_numpy()})
        out_df.to_csv(out_path, index=False)

        print(
            f"[OK] {ticker} -> {out_path.name} ({len(out_df):,} rows; {out_df['Date'].iloc[0]} .. {out_df['Date'].iloc[-1]})"
        )

    print(f"Done at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
