"""Download Euro STOXX 50 and derive a realized-vol proxy for V2X replacements."""
from argparse import ArgumentParser
from pathlib import Path
from typing import Optional

import numpy as np
import yfinance as yf

OUT_DIR = Path("data_repository/raw/providers/derived_risk_drivers")
OUT_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_START_DATE = "1990-01-01"
DEFAULT_INDEX = "^STOXX50E"
DEFAULT_WINDOW = 20


def parse_args() -> ArgumentParser:
    parser = ArgumentParser(
        description="Download STOXX 50 prices and derive a realized-vol proxy for V2X replacements."
    )
    parser.add_argument("--start", default=DEFAULT_START_DATE, help="start date for price download")
    parser.add_argument("--end", help="end date for price download (default: latest available)")
    parser.add_argument("--index", default=DEFAULT_INDEX, help="index ticker used for the proxy")
    parser.add_argument("--window", type=int, default=DEFAULT_WINDOW, help="rolling window for vol in days")
    parser.add_argument(
        "--output",
        default="Euro_STOXX_RealizedVol20.csv",
        help="filename for the realized-vol output",
    )
    parser.add_argument(
        "--price-output",
        default="Euro_STOXX_50.csv",
        help="filename for the raw price series",
    )
    return parser


def download_data(ticker: str, start: str, end: Optional[str], filename: str):
    path = OUT_DIR / filename
    data = yf.download(ticker, start=start, end=end, auto_adjust=False)
    if data.empty:
        raise SystemExit(f"{ticker} download returned no rows")
    data = data[["Adj Close"]]
    data.columns = ["adj_close"]
    data.to_csv(path)
    return data


def build_realized_vol(window: int, data, filename: str) -> None:
    data = data.assign(log_ret=np.log(data["adj_close"]).diff())
    vol = data["log_ret"].rolling(window).std() * np.sqrt(252)
    vol.dropna().to_csv(OUT_DIR / filename, header=True)


def main() -> None:
    parser = parse_args()
    args = parser.parse_args()
    data = download_data(args.index, args.start, args.end, args.price_output)
    build_realized_vol(args.window, data, args.output)
    print("Euro STOXX price and realized-vol proxy stored in", OUT_DIR)


if __name__ == "__main__":
    main()
