"""
Fetch CSUSHPISA (S&P/Case-Shiller U.S. National Home Price Index) from FRED
and save as USA_HPI_REAL for the merge pipeline.

Usage:
    python scripts/fetch_usa_hpi.py
"""
from pathlib import Path
import pandas as pd
import os

try:
    from fredapi import Fred
    from dotenv import load_dotenv
    load_dotenv()
    FRED_API_KEY = os.getenv("FRED_API_KEY")
    if not FRED_API_KEY:
        raise ValueError("FRED_API_KEY not found in .env")
    fred = Fred(api_key=FRED_API_KEY)
except ImportError:
    print("[ERROR] fredapi or dotenv not installed. Install with: pip install fredapi python-dotenv")
    exit(1)

ROOT = Path('.').resolve()
OUTPUT_DIR = ROOT / 'data_repository' / 'raw' / 'fred'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = OUTPUT_DIR / 'CSUSHPISA.csv'

def main():
    print("Fetching CSUSHPISA from FRED...")
    data = fred.get_series('CSUSHPISA')
    
    df = pd.DataFrame({'Date': data.index, 'CSUSHPISA': data.values})
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"[SAVED] {OUTPUT_PATH}")
    print(f"  {len(data)} observations from {data.index.min()} to {data.index.max()}")

if __name__ == '__main__':
    main()
