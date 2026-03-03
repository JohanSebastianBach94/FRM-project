
"""
Industry Data Expansion Module
Extends data_pipeline with sector equities, commodities, FX, and credit ETFs

Usage:
    python collect_industry_data.py

Output:
    - industry_data_raw.csv: All 52+ newly collected series
    - Merges with existing stress_indicators.csv to create expanded dataset
"""

import pandas as pd
import yfinance as yf
from fredapi import Fred
from datetime import datetime
import os
from dotenv import load_dotenv
import json

# Load FRED API key from .env
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

if not FRED_API_KEY:
    raise ValueError("FRED_API_KEY not found in .env file. Get one at https://fred.stlouisfed.org/docs/api/api_key.html")

fred = Fred(api_key=FRED_API_KEY)

# Date range (matching existing pipeline)
START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")

print("=" * 80)
print("INDUSTRY DATA EXPANSION - FULL COLLECTION")
print("=" * 80)
print(f"Date range: {START_DATE} to {END_DATE}")

# ============================================================================
# 1. SECTOR EQUITY INDICES (Yahoo Finance)
# ============================================================================

print("\n[1/4] Collecting Sector Equity Indices...")

sector_tickers = {
    # US GICS Sectors (11)
    "XLF": "US Financials",
    "XLE": "US Energy", 
    "XLV": "US Healthcare",
    "XLK": "US Technology",
    "XLI": "US Industrials",
    "XLP": "US Consumer Staples",
    "XLY": "US Consumer Discretionary",
    "XLU": "US Utilities",
    "XLB": "US Materials",
    "XLRE": "US Real Estate",
    "XLC": "US Communication Services",
    
    # Additional sector/regional coverage (13)
    "EWJ": "Japan MSCI",
    "EWT": "Taiwan MSCI",
    "EWY": "South Korea MSCI",
    "EWZ": "Brazil MSCI",
    "EWW": "Mexico MSCI",
    "FXI": "China Large Cap",
    "EWU": "UK MSCI",
    "EWG": "Germany MSCI",
    "EWQ": "France MSCI",
    "EWI": "Italy MSCI",
    "EWP": "Spain MSCI",
    "EWC": "Canada MSCI",
    "EZU": "Eurozone MSCI"
}

sector_data = {}
for ticker, description in sector_tickers.items():
    try:
        print(f"  Downloading {ticker:8} ({description})...", end="")
        df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)
        
        # Handle MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df = df["Close"]  # Extract Close prices
        elif "Close" in df.columns:
            df = df["Close"]
        else:
            # Use first available price column
            df = df.iloc[:, 0]
        
        df = df.dropna()
        sector_data[ticker] = df
        print(f" {len(df)} days")
    except Exception as e:
        print(f" [ERROR] {e}")

print(f"  Collected {len(sector_data)}/{len(sector_tickers)} sector indices")

# ============================================================================
# 2. CREDIT SPREADS & ETF PROXIES (FRED + Yahoo)
# ============================================================================

print("\n[2/4] Collecting Credit Spreads...")

# FRED credit spreads
fred_credit = {
    "BAMLC0A1CAAAEY": "US AAA OAS",
    "BAMLC0A2CAAEY": "US AA OAS",
    "BAMLC0A3CAEY": "US A OAS",
    "BAMLH0A1HYBB": "US BB OAS",
    "BAMLH0A2HYBEY": "US B OAS",
}

credit_data = {}
for series_id, description in fred_credit.items():
    try:
        print(f"  Downloading {series_id:20} ({description})...", end="")
        df = fred.get_series(series_id, observation_start=START_DATE)
        df = df.dropna()
        credit_data[series_id] = df
        print(f" {len(df)} days")
    except Exception as e:
        print(f" [ERROR] {e}")

# Credit ETF proxies (for sector-specific credit risk)
credit_etfs = {
    "HYG": "US High Yield ETF",
    "LQD": "US Investment Grade ETF",
    "VCIT": "Intermediate Corp Bond ETF",
    "JNK": "High Yield Bond ETF",
    "EMLC": "EM Local Currency Bond ETF"
}

for ticker, description in credit_etfs.items():
    try:
        print(f"  Downloading {ticker:8} ({description})...", end="")
        df = yf.download(ticker, start=START_DATE, end=END_DATE, progress=False)
        
        if isinstance(df.columns, pd.MultiIndex):
            df = df["Close"]
        elif "Close" in df.columns:
            df = df["Close"]
        else:
            df = df.iloc[:, 0]
        
        df = df.dropna()
        credit_data[ticker] = df
        print(f" {len(df)} days")
    except Exception as e:
        print(f" [ERROR] {e}")

print(f"  Collected {len(credit_data)}/{len(fred_credit) + len(credit_etfs)} credit series")

# ============================================================================
# 3. COMMODITIES (FRED)
# ============================================================================

print("\n[3/4] Collecting Commodities...")

commodities = {
    # Energy
    "DCOILBRENTEU": "Brent Crude",
    "DCOILWTICO": "WTI Crude",
    "DHHNGSP": "Natural Gas",
    
    # Metals
    "GOLDAMGBD228NLBM": "Gold",
    "PCOPPUSDM": "Copper",
    "PALUMUSDM": "Aluminum",
    "PIORECRUSDM": "Iron Ore",
    
    # Agriculture
    "PWHEAMTUSDM": "Wheat",
    "PMAIZMTUSDM": "Corn",
    "PSOYBUSDQ": "Soybeans"
}

commodity_data = {}
for series_id, description in commodities.items():
    try:
        print(f"  Downloading {series_id:20} ({description})...", end="")
        df = fred.get_series(series_id, observation_start=START_DATE)
        df = df.dropna()
        commodity_data[series_id] = df
        print(f" {len(df)} days")
    except Exception as e:
        print(f" [ERROR] {e}")

print(f"  Collected {len(commodity_data)}/{len(commodities)} commodities")

# ============================================================================
# 4. FX PAIRS (FRED)
# ============================================================================

print("\n[4/4] Collecting FX Pairs...")

fx_pairs = {
    # Major
    "DEXUSEU": "USD/EUR",
    "DEXJPUS": "JPY/USD",
    "DEXUSUK": "USD/GBP",
    "DEXSZUS": "CHF/USD",
    
    # Emerging Markets
    "DEXBZUS": "BRL/USD",
    "DEXMXUS": "MXN/USD",
    "DEXCHUS": "CNY/USD",
    "DEXKOUS": "KRW/USD"
}

fx_data = {}
for series_id, description in fx_pairs.items():
    try:
        print(f"  Downloading {series_id:20} ({description})...", end="")
        df = fred.get_series(series_id, observation_start=START_DATE)
        df = df.dropna()
        fx_data[series_id] = df
        print(f" {len(df)} days")
    except Exception as e:
        print(f" [ERROR] {e}")

print(f"  Collected {len(fx_data)}/{len(fx_pairs)} FX pairs")

# ============================================================================
# 5. COMBINE AND SAVE
# ============================================================================

print("\n" + "=" * 80)
print("COMBINING ALL DATA...")
print("=" * 80)

# Combine all dictionaries
all_data = {}
all_data.update(sector_data)
all_data.update(credit_data)
all_data.update(commodity_data)
all_data.update(fx_data)

print(f"\nTotal series collected: {len(all_data)}")

# Convert to DataFrame with proper date alignment
# Flatten any 2D Series objects (from yf.download with single ticker)
for key, value in all_data.items():
    if isinstance(value, pd.DataFrame):
        # Extract the first column if it's a DataFrame
        all_data[key] = value.iloc[:, 0]
    elif isinstance(value, pd.Series) and len(value.shape) > 1:
        all_data[key] = value.squeeze()

df_combined = pd.DataFrame(all_data)
df_combined.index.name = "Date"

print(f"Date range: {df_combined.index.min()} to {df_combined.index.max()}")
print(f"Total rows: {len(df_combined)}")
print(f"Total columns: {len(df_combined.columns)}")

# Save raw data
output_file = "industry_data_raw.csv"
df_combined.to_csv(output_file)
print(f"\n[SAVED] {output_file}")

# Generate summary statistics
summary = {
    "collection_date": datetime.now().isoformat(),
    "total_series": len(all_data),
    "categories": {
        "sector_equities": len(sector_data),
        "credit_spreads": len(credit_data),
        "commodities": len(commodity_data),
        "fx_pairs": len(fx_data)
    },
    "date_range": {
        "start": str(df_combined.index.min()),
        "end": str(df_combined.index.max()),
        "n_days": len(df_combined)
    },
    "series_list": list(all_data.keys())
}

with open("industry_data_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print("[SAVED] industry_data_summary.json")

print("\n" + "=" * 80)
print("COLLECTION COMPLETE!")
print("=" * 80)
print(f"Next step: Merge with existing stress_indicators.csv")
print(f"Expected total: 72 (existing) + {len(all_data)} (new) = {72 + len(all_data)} series")
