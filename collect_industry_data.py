
"""
Industry Data Expansion Module
Extends data_pipeline with sector equities, commodities, FX, and credit ETFs

Usage:
    python collect_industry_data.py

Output:
    - industry_data_raw.csv: All 52+ newly collected series
    - Merges with existing stress_indicators.csv to create expanded dataset
"""

import json
import os
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set

import pandas as pd
import yaml
import yfinance as yf
from dotenv import load_dotenv
from fredapi import Fred
import requests

# Load FRED API key from .env
load_dotenv()
FRED_API_KEY = os.getenv("FRED_API_KEY")

fred: Fred | None
if not FRED_API_KEY:
    fred = None
    print(
        "[WARN] FRED_API_KEY not found in .env; FRED downloads will be skipped unless cached series are available."
    )
else:
    fred = Fred(api_key=FRED_API_KEY)
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config" / "country_blocks_extended.yaml"
DATE_CANDIDATES = [
    "date",
    "Date",
    "DATE",
    "period",
    "Period",
    "PERIOD",
    "time",
    "Time",
    "TIME",
    "observation_date",
    "Month",
    "month",
]
VALUE_CANDIDATES = [
    "value",
    "Value",
    "VALUE",
    "close",
    "Close",
    "CLOSE",
    "price",
    "Price",
    "spread",
    "Spread",
    "rate",
    "Rate",
    "yield",
    "Yield",
    "nominal_gdp_usd",
    "euribor_3m",
    "sofr_3m",
    "cds_proxy",
]

SERIES_METADATA_PATH = BASE_DIR / "config" / "series_metadata.yaml"
DEFAULT_SERIES_METADATA = {"frequency": "daily", "measurement_type": "level"}
DIAGNOSTIC_OUTPUT_DIR = BASE_DIR / "analysis_outputs" / "diagnostics"
OUTPUT_FILE = BASE_DIR / "industry_data_raw.csv"
SUMMARY_FILE = BASE_DIR / "industry_data_summary.json"
EUROSTAT_MACRO_FILE = BASE_DIR / "data" / "eurostat_macro_raw.csv"
_CACHED_INDUSTRY_DATA: pd.DataFrame | None = None


def load_eurostat_macro_series() -> Dict[str, pd.Series]:
    """Load pre-fetched Eurostat macro series.

    Expected format: a wide CSV with a date-like index column and one column per series.
    """

    if not EUROSTAT_MACRO_FILE.exists():
        return {}

    try:
        df = pd.read_csv(EUROSTAT_MACRO_FILE, index_col=0, parse_dates=True)
    except Exception as exc:
        print(f"  [WARN] Failed to read Eurostat macro file {EUROSTAT_MACRO_FILE}: {exc}")
        return {}

    if df.empty:
        return {}

    try:
        idx = pd.to_datetime(df.index, errors="coerce").normalize()
        df.index = idx
        df = df[~df.index.isna()]
        df = df[~df.index.duplicated(keep="last")].sort_index()
    except Exception:
        pass

    out: Dict[str, pd.Series] = {}
    for col in df.columns:
        if not isinstance(col, str) or not col.strip():
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        s = pd.Series(series.values, index=df.index, name=col).dropna()
        if s.empty:
            continue
        out[col] = s
        _register_series_metadata(col)

    if out:
        print(f"  [eurostat] Loaded {len(out)} macro series from {EUROSTAT_MACRO_FILE.name}")
    return out


def _coerce_datetime(values: pd.Series) -> pd.DatetimeIndex:
    text = values.astype(str)
    if text.str.contains("Q").any():
        try:
            period_index = pd.PeriodIndex(text, freq="Q", name="date")
            return period_index.to_timestamp(how="end")
        except Exception:
            pass
    try:
        return pd.to_datetime(text, errors="coerce")
    except Exception:
        return pd.to_datetime([], errors="coerce")


def _detect_date_column(df: pd.DataFrame) -> str:
    for candidate in DATE_CANDIDATES:
        if candidate in df.columns:
            return candidate
    return df.columns[0]


def _detect_value_column(df: pd.DataFrame, exclude: str) -> str:
    for candidate in VALUE_CANDIDATES:
        if candidate in df.columns and candidate != exclude:
            return candidate
    for column in df.columns:
        if column != exclude:
            return column
    raise ValueError("No value column found")


def _load_series_from_csv(path: Path) -> Optional[pd.Series]:
    try:
        df = pd.read_csv(path)
    except Exception as exc:
        print(f"  [WARN] Failed to read {path}: {exc}")
        return None
    if df.empty:
        return None
    date_col = _detect_date_column(df)
    value_col = _detect_value_column(df, exclude=date_col)
    idx = _coerce_datetime(df[date_col])
    series = pd.to_numeric(df[value_col], errors="coerce")
    data = pd.Series(series.values, index=idx, name=path.stem)
    data = data.dropna()
    data = data[~data.index.duplicated(keep="last")].sort_index()
    return data


def _load_series_metadata_config() -> Dict[str, Dict[str, str]]:
    if not SERIES_METADATA_PATH.exists():
        return {}
    try:
        with SERIES_METADATA_PATH.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
        return raw.get("series_metadata", {})
    except Exception as exc:
        print(f"  [WARN] Failed to read series metadata: {exc}")
        return {}


SERIES_METADATA = _load_series_metadata_config()
SERIES_METADATA_USAGE: Dict[str, Dict[str, str]] = {}
SERIES_METADATA_DEFAULTED: Set[str] = set()


def _register_series_metadata(series_name: str) -> Dict[str, str]:
    entry = SERIES_METADATA.get(series_name, {})
    use_default = not bool(entry)
    metadata = {
        "frequency": entry.get("frequency", DEFAULT_SERIES_METADATA["frequency"]),
        "measurement_type": entry.get("measurement_type", DEFAULT_SERIES_METADATA["measurement_type"]),
        "source": "config" if not use_default else "default",
    }
    SERIES_METADATA_USAGE[series_name] = metadata
    if use_default:
        SERIES_METADATA_DEFAULTED.add(series_name)
    return metadata


def _write_metadata_review() -> None:
    DIAGNOSTIC_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    freq_counts = Counter(m["frequency"] for m in SERIES_METADATA_USAGE.values())
    measurement_counts = Counter(m["measurement_type"] for m in SERIES_METADATA_USAGE.values())
    summary = {
        "timestamp": datetime.now().isoformat(),
        "total_series": len(SERIES_METADATA_USAGE),
        "defaulted_series": sorted(list(SERIES_METADATA_DEFAULTED)),
        "defaulted_count": len(SERIES_METADATA_DEFAULTED),
        "frequency_distribution": dict(freq_counts),
        "measurement_distribution": dict(measurement_counts),
        "default_sample": sorted(list(SERIES_METADATA_DEFAULTED))[:20],
    }
    summary_path = DIAGNOSTIC_OUTPUT_DIR / "collection_series_metadata_review.json"
    with summary_path.open("w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    print(
        f"  [INFO] Wrote metadata review: {summary['total_series']} series tracked, {summary['defaulted_count']} used defaults -> {summary_path}"
    )


def _load_summary_cache() -> Dict[str, Any]:
    if not SUMMARY_FILE.exists():
        return {}
    try:
        with SUMMARY_FILE.open("r", encoding="utf-8") as fh:
            return json.load(fh) or {}
    except Exception as exc:
        print(f"  [WARN] Failed to read summary cache: {exc}")
        return {}


def _collection_completed_today() -> bool:
    if not OUTPUT_FILE.exists():
        return False
    summary = _load_summary_cache()
    timestamp = summary.get("collection_date")
    if not timestamp:
        return False
    try:
        recorded = datetime.fromisoformat(timestamp)
    except ValueError:
        return False
    return recorded.date() == datetime.now().date()


def _get_cached_industry_df() -> Optional[pd.DataFrame]:
    global _CACHED_INDUSTRY_DATA
    if _CACHED_INDUSTRY_DATA is not None:
        return _CACHED_INDUSTRY_DATA
    if not OUTPUT_FILE.exists():
        return None
    try:
        df = pd.read_csv(OUTPUT_FILE, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index, errors="coerce")
        df = df[~df.index.isna()]
        if df.index.name is None:
            df.index.name = "Date"
        _CACHED_INDUSTRY_DATA = df
        return df
    except Exception as exc:
        print(f"  [WARN] Failed to read cached industry data: {exc}")
        return None


def _load_cached_industry_data() -> pd.DataFrame:
    df = _get_cached_industry_df()
    if df is None:
        raise RuntimeError("Cached industry data is not available")
    return df


def _load_cached_series(series_name: str) -> Optional[pd.Series]:
    df = _get_cached_industry_df()
    if df is None or series_name not in df.columns:
        return None
    series = df[series_name].dropna()
    if series.empty:
        return None
    return series


def _populate_from_cache(series_name: str, target: Dict[str, pd.Series], err: Exception) -> bool:
    cached = _load_cached_series(series_name)
    if cached is None:
        return False
    print(f"  [WARN] Using cached {series_name} after download failure: {err}")
    target[series_name] = cached
    _register_series_metadata(series_name)
    return True


def _print_summary_statistics(df: pd.DataFrame) -> None:
    if df.empty:
        print("  [INFO] Cached dataset is empty.")
        return
    idx = pd.to_datetime(df.index, errors="coerce")
    timestamp_info = "unknown"
    if idx.notna().any():
        timestamp_info = f"{idx.min()} to {idx.max()}"
    print(f"Date range: {timestamp_info}")
    print(f"Total rows: {len(df)}")
    print(f"Total columns: {len(df.columns)}")


def _reuse_cached_data() -> None:
    print("\n[cache] Cached download already run today. Skipping API calls.")
    df_combined = _load_cached_industry_data()

    # Even on cached runs, ingest local provider files (e.g., Eurostat macro) so
    # downstream merges see the updated universe without forcing API refresh.
    eurostat_series = load_eurostat_macro_series()
    if eurostat_series:
        euro_cols = list(eurostat_series.keys())
        df_base = df_combined.drop(columns=[c for c in euro_cols if c in df_combined.columns], errors="ignore")
        df_euro = pd.DataFrame(eurostat_series)
        df_combined = pd.concat([df_base, df_euro], axis=1)
        df_combined.index.name = "Date"
        df_combined.to_csv(OUTPUT_FILE)
        print(f"[cache] Refreshed Eurostat macro columns ({len(euro_cols)} series) -> {OUTPUT_FILE.name}")

    # Also refresh any local manual series configured in the country-block YAML
    # (e.g., swaption vol proxies) so upstream edits propagate without forcing an API run.
    manual_series = load_manual_series_from_config()
    if manual_series:
        manual_cols = list(manual_series.keys())
        df_base = df_combined.drop(columns=[c for c in manual_cols if c in df_combined.columns], errors="ignore")
        df_manual = pd.DataFrame(manual_series)
        df_combined = pd.concat([df_base, df_manual], axis=1)
        df_combined.index.name = "Date"
        df_combined.to_csv(OUTPUT_FILE)
        print(f"[cache] Refreshed manual local columns ({len(manual_cols)} series) -> {OUTPUT_FILE.name}")
    _print_summary_statistics(df_combined)
    summary = _load_summary_cache()
    cached_time = summary.get("collection_date")
    if cached_time:
        print(f"[cache] Cached collection timestamp: {cached_time}")
    print(f"[cache] Using {OUTPUT_FILE.name}")
    if SUMMARY_FILE.exists():
        print(f"[cache] Summary file: {SUMMARY_FILE.name}")
    print("\n" + "=" * 80)
    print("COLLECTION COMPLETE! (cached)")
    print("=" * 80)
    print("Next step: Merge with existing stress_indicators.csv")
    print(f"Expected total: 72 (existing) + {len(df_combined.columns)} (new) = {72 + len(df_combined.columns)} series")


def load_manual_series_from_config() -> Dict[str, pd.Series]:
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    mapping: Dict[str, Dict[str, str]] = {}
    for country in config.get("country_blocks", []):
        for block in country.get("blocks", []):
            local_map = block.get("local_series_files", {}) or {}
            for series_name, rel_path in local_map.items():
                if not isinstance(rel_path, str):
                    continue
                if series_name not in mapping:
                    mapping[series_name] = {"path": rel_path}
    loaded: Dict[str, pd.Series] = {}
    cache: Dict[Path, pd.Series] = {}
    print(f"\n[manual] Loading {len(mapping)} configured local series...")
    for series_name, spec in mapping.items():
        rel_path = spec["path"]
        path = (BASE_DIR / rel_path).resolve()
        if not path.exists():
            print(f"  [WARN] Missing file for {series_name}: {rel_path}")
            continue
        base_series = cache.get(path)
        if base_series is None:
            base_series = _load_series_from_csv(path)
            if base_series is None or base_series.empty:
                continue
            cache[path] = base_series
        loaded[series_name] = base_series.copy().rename(series_name)
        _register_series_metadata(series_name)
    print(f"  -> Loaded {len(loaded)} manual series")
    return loaded

# Date range (matching existing pipeline)
START_DATE = "1990-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d")


def safe_get_fred_series(
    series_id: str,
    observation_start: str = START_DATE,
    force_api: bool = False,
) -> pd.Series:
    """Fetch a FRED series with a fallback that avoids date parsing issues."""

    if not FRED_API_KEY or fred is None:
        cached_series = _load_cached_series(series_id)
        if cached_series is not None:
            print(f"  [WARN] Using cached {series_id} (FRED API key not configured)")
            return cached_series
        raise RuntimeError(f"FRED_API_KEY not configured and no cached series available for {series_id}")

    last_exc: Exception | None = None
    if not force_api:
        try:
            series = fred.get_series(series_id, observation_start=observation_start)
            return pd.Series(series).dropna()
        except Exception as exc:  # pragma: no cover - network path
            last_exc = exc

    fallback_url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id={series_id}&api_key={FRED_API_KEY}&observation_start={observation_start}&file_type=json"
    )
    fallback_exc: Exception | None = None
    try:
        resp = requests.get(fallback_url, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("observations", [])
        if not data:
            raise RuntimeError(f"No observations returned for {series_id}")
        dates = pd.to_datetime([item["date"] for item in data])
        values = pd.to_numeric([item["value"] for item in data], errors="coerce")
        series = pd.Series(values, index=dates, name=series_id).dropna()
        if series.empty:
            raise RuntimeError(f"No valid data extracted for {series_id}")
        return series
    except Exception as exc:
        fallback_exc = exc

    cached_series = _load_cached_series(series_id)
    if cached_series is not None:
        source_err = fallback_exc or last_exc or Exception("unknown reason")
        print(f"  [WARN] Using cached {series_id} after FRED download failure: {source_err}")
        return cached_series

    if fallback_exc:
        raise RuntimeError(f"Failed to download {series_id}") from fallback_exc
    if last_exc:
        raise RuntimeError(f"Failed to download {series_id}") from last_exc
    raise RuntimeError(f"Failed to download {series_id}")

print("=" * 80)
print("INDUSTRY DATA EXPANSION - FULL COLLECTION")
print("=" * 80)
print(f"Date range: {START_DATE} to {END_DATE}")

if _collection_completed_today():
    _reuse_cached_data()
    raise SystemExit(0)

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
        _register_series_metadata(ticker)
        print(f" {len(df)} days")
    except Exception as e:
        if _populate_from_cache(ticker, sector_data, e):
            continue
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
    "BAMLC0A0CM": "US Corporate Master OAS",
    "BAMLH0A1HYBB": "US BB OAS",
    "BAMLH0A2HYBEY": "US B OAS",
}

credit_data = {}
for series_id, description in fred_credit.items():
    try:
        print(f"  Downloading {series_id:20} ({description})...", end="")
        df = safe_get_fred_series(series_id)
        credit_data[series_id] = df
        _register_series_metadata(series_id)
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
        _register_series_metadata(ticker)
        print(f" {len(df)} days")
    except Exception as e:
        if _populate_from_cache(ticker, credit_data, e):
            continue
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
        df = safe_get_fred_series(series_id)
        commodity_data[series_id] = df
        _register_series_metadata(series_id)
        print(f" {len(df)} days")
    except Exception as e:
        if _populate_from_cache(series_id, commodity_data, e):
            continue
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
        df = safe_get_fred_series(series_id, force_api=True)
        fx_data[series_id] = df
        _register_series_metadata(series_id)
        print(f" {len(df)} days")
    except Exception as e:
        if _populate_from_cache(series_id, fx_data, e):
            continue
        print(f" [ERROR] {e}")

print(f"  Collected {len(fx_data)}/{len(fx_pairs)} FX pairs")

# ============================================================================
# 5. MANUAL / CONFIG-DEFINED PRIORITY SERIES
# ============================================================================

manual_series = load_manual_series_from_config()
print(f"\n[extra] Collected {len(manual_series)} manual/config series")

# ============================================================================
# 6. LOCAL EUROSTAT MACRO (pre-fetched)
# ============================================================================

eurostat_macro = load_eurostat_macro_series()

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
all_data.update(manual_series)
all_data.update(eurostat_macro)

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

_print_summary_statistics(df_combined)

# Save raw data
df_combined.to_csv(OUTPUT_FILE)
print(f"\n[SAVED] {OUTPUT_FILE.name}")

# Generate summary statistics
summary = {
    "collection_date": datetime.now().isoformat(),
    "requested_end_date": END_DATE,
    "total_series": len(all_data),
    "categories": {
        "sector_equities": len(sector_data),
        "credit_spreads": len(credit_data),
        "commodities": len(commodity_data),
        "fx_pairs": len(fx_data),
        "manual_series": len(manual_series),
        "eurostat_macro": len(eurostat_macro),
    },
    "date_range": {
        "start": str(df_combined.index.min()),
        "end": str(df_combined.index.max()),
        "n_days": len(df_combined)
    },
    "as_of": {
        "as_of_date": str(df_combined.index.max()),
        "as_of_source": "max_index_date",
    },
    "series_list": list(all_data.keys())
}

with SUMMARY_FILE.open("w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2)

print(f"[SAVED] {SUMMARY_FILE.name}")

_write_metadata_review()

print("\n" + "=" * 80)
print("COLLECTION COMPLETE!")
print("=" * 80)
print(f"Next step: Merge with existing stress_indicators.csv")
print(f"Expected total: 72 (existing) + {len(all_data)} (new) = {72 + len(all_data)} series")
