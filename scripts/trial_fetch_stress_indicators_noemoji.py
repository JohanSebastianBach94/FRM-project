"""
 TRIAL: Fetch Stress Testing Indicators
Smart, configuration-driven approach using FRED + Yahoo Finance

Strategy:
1. Batch fetch all FRED series in ONE API call (efficient!)
2. Fetch Yahoo Finance commodities (3 series, fast)
3. Compute derived indicators (sovereign spreads)
4. Align to common date range
5. Export to trial folder with quality report

Time: ~5 minutes to run (batch API calls are fast!)
"""

import sys
from pathlib import Path

# Add parent directory to path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Import configuration
from config.stress_indicators_config import (
    ALL_FRED_SERIES,
    COMMODITIES_YAHOO,
    EQUITY_INDICES_YAHOO,
    FX_RATES_YAHOO,
    ALL_YAHOO_SERIES,
    COMPUTED_SPREADS,
    DEFAULT_START_DATE,
    DEFAULT_END_DATE,
    STRESS_TEST_COUNTRIES,
    get_series_codes_list,
)

print("="*70)
print(" TRIAL: STRESS TESTING INDICATORS FETCHER")
print("="*70)
print("Strategy: FRED (batch) + Yahoo Finance (commodities, equities, FX)")
print(f"Target: {len(ALL_FRED_SERIES)} FRED series + {len(ALL_YAHOO_SERIES)} Yahoo series")
print(f"Date range: {DEFAULT_START_DATE} to {DEFAULT_END_DATE}")
print("="*70)

# ==============================================
#  SETUP OUTPUT DIRECTORIES
# ==============================================

OUTPUT_DIR = BASE_DIR / "output" / "trial data folder" / "stress_indicators"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"\n Output directory: {OUTPUT_DIR}")

# ==============================================
#  STEP 1: FETCH FRED DATA (BATCH MODE)
# ==============================================

print("\n" + "="*70)
print(" STEP 1: Fetching FRED Data (Batch Mode)")
print("="*70)

try:
    from fredapi import Fred
    
    # NOTE: User needs to set FRED_API_KEY environment variable
    # or replace 'your_api_key_here' with actual key
    try:
        import os
        FRED_API_KEY = os.getenv('FRED_API_KEY', 'your_api_key_here')
        fred = Fred(api_key=FRED_API_KEY)
        print(f" FRED API initialized")
    except Exception as e:
        print(f"  FRED API key issue: {e}")
        print("   Set environment variable: FRED_API_KEY")
        print("   Or get free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        fred = None
    
    fred_data = {}
    
    if fred:
        series_codes = get_series_codes_list()
        print(f"\n Fetching {len(series_codes)} FRED series...")
        
        for i, code in enumerate(series_codes, 1):
            try:
                series_name = ALL_FRED_SERIES[code]['name']
                print(f"   [{i}/{len(series_codes)}] Fetching {code} ({series_name})...", end='\r')
                
                # Fetch series data
                series_data = fred.get_series(
                    code,
                    observation_start=DEFAULT_START_DATE,
                    observation_end=DEFAULT_END_DATE
                )
                
                if series_data is not None and len(series_data) > 0:
                    fred_data[code] = series_data
                
                # Small delay to avoid rate limiting (120 calls/min = ~0.5s per call)
                import time
                time.sleep(0.6)
                    
            except Exception as e:
                print(f"\n     Failed to fetch {code}: {str(e)[:50]}")
                continue
        
        print(f"\n Successfully fetched {len(fred_data)}/{len(series_codes)} FRED series")
        
        # Create DataFrame with all FRED data
        fred_df = pd.DataFrame(fred_data)
        fred_df.index.name = 'date'
        
        # Add metadata columns
        fred_metadata = pd.DataFrame([
            {
                'series_code': code,
                'name': ALL_FRED_SERIES[code]['name'],
                'frequency': ALL_FRED_SERIES[code]['frequency'],
                'category': ALL_FRED_SERIES[code]['category'],
                'country': ALL_FRED_SERIES[code].get('country', 'N/A'),
                'first_date': fred_df[code].first_valid_index(),
                'last_date': fred_df[code].last_valid_index(),
                'observations': fred_df[code].notna().sum(),
                'missing_pct': (fred_df[code].isna().sum() / len(fred_df)) * 100
            }
            for code in fred_df.columns
        ])
        
        print(f"\n FRED Data Summary:")
        print(f"   Total series: {len(fred_df.columns)}")
        print(f"   Date range: {fred_df.index.min()} to {fred_df.index.max()}")
        print(f"   Total observations: {len(fred_df):,}")
        
    else:
        print(" FRED API not available - skipping FRED data")
        fred_df = pd.DataFrame()
        fred_metadata = pd.DataFrame()

except ImportError:
    print(" fredapi package not installed")
    print("   Install with: pip install fredapi")
    fred_df = pd.DataFrame()
    fred_metadata = pd.DataFrame()

# ==============================================
#  STEP 2: FETCH YAHOO FINANCE COMMODITIES
# ==============================================

print("\n" + "="*70)
print(" STEP 2: Fetching Yahoo Finance Data")
print("="*70)

try:
    import yfinance as yf
    
    yahoo_data = {}
    total_yahoo = len(ALL_YAHOO_SERIES)
    
    print(f"\n Fetching {total_yahoo} Yahoo Finance series...")
    print(f"   - Commodities: {len(COMMODITIES_YAHOO)}")
    print(f"   - Equity Indices: {len(EQUITY_INDICES_YAHOO)}")
    print(f"   - FX Rates: {len(FX_RATES_YAHOO)}")
    
    for i, (ticker, meta) in enumerate(ALL_YAHOO_SERIES.items(), 1):
        try:
            name = meta['name']
            category = meta.get('category', 'unknown')
            print(f"   [{i}/{total_yahoo}] Fetching {ticker} ({name}) [{category}]...", end='\r')
            
            # Download data
            data = yf.download(
                ticker,
                start=DEFAULT_START_DATE,
                end=DEFAULT_END_DATE,
                progress=False
            )
            
            if not data.empty:
                # Use 'Close' price for all assets
                yahoo_data[name] = data['Close']
                
        except Exception as e:
            print(f"\n     Failed to fetch {ticker}: {str(e)[:50]}")
            continue
    
    print(f"\n Successfully fetched {len(yahoo_data)}/{total_yahoo} Yahoo series")
    
    # Create DataFrame from dictionary of Series using concat
    if yahoo_data:
        yahoo_df = pd.concat(yahoo_data, axis=1)
        yahoo_df.index.name = 'date'
    else:
        yahoo_df = pd.DataFrame()
    
    # Metadata
    yahoo_metadata = pd.DataFrame([
        {
            'ticker': ticker,
            'name': name,
            'category': meta.get('category', 'unknown'),
            'first_date': yahoo_df[name].first_valid_index().strftime('%Y-%m-%d') if name in yahoo_df.columns else None,
            'last_date': yahoo_df[name].last_valid_index().strftime('%Y-%m-%d') if name in yahoo_df.columns else None,
            'observations': yahoo_df[name].notna().sum() if name in yahoo_df.columns else 0,
        }
        for ticker, meta in ALL_YAHOO_SERIES.items()
        for name in [meta['name']]
        if name in yahoo_df.columns
    ])
    
    print(f"\n Yahoo Finance Data Summary:")
    print(f"   Total series: {len(yahoo_df.columns)}")
    print(f"   Date range: {yahoo_df.index.min()} to {yahoo_df.index.max()}")
    print(f"   Total observations: {len(yahoo_df):,}")

except ImportError:
    print(" yfinance package not installed")
    print("   Install with: pip install yfinance")
    yahoo_df = pd.DataFrame()
    yahoo_metadata = pd.DataFrame()

# ==============================================
#  STEP 3: COMPUTE DERIVED INDICATORS
# ==============================================

print("\n" + "="*70)
print(" STEP 3: Computing Derived Indicators (Sovereign Spreads)")
print("="*70)

computed_data = {}

if not fred_df.empty:
    print(f"\n Computing {len(COMPUTED_SPREADS)} sovereign spreads...")
    
    for spread_name, spread_info in COMPUTED_SPREADS.items():
        try:
            components = spread_info['components']
            
            # Check if both components exist
            if all(comp in fred_df.columns for comp in components):
                # Compute spread
                spread = fred_df[components[0]] - fred_df[components[1]]
                computed_data[spread_name] = spread
                
                print(f"    {spread_name}: {spread_info['description']}")
            else:
                missing = [c for c in components if c not in fred_df.columns]
                print(f"     {spread_name}: Missing components {missing}")
                
        except Exception as e:
            print(f"    {spread_name}: Failed - {str(e)[:50]}")
    
    computed_df = pd.DataFrame(computed_data)
    computed_df.index.name = 'date'
    
    print(f"\n Computed {len(computed_df.columns)} derived indicators")
else:
    computed_df = pd.DataFrame()
    print("  No FRED data available for computing spreads")

# ==============================================
#  STEP 4: EXPORT DATA
# ==============================================

print("\n" + "="*70)
print(" STEP 4: Exporting Data to Trial Folder")
print("="*70)

# Export FRED data
if not fred_df.empty:
    fred_export = OUTPUT_DIR / "fred_stress_indicators.csv"
    fred_df.to_csv(fred_export)
    print(f" FRED data exported: {fred_export}")
    print(f"   {len(fred_df.columns)} series, {len(fred_df):,} dates")
    
    # Export FRED metadata
    fred_meta_export = OUTPUT_DIR / "fred_metadata.csv"
    fred_metadata.to_csv(fred_meta_export, index=False)
    print(f" FRED metadata exported: {fred_meta_export}")

# Export Yahoo data
if not yahoo_df.empty:
    yahoo_export = OUTPUT_DIR / "yahoo_market_data.csv"
    yahoo_df.to_csv(yahoo_export)
    print(f" Yahoo Finance data exported: {yahoo_export}")
    print(f"   {len(yahoo_df.columns)} series, {len(yahoo_df):,} dates")
    
    # Export Yahoo metadata
    yahoo_meta_export = OUTPUT_DIR / "yahoo_metadata.csv"
    yahoo_metadata.to_csv(yahoo_meta_export, index=False)
    print(f" Yahoo metadata exported: {yahoo_meta_export}")

# Export computed spreads
if not computed_df.empty:
    computed_export = OUTPUT_DIR / "sovereign_spreads.csv"
    computed_df.to_csv(computed_export)
    print(f" Sovereign spreads exported: {computed_export}")
    print(f"   {len(computed_df.columns)} spreads, {len(computed_df):,} dates")

# ==============================================
#  STEP 5: GENERATE QUALITY REPORT
# ==============================================

print("\n" + "="*70)
print(" STEP 5: Data Quality Report")
print("="*70)

report_lines = []
report_lines.append("="*70)
report_lines.append("STRESS TESTING INDICATORS - DATA QUALITY REPORT")
report_lines.append("="*70)
report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
report_lines.append(f"Date Range: {DEFAULT_START_DATE} to {DEFAULT_END_DATE}")
report_lines.append("")

# FRED Summary
report_lines.append("1. FRED DATA SUMMARY")
report_lines.append("-" * 70)
if not fred_df.empty:
    report_lines.append(f"Total Series Fetched: {len(fred_df.columns)}")
    report_lines.append(f"Date Range: {fred_df.index.min()} to {fred_df.index.max()}")
    report_lines.append(f"Total Observations: {len(fred_df):,}")
    report_lines.append("")
    
    # By category
    report_lines.append("By Category:")
    for category in ['credit', 'sovereign', 'inflation', 'macro', 'monetary', 'commodity']:
        cat_series = [c for c in fred_df.columns if ALL_FRED_SERIES.get(c, {}).get('category') == category]
        if cat_series:
            report_lines.append(f"  {category.capitalize()}: {len(cat_series)} series")
    
    report_lines.append("")
    
    # Data quality
    report_lines.append("Data Quality:")
    missing_pct = (fred_df.isna().sum().sum() / (len(fred_df) * len(fred_df.columns))) * 100
    report_lines.append(f"  Overall Missing Data: {missing_pct:.2f}%")
    
    # Series with most missing data
    missing_by_series = ((fred_df.isna().sum() / len(fred_df)) * 100).sort_values(ascending=False)
    report_lines.append("  Top 5 Series with Missing Data:")
    for series, pct in missing_by_series.head(5).items():
        series_name = ALL_FRED_SERIES.get(series, {}).get('name', series)
        report_lines.append(f"    {series_name}: {pct:.1f}%")
else:
    report_lines.append("No FRED data available")

report_lines.append("")

# Yahoo Finance Summary
report_lines.append("2. YAHOO FINANCE DATA SUMMARY")
report_lines.append("-" * 70)
if not yahoo_df.empty:
    report_lines.append(f"Total Series Fetched: {len(yahoo_df.columns)}")
    report_lines.append(f"Date Range: {yahoo_df.index.min()} to {yahoo_df.index.max()}")
    report_lines.append(f"Total Observations: {len(yahoo_df):,}")
    
    missing_pct = (yahoo_df.isna().sum().sum() / (len(yahoo_df) * len(yahoo_df.columns))) * 100
    report_lines.append(f"Overall Missing Data: {missing_pct:.2f}%")
else:
    report_lines.append("No Yahoo Finance data available")

report_lines.append("")

# Computed Spreads Summary
report_lines.append("3. COMPUTED SOVEREIGN SPREADS")
report_lines.append("-" * 70)
if not computed_df.empty:
    report_lines.append(f"Total Spreads Computed: {len(computed_df.columns)}")
    report_lines.append("")
    report_lines.append("Latest Values:")
    latest = computed_df.iloc[-1]
    for spread, value in latest.items():
        if not pd.isna(value):
            report_lines.append(f"  {spread}: {value:.2f} bps")
else:
    report_lines.append("No spreads computed")

report_lines.append("")
report_lines.append("="*70)
report_lines.append("COVERAGE ASSESSMENT")
report_lines.append("="*70)

total_target = len(ALL_FRED_SERIES) + len(COMMODITIES_YAHOO)
total_fetched = len(fred_df.columns if not fred_df.empty else []) + len(yahoo_df.columns if not yahoo_df.empty else [])
coverage_pct = (total_fetched / total_target) * 100

report_lines.append(f"Target Indicators: {total_target}")
report_lines.append(f"Successfully Fetched: {total_fetched}")
report_lines.append(f"Coverage: {coverage_pct:.1f}%")
report_lines.append("")

if coverage_pct >= 90:
    report_lines.append("[OK] EXCELLENT - Ready for stress testing!")
elif coverage_pct >= 70:
    report_lines.append("[OK] GOOD - Sufficient for stress testing")
elif coverage_pct >= 50:
    report_lines.append("[!!] FAIR - May need additional data sources")
else:
    report_lines.append("[!!] POOR - Review API keys and connectivity")

report_lines.append("")
report_lines.append("="*70)

# Print report
print("")
for line in report_lines:
    print(line)

# Save report
report_file = OUTPUT_DIR / "data_quality_report.txt"
with open(report_file, 'w') as f:
    f.write('\n'.join(report_lines))

print(f"\n Quality report saved: {report_file}")

print("\n" + "="*70)
print(" TRIAL FETCH COMPLETE!")
print("="*70)
print(f"\n All outputs saved to: {OUTPUT_DIR}")
print("\nNext Steps:")
print("1. Review data_quality_report.txt")
print("2. Check coverage percentage")
print("3. If satisfied, integrate into main pipeline")
print("4. If gaps, investigate missing series")
