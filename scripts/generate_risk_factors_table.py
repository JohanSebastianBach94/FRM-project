"""
Generate comprehensive risk factors summary table for stress testing indicators.
Shows each risk factor with data series, frequency, date range, and completeness.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# Configuration
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output" / "trial data folder" / "stress_indicators"

# Load metadata and data
print("Loading data...")
fred_meta = pd.read_csv(OUTPUT_DIR / 'fred_metadata.csv')
yahoo_meta = pd.read_csv(OUTPUT_DIR / 'yahoo_metadata.csv')
fred_data = pd.read_csv(OUTPUT_DIR / 'fred_stress_indicators.csv', index_col='date', parse_dates=True)
yahoo_data = pd.read_csv(OUTPUT_DIR / 'yahoo_commodities.csv', index_col=0, parse_dates=True)
spreads_data = pd.read_csv(OUTPUT_DIR / 'sovereign_spreads.csv', index_col='date', parse_dates=True)

# Define full date range
full_start = pd.Timestamp('1990-01-01')
full_end = pd.Timestamp('2025-10-24')
total_days = (full_end - full_start).days + 1

print(f"Analyzing data completeness over {total_days} days ({full_start.date()} to {full_end.date()})")

# Build comprehensive table
rows = []

# 1. CREDIT SPREADS
print("\nProcessing Credit Spreads...")
for _, row in fred_meta[fred_meta['category'] == 'credit'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    rows.append({
        'Risk Factor': 'Credit Spread - ' + row['name'].replace('_', ' '),
        'Data Series (Code)': row['series_code'],
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# 2. SOVEREIGN RISK (10Y Yields)
print("Processing Sovereign 10Y Yields...")
for _, row in fred_meta[fred_meta['category'] == 'sovereign'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = row['country']
    rows.append({
        'Risk Factor': f'Sovereign Risk - {country} 10Y Yield',
        'Data Series (Code)': row['series_code'],
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# 3. SOVEREIGN SPREADS (computed)
print("Processing Sovereign Spreads...")
spread_names = {
    'BTP_Bund_Spread': 'Sovereign Spread - Italy vs Germany (BTP-Bund)',
    'Bonos_Bund_Spread': 'Sovereign Spread - Spain vs Germany (Bonos-Bund)',
    'OAT_Bund_Spread': 'Sovereign Spread - France vs Germany (OAT-Bund)'
}
for col, name in spread_names.items():
    series_data = spreads_data[col].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    if len(series_data) > 0:
        start_date = spreads_data[col].first_valid_index().strftime('%Y-%m-%d')
        end_date = spreads_data[col].last_valid_index().strftime('%Y-%m-%d')
    else:
        start_date = 'N/A'
        end_date = 'N/A'
    
    rows.append({
        'Risk Factor': name,
        'Data Series (Code)': 'Computed (ITA/ESP/FRA - DEU)',
        'Frequency': 'Monthly',
        'Start Date': start_date,
        'End Date': end_date,
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# 4. INFLATION
print("Processing Inflation Indicators...")
for _, row in fred_meta[fred_meta['category'] == 'inflation'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = row['country']
    rows.append({
        'Risk Factor': f'Inflation Risk - {country} CPI',
        'Data Series (Code)': row['series_code'],
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# 5. GDP GROWTH
print("Processing GDP Growth...")
for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'GDP' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        country = row['country']
        rows.append({
            'Risk Factor': f'GDP Growth Risk - {country}',
            'Data Series (Code)': row['series_code'],
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': actual_days,
            'Completeness (%)': f'{completeness:.2f}%'
        })

# 6. UNEMPLOYMENT
print("Processing Unemployment...")
for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'Unemployment' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        country = row['country']
        rows.append({
            'Risk Factor': f'Unemployment Risk - {country}',
            'Data Series (Code)': row['series_code'],
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': actual_days,
            'Completeness (%)': f'{completeness:.2f}%'
        })

# 7. MONETARY POLICY RATES
print("Processing Monetary Policy Rates...")
for _, row in fred_meta[fred_meta['category'] == 'monetary'].iterrows():
    if 'Rate' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        rows.append({
            'Risk Factor': 'Monetary Policy - ' + row['name'].replace('_', ' '),
            'Data Series (Code)': row['series_code'],
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': actual_days,
            'Completeness (%)': f'{completeness:.2f}%'
        })

# 8. LIQUIDITY INDICATORS
print("Processing Liquidity Indicators...")
liquidity_series = ['M3', 'Balance_Sheet', 'STR', 'TED']
for _, row in fred_meta[fred_meta['category'] == 'monetary'].iterrows():
    if any(x in row['name'] for x in liquidity_series):
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        rows.append({
            'Risk Factor': 'Liquidity Risk - ' + row['name'].replace('_', ' '),
            'Data Series (Code)': row['series_code'],
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': actual_days,
            'Completeness (%)': f'{completeness:.2f}%'
        })

# 9. COMMODITY RISK (Yahoo)
print("Processing Commodities (Yahoo Finance)...")
for _, row in yahoo_meta.iterrows():
    series_data = yahoo_data[row['name']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    rows.append({
        'Risk Factor': 'Commodity Risk - ' + row['name'].replace('_', ' '),
        'Data Series (Code)': row['ticker'] + ' (Yahoo)',
        'Frequency': 'Daily',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# 10. COMMODITY RISK (FRED)
print("Processing Commodities (FRED)...")
for _, row in fred_meta[fred_meta['category'] == 'commodity'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    rows.append({
        'Risk Factor': 'Commodity Risk - ' + row['name'].replace('_', ' '),
        'Data Series (Code)': row['series_code'],
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': actual_days,
        'Completeness (%)': f'{completeness:.2f}%'
    })

# Create DataFrame
df = pd.DataFrame(rows)

# Reorder columns for better readability
column_order = ['Risk Factor', 'Data Series (Code)', 'Frequency', 'Start Date', 'End Date', 'Observations', 'Completeness (%)']
df = df[column_order]

# Save to CSV
output_path = OUTPUT_DIR / 'risk_factors_summary_table.csv'
df.to_csv(output_path, index=False)

print(f'\n{"="*80}')
print(f'✅ Saved comprehensive table to: {output_path}')
print(f'{"="*80}')
print(f'\nTotal Risk Factors: {len(df)}')
print(f'\nSummary by Risk Category:')
for category in ['Credit Spread', 'Sovereign Risk', 'Sovereign Spread', 'Inflation Risk', 
                 'GDP Growth Risk', 'Unemployment Risk', 'Monetary Policy', 'Liquidity Risk', 'Commodity Risk']:
    count = df[df['Risk Factor'].str.contains(category)].shape[0]
    if count > 0:
        print(f'  {category}: {count} indicators')

print(f'\n{"="*80}')
print('FULL TABLE:')
print(f'{"="*80}')
print(df.to_string(index=False))
