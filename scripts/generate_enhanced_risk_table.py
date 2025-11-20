"""
Generate enhanced risk factors table with frequency-adjusted completeness
"""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# Load data
OUTPUT_DIR = Path(__file__).parent.parent / "output" / "trial data folder" / "stress_indicators"
fred_meta = pd.read_csv(OUTPUT_DIR / 'fred_metadata.csv')
yahoo_meta = pd.read_csv(OUTPUT_DIR / 'yahoo_metadata.csv')
fred_data = pd.read_csv(OUTPUT_DIR / 'fred_stress_indicators.csv', index_col='date', parse_dates=True)
yahoo_data = pd.read_csv(OUTPUT_DIR / 'yahoo_market_data.csv', index_col=0, parse_dates=True)
spreads_data = pd.read_csv(OUTPUT_DIR / 'sovereign_spreads.csv', index_col='date', parse_dates=True)

# Define date range
full_start = pd.Timestamp('1990-01-01')
full_end = pd.Timestamp('2025-10-24')

# Calculate expected observations by frequency
def get_expected_obs(start, end, frequency):
    if frequency == 'daily':
        # Business days only
        return len(pd.bdate_range(start, end))
    elif frequency == 'weekly':
        # 52 weeks per year
        years = (end - start).days / 365.25
        return int(years * 52)
    elif frequency == 'monthly':
        return (end.year - start.year) * 12 + (end.month - start.month) + 1
    elif frequency == 'quarterly':
        return ((end.year - start.year) * 4) + ((end.month - start.month) // 3) + 1
    else:
        return (end - start).days + 1

# Build enhanced table
rows = []

# 1. CREDIT RISK - Enhanced Credit Spreads
for _, row in fred_meta[fred_meta['category'] == 'credit'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    # Determine subcategory
    if 'AAA' in row['name']:
        subcat = 'AAA Corporate'
    elif 'Single-A' in row['name'] or row['series_code'] == 'BAMLC0A3CAEY':
        subcat = 'A Corporate'
    elif 'BBB' in row['name']:
        subcat = 'BBB Corporate'
    elif 'High Yield' in row['name'] or 'HighYield' in row['name']:
        if 'Bank' in row['name']:
            subcat = 'HY Financial'
        else:
            subcat = 'High Yield'
    elif 'Euro' in row['name']:
        subcat = 'Euro HY'
    else:
        subcat = 'Investment Grade'
    
    rows.append({
        'Risk Category': 'Credit Risk',
        'Subcategory': subcat,
        'Risk Factor': row['name'].replace('_', ' '),
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': 'USA' if 'Euro' not in row['name'] else 'EUR',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 2. INFLATION RISK
for _, row in fred_meta[fred_meta['category'] == 'inflation'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    rows.append({
        'Risk Category': 'Inflation Risk',
        'Subcategory': 'CPI',
        'Risk Factor': f"{row['country']} Consumer Price Index",
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': row['country'],
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 3. GDP GROWTH RISK
for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'GDP' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        start = pd.Timestamp(row['first_date'])
        end = pd.Timestamp(row['last_date'])
        
        expected = get_expected_obs(start, end, row['frequency'])
        actual = len(series_data)
        completeness = (actual / expected * 100) if expected > 0 else 0
        
        rows.append({
            'Risk Category': 'GDP Growth Risk',
            'Subcategory': 'Real GDP',
            'Risk Factor': f"{row['country']} GDP Growth",
            'Data Series': row['series_code'],
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Country/Region': row['country'],
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': f"{actual}/{expected}",
            'Completeness': f"{completeness:.1f}%"
        })

# 4. UNEMPLOYMENT RISK
for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'Unemployment' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        start = pd.Timestamp(row['first_date'])
        end = pd.Timestamp(row['last_date'])
        
        expected = get_expected_obs(start, end, row['frequency'])
        actual = len(series_data)
        completeness = (actual / expected * 100) if expected > 0 else 0
        
        rows.append({
            'Risk Category': 'Unemployment Risk',
            'Subcategory': 'Labor Market',
            'Risk Factor': f"{row['country']} Unemployment Rate",
            'Data Series': row['series_code'],
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Country/Region': row['country'],
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': f"{actual}/{expected}",
            'Completeness': f"{completeness:.1f}%"
        })

# 5. MONETARY POLICY RISK
for _, row in fred_meta[fred_meta['category'] == 'monetary'].iterrows():
    if 'Rate' in row['name'] or 'Deposit' in row['name'] or 'Funds' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        start = pd.Timestamp(row['first_date'])
        end = pd.Timestamp(row['last_date'])
        
        expected = get_expected_obs(start, end, row['frequency'])
        actual = len(series_data)
        completeness = (actual / expected * 100) if expected > 0 else 0
        
        region = row.get('region', row.get('country', 'N/A'))
        
        rows.append({
            'Risk Category': 'Monetary Policy',
            'Subcategory': 'Policy Rates',
            'Risk Factor': row['name'].replace('_', ' '),
            'Data Series': row['series_code'],
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Country/Region': region,
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': f"{actual}/{expected}",
            'Completeness': f"{completeness:.1f}%"
        })

# 6. LIQUIDITY RISK
liquidity_series = ['M3', 'Balance_Sheet', 'STR', 'TED', 'Libor']
for _, row in fred_meta[fred_meta['category'].isin(['monetary', 'banking_stress'])].iterrows():
    if any(x in row['name'] for x in liquidity_series):
        series_data = fred_data[row['series_code']].dropna()
        start = pd.Timestamp(row['first_date'])
        end = pd.Timestamp(row['last_date'])
        
        expected = get_expected_obs(start, end, row['frequency'])
        actual = len(series_data)
        completeness = (actual / expected * 100) if expected > 0 else 0
        
        # Determine subcategory
        if 'M3' in row['name']:
            subcat = 'Money Supply'
        elif 'Balance' in row['name']:
            subcat = 'Central Bank BS'
        elif 'TED' in row['name'] or 'Libor' in row['name']:
            subcat = 'Bank Funding'
        else:
            subcat = 'Short Rates'
        
        region = row.get('region', row.get('country', 'N/A'))
        
        rows.append({
            'Risk Category': 'Liquidity Risk',
            'Subcategory': subcat,
            'Risk Factor': row['name'].replace('_', ' '),
            'Data Series': row['series_code'],
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Country/Region': region,
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Observations': f"{actual}/{expected}",
            'Completeness': f"{completeness:.1f}%"
        })

# 7. MARKET VOLATILITY
for _, row in fred_meta[fred_meta['category'] == 'market_volatility'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    rows.append({
        'Risk Category': 'Market Volatility',
        'Subcategory': 'Equity Vol',
        'Risk Factor': row['name'],
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': 'USA',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 8. REAL ESTATE RISK
for _, row in fred_meta[fred_meta['category'] == 'real_estate'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    country = row.get('country', 'N/A')
    
    rows.append({
        'Risk Category': 'Real Estate Risk',
        'Subcategory': 'Property Prices',
        'Risk Factor': row['name'].replace('_', ' '),
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': country,
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 9. INTEREST RATE DERIVATIVES
for _, row in fred_meta[fred_meta['category'] == 'interest_rate_derivative'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    subcat = 'Swap Rates' if 'Swap' in row['name'] else 'Mortgage Rates'
    
    rows.append({
        'Risk Category': 'Interest Rate Risk',
        'Subcategory': subcat,
        'Risk Factor': row['name'].replace('_', ' '),
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': 'USA',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 10. COMMODITY RISK (FRED)
for _, row in fred_meta[fred_meta['category'] == 'commodity'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    expected = get_expected_obs(start, end, row['frequency'])
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    if 'Brent' in row['name']:
        subcat = 'Oil - Brent'
    elif 'WTI' in row['name']:
        subcat = 'Oil - WTI'
    else:
        subcat = 'Precious Metals'
    
    rows.append({
        'Risk Category': 'Commodity Risk',
        'Subcategory': subcat,
        'Risk Factor': row['name'].replace('_', ' '),
        'Data Series': row['series_code'],
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Country/Region': 'Global',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 11. YAHOO FINANCE DATA
for _, row in yahoo_meta.iterrows():
    series_data = yahoo_data[row['name']].dropna()
    start = pd.Timestamp(row['first_date'])
    end = pd.Timestamp(row['last_date'])
    
    # Yahoo is always daily business days
    expected = get_expected_obs(start, end, 'daily')
    actual = len(series_data)
    completeness = (actual / expected * 100) if expected > 0 else 0
    
    category = row['category']
    
    if category == 'commodity':
        risk_cat = 'Commodity Risk'
        if 'Brent' in row['name']:
            subcat = 'Oil - Brent'
        elif 'WTI' in row['name']:
            subcat = 'Oil - WTI'
        else:
            subcat = 'Precious Metals'
        country = 'Global'
    elif category == 'equity_index':
        risk_cat = 'Equity Market Risk'
        subcat = 'National Indices'
        if 'DAX' in row['name']:
            country = 'DEU'
        elif 'IBEX' in row['name']:
            country = 'ESP'
        elif 'SP500' in row['name']:
            country = 'USA'
        else:
            country = 'N/A'
    elif category == 'fx':
        risk_cat = 'FX Risk'
        subcat = 'EUR Cross Rates'
        country = 'EUR Zone'
    else:
        risk_cat = 'Other'
        subcat = 'N/A'
        country = 'N/A'
    
    rows.append({
        'Risk Category': risk_cat,
        'Subcategory': subcat,
        'Risk Factor': row['name'].replace('_', ' '),
        'Data Series': row['ticker'],
        'Source': 'Yahoo Finance',
        'Frequency': 'Daily',
        'Country/Region': country,
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Observations': f"{actual}/{expected}",
        'Completeness': f"{completeness:.1f}%"
    })

# 12. SOVEREIGN SPREADS (Computed) - NOTE: We have full curves, so this is just for monitoring
spread_names = {
    'BTP_Bund_Spread': ('Sovereign Spread Risk', 'ITA vs DEU', 'ITA, DEU'),
    'Bonos_Bund_Spread': ('Sovereign Spread Risk', 'ESP vs DEU', 'ESP, DEU'),
    'OAT_Bund_Spread': ('Sovereign Spread Risk', 'FRA vs DEU', 'FRA, DEU')
}
for col, (cat, subcat, country) in spread_names.items():
    series_data = spreads_data[col].dropna()
    if len(series_data) > 0:
        start = series_data.index[0]
        end = series_data.index[-1]
        
        expected = get_expected_obs(start, end, 'monthly')
        actual = len(series_data)
        completeness = (actual / expected * 100) if expected > 0 else 0
        
        rows.append({
            'Risk Category': cat,
            'Subcategory': subcat,
            'Risk Factor': col.replace('_', ' '),
            'Data Series': 'Computed (10Y Yields)',
            'Source': 'FRED (Computed)',
            'Frequency': 'Monthly',
            'Country/Region': country,
            'Start Date': start.strftime('%Y-%m-%d'),
            'End Date': end.strftime('%Y-%m-%d'),
            'Observations': f"{actual}/{expected}",
            'Completeness': f"{completeness:.1f}%"
        })

# Create DataFrame
df = pd.DataFrame(rows)

# Save to CSV
df.to_csv(OUTPUT_DIR / 'risk_factors_enhanced_table.csv', index=False)

print('='*80)
print('ENHANCED TABLE GENERATED')
print('='*80)
print(f'Total indicators: {len(df)}')
print(f'\nCompleteness Summary by Risk Category:')
completeness_summary = df.groupby('Risk Category')['Completeness'].apply(
    lambda x: x.str.rstrip('%').astype(float).mean()
).sort_values(ascending=False)
print(completeness_summary)

print(f'\n Data Quality Assessment:')
print(f'Average Completeness: {df["Completeness"].str.rstrip("%").astype(float).mean():.1f}%')
print(f'\nSeries with >95% completeness: {(df["Completeness"].str.rstrip("%").astype(float) > 95).sum()}')
print(f'Series with >90% completeness: {(df["Completeness"].str.rstrip("%").astype(float) > 90).sum()}')
print(f'Series with >80% completeness: {(df["Completeness"].str.rstrip("%").astype(float) > 80).sum()}')
print(f'Series with <50% completeness: {(df["Completeness"].str.rstrip("%").astype(float) < 50).sum()}')

print(f'\nTable saved to: {OUTPUT_DIR / "risk_factors_enhanced_table.csv"}')
