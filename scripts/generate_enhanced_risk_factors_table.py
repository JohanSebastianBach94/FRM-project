"""
Generate Enhanced Risk Factors Summary Table with:
- Risk Category
- Risk Subcategory
- Country Coverage (EU = ITA, FRA, DEU, ESP)
- Data Source (FRED/Yahoo Finance)
- Frequency
- Date Coverage
- Completeness

Note: Excludes 10Y yields since we have full yield curves from NSS model
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
import numpy as np

# Configuration
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output" / "trial data folder" / "stress_indicators"

# Load metadata
print("Loading data...")
fred_meta = pd.read_csv(OUTPUT_DIR / 'fred_metadata.csv')
yahoo_meta = pd.read_csv(OUTPUT_DIR / 'yahoo_metadata.csv')
fred_data = pd.read_csv(OUTPUT_DIR / 'fred_stress_indicators.csv', index_col='date', parse_dates=True)
yahoo_data = pd.read_csv(OUTPUT_DIR / 'yahoo_market_data.csv', index_col=0, parse_dates=True)
spreads_data = pd.read_csv(OUTPUT_DIR / 'sovereign_spreads.csv', index_col='date', parse_dates=True)

# Define full date range
full_start = pd.Timestamp('1990-01-01')
full_end = pd.Timestamp('2025-10-24')
total_days = (full_end - full_start).days + 1

print(f"Analyzing data over {total_days} days ({full_start.date()} to {full_end.date()})")

# Build enhanced table with proper categorization
rows = []

# ==============================================
# 1. CREDIT RISK
# ==============================================
print("\n1. Processing Credit Risk...")

credit_subcategories = {
    'AAA_Corporate_Yield': 'Investment Grade AAA',
    'A_Corporate_Yield': 'Investment Grade A',
    'BBB_Corporate_OAS': 'Investment Grade BBB',
    'Corp_Master_OAS': 'Investment Grade (Aggregate)',
    'HighYield_OAS': 'High Yield (US)',
    'HighYield_Banks': 'High Yield (Financial Sector)',
    'Euro_HighYield_Yield': 'High Yield (Europe)',
}

for _, row in fred_meta[fred_meta['category'] == 'credit'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    
    name = row['name']
    subcategory = credit_subcategories.get(name, 'Corporate Credit')
    country = 'EUR' if 'Euro' in name else 'USA'
    
    rows.append({
        'Risk Category': 'Credit Risk',
        'Risk Subcategory': subcategory,
        'Indicator': row['series_code'],
        'Country': country,
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 2. SOVEREIGN RISK (Spreads only - we have NSS curves)
# ==============================================
print("2. Processing Sovereign Risk (Spreads)...")

spread_info = {
    'BTP_Bund_Spread': {'country': 'ITA', 'subcategory': 'Italy vs Germany Spread'},
    'Bonos_Bund_Spread': {'country': 'ESP', 'subcategory': 'Spain vs Germany Spread'},
    'OAT_Bund_Spread': {'country': 'FRA', 'subcategory': 'France vs Germany Spread'},
}

for col, info in spread_info.items():
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
        'Risk Category': 'Sovereign Risk',
        'Risk Subcategory': info['subcategory'],
        'Indicator': 'Computed Spread',
        'Country': info['country'],
        'Source': 'FRED (Computed)',
        'Frequency': 'Monthly',
        'Start Date': start_date,
        'End Date': end_date,
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 3. INFLATION RISK
# ==============================================
print("3. Processing Inflation Risk...")

for _, row in fred_meta[fred_meta['category'] == 'inflation'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = row['country']
    
    rows.append({
        'Risk Category': 'Inflation Risk',
        'Risk Subcategory': 'Consumer Price Index',
        'Indicator': row['series_code'],
        'Country': country,
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 4. GDP GROWTH RISK
# ==============================================
print("4. Processing GDP Growth Risk...")

for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'GDP' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        country = row['country']
        
        rows.append({
            'Risk Category': 'GDP Growth Risk',
            'Risk Subcategory': 'Real GDP Growth',
            'Indicator': row['series_code'],
            'Country': country,
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Obs': actual_days,
            'Complete%': f'{completeness:.1f}%'
        })

# ==============================================
# 5. UNEMPLOYMENT RISK
# ==============================================
print("5. Processing Unemployment Risk...")

for _, row in fred_meta[fred_meta['category'] == 'macro'].iterrows():
    if 'Unemployment' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        country = row['country']
        
        rows.append({
            'Risk Category': 'Unemployment Risk',
            'Risk Subcategory': 'Unemployment Rate',
            'Indicator': row['series_code'],
            'Country': country,
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Obs': actual_days,
            'Complete%': f'{completeness:.1f}%'
        })

# ==============================================
# 6. MONETARY POLICY RISK
# ==============================================
print("6. Processing Monetary Policy Risk...")

for _, row in fred_meta[fred_meta['category'] == 'monetary'].iterrows():
    if 'Rate' in row['name'] and 'Deposit' in row['name']:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        
        rows.append({
            'Risk Category': 'Monetary Policy Risk',
            'Risk Subcategory': 'Policy Rates',
            'Indicator': row['series_code'],
            'Country': 'EUR: ITA,FRA,DEU,ESP',
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Obs': actual_days,
            'Complete%': f'{completeness:.1f}%'
        })
    elif row['name'] == 'Fed_Funds_Rate':
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        
        rows.append({
            'Risk Category': 'Monetary Policy Risk',
            'Risk Subcategory': 'Policy Rates',
            'Indicator': row['series_code'],
            'Country': 'USA',
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Obs': actual_days,
            'Complete%': f'{completeness:.1f}%'
        })

# ==============================================
# 7. LIQUIDITY & FUNDING RISK
# ==============================================
print("7. Processing Liquidity & Funding Risk...")

liquidity_subcategories = {
    'Eurozone_M3': 'Money Supply',
    'ECB_Balance_Sheet': 'Central Bank Balance Sheet',
    'Fed_Balance_Sheet': 'Central Bank Balance Sheet',
    'Euro_STR': 'Short-Term Rates',
    'TED_Spread': 'Bank Funding Stress',
    'USD_Libor_3M': 'Interbank Rates',
}

for _, row in fred_meta[fred_meta['category'].isin(['monetary', 'banking_stress'])].iterrows():
    if row['name'] in liquidity_subcategories:
        series_data = fred_data[row['series_code']].dropna()
        actual_days = len(series_data)
        completeness = (actual_days / total_days) * 100
        
        subcategory = liquidity_subcategories[row['name']]
        
        if 'Euro' in row['name'] or 'ECB' in row['name']:
            country = 'EUR: ITA,FRA,DEU,ESP'
        elif 'TED' in row['name']:
            country = 'USA (Global)'
        else:
            country = row.get('country', 'USA')
        
        rows.append({
            'Risk Category': 'Liquidity & Funding Risk',
            'Risk Subcategory': subcategory,
            'Indicator': row['series_code'],
            'Country': country,
            'Source': 'FRED',
            'Frequency': row['frequency'].title(),
            'Start Date': row['first_date'],
            'End Date': row['last_date'],
            'Obs': actual_days,
            'Complete%': f'{completeness:.1f}%'
        })

# ==============================================
# 8. MARKET VOLATILITY RISK
# ==============================================
print("8. Processing Market Volatility Risk...")

for _, row in fred_meta[fred_meta['category'] == 'market_volatility'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    
    rows.append({
        'Risk Category': 'Market Volatility Risk',
        'Risk Subcategory': 'Equity Volatility (VIX)',
        'Indicator': row['series_code'],
        'Country': 'USA (Global)',
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 9. REAL ESTATE RISK
# ==============================================
print("9. Processing Real Estate Risk...")

for _, row in fred_meta[fred_meta['category'] == 'real_estate'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = row['country']
    
    rows.append({
        'Risk Category': 'Real Estate Risk',
        'Risk Subcategory': 'Property Prices',
        'Indicator': row['series_code'],
        'Country': country,
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 10. INTEREST RATE DERIVATIVE RISK
# ==============================================
print("10. Processing Interest Rate Derivative Risk...")

for _, row in fred_meta[fred_meta['category'] == 'interest_rate_derivative'].iterrows():
    series_data = fred_data[row['series_code']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    
    subcategory = 'Swap Rates' if 'Swap' in row['name'] else 'Mortgage Rates'
    
    rows.append({
        'Risk Category': 'Interest Rate Derivative Risk',
        'Risk Subcategory': subcategory,
        'Indicator': row['series_code'],
        'Country': 'USA',
        'Source': 'FRED',
        'Frequency': row['frequency'].title(),
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 11. EQUITY MARKET RISK (Yahoo Finance)
# ==============================================
print("11. Processing Equity Market Risk...")

equity_country_map = {
    'DAX': 'DEU',
    'IBEX_35': 'ESP',
    'SP500': 'USA',
}

for _, row in yahoo_meta[yahoo_meta['category'] == 'equity_index'].iterrows():
    series_data = yahoo_data[row['name']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = equity_country_map.get(row['name'], 'Unknown')
    
    rows.append({
        'Risk Category': 'Equity Market Risk',
        'Risk Subcategory': 'Major Stock Indices',
        'Indicator': row['ticker'],
        'Country': country,
        'Source': 'Yahoo Finance',
        'Frequency': 'Daily',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 12. FX RISK (Yahoo Finance)
# ==============================================
print("12. Processing FX Risk...")

fx_country_map = {
    'EUR_GBP': 'EUR-GBP',
    'EUR_CHF': 'EUR-CHF',
    'EUR_JPY': 'EUR-JPY',
}

for _, row in yahoo_meta[yahoo_meta['category'] == 'fx'].iterrows():
    series_data = yahoo_data[row['name']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    country = fx_country_map.get(row['name'], 'Cross-Border')
    
    rows.append({
        'Risk Category': 'FX Risk',
        'Risk Subcategory': 'Exchange Rates',
        'Indicator': row['ticker'],
        'Country': country,
        'Source': 'Yahoo Finance',
        'Frequency': 'Daily',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# 13. COMMODITY RISK (Yahoo Finance)
# ==============================================
print("13. Processing Commodity Risk...")

for _, row in yahoo_meta[yahoo_meta['category'] == 'commodity'].iterrows():
    series_data = yahoo_data[row['name']].dropna()
    actual_days = len(series_data)
    completeness = (actual_days / total_days) * 100
    
    if 'Crude' in row['name']:
        subcategory = 'Energy (Oil)'
    elif 'Gold' in row['name']:
        subcategory = 'Precious Metals'
    else:
        subcategory = 'Commodities'
    
    rows.append({
        'Risk Category': 'Commodity Risk',
        'Risk Subcategory': subcategory,
        'Indicator': row['ticker'],
        'Country': 'Global',
        'Source': 'Yahoo Finance',
        'Frequency': 'Daily',
        'Start Date': row['first_date'],
        'End Date': row['last_date'],
        'Obs': actual_days,
        'Complete%': f'{completeness:.1f}%'
    })

# ==============================================
# CREATE DATAFRAME AND EXPORT
# ==============================================

df = pd.DataFrame(rows)

# Save CSV
csv_path = OUTPUT_DIR / 'enhanced_risk_factors_table.csv'
df.to_csv(csv_path, index=False)
print(f'\n✅ Saved CSV to: {csv_path}')

# ==============================================
# CREATE VISUAL TABLE AS IMAGE
# ==============================================

print('\n📊 Generating visual table...')

# Prepare data for visualization
fig, ax = plt.subplots(figsize=(20, 24))
ax.axis('tight')
ax.axis('off')

# Create color mapping for risk categories
category_colors = {
    'Credit Risk': '#FF6B6B',
    'Sovereign Risk': '#4ECDC4',
    'Inflation Risk': '#FFE66D',
    'GDP Growth Risk': '#95E1D3',
    'Unemployment Risk': '#F38181',
    'Monetary Policy Risk': '#AA96DA',
    'Liquidity & Funding Risk': '#FCBAD3',
    'Market Volatility Risk': '#FF8C42',
    'Real Estate Risk': '#A8E6CF',
    'Interest Rate Derivative Risk': '#FFD3B6',
    'Equity Market Risk': '#FFAAA5',
    'FX Risk': '#A8DADC',
    'Commodity Risk': '#F1C40F',
}

# Color the rows by category
cell_colors = []
for _, row in df.iterrows():
    category = row['Risk Category']
    base_color = category_colors.get(category, '#FFFFFF')
    cell_colors.append([base_color] * len(df.columns))

# Create table
table = ax.table(
    cellText=df.values,
    colLabels=df.columns,
    cellLoc='left',
    loc='center',
    cellColours=cell_colors,
    colWidths=[0.15, 0.14, 0.12, 0.10, 0.08, 0.08, 0.09, 0.09, 0.07, 0.08]
)

table.auto_set_font_size(False)
table.set_fontsize(9)
table.scale(1, 2.5)

# Style header
for i in range(len(df.columns)):
    table[(0, i)].set_facecolor('#34495E')
    table[(0, i)].set_text_props(weight='bold', color='white', fontsize=10)

# Add title
title_text = 'STRESS TESTING RISK FACTORS - COMPREHENSIVE OVERVIEW\n'
title_text += f'Data Period: 1990-01-01 to 2025-10-24 | Total Indicators: {len(df)}\n'
title_text += 'Note: 10Y Sovereign Yields excluded (full yield curves available from NSS model)'
plt.suptitle(title_text, fontsize=14, fontweight='bold', y=0.995)

# Save as high-resolution image
img_path = OUTPUT_DIR / 'enhanced_risk_factors_table.png'
plt.savefig(img_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f'✅ Saved table image to: {img_path}')

# ==============================================
# SUMMARY STATISTICS
# ==============================================

print(f'\n{"="*80}')
print('SUMMARY STATISTICS')
print(f'{"="*80}')
print(f'Total Risk Factors: {len(df)}')
print(f'\nBy Risk Category:')
for category in df['Risk Category'].unique():
    count = len(df[df['Risk Category'] == category])
    print(f'  {category}: {count} indicators')

print(f'\nBy Data Source:')
for source in df['Source'].unique():
    count = len(df[df['Source'] == source])
    print(f'  {source}: {count} indicators')

print(f'\nBy Country Coverage:')
country_counts = df['Country'].value_counts()
for country, count in country_counts.head(10).items():
    print(f'  {country}: {count} indicators')

print(f'\n{"="*80}')
print('✅ ENHANCED RISK FACTORS TABLE GENERATION COMPLETE!')
print(f'{"="*80}')
