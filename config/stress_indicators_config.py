"""
Configuration for Stress Testing Indicators
Organized by category with FRED series codes and metadata
"""

from typing import Dict, List

# ==============================================
# 🏦 CREDIT & FINANCIAL SPREADS
# ==============================================

CREDIT_SPREADS = {
    # Corporate Credit Spreads (Daily)
    'BAMLC0A4CBBB': {
        'name': 'BBB_Corporate_OAS',
        'description': 'ICE BofA BBB US Corporate Option-Adjusted Spread',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    'BAMLH0A0HYM2': {
        'name': 'HighYield_OAS',
        'description': 'ICE BofA US High Yield Master II Option-Adjusted Spread',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    'BAMLC0A0CM': {
        'name': 'Corp_Master_OAS',
        'description': 'ICE BofA US Corporate Master Option-Adjusted Spread',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    
    # European Corporate Spreads (Proxies for iTraxx)
    'BAMLHE00EHYIEY': {
        'name': 'Euro_HighYield_Yield',
        'description': 'ICE BofA Euro High Yield Index Effective Yield',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    
    # Enhanced Credit Quality Granularity
    'BAMLC0A1CAAAEY': {
        'name': 'AAA_Corporate_Yield',
        'description': 'ICE BofA AAA US Corporate Index Effective Yield',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    'BAMLC0A3CAEY': {
        'name': 'A_Corporate_Yield',
        'description': 'ICE BofA Single-A US Corporate Index Effective Yield',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
    
    # Financial Sector Credit
    'BAMLH0A1HYBB': {
        'name': 'HighYield_Banks',
        'description': 'ICE BofA US High Yield Banking Index Effective Yield',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'credit'
    },
}

# Sovereign 10Y Yields (for computing spreads)
SOVEREIGN_YIELDS_10Y = {
    'IRLTLT01ITM156N': {
        'name': 'Italy_10Y_Yield',
        'description': 'Long-Term Government Bond Yields: 10-year: Main (Including Benchmark) for Italy',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'sovereign',
        'country': 'ITA'
    },
    'IRLTLT01FRM156N': {
        'name': 'France_10Y_Yield',
        'description': 'Long-Term Government Bond Yields: 10-year: Main (Including Benchmark) for France',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'sovereign',
        'country': 'FRA'
    },
    'IRLTLT01DEM156N': {
        'name': 'Germany_10Y_Yield',
        'description': 'Long-Term Government Bond Yields: 10-year: Main (Including Benchmark) for Germany',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'sovereign',
        'country': 'DEU'
    },
    'IRLTLT01ESM156N': {
        'name': 'Spain_10Y_Yield',
        'description': 'Long-Term Government Bond Yields: 10-year: Main (Including Benchmark) for Spain',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'sovereign',
        'country': 'ESP'
    },
    'IRLTLT01USM156N': {
        'name': 'USA_10Y_Yield',
        'description': 'Long-Term Government Bond Yields: 10-year: Main (Including Benchmark) for United States',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'sovereign',
        'country': 'USA'
    },
}

# ==============================================
# 📈 INFLATION & MACRO FUNDAMENTALS
# ==============================================

INFLATION_CPI = {
    'ITACPIALLMINMEI': {
        'name': 'Italy_CPI',
        'description': 'Consumer Price Index: All Items for Italy',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'inflation',
        'country': 'ITA'
    },
    'FRACPIALLMINMEI': {
        'name': 'France_CPI',
        'description': 'Consumer Price Index: All Items for France',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'inflation',
        'country': 'FRA'
    },
    'DEUCPIALLMINMEI': {
        'name': 'Germany_CPI',
        'description': 'Consumer Price Index: All Items for Germany',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'inflation',
        'country': 'DEU'
    },
    'ESPCPIALLMINMEI': {
        'name': 'Spain_CPI',
        'description': 'Consumer Price Index: All Items for Spain',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'inflation',
        'country': 'ESP'
    },
    'CPIAUCSL': {
        'name': 'USA_CPI',
        'description': 'Consumer Price Index for All Urban Consumers: All Items in U.S. City Average',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'inflation',
        'country': 'USA'
    },
}

GDP_GROWTH = {
    'NAEXKP01ITQ661S': {
        'name': 'Italy_GDP_Growth',
        'description': 'Real Gross Domestic Product for Italy',
        'frequency': 'quarterly',
        'unit': 'percent_change',
        'category': 'macro',
        'country': 'ITA'
    },
    'NAEXKP01FRQ661S': {
        'name': 'France_GDP_Growth',
        'description': 'Real Gross Domestic Product for France',
        'frequency': 'quarterly',
        'unit': 'percent_change',
        'category': 'macro',
        'country': 'FRA'
    },
    'NAEXKP01DEQ661S': {
        'name': 'Germany_GDP_Growth',
        'description': 'Real Gross Domestic Product for Germany',
        'frequency': 'quarterly',
        'unit': 'percent_change',
        'category': 'macro',
        'country': 'DEU'
    },
    'NAEXKP01ESQ661S': {
        'name': 'Spain_GDP_Growth',
        'description': 'Real Gross Domestic Product for Spain',
        'frequency': 'quarterly',
        'unit': 'percent_change',
        'category': 'macro',
        'country': 'ESP'
    },
    'NAEXKP01USQ661S': {
        'name': 'USA_GDP_Growth_OECD',
        'description': 'Real Gross Domestic Product for United States (OECD series)',
        'frequency': 'quarterly',
        'unit': 'percent_change',
        'category': 'macro',
        'country': 'USA'
    },
    'GDPC1': {
        'name': 'USA_GDP',
        'description': 'Real Gross Domestic Product',
        'frequency': 'quarterly',
        'unit': 'billions_chained_2017_dollars',
        'category': 'macro',
        'country': 'USA'
    },
}

# ==============================================
# 🧾 BIS-BASED NPL RATIO PROXIES
# ==============================================

NPL_RATIO_PROXIES = {
    'NPL_PROXY_DEU': {
        'name': 'Germany_NPL_Ratio_Proxy',
        'description': 'Year-over-year change in BIS household credit outstanding (proxy for IMF FSI NPL ratio)',
        'frequency': 'quarterly',
        'unit': 'percent',
        'category': 'credit',
        'country': 'DEU',
        'notes': 'Derived from data_repository/processed/bis_lbs_household_Germany_agg.csv; used when IMF FSI NPLR series is unreachable.'
    },
    'NPL_PROXY_FRA': {
        'name': 'France_NPL_Ratio_Proxy',
        'description': 'Year-over-year change in BIS household credit outstanding (proxy for IMF FSI NPL ratio)',
        'frequency': 'quarterly',
        'unit': 'percent',
        'category': 'credit',
        'country': 'FRA',
        'notes': 'Derived from data_repository/processed/bis_lbs_household_France_agg.csv; used when IMF FSI NPLR series is unreachable.'
    },
    'NPL_PROXY_ITA': {
        'name': 'Italy_NPL_Ratio_Proxy',
        'description': 'Year-over-year change in BIS household credit outstanding (proxy for IMF FSI NPL ratio)',
        'frequency': 'quarterly',
        'unit': 'percent',
        'category': 'credit',
        'country': 'ITA',
        'notes': 'Derived from data_repository/processed/bis_lbs_household_Italy_agg.csv; used when IMF FSI NPLR series is unreachable.'
    },
    'NPL_PROXY_ESP': {
        'name': 'Spain_NPL_Ratio_Proxy',
        'description': 'Year-over-year change in BIS household credit outstanding (proxy for IMF FSI NPL ratio)',
        'frequency': 'quarterly',
        'unit': 'percent',
        'category': 'credit',
        'country': 'ESP',
        'notes': 'Derived from data_repository/processed/bis_lbs_household_Spain_agg.csv; used when IMF FSI NPLR series is unreachable.'
    },
    'NPL_PROXY_USA': {
        'name': 'USA_NPL_Ratio_Proxy',
        'description': 'Year-over-year change in BIS household credit outstanding (proxy for IMF FSI NPL ratio)',
        'frequency': 'quarterly',
        'unit': 'percent',
        'category': 'credit',
        'country': 'USA',
        'notes': 'Derived from data_repository/processed/bis_lbs_household_United States_agg.csv; used when IMF FSI NPLR series is unreachable.'
    },
}

UNEMPLOYMENT = {
    'LRHUTTTTITM156S': {
        'name': 'Italy_Unemployment',
        'description': 'Harmonized Unemployment Rate: Total: All Persons for Italy',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'macro',
        'country': 'ITA'
    },
    'LRHUTTTTFRM156S': {
        'name': 'France_Unemployment',
        'description': 'Harmonized Unemployment Rate: Total: All Persons for France',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'macro',
        'country': 'FRA'
    },
    'LRHUTTTTDEM156S': {
        'name': 'Germany_Unemployment',
        'description': 'Harmonized Unemployment Rate: Total: All Persons for Germany',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'macro',
        'country': 'DEU'
    },
    'LRHUTTTTESM156S': {
        'name': 'Spain_Unemployment',
        'description': 'Harmonized Unemployment Rate: Total: All Persons for Spain',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'macro',
        'country': 'ESP'
    },
    'UNRATE': {
        'name': 'USA_Unemployment',
        'description': 'Unemployment Rate',
        'frequency': 'monthly',
        'unit': 'percent',
        'category': 'macro',
        'country': 'USA'
    },
}

POLICY_RATES = {
    'ECBDFR': {
        'name': 'ECB_Deposit_Rate',
        'description': 'ECB Deposit Facility Rate for Euro Area',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'monetary',
        'region': 'EUR'
    },
    'DFF': {
        'name': 'Fed_Funds_Rate',
        'description': 'Federal Funds Effective Rate',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'monetary',
        'country': 'USA'
    },
}

# ==============================================
# 💵 MONETARY & LIQUIDITY INDICATORS
# ==============================================

MONETARY_LIQUIDITY = {
    'EUR3MTD156N': {
        'name': 'Euribor_3M',
        'description': '3-Month Euro Interbank Offered Rate (Euribor)',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'monetary',
        'region': 'EUR'
    },
    'MABMM301EZM189S': {
        'name': 'Eurozone_M3',
        'description': 'M3 for Euro Area',
        'frequency': 'monthly',
        'unit': 'millions_euros',
        'category': 'monetary',
        'region': 'EUR'
    },
    'MYAGM2EZM196N': {
        'name': 'EuroArea_M2_IFS',
        'description': 'Euro Area Money Stock M2 from IMF IFS (FRED)',
        'frequency': 'monthly',
        'unit': 'millions_euros',
        'category': 'monetary',
        'region': 'EUR'
    },
    'MYAGM1EZM196N': {
        'name': 'EuroArea_M1_IFS',
        'description': 'Euro Area Money Stock M1 from IMF IFS (FRED)',
        'frequency': 'monthly',
        'unit': 'millions_euros',
        'category': 'monetary',
        'region': 'EUR'
    },
    'ECBASSETS': {
        'name': 'ECB_Balance_Sheet',
        'description': 'Central Bank Assets for Euro Area',
        'frequency': 'weekly',
        'unit': 'millions_euros',
        'category': 'monetary',
        'region': 'EUR'
    },
    'WALCL': {
        'name': 'Fed_Balance_Sheet',
        'description': 'Assets: Total Assets: Total Assets (Less Eliminations from Consolidation): Wednesday Level',
        'frequency': 'weekly',
        'unit': 'millions_dollars',
        'category': 'monetary',
        'country': 'USA'
    },
    'M1SL': {
        'name': 'USA_M1',
        'description': 'United States Money Stock M1 (seasonally adjusted)',
        'frequency': 'monthly',
        'unit': 'billions_dollars',
        'category': 'monetary',
        'country': 'USA'
    },
    'M2SL': {
        'name': 'USA_M2',
        'description': 'United States Money Stock M2 (seasonally adjusted)',
        'frequency': 'monthly',
        'unit': 'billions_dollars',
        'category': 'monetary',
        'country': 'USA'
    },
    'ECBESTRVOLWGTTRMDMNRT': {
        'name': 'Euro_STR',
        'description': 'Euro Short-Term Rate',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'monetary',
        'region': 'EUR'
    },
    'TEDRATE': {
        'name': 'TED_Spread',
        'description': 'TED Spread (3-Month LIBOR - 3-Month Treasury)',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'banking_stress',
    },
    
    # Bank Funding Stress
    'USD3MTD156N': {
        'name': 'USD_Libor_3M',
        'description': '3-Month London Interbank Offered Rate (LIBOR), based on U.S. Dollar',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'banking_stress',
        'country': 'USA'
    },
}

# ----------------------------------------------
# Backward compatibility constants for legacy imports
# ----------------------------------------------

# Older test suites expect a single INFLATION dict; expose the CPI block under
# that historical name so existing imports continue to work.
INFLATION = INFLATION_CPI

# Legacy "MACRO_INDICATORS" bundles GDP, unemployment, and monetary policy
# metrics into one mapping. The modern configuration keeps them in dedicated
# dicts, but we expose the merged view for backwards compatibility.
MACRO_INDICATORS = {
    **GDP_GROWTH,
    **UNEMPLOYMENT,
    **POLICY_RATES,
    **MONETARY_LIQUIDITY,
}

# ==============================================
# 🛢️ COMMODITIES (FRED - backup to Yahoo Finance)
# ==============================================

COMMODITIES_FRED = {
    'DCOILBRENTEU': {
        'name': 'Brent_Crude',
        'description': 'Crude Oil Prices: Brent - Europe',
        'frequency': 'daily',
        'unit': 'dollars_per_barrel',
        'category': 'commodity'
    },
    'DCOILWTICO': {
        'name': 'WTI_Crude',
        'description': 'Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma',
        'frequency': 'daily',
        'unit': 'dollars_per_barrel',
        'category': 'commodity'
    },
    'GOLDAMGBD228NLBM': {
        'name': 'Gold',
        'description': 'Gold Fixing Price 10:30 A.M. (London time) in London Bullion Market, based in U.S. Dollars',
        'frequency': 'daily',
        'unit': 'dollars_per_troy_ounce',
        'category': 'commodity'
    },
}

# ==============================================
# 🌐 YAHOO FINANCE COMMODITIES (PREFERRED)
# ==============================================

COMMODITIES_YAHOO = {
    'BZ=F': {
        'name': 'Brent_Crude_Futures',
        'description': 'Brent Crude Oil Futures',
        'frequency': 'daily',
        'unit': 'dollars_per_barrel',
        'category': 'commodity'
    },
    'CL=F': {
        'name': 'WTI_Crude_Futures',
        'description': 'Crude Oil WTI Futures',
        'frequency': 'daily',
        'unit': 'dollars_per_barrel',
        'category': 'commodity'
    },
    'GC=F': {
        'name': 'Gold_Futures',
        'description': 'Gold Futures',
        'frequency': 'daily',
        'unit': 'dollars_per_troy_ounce',
        'category': 'commodity'
    },
}

# ==============================================
# 📊 MARKET RISK INDICATORS (FRED)
# ==============================================

MARKET_RISK_FRED = {
    # Equity Volatility - CRITICAL
    'VIXCLS': {
        'name': 'VIX',
        'description': 'CBOE Volatility Index: VIX',
        'frequency': 'daily',
        'unit': 'index',
        'category': 'market_volatility'
    },
    
    # Real Estate Prices
    'CSUSHPISA': {
        'name': 'US_Home_Price_Index',
        'description': 'S&P/Case-Shiller U.S. National Home Price Index',
        'frequency': 'monthly',
        'unit': 'index',
        'category': 'real_estate',
        'country': 'USA'
    },
    'ITALRPPPPLOPM': {
        'name': 'Italy_Property_Prices',
        'description': 'Real Residential Property Prices for Italy',
        'frequency': 'quarterly',
        'unit': 'index',
        'category': 'real_estate',
        'country': 'ITA'
    },
    
    # Interest Rate Derivatives
    'DSWP10': {
        'name': 'USD_10Y_Swap_Rate',
        'description': '10-Year Swap Rate',
        'frequency': 'daily',
        'unit': 'percent',
        'category': 'interest_rate_derivative',
        'country': 'USA'
    },
    'MORTGAGE30US': {
        'name': 'US_30Y_Mortgage_Rate',
        'description': '30-Year Fixed Rate Mortgage Average in the United States',
        'frequency': 'weekly',
        'unit': 'percent',
        'category': 'interest_rate_derivative',
        'country': 'USA'
    },
}

# ==============================================
# 📈 EQUITY INDICES (YAHOO FINANCE)
# ==============================================

EQUITY_INDICES_YAHOO = {
    '^GDAXI': {
        'name': 'DAX',
        'description': 'DAX Performance Index (Germany)',
        'frequency': 'daily',
        'unit': 'index',
        'category': 'equity_index',
        'country': 'DEU'
    },
    '^IBEX': {
        'name': 'IBEX_35',
        'description': 'IBEX 35 Index (Spain)',
        'frequency': 'daily',
        'unit': 'index',
        'category': 'equity_index',
        'country': 'ESP'
    },
    '^GSPC': {
        'name': 'SP500',
        'description': 'S&P 500 Index (Broader US Market)',
        'frequency': 'daily',
        'unit': 'index',
        'category': 'equity_index',
        'country': 'USA'
    },
}

# ==============================================
# 💱 FX RATES (YAHOO FINANCE)
# ==============================================

FX_RATES_YAHOO = {
    'EURGBP=X': {
        'name': 'EUR_GBP',
        'description': 'EUR/GBP Exchange Rate',
        'frequency': 'daily',
        'unit': 'exchange_rate',
        'category': 'fx'
    },
    'EURCHF=X': {
        'name': 'EUR_CHF',
        'description': 'EUR/CHF Exchange Rate (Safe Haven)',
        'frequency': 'daily',
        'unit': 'exchange_rate',
        'category': 'fx'
    },
    'EURJPY=X': {
        'name': 'EUR_JPY',
        'description': 'EUR/JPY Exchange Rate',
        'frequency': 'daily',
        'unit': 'exchange_rate',
        'category': 'fx'
    },
}

# ==============================================
# 🎯 AGGREGATED CONFIGURATIONS
# ==============================================

# All FRED series combined
ALL_FRED_SERIES = {
    **CREDIT_SPREADS,
    **SOVEREIGN_YIELDS_10Y,
    **INFLATION_CPI,
    **GDP_GROWTH,
    **UNEMPLOYMENT,
    **POLICY_RATES,
    **MONETARY_LIQUIDITY,
    **MARKET_RISK_FRED,  # NEW: VIX, Real Estate, Swaps, Mortgages
    **COMMODITIES_FRED,  # Backup option
}

# All Yahoo Finance series combined
ALL_YAHOO_SERIES = {
    **COMMODITIES_YAHOO,
    **EQUITY_INDICES_YAHOO,  # NEW: DAX, IBEX, S&P 500
    **FX_RATES_YAHOO,  # NEW: EUR/GBP, EUR/CHF, EUR/JPY
}

# Compute sovereign spreads (derived indicators)
COMPUTED_SPREADS = {
    'BTP_Bund_Spread': {
        'formula': 'Italy_10Y_Yield - Germany_10Y_Yield',
        'components': ['IRLTLT01ITM156N', 'IRLTLT01DEM156N'],
        'description': 'Italian-German 10Y Sovereign Spread',
        'category': 'sovereign_spread',
        'country': 'ITA'
    },
    'Bonos_Bund_Spread': {
        'formula': 'Spain_10Y_Yield - Germany_10Y_Yield',
        'components': ['IRLTLT01ESM156N', 'IRLTLT01DEM156N'],
        'description': 'Spanish-German 10Y Sovereign Spread',
        'category': 'sovereign_spread',
        'country': 'ESP'
    },
    'OAT_Bund_Spread': {
        'formula': 'France_10Y_Yield - Germany_10Y_Yield',
        'components': ['IRLTLT01FRM156N', 'IRLTLT01DEM156N'],
        'description': 'French-German 10Y Sovereign Spread',
        'category': 'sovereign_spread',
        'country': 'FRA'
    },
    'Treasury_Bund_Spread': {
        'formula': 'USA_10Y_Yield - Germany_10Y_Yield',
        'components': ['IRLTLT01USM156N', 'IRLTLT01DEM156N'],
        'description': 'US Treasury vs German Bund 10Y Spread',
        'category': 'sovereign_spread',
        'country': 'USA'
    },
}

# Countries for stress testing
STRESS_TEST_COUNTRIES = ['ITA', 'FRA', 'DEU', 'ESP', 'USA']

# Date range configuration
DEFAULT_START_DATE = '1990-01-01'
DEFAULT_END_DATE = '2025-10-31'

def get_fred_series_by_category(category: str) -> Dict:
    """Get all FRED series for a specific category"""
    return {
        code: meta for code, meta in ALL_FRED_SERIES.items()
        if meta.get('category') == category
    }

def get_fred_series_by_frequency(frequency: str) -> Dict:
    """Get all FRED series for a specific frequency"""
    return {
        code: meta for code, meta in ALL_FRED_SERIES.items()
        if meta.get('frequency') == frequency
    }

def get_series_codes_list() -> List[str]:
    """Get list of all FRED series codes"""
    return list(ALL_FRED_SERIES.keys())
