import csv
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "catalog.csv"
MAPPING = {
    "BAMLC0A0CM": "US Corporate Credit – Broad Market Option-Adjusted Spread",
    "BAMLC0A1CAAAEY": "US Corporate Credit – AAA Investment Grade Option-Adjusted Spread",
    "BAMLC0A2CAAEY": "US Corporate Credit – AA Investment Grade Option-Adjusted Spread",
    "BAMLC0A3CAEY": "US Corporate Credit – A Investment Grade Option-Adjusted Spread",
    "BAMLC0A4CBBB": "US Corporate Credit – BBB Investment Grade Option-Adjusted Spread",
    "BAMLH0A0CM": "US Corporate Credit – High Yield Broad Market Option-Adjusted Spread",
    "BAMLH0A0HYM2": "US Corporate Credit – High Yield Master II Option-Adjusted Spread",
    "BAMLH0A1HYBB": "US Corporate Credit – High Yield BB Rating Option-Adjusted Spread",
    "BAMLH0A2HYBEY": "US Corporate Credit – High Yield B Rating Option-Adjusted Spread",
    "CDS_5Y_USA": "United States Sovereign Risk – 5-Year Credit Default Swap Spread",
    "CDS_5Y_ITA": "Italy Sovereign Risk – 5-Year Credit Default Swap Spread",
    "CDS_5Y_FRA": "France Sovereign Risk – 5-Year Credit Default Swap Spread",
    "CDS_5Y_ESP": "Spain Sovereign Risk – 5-Year Credit Default Swap Spread",
    "CDS_5Y_DEU": "Germany Sovereign Risk – 5-Year Credit Default Swap Spread",
    "COMM_PAPER_SPREAD_USA": "United States Short-Term Funding Stress – Commercial Paper Spread",
    "COMM_PAPER_SPREAD_EUR": "Euro Area Short-Term Funding Stress – Commercial Paper Spread",
    "EURIBOR_3M": "Euro Area Interbank Funding Cost – 3-Month EURIBOR Rate",
    "DFF": "United States Monetary Policy Stance – Effective Federal Funds Rate",
    "ECBDFR": "Euro Area Monetary Policy Stance – ECB Deposit Facility Rate",
    "ECBASSETS": "Euro Area Central Bank Liquidity – ECB Total Assets",
    "MABMM301EZM189S": "Euro Area Broad Money Supply – M3 Aggregate",
    "IRLTLT01USM156N": "United States Sovereign Yield Curve – 10-Year Government Bond Yield",
    "IRLTLT01DEM156N": "Germany Sovereign Yield Curve – 10-Year Government Bond Yield",
    "IRLTLT01ITM156N": "Italy Sovereign Yield Curve – 10-Year Government Bond Yield",
    "IRLTLT01FRM156N": "France Sovereign Yield Curve – 10-Year Government Bond Yield",
    "IRLTLT01ESM156N": "Spain Sovereign Yield Curve – 10-Year Government Bond Yield",
    "BTP_BUND_SPREAD": "Italy Sovereign Risk Premium – BTP vs Bund Yield Spread",
    "BONOS_BUND_SPREAD": "Spain Sovereign Risk Premium – Bonos vs Bund Yield Spread",
    "MOVE": "US Treasury Market Uncertainty – Implied Yield Curve Volatility Index",
    "DSWP10": "US Long-Term Interest Rate Volatility – 10-Year Swap Volatility",
    "GDPC1": "United States Real Economic Output – Real Gross Domestic Product",
    "NOMINAL_GDP_USA": "United States Nominal Economic Output – Gross Domestic Product",
    "LRHUTTTTUSM156S": "United States Labor Market Slack – Unemployment Rate",
    "LRHUTTTTDEM156S": "Germany Labor Market Slack – Unemployment Rate",
    "LRHUTTTTITM156S": "Italy Labor Market Slack – Unemployment Rate",
    "LRHUTTTTFRM156S": "France Labor Market Slack – Unemployment Rate",
    "LRHUTTTTESM156S": "Spain Labor Market Slack – Unemployment Rate",
    "CPIAUCSL": "United States Consumer Price Dynamics – Headline CPI Index",
    "ITACPIALLMINMEI": "Italy Consumer Price Dynamics – Headline CPI Index",
    "FRACPIALLMINMEI": "France Consumer Price Dynamics – Headline CPI Index",
    "ESPCPIALLMINMEI": "Spain Consumer Price Dynamics – Headline CPI Index",
    "DEUCPIALLMINMEI": "Germany Consumer Price Dynamics – Headline CPI Index",
    "USA_HPI_REAL": "United States Residential Real Estate – Real House Price Index",
    "ITA_HPI_REAL": "Italy Residential Real Estate – Real House Price Index",
    "FRA_HPI_REAL": "France Residential Real Estate – Real House Price Index",
    "ESP_HPI_REAL": "Spain Residential Real Estate – Real House Price Index",
    "DEU_HPI_REAL": "Germany Residential Real Estate – Real House Price Index",
    "MORTGAGE_RATE_USA": "United States Household Credit Conditions – Mortgage Interest Rate",
    "BANK_EQUITY_INDEX_USA": "United States Banking Sector Valuation – Equity Price Index",
    "BANK_EQUITY_INDEX_ITA": "Italy Banking Sector Valuation – Equity Price Index",
    "BANK_EQUITY_INDEX_FRA": "France Banking Sector Valuation – Equity Price Index",
    "BANK_EQUITY_INDEX_ESP": "Spain Banking Sector Valuation – Equity Price Index",
    "BANK_EQUITY_INDEX_DEU": "Germany Banking Sector Valuation – Equity Price Index",
    "FTSEMIB": "Italy Equity Market Performance – FTSE MIB Index",
    "HYG": "US Corporate Credit Risk – High Yield Bond ETF",
    "JNK": "US Corporate Credit Risk – Speculative Grade Bond ETF",
    "LQD": "US Corporate Credit Risk – Investment Grade Bond ETF",
    "EMLC": "Emerging Market Sovereign Risk – Local Currency Debt ETF",
    "EWG": "Germany Equity Market Exposure – MSCI Germany ETF",
    "EWQ": "France Equity Market Exposure – MSCI France ETF",
    "EWP": "Spain Equity Market Exposure – MSCI Spain ETF",
    "EZU": "Euro Area Equity Market Exposure – MSCI EMU ETF",
    "EWU": "United Kingdom Equity Market Exposure – MSCI UK ETF",
    "EWJ": "Japan Equity Market Exposure – MSCI Japan ETF",
    "USA_BETA0": "United States Yield Curve – Level Factor (DNSS Beta 0)",
    "USA_BETA1": "United States Yield Curve – Slope Factor (DNSS Beta 1)",
    "USA_BETA2": "United States Yield Curve – Curvature Factor (DNSS Beta 2)",
    "USA_BETA3": "United States Yield Curve – Twist Factor (DNSS Beta 3)",
    "DEU_BETA0": "Germany Yield Curve – Level Factor (DNSS Beta 0)",
    "DEU_BETA1": "Germany Yield Curve – Slope Factor (DNSS Beta 1)",
    "DEU_BETA2": "Germany Yield Curve – Curvature Factor (DNSS Beta 2)",
    "DEU_BETA3": "Germany Yield Curve – Twist Factor (DNSS Beta 3)",
    "FRA_BETA0": "France Yield Curve – Level Factor (DNSS Beta 0)",
    "FRA_BETA1": "France Yield Curve – Slope Factor (DNSS Beta 1)",
    "FRA_BETA2": "France Yield Curve – Curvature Factor (DNSS Beta 2)",
    "FRA_BETA3": "France Yield Curve – Twist Factor (DNSS Beta 3)",
    "ITA_BETA0": "Italy Yield Curve – Level Factor (DNSS Beta 0)",
    "ITA_BETA1": "Italy Yield Curve – Slope Factor (DNSS Beta 1)",
    "ITA_BETA2": "Italy Yield Curve – Curvature Factor (DNSS Beta 2)",
    "ITA_BETA3": "Italy Yield Curve – Twist Factor (DNSS Beta 3)",
    "ESP_BETA0": "Spain Yield Curve – Level Factor (DNSS Beta 0)",
    "ESP_BETA1": "Spain Yield Curve – Slope Factor (DNSS Beta 1)",
    "ESP_BETA2": "Spain Yield Curve – Curvature Factor (DNSS Beta 2)",
    "ESP_BETA3": "Spain Yield Curve – Twist Factor (DNSS Beta 3)",
    "EUR_USD": "Euro Area External Value – EUR/USD Exchange Rate",
    "GBP_USD": "United Kingdom External Value – GBP/USD Exchange Rate",
    "DXY": "Global Reserve Currency Strength – US Dollar Index",
    "DCOILWTICO": "Global Energy Prices – WTI Crude Oil Spot Price",
    "DCOILBRENTEU": "Global Energy Prices – Brent Crude Oil Spot Price",
    "GOLD_SPOT": "Global Safe Haven Asset – Gold Spot Price",
    "FAO_AG_INDEX": "Global Food Commodity Prices – FAO Agriculture Index",
    "LME_METALS_INDEX": "Global Industrial Inputs – Base Metals Price Index",
    "BIS_LBS_HOUSEHOLD_LOANS": "Household Credit Supply – BIS Bank Lending to Households",
    "BIS_LBS_PRIVATE_NFC_DEU": "Corporate Credit Supply – BIS Bank Lending to Non-Financial Corporations",
    "BIS_LBS_PRIVATE_NFC_ITA": "Corporate Credit Supply – BIS Bank Lending to Non-Financial Corporations",
    "BIS_LBS_PRIVATE_NFC_TOTAL": "Corporate Credit Supply – BIS Bank Lending to Non-Financial Corporations",
    "NPL_PROXY_ITA": "Italy Banking System Asset Quality – Non-Performing Loan Proxy",
}

with CATALOG.open("r", newline="", encoding="utf-8") as infile:
    reader = list(csv.DictReader(infile))
fieldnames = reader[0].keys() if reader else []

rows = []
for row in reader:
    key = row["series"].upper()
    if key in MAPPING:
        row["entity"] = MAPPING[key]
    rows.append(row)

with CATALOG.open("w", newline="", encoding="utf-8") as outfile:
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print("Rewrote catalog entity descriptions for", sum(1 for row in rows if row["series"].upper() in MAPPING))
