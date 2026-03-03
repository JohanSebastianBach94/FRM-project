import csv
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "catalog.csv"
USER_SERIES = [
    "BAMLC0A0CM", "BAMLC0A1CAAAEY", "BAMLC0A2CAAEY", "BAMLC0A3CAEY", "BAMLC0A4CBBB",
    "BAMLH0A0CM", "BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYBEY",
    "CDS_5Y_USA", "CDS_5Y_ITA", "CDS_5Y_FRA", "CDS_5Y_ESP", "CDS_5Y_DEU",
    "COMM_PAPER_SPREAD_USA", "COMM_PAPER_SPREAD_EUR", "EURIBOR_3M",
    "DFF", "ECBDFR", "ECBASSETS", "MABMM301EZM189S",
    "IRLTLT01USM156N", "IRLTLT01DEM156N", "IRLTLT01ITM156N", "IRLTLT01FRM156N", "IRLTLT01ESM156N",
    "BTP_BUND_SPREAD", "BONOS_BUND_SPREAD",
    "MOVE", "DSWP10",
    "GDPC1", "NOMINAL_GDP_USA", "NAEXKP01USQ657S", "LRHUTTTTUSM156S", "LRHUTTTTITM156S", "LRHUTTTTFRM156S", "LRHUTTTTESM156S", "LRHUTTTTDEM156S",
    "CPIAUCSL", "ITACPIALLMINMEI", "FRACPIALLMINMEI", "ESPCPIALLMINMEI", "DEUCPIALLMINMEI",
    "USA_HPI_REAL", "ITA_HPI_REAL", "FRA_HPI_REAL", "ESP_HPI_REAL", "DEU_HPI_REAL", "MORTGAGE_RATE_USA",
    "BANK_EQUITY_INDEX_USA", "BANK_EQUITY_INDEX_ITA", "BANK_EQUITY_INDEX_FRA", "BANK_EQUITY_INDEX_ESP", "BANK_EQUITY_INDEX_DEU", "FTSEMIB",
    "HYG", "JNK", "LQD", "EMLC", "EWG", "EWQ", "EWP", "EZU", "EWU", "EWJ",
    "BETA0_USA", "BETA1_USA", "BETA2_USA", "BETA3_USA",
    "EUR_USD", "GBP_USD", "DXY",
    "DCOILWTICO", "DCOILBRENTEU", "GOLD_SPOT", "FAO_AG_INDEX", "LME_METALS_INDEX",
    "BIS_LBS_HH", "BIS_LBS_NFC", "NPL_PROXY_ITA",
]

with CATALOG.open("r", encoding="utf-8") as infile:
    reader = csv.reader(infile)
    headers = next(reader)
    catalog = {row[0].strip().upper() for row in reader if row}

missing = [name for name in USER_SERIES if name.upper() not in catalog]
extra = [name for name in catalog if name not in {n.upper() for n in USER_SERIES}]
print("missing", missing)
print("extra_count", len(extra))
print("user_list", len(USER_SERIES))
print("catalog", len(catalog))
