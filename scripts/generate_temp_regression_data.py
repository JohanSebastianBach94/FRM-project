import numpy as np
import pandas as pd
from pathlib import Path

codes = [
    "GDPC1",
    "CPIAUCSL",
    "UNRATE",
    "BAMLC0A4CBBB",
    "VIXCLS",
    "^GSPC",
    "DCOILWTICO",
    "NAEXKP01DEQ661S",
    "IRLTLT01DEM156N",
    "DEUCPIALLMINMEI",
    "TEDRATE",
    "NAEXKP01FRQ661S",
    "IRLTLT01FRM156N",
    "FRACPIALLMINMEI",
    "LRHUTTTTFRM156S",
    "OAT_Bund_Spread",
    "NAEXKP01ITQ661S",
    "IRLTLT01ITM156N",
    "ITACPIALLMINMEI",
    "LRHUTTTTITM156S",
    "BTP_Bund_Spread",
    "NAEXKP01ESQ661S",
    "IRLTLT01ESM156N",
    "ESPCPIALLMINMEI",
    "LRHUTTTTESM156S",
    "Bonos_Bund_Spread",
]
dates = pd.date_range("2023-01-01", periods=120, freq="W")
rows = []
np.random.seed(0)
for date in dates:
    base = np.sin(date.toordinal() / 365 * 2 * np.pi)
    for code in codes:
        value = base + np.random.normal(scale=0.01)
        rows.append({"date": date, "series_code": code, "value": value})
output = Path("temp_regression_data.csv")
if output.exists():
    output.unlink()
pd.DataFrame(rows).to_csv(output, index=False)
