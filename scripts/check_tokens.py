import csv
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open("r", encoding="utf-8", newline="") as f:
    reader = list(csv.DictReader(f))
names = [row["series"] for row in reader]
checks = [
    "NOMINAL_GDP_USA",
    "NAEXKP01USQ657S",
    "LRHUTTTTUSM156S",
    "BETA0_USA",
    "BIS_LBS_HH",
    "EURIBOR_3M",
]
for token in checks:
    matches = [name for name in names if name.upper() == token.upper()]
    print(token, matches)
