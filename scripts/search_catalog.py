import csv
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "catalog.csv"
FILTERS = [
    "EURIBOR",
    "CDS_5y",
    "BIS_LBS",
    "BETA",
    "BANK_EQUITY",
]

with CATALOG.open("r", encoding="utf-8", newline="") as infile:
    reader = csv.DictReader(infile)
    names = [row["series"] for row in reader]

for token in FILTERS:
    matches = [name for name in names if token in name.upper()]
    print(token, len(matches), matches[:5])

print("unique count", len(names))
