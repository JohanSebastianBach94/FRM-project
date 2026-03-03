import csv
from pathlib import Path

CATALOG = Path(__file__).resolve().parents[1] / "catalog.csv"
with CATALOG.open("r", encoding="utf-8", newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        series = row["series"]
        if "beta" in series.lower():
            print(series)
