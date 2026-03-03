import csv
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open("r", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        print(row.get("series", ""))
