import csv
from pathlib import Path

targets = {"EWC", "BAMLH0A0CM", "USD_JPY"}
path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open(newline="") as fp:
    reader = csv.DictReader(fp)
    for row in reader:
        if row["series"] in targets:
            print(row)
