import csv
from pathlib import Path

def foreign_check(token: str) -> None:
    print(token, any(token == row["series"].upper() for row in rows))

def contains(token: str) -> None:
    print(token, [row["series"] for row in rows if token in row["series"].upper()])

path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open("r", encoding="utf-8") as f:
    reader = list(csv.DictReader(f))
rows = reader
print(any("NAEXKP01USQ657S" == row["series"].upper() for row in rows))
contains("LRHUTTTT")
contains("NAEXK")
contains("MORTGAGE_RATE_USA")
contains("BANK_EQUITY_INDEX")
contains("BETA0")
contains("BIS_LBS")
