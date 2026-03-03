import csv
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open('r', encoding='utf-8', newline='') as f:
    reader = csv.DictReader(f)
    cds = [row['series'] for row in reader if row['series'].upper().startswith('CDS')]
print(cds)
