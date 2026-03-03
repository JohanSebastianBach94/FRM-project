from __future__ import annotations

import csv
from pathlib import Path

PATH = Path('data_repository/processed/BIS_catalog.csv')
if not PATH.exists():
    raise SystemExit(f"Missing catalog {PATH}")

with PATH.open('r', newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row.get('approval', '').upper() == 'A' and row.get('bis_source_file'):
            print(row['series'], row['bis_flow'], row['series_key'])
