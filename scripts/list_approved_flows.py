#!/usr/bin/env python3
import csv
from pathlib import Path

P = Path('data_repository/processed/BIS_catalog.csv')
if not P.exists():
    print('Missing catalog', P)
    raise SystemExit(1)

approved = []
flows = {}
with P.open(newline='', encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        if (row.get('approval') or '').strip().upper() == 'A':
            series = (row.get('series') or '').strip()
            flow = (row.get('bis_flow') or '').strip()
            approved.append((series, flow))
            flows.setdefault(flow, []).append(series)

print('Approved rows:', len(approved))
print('Unique flows:', len([f for f in flows.keys() if f]))
for flow, series_list in flows.items():
    print('FLOW:', repr(flow), '->', len(series_list), 'series')

print('\nSample approved series (first 30):')
for s,f in approved[:30]:
    print(s, '->', f)
