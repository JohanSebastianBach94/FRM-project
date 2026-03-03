#!/usr/bin/env python3
"""Search FRED for a small list of commodity-related targets and write a CSV report.

Usage: python scripts/search_fred_commodities.py
Requires environment variable `FRED_API_KEY` to be set.
"""
import os
import sys
import json
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT / 'fred_commodities_search.csv'

FRED_KEY = os.environ.get('FRED_API_KEY')
if not FRED_KEY:
    print('FRED_API_KEY not set in environment; set it and re-run')
    sys.exit(2)

BASE = 'https://api.stlouisfed.org/fred'
SEARCH = BASE + '/series/search?search_text={q}&api_key={k}&file_type=json'

targets = {
    'TTF_GAS': ['ttf gas', 'ttf natural gas', 'european natural gas ttf'],
    'LME_METALS_INDEX': ['lme metals', 'lme index', 'lme metals index'],
    'FAO_AG_INDEX': ['fao food price index', 'fao food price', 'fao agriculture index', 'fao fpi'],
    'GOLD_SPOT': ['gold spot', 'gold price', 'gold spot price']
}

rows = []

for t, keywords in targets.items():
    found = None
    from urllib.parse import quote_plus
    for q in keywords:
        url = SEARCH.format(q=quote_plus(q), k=FRED_KEY)
        try:
            with urlopen(url, timeout=30) as r:
                j = json.load(r)
        except Exception as e:
            rows.append({'target': t, 'keyword': q, 'error': str(e)})
            continue

        # pick first seriess result if present
        seriess = j.get('seriess') or j.get('seriess')
        if seriess and len(seriess) > 0:
            s = seriess[0]
            rows.append({
                'target': t,
                'keyword': q,
                'fred_id': s.get('id'),
                'title': s.get('title'),
                'units': s.get('units'),
                'frequency': s.get('frequency'),
                'seasonal_adjustment': s.get('seasonal_adjustment')
            })
            found = True
            break
    if not found:
        rows.append({'target': t, 'keyword': ','.join(keywords), 'fred_id': '', 'title': '', 'units': '', 'frequency': '', 'seasonal_adjustment': ''})

# write CSV
import csv
with open(OUT_FILE, 'w', newline='', encoding='utf-8') as fh:
    w = csv.DictWriter(fh, fieldnames=['target','keyword','fred_id','title','units','frequency','seasonal_adjustment','error'])
    w.writeheader()
    for r in rows:
        w.writerow(r)

print('Wrote', OUT_FILE)
