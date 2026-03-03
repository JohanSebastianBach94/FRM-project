#!/usr/bin/env python3
"""Search FRED for targeted gold and per-metal series and write a CSV report.
Requires environment variable `FRED_API_KEY` to be set.
"""
import os
import sys
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import quote_plus

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT / 'fred_gold_and_metals_search.csv'

FRED_KEY = os.environ.get('FRED_API_KEY')
if not FRED_KEY:
    print('FRED_API_KEY not set in environment; set it and re-run')
    sys.exit(2)

BASE = 'https://api.stlouisfed.org/fred'
SEARCH = BASE + '/series/search?search_text={q}&api_key={k}&file_type=json&limit=10'

# Extended targets and keywords
targets = {
    'GOLD_SPOT': [
        'gold spot', 'gold price', 'gold spot price', 'lbma gold price', 'gold pm', 'gold am', 'xau usd', 'XAUUSD'
    ],
    'COPPER': ['copper price', 'copper spot', 'copper price lbma', 'copper usd'],
    'ALUMINUM': ['aluminium price', 'aluminum price', 'aluminum spot', 'aluminium usd'],
    'NICKEL': ['nickel price', 'nickel spot', 'nickel usd']
}

rows = []

for t, keywords in targets.items():
    found = False
    for q in keywords:
        url = SEARCH.format(q=quote_plus(q), k=FRED_KEY)
        try:
            with urlopen(url, timeout=30) as r:
                j = json.load(r)
        except Exception as e:
            rows.append({'target': t, 'keyword': q, 'error': str(e)})
            continue

        seriess = j.get('seriess') or j.get('seriess')
        if seriess and len(seriess) > 0:
            # append top 3 hits for context
            for s in seriess[:3]:
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
