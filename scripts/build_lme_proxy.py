#!/usr/bin/env python3
"""Build an equally-weighted LME metals proxy from FRED per-metal series.
Uses environment `FRED_API_KEY`. Fetches observations for copper, aluminum, nickel (monthly USD/ton series found earlier), aligns by date, computes equal-weight mean, and writes CSV + meta.
"""
import os
import sys
import json
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urlencode
from datetime import datetime
import csv

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'data_repository' / 'raw' / 'commodities'
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_CSV = OUT_DIR / 'lme_proxy_equal.csv'
OUT_META = OUT_DIR / 'lme_proxy_equal.meta.json'

FRED_KEY = os.environ.get('FRED_API_KEY')
if not FRED_KEY:
    print('FRED_API_KEY not set; set it and re-run')
    sys.exit(2)

BASE = 'https://api.stlouisfed.org/fred/series/observations'
# Series selected (from previous search): monthly USD/ton
SERIES = {
    'COPPER': 'PCOPPUSDM',
    'ALUMINUM': 'PALUMUSDM',
    'NICKEL': 'PNICKUSDM'
}

def fetch_series(series_id):
    params = {'series_id': series_id, 'api_key': FRED_KEY, 'file_type': 'json'}
    url = BASE + '?' + urlencode(params)
    try:
        with urlopen(url, timeout=30) as r:
            j = json.load(r)
            obs = j.get('observations', [])
            return obs
    except Exception as e:
        print('Error fetching', series_id, e)
        return None

# fetch all series
all_obs = {}
for name, sid in SERIES.items():
    obs = fetch_series(sid)
    if obs is None:
        print('Failed to fetch', sid)
        sys.exit(1)
    # map date -> value (float or None)
    dmap = {}
    for o in obs:
        date = o.get('date')
        val = o.get('value')
        if val is None or val == '.' or val == '':
            dmap[date] = None
        else:
            try:
                dmap[date] = float(val)
            except:
                dmap[date] = None
    all_obs[name] = dmap

# union of dates
dates = sorted({d for m in all_obs.values() for d in m.keys()})

# build rows: date, copper, aluminum, nickel, composite_mean
rows = []
for d in dates:
    vals = []
    row = {'date': d}
    for name in SERIES.keys():
        v = all_obs[name].get(d)
        row[name] = '' if v is None else v
        if v is not None:
            vals.append(v)
    comp = ''
    if vals:
        comp = sum(vals)/len(vals)
    row['LME_PROXY_EQUAL'] = '' if comp == '' else comp
    rows.append(row)

# write CSV
fieldnames = ['date'] + list(SERIES.keys()) + ['LME_PROXY_EQUAL']
with open(OUT_CSV, 'w', newline='', encoding='utf-8') as fh:
    w = csv.DictWriter(fh, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)

meta = {
    'created_at': datetime.utcnow().isoformat() + 'Z',
    'method': 'equally-weighted mean of available metals per date',
    'series': SERIES,
    'rows': len(rows),
}
with open(OUT_META, 'w', encoding='utf-8') as f:
    json.dump(meta, f, indent=2)

print('Wrote', OUT_CSV)
print('Wrote', OUT_META)
