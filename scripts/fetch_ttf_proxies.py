#!/usr/bin/env python3
"""Attempt to fetch public TTF/European gas hub proxies.
Tries a few known public CSV endpoints (ENTSO-G / gas hub public pages) and writes meta with any found files.
"""
import os
from pathlib import Path
from urllib.request import urlopen
import json

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data_repository' / 'raw' / 'ttf'
OUT.mkdir(parents=True, exist_ok=True)
META = OUT / 'TTF_proxies.meta.json'

candidates = [
    # ENTSOG transparency platform (may require query construction) - placeholder
    'https://transparency.entsog.eu/api/v1/aggregation',
    # TTF Historical price CSVs sometimes available from public aggregators (placeholder)
    'https://www.theice.com/products/1/UK-NBP-Futures/data',
]

meta = {'attempted': [], 'success': False}
for url in candidates:
    meta['attempted'].append(url)
    try:
        with urlopen(url, timeout=20) as r:
            content = r.read()
            outp = OUT / 'ttf_candidate_1.html'
            with open(outp, 'wb') as f:
                f.write(content)
            meta['success'] = True
            meta['source'] = url
            break
    except Exception as e:
        meta.setdefault('errors', []).append({'url': url, 'error': str(e)})

with open(META, 'w', encoding='utf-8') as f:
    json.dump(meta, f, indent=2)

print('TTF proxy fetch attempted; meta written to', META)
