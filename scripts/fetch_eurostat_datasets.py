#!/usr/bin/env python3
"""Attempt to fetch several candidate Eurostat datasets that may contain government gross debt.
Saves JSON responses to `data_repository/raw/macro/euro_{dataset}_{ISO}.json`.
"""
import requests
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
MACRO.mkdir(parents=True, exist_ok=True)

datasets = [
    'gov_10q_ggdebt',
    'gov_10dd_edpt1',
    'gov_10dd_edpt2',
    'gov_10dd_edpt3',
    'gov_10a_main',
    'gov_10dd',
    'gov_10dd_edpt',
]

isos = ['DEU','FRA','ITA','ESP','USA','GBR','CHE']

base_url = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'

for ds in datasets:
    for iso in isos:
        out = MACRO / f'euro_{ds}_{iso}.json'
        url = f'{base_url}/{ds}?geo={iso}&format=JSON'
        try:
            print('Fetching', url)
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                out.write_text(r.text, encoding='utf-8')
                print('  saved', out.name)
            else:
                print('  HTTP', r.status_code, 'for', ds, iso)
        except Exception as e:
            print('  error', e, 'for', ds, iso)
