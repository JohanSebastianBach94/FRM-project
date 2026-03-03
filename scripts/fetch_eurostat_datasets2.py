#!/usr/bin/env python3
"""Fetch Eurostat debt datasets using ISO2 geo codes for EU countries.
"""
import requests
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
MACRO.mkdir(parents=True, exist_ok=True)

datasets = ['gov_10dd_edpt1','gov_10dd','gov_10a_main']
iso3_to_iso2 = {'DEU':'DE','FRA':'FR','ITA':'IT','ESP':'ES'}

base_url = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data'

for ds in datasets:
    for iso3, iso2 in iso3_to_iso2.items():
        out = MACRO / f'euro_{ds}_{iso3}.json'
        url = f'{base_url}/{ds}?geo={iso2}&format=JSON'
        try:
            print('Fetching', url)
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                out.write_text(r.text, encoding='utf-8')
                print('  saved', out.name)
            else:
                print('  HTTP', r.status_code, 'for', ds, iso2)
        except Exception as e:
            print('  error', e, 'for', ds, iso2)
