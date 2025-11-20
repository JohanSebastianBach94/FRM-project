#!/usr/bin/env python3
"""Fetch small sample of public structural series (World Bank) and save to CSV.

This script is intentionally dependency-free (uses urllib + json) so it runs in minimal envs.
It demonstrates what's trivially available via free APIs and writes outputs to data/raw/.

Usage: python scripts/fetch_structural_data.py
"""
import os
import sys
import json
import urllib.request
import urllib.parse
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_DIR = os.path.join(BASE_DIR, 'data', 'raw')
os.makedirs(OUT_DIR, exist_ok=True)

COUNTRIES = ['FRA','DEU','ITA','ESP','USA','GBR','CHE']
INDICATORS = {
    'GC.DOD.TOTL.GD.ZS': 'general_government_gross_debt_pct_gdp'
}

def fetch_worldbank(country, indicator):
    url = f'https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json&per_page=2000'
    # Simple GET
    with urllib.request.urlopen(url, timeout=30) as resp:
        data = resp.read()
    return json.loads(data)

def parse_and_write(country, indicator, outpath):
    try:
        payload = fetch_worldbank(country, indicator)
    except Exception as e:
        print(f'ERROR fetching {indicator} for {country}: {e}')
        return False

    if not isinstance(payload, list) or len(payload) < 2:
        print(f'No data returned for {country} {indicator}')
        return False

    records = payload[1]
    # Write CSV: country, date, value, indicator
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write('country,year,value,indicator\n')
        for r in records:
            date = r.get('date')
            val = r.get('value')
            if val is None:
                continue
            # World Bank returns annual/quarterly as year strings
            f.write(f'{country},{date},{val},{indicator}\n')
    return True

def main():
    print('Starting sample fetch of World Bank structural series...')
    timestamp = datetime.utcnow().isoformat()
    summary = []
    for country in COUNTRIES:
        for indicator, name in INDICATORS.items():
            fname = f'{name}_{country}.csv'
            outpath = os.path.join(OUT_DIR, fname)
            ok = parse_and_write(country, indicator, outpath)
            summary.append((country, indicator, outpath, ok))
            print(f'Wrote {outpath}' if ok else f'Failed {country} {indicator}')

    print('\nFetch summary:')
    for country, indicator, outpath, ok in summary:
        print(country, indicator, 'OK' if ok else 'FAIL', outpath)

if __name__ == '__main__':
    main()
