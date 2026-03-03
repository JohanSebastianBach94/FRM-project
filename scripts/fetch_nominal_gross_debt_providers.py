#!/usr/bin/env python3
"""
Fetch nominal gross-debt provider payloads (Eurostat + OECD) for a list of ISOs.

Run this on a machine with Internet access:
    python scripts/fetch_nominal_gross_debt_providers.py

It will save raw responses under `data_repository/raw/macro/` as:
  - euro_gov_10_gdp_{ISO}.json
  - oecd_gov_debt_{ISO}.xml

Notes:
- Eurostat: `gov_10_gdp` contains government debt statistics; unit dimension varies.
- OECD: `GOV_DEBT` SDMX endpoint returns XML; parsing must handle SDMX structure.
- This script saves raw payloads so they can be inspected/parsed later.
"""
import os
import time
import urllib.request
import urllib.error
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
MACRO_DIR = BASE_DIR / 'data_repository' / 'raw' / 'macro'
MACRO_DIR.mkdir(parents=True, exist_ok=True)

# Target countries - use iso3 and iso2 for Eurostat
COUNTRIES = {
    'DEU': {'iso2': 'DE'},
    'FRA': {'iso2': 'FR'},
    'ITA': {'iso2': 'IT'},
    'ESP': {'iso2': 'ES'},
    'USA': {'iso2': 'US'},
}

HEADERS = {'User-Agent': 'FRM-fetch/1.0 (+https://example.org)'}


def fetch_url(url, timeout=30):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return data, None
    except Exception as e:
        return None, str(e)


def save_file(path: Path, data: bytes):
    with open(path, 'wb') as f:
        f.write(data)
    print(f'  Saved: {path}')


def fetch_eurostat(iso2):
    """Fetch Eurostat gov_10_gdp for a given ISO2. Save raw JSON.
    Eurostat uses table IDs and dimensions; this call attempts a basic REST query.
    """
    # Try basic query; Eurostat may require specific unit codes to get levels.
    url = f'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10_gdp?geo={iso2}&format=JSON'
    print('  Eurostat URL:', url)
    data, err = fetch_url(url, timeout=60)
    return data, err, url


def fetch_oecd(iso3):
    """Fetch OECD GOV_DEBT SDMX (compact) for iso3.
    Saves raw XML (SDMX)."""
    url = f'https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/GOV_DEBT/{iso3}/all?format=compact_v2'
    print('  OECD URL:', url)
    data, err = fetch_url(url, timeout=60)
    return data, err, url


def main():
    print('Fetching nominal gross debt candidates (Eurostat + OECD)')
    for iso3, codes in COUNTRIES.items():
        print(f'-- {iso3} --')
        iso2 = codes['iso2']

        # Eurostat
        data, err, url = fetch_eurostat(iso2)
        out_path = MACRO_DIR / f'euro_gov_10_gdp_{iso3}.json'
        if data:
            save_file(out_path, data)
        else:
            print('  Eurostat fetch failed:', err)

        time.sleep(1)

        # OECD
        data, err, url = fetch_oecd(iso3)
        out_path = MACRO_DIR / f'oecd_gov_debt_{iso3}.xml'
        if data:
            save_file(out_path, data)
        else:
            print('  OECD fetch failed:', err)

        time.sleep(1)

    print('\nDone. Inspect saved files in data_repository/raw/macro/ and then run parsing/ingest steps.')


if __name__ == '__main__':
    main()
