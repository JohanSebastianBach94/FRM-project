#!/usr/bin/env python3
"""
Robust fetcher using pandasdmx for OECD/IMF/Eurostat with HTTP fallbacks.
Saves raw payloads under `data_repository/raw/macro/`.

Run on a connected machine:
  python scripts\fetch_nominal_gross_debt_pandasdmx.py
"""
import time
import json
import traceback
from pathlib import Path

try:
    from pandasdmx import Request
except Exception:
    Request = None

import urllib.request

BASE_DIR = Path(__file__).resolve().parents[1]
MACRO_DIR = BASE_DIR / 'data_repository' / 'raw' / 'macro'
MACRO_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = {
    'DEU': {'iso2': 'DE'},
    'FRA': {'iso2': 'FR'},
    'ITA': {'iso2': 'IT'},
    'ESP': {'iso2': 'ES'},
    'USA': {'iso2': 'US'},
}

HEADERS = {'User-Agent': 'FRM-fetch/1.0 (+https://example.org)'}


def save(path: Path, data: bytes):
    path.write_bytes(data)
    print('  Saved', path)


def fetch_http(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read(), None
    except Exception as e:
        return None, str(e)


def try_pandasdmx_fetch(provider, flow, key=None, params=None):
    if Request is None:
        return None, 'pandasdmx not available'
    try:
        r = Request(provider)
        print(f'  pandasdmx: requesting {flow} from {provider} ...')
        data = r.data(flow, key=key, params=params)
        # Return text representation
        txt = data.to_sdmx() if hasattr(data, 'to_sdmx') else str(data)
        return txt.encode('utf-8'), None
    except Exception as e:
        return None, str(e)


def fetch_eurostat_with_variants(iso2):
    # Try pandasdmx first (dataset id may vary); then try REST ways.
    attempts = []
    # Common Eurostat dataset candidates for government debt: 'gov_10_gdp', 'gov_10a_main', 'gov_10dd'
    candidates = ['gov_10_gdp', 'gov_10a_main', 'gov_10dd', 'gov_10a_main']
    if Request is not None:
        for cand in candidates:
            data, err = try_pandasdmx_fetch('ESTAT', cand, key={'geo': iso2})
            attempts.append((cand, data is not None, err))
            if data:
                return data, f'pandasdmx:{cand}'
    # Fallback to REST queries
    for cand in candidates:
        url = f'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{cand}?geo={iso2}&format=JSON'
        data, err = fetch_http(url, timeout=60)
        attempts.append((f'http:{cand}', data is not None, err))
        if data:
            return data, f'http:{cand}'
    print('  Eurostat attempts:', attempts)
    return None, 'no eurostat payload'


def fetch_oecd_with_pandasdmx(iso3):
    if Request is not None:
        # OECD flow often 'GOV_DEBT' or check dataflow list; try GOV_DEBT
        data, err = try_pandasdmx_fetch('OECD', 'GOV_DEBT', key={None: iso3})
        if data:
            return data, 'pandasdmx:GOV_DEBT'
        # try empty key (some providers accept parameters in params)
        data, err = try_pandasdmx_fetch('OECD', 'GOV_DEBT')
        if data:
            return data, 'pandasdmx:GOV_DEBT_all'
    # Fallback to REST endpoint used previously
    url = f'https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/GOV_DEBT/{iso3}/all?format=compact_v2'
    data, err = fetch_http(url, timeout=60)
    if data:
        return data, 'http:GOV_DEBT'
    return None, err


def fetch_imf_gov_debt(iso3):
    if Request is not None:
        # Try IMF dataflows commonly 'GFS' or 'IFS' depending on series; attempt 'GFS'
        data, err = try_pandasdmx_fetch('IMF', 'GFS', key={'COUNTRY': iso3})
        if data:
            return data, 'pandasdmx:GFS'
    # No generic IMF REST here — leave for manual retrieval
    return None, 'no_imf_fallback'


def main():
    print('Robust fetch: pandasdmx + HTTP fallbacks')
    for iso3, codes in COUNTRIES.items():
        print('--', iso3)
        iso2 = codes['iso2']
        try:
            # Eurostat
            data, source = fetch_eurostat_with_variants(iso2)
            if data:
                out = MACRO_DIR / f'euro_gov_10_gdp_{iso3}.json'
                save(out, data)
                print('  eurostat source=', source)
            else:
                print('  Eurostat fetch failed:', source)

            time.sleep(1)

            # OECD
            data, source = fetch_oecd_with_pandasdmx(iso3)
            if data:
                out = MACRO_DIR / f'oecd_gov_debt_{iso3}.xml'
                save(out, data)
                print('  oecd source=', source)
            else:
                print('  OECD fetch failed:', source)

            time.sleep(1)

            # IMF (best-effort): save if found
            data, source = fetch_imf_gov_debt(iso3)
            if data:
                out = MACRO_DIR / f'imf_gov_debt_{iso3}.xml'
                save(out, data)
                print('  imf source=', source)
            else:
                print('  IMF fetch not available or failed:', source)

        except Exception:
            print('  Unexpected error for', iso3)
            traceback.print_exc()

    print('\nFetch attempts complete. Check files in data_repository/raw/macro/')

if __name__ == '__main__':
    main()
