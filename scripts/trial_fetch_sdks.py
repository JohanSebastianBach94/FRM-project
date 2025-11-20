#!/usr/bin/env python3
"""Trial fetcher for public APIs: ECB SDW and IMF SDMX (graceful, logs outputs).

This script attempts to download example series and save responses to data/raw/.
It is intentionally robust: it will not crash if an endpoint returns an error.
"""
import os
import urllib.request
import urllib.error
import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
OUT_DIR = os.path.join(BASE_DIR, 'data', 'raw')
os.makedirs(OUT_DIR, exist_ok=True)

SAMPLES = [
    {
        'name': 'ecb_bsi_sample',
        'url': 'https://sdw-wsrest.ecb.europa.eu/service/data/BSI/M.U2.N.4.A..+X._T.A?detail=dataonly&startPeriod=2019-01&endPeriod=2020-12',
        'note': 'ECB BSI example (MFI balance sheet). Query may need tuning per country/consolidation.'
    },
    {
        'name': 'imf_ifs_sample',
        'url': 'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/USA.PCPI?startPeriod=2019&endPeriod=2020',
        'note': 'IMF IFS example (consumer prices) — SDMX CompactData JSON endpoint.'
    }
]

def fetch_to_file(entry):
    name = entry['name']
    url = entry['url']
    outpath = os.path.join(OUT_DIR, f'{name}.json')
    print(f'Fetching {name} from {url} ...')
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'FRM-trial/1.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
        with open(outpath, 'wb') as f:
            f.write(data)
        print(f'  -> Saved to {outpath} ({len(data)} bytes)')
        return True, outpath
    except urllib.error.HTTPError as e:
        print(f'  HTTPError {e.code} for {name}: {e.reason}')
        return False, str(e)
    except urllib.error.URLError as e:
        print(f'  URLError for {name}: {e}')
        return False, str(e)
    except Exception as e:
        print(f'  Unexpected error for {name}: {e}')
        return False, str(e)

def main():
    print('Trial SDK fetch started at', datetime.datetime.utcnow().isoformat())
    results = []
    for s in SAMPLES:
        ok, info = fetch_to_file(s)
        results.append((s['name'], ok, info, s.get('note')))

    print('\nSummary:')
    for name, ok, info, note in results:
        status = 'OK' if ok else 'FAIL'
        print(f' - {name}: {status} -> {info}  # {note}')

if __name__ == '__main__':
    main()
