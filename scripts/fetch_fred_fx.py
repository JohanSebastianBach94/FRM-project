#!/usr/bin/env python3
"""Fetch FX indices / cross rates from FRED.

Usage:
  - Set environment variable `FRED_API_KEY` with your API key.
  - Run: python scripts/fetch_fred_fx.py

What it does:
 - For each target name it runs a FRED series search to find candidate series.
 - Picks the top candidate and downloads observations from 1990-01-01 to today.
 - Saves canonical CSVs under `data_repository/raw/fred/{series_id}.csv` and
   writes a small `{series_id}.meta.json` with provenance.
 - Writes `outputs/fred_fetch_report.csv` summarizing actions.

Note: This script uses only the public FRED API and requires your API key.
"""
import os
import sys
import urllib.request
import urllib.parse
import json
import csv
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'data_repository' / 'raw' / 'fred'
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_REPORT = ROOT / 'outputs' / 'fred_fetch_report.csv'
OUT_REPORT.parent.mkdir(exist_ok=True)

FRED_API_KEY = os.environ.get('FRED_API_KEY')
if not FRED_API_KEY:
    print('FRED_API_KEY environment variable not set. Please set it and re-run.')
    sys.exit(2)

FRED_BASE = 'https://api.stlouisfed.org/fred'

TARGETS = {
    'DXY': ['dxy', 'us dollar index', 'index of dollars'],
    'USD_BROAD_INDEX': ['trade weighted dollar broad', 'trade weighted u.s. dollar index broad'],
    'USD_JPY': ['usd jpy', 'usd/jpy', 'usd to jpy'],
    'GBP_USD': ['gbp usd', 'gbpusd', 'gbp/usd'],
    'USD_CNY': ['usd cny', 'usdcny', 'usd/cny'],
}

SEARCH_URL = FRED_BASE + '/series/search?search_text={q}&api_key={key}&file_type=json'
SERIES_OBS_URL = FRED_BASE + '/series/observations?series_id={sid}&api_key={key}&file_type=json&observation_start={start}'


def fetch_url(url, timeout=30):
    req = urllib.request.Request(url, headers={'User-Agent': 'FRM-fetch/1.0'})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode('utf-8')


def search_series(query):
    q = urllib.parse.quote(query)
    url = SEARCH_URL.format(q=q, key=FRED_API_KEY)
    try:
        txt = fetch_url(url)
        payload = json.loads(txt)
        results = payload.get('seriess', []) or payload.get('seriess', [])
        # payload typically: {'count':.., 'seriess': [...]}
        return payload
    except Exception as e:
        print('Search failed for', query, e)
        return None


def pick_top_series(search_payload):
    if not search_payload:
        return None
    # The FRED search response uses 'seriess' (yes plural) - take first
    series_list = search_payload.get('seriess') or search_payload.get('seriess') or []
    if not series_list and 'seriess' in search_payload:
        series_list = search_payload['seriess']
    if not series_list and isinstance(search_payload.get('seriess'), list):
        series_list = search_payload.get('seriess')
    if isinstance(series_list, list) and len(series_list) > 0:
        # each item is a dict with 'id'
        return series_list[0]
    # Some responses embed differently; try 'seriess' singular
    if 'seriess' in search_payload and isinstance(search_payload['seriess'], dict):
        return search_payload['seriess']
    return None


def fetch_observations(series_id, start='1990-01-01'):
    url = SERIES_OBS_URL.format(sid=series_id, key=FRED_API_KEY, start=start)
    try:
        txt = fetch_url(url, timeout=60)
        payload = json.loads(txt)
        obs = payload.get('observations', [])
        return obs
    except Exception as e:
        print('Failed to fetch observations for', series_id, e)
        return None


def write_csv(series_id, series_title, observations):
    out_csv = OUT_DIR / f'{series_id}.csv'
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'value'])
        for o in observations:
            date = o.get('date')
            val = o.get('value')
            if val is None or val == '.':
                continue
            writer.writerow([date, val])
    # write meta
    meta = {
        'series_id': series_id,
        'title': series_title,
        'source': 'FRED',
        'fetched_at': datetime.utcnow().isoformat(),
        'observation_count': len(observations),
        'note': 'Fetched via FRED API search + observations. Start=1990-01-01'
    }
    with open(OUT_DIR / f'{series_id}.meta.json', 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)
    return str(out_csv.relative_to(ROOT))


def main():
    report_rows = []
    for key, keywords in TARGETS.items():
        print('\nSearching for target:', key)
        chosen = None
        chosen_title = None
        chosen_id = None
        for kw in keywords:
            print('  trying keyword:', kw)
            payload = search_series(kw)
            if not payload:
                continue
            top = pick_top_series(payload)
            if top and top.get('id'):
                chosen = top
                chosen_id = top.get('id')
                chosen_title = top.get('title') or top.get('title', '')
                print('   -> picked', chosen_id, chosen_title)
                break
        if not chosen:
            print('  No candidate found for', key)
            report_rows.append((key, '', '', 'not_found'))
            continue
        # fetch observations
        obs = fetch_observations(chosen_id, start='1990-01-01')
        if not obs:
            print('  No observations for', chosen_id)
            report_rows.append((key, chosen_id, chosen_title, 'no_obs'))
            continue
        # write csv + meta
        relpath = write_csv(chosen_id, chosen_title, obs)
        print('  saved to', relpath)
        report_rows.append((key, chosen_id, chosen_title, relpath))

    # write report
    with open(OUT_REPORT, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['target','fred_series_id','title','local_path_or_status'])
        for r in report_rows:
            w.writerow(r)
    print('\nReport written to', OUT_REPORT)

if __name__ == '__main__':
    main()
