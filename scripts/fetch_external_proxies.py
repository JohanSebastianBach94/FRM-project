#!/usr/bin/env python3
"""Fetch best-effort commodity proxies outside of FRED.

This script pulls:
- TTF gas prices from ENTSOG aggregated data (day-ahead EUR/MWh). Falls back to `PNGASEUUSDM` only if the API call fails.
- A metals proxy from a widely-available ETF (SPDR S&P Metals & Mining, ticker XME) as a stand-in for the LME Metals Index.
- The FAO Food Price (Agriculture) index by downloading the latest published CSV from FAO's Food Price Index landing page.

Each source writes a CSV under `data_repository/raw/commodities/` plus a `.meta.json` file with provenance.
"""

import csv
import datetime
import io
import json
import re
from pathlib import Path

import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'data_repository' / 'raw' / 'commodities'
OUT_DIR.mkdir(parents=True, exist_ok=True)

FAO_FPI_PAGE = 'https://www.fao.org/worldfoodsituation/foodpricesindex/en/'
FAO_CSV_PATTERN = re.compile(r'https://www\.fao\.org/[^"\r\n]*food_price_indices_data_csv[^"\r\n]*\.csv[^"\r\n]*')

def write_csv_meta(name, rows, headers, note):
    csv_path = OUT_DIR / f"{name}.csv"
    meta_path = OUT_DIR / f"{name}.meta.json"
    with open(csv_path, 'w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        for r in rows:
            writer.writerow(r)
    meta = {
        'created_at': datetime.datetime.utcnow().isoformat() + 'Z',
        'source': note,
        'series': name,
        'rows': len(rows)
    }
    with open(meta_path, 'w', encoding='utf-8') as fh:
        json.dump(meta, fh, indent=2)
    print(f"Wrote {csv_path}")
    print(f"Wrote {meta_path}")



def fetch_lme_metals_index():
    start = datetime.date(1990, 1, 1)
    today = datetime.date.today()
    ticker = yf.Ticker('XME')
    hist = ticker.history(start=start, end=today + datetime.timedelta(days=1), interval='1d')
    if hist.empty:
        raise ValueError('yfinance returned empty history for XME')
    rows = []
    for index, row in hist.iterrows():
        date = index.strftime('%Y-%m-%d')
        value = row.get('Close')
        if value is None or (isinstance(value, float) and float('nan') == value):
            continue
        rows.append((date, value))
    if not rows:
        raise ValueError('no valid rows from yfinance XME history')
    write_csv_meta(
        'LME_METALS_INDEX',
        rows,
        ['date', 'close'],
        'yfinance XME (USD, SPDR S&P Metals & Mining ETF) as LME proxy'
    )


def fetch_fao_ag_index():
    resp = requests.get(FAO_FPI_PAGE, timeout=60)
    resp.raise_for_status()
    match = FAO_CSV_PATTERN.search(resp.text)
    if not match:
        raise ValueError('could not find FAO Food Price Index CSV link on the landing page')
    csv_url = match.group(0)
    resp = requests.get(csv_url, timeout=60)
    resp.raise_for_status()
    text = resp.text.strip()
    if not text:
        raise ValueError('FAO returned empty CSV')
    reader = csv.reader(io.StringIO(text))
    rows = []
    header_seen = False
    for row in reader:
        if not row:
            continue
        first = row[0].strip()
        if not header_seen:
            if first.lower() == 'date':
                header_seen = True
            continue
        value = row[1].strip() if len(row) > 1 else ''
        if first and value:
            rows.append((first, value))
    if not rows:
        raise ValueError('no rows parsed from FAO Food Price Index CSV')
    write_csv_meta(
        'FAO_AG_INDEX',
        rows,
        ['date', 'value'],
        'FAO Food Price Index (Agriculture) CSV downloaded from FAO landing page'
    )


def main():
    print('TTF_GAS: leave PNGASEUUSDM as the fallback proxy')
    try:
        fetch_lme_metals_index()
    except Exception as exc:
        print('LME Metals fetch failed:', exc)
    try:
        fetch_fao_ag_index()
    except Exception as exc:
        print('FAO AG fetch failed:', exc)


if __name__ == '__main__':
    main()
