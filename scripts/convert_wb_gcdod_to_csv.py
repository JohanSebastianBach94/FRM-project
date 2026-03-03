#!/usr/bin/env python3
"""Convert World Bank WDI GC.DOD.TOTL.GD.ZS JSON payloads into canonical CSVs.
Reads files named `wb_GC.DOD.TOTL.GD.ZS_{ISO}.json` under `data_repository/raw/macro`.
Writes `general_government_gross_debt_pct_gdp_{ISO}.csv` with columns year,debt_pct,qc_flag
and a `.meta.json` sidecar recording provenance.
"""
import json
from pathlib import Path
import pandas as pd
import math

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'


def extract_wb_series(p):
    raw = json.loads(p.read_text(encoding='utf-8'))
    # standard WB WDI response is [metadata, [ { 'date': '2023', 'value': 123 }, ... ]]
    if isinstance(raw, list) and len(raw) >= 2 and isinstance(raw[1], list):
        entries = raw[1]
        data = {}
        for e in entries:
            d = e.get('date')
            v = e.get('value')
            if d is None:
                continue
            try:
                y = int(d)
            except Exception:
                continue
            if v is None:
                continue
            try:
                data[y] = float(v)
            except Exception:
                continue
        return data
    # fallback: try to parse simple year:value pairs
    txt = p.read_text(encoding='utf-8', errors='ignore')
    import re
    YEAR_RE = re.compile(r'(?P<year>19\d{2}|20\d{2})')
    NUM_RE = re.compile(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+\.\d+')
    pairs = {}
    for m in YEAR_RE.finditer(txt):
        y = int(m.group('year'))
        window = txt[m.end(): m.end()+160]
        num_m = NUM_RE.search(window)
        if num_m:
            rawn = num_m.group(0).replace(',','')
            try:
                pairs[y] = float(rawn)
            except Exception:
                continue
    return pairs


def main():
    files = sorted(MACRO.glob('wb_GC.DOD.TOTL.GD.ZS_*.json'))
    if not files:
        print('No WB GC.DOD WDI files found in', MACRO)
        return
    for p in files:
        iso = p.name.split('_')[-1].replace('.json','')
        print('Processing', iso)
        series = extract_wb_series(p)
        if not series:
            print('  no data in', p.name)
            continue
        rows = []
        for y in sorted(series.keys()):
            rows.append({'year': int(y), 'debt_pct': series[y], 'qc_flag': None})
        out_csv = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.csv'
        pd.DataFrame(rows).to_csv(out_csv, index=False)
        meta = {
            'iso': iso,
            'source_file': p.name,
            'provenance': 'worldbank_wdi_GC.DOD.TOTL.GD.ZS'
        }
        out_meta = out_csv.with_suffix('.meta.json')
        out_meta.write_text(json.dumps(meta, indent=2))
        print('  wrote', out_csv.name)

if __name__ == '__main__':
    main()
