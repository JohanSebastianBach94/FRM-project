#!/usr/bin/env python3
import re
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
YEAR_RE = re.compile(r'(?P<year>19\d{2}|20\d{2})')
NUM_RE = re.compile(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+\.\d+')

ISOS = ['DEU','FRA','ITA','ESP','USA']


def find_year_number_pairs_in_text(text: str):
    pairs = {}
    for m in YEAR_RE.finditer(text):
        y = int(m.group('year'))
        window = text[m.end(): m.end() + 160]
        num_m = NUM_RE.search(window)
        if num_m:
            raw = num_m.group(0).replace(',', '')
            try:
                val = float(raw)
                pairs[y] = val
            except Exception:
                continue
    return pairs


def main():
    for iso in ISOS:
        p = MACRO / f'wb_NY.GDP.MKTP.CD_{iso}.json'
        if not p.exists():
            print(iso, 'WB GDP JSON missing')
            continue
        txt = p.read_text(encoding='utf-8', errors='ignore')
        pairs = find_year_number_pairs_in_text(txt)
        negatives = {y:v for y,v in pairs.items() if v <= 0}
        print('\nISO:', iso)
        if not pairs:
            print('  No year:value pairs found in WB GDP JSON')
            continue
        print(f'  Years found: {min(pairs.keys())}-{max(pairs.keys())} count={len(pairs)}')
        if negatives:
            print('  Non-positive GDP years:')
            for y in sorted(negatives):
                print(f'    {y}: {negatives[y]}')
        else:
            print('  No non-positive GDP values found')

if __name__ == "__main__":
    main()
