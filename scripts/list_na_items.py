#!/usr/bin/env python3
import json
from pathlib import Path
BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
files = sorted(MACRO.glob('euro_gov_10_gdp_*.json'))
for p in files:
    raw = json.loads(p.read_text(encoding='utf-8'))
    print('\nFile:', p.name)
    dim_order = raw.get('id') or list(raw['dimension'].keys())
    na_cat = raw['dimension']['na_item']['category']
    idx = na_cat.get('index', {})
    lab = na_cat.get('label', {})
    # print mapping code->position and label
    for code,pos in idx.items():
        label = lab.get(code)
        print(' ', code, '-> pos=', pos, 'label=', label)
