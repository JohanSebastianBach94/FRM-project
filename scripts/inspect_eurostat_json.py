#!/usr/bin/env python3
import json
from pathlib import Path
p=Path('data_repository/raw/macro/euro_gov_10_gdp_DEU.json')
raw=json.loads(p.read_text(encoding='utf-8'))
print('dimension keys:', list(raw['dimension'].keys()))
for dname,dval in raw['dimension'].items():
    print('\n--',dname,'--')
    cat = dval.get('category')
    if not cat:
        continue
    idx = cat.get('index')
    lab = cat.get('label')
    sample = list(idx.items())[:30]
    print(' sample index->label:')
    for k,v in sample:
        lbl = lab.get(k) if lab else None
        print('  ',k,'->',v,'label=',lbl)

# print a few value items
vals = raw.get('value',{})
print('\nvalue sample (first 40):')
for i,(k,v) in enumerate(list(vals.items())[:40]):
    print(' ',k,'->',v)
