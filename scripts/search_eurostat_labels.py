#!/usr/bin/env python3
import json
from pathlib import Path
p=Path('data_repository/raw/macro/euro_gov_10_gdp_DEU.json')
raw=json.loads(p.read_text(encoding='utf-8'))

found=False
for dname,dval in raw['dimension'].items():
    cat=dval.get('category')
    if not cat: continue
    lab=cat.get('label',{})
    for k,v in lab.items():
        if v and any(x in v.lower() for x in ('debt','gross','general government')):
            print(dname, k, v)
            found=True

if not found:
    print('No matching labels found')
