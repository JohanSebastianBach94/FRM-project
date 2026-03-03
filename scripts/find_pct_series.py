#!/usr/bin/env python3
"""Find candidate percent-of-GDP series (unit == 'PC_GDP') for sector S13.
Print candidate na_item codes and median pct over 1995-2022 to help pick gross debt series.
"""
import json
from pathlib import Path
import math
BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'

files = sorted(MACRO.glob('euro_gov_10_gdp_*.json'))

for p in files:
    raw = json.loads(p.read_text(encoding='utf-8'))
    iso = p.name.replace('euro_gov_10_gdp_','').replace('.json','')
    dim_order = raw.get('id') or list(raw['dimension'].keys())
    sizes = raw.get('size') or [len(raw['dimension'][d]['category']['index']) for d in dim_order]
    pos_code_maps = {}
    label_maps = {}
    for d in dim_order:
        cat = raw['dimension'][d].get('category', {})
        idx_map = cat.get('index', {})
        lab_map = cat.get('label', {})
        pos_code_maps[d] = {int(v):k for k,v in idx_map.items()}
        label_maps[d] = lab_map
    vals = raw.get('value', {})
    candidates = {}
    for flat_k, v in vals.items():
        idx = int(flat_k)
        coords = [0]*len(sizes)
        tmp = idx
        for i in range(len(sizes)-1, -1, -1):
            if sizes[i] > 0:
                coords[i] = tmp % sizes[i]
                tmp = tmp // sizes[i]
        coord_map = {}
        for i,d in enumerate(dim_order):
            pos = coords[i]
            code = pos_code_maps[d].get(pos)
            coord_map[d] = code
        if coord_map.get('unit') != 'PC_GDP':
            continue
        if coord_map.get('sector') != 'S13':
            continue
        na = coord_map.get('na_item')
        time_code = coord_map.get('time')
        try:
            year = int(label_maps['time'].get(time_code, time_code))
        except Exception:
            continue
        if na not in candidates:
            candidates[na] = {}
        if v is None:
            continue
        candidates[na][year] = float(v)
    # compute median 1995-2022
    print('\nISO', iso)
    medians = []
    for na, series in candidates.items():
        pcts = [v for y,v in series.items() if 1995 <= y <= 2022 and v is not None and math.isfinite(v)]
        if pcts:
            med = sorted(pcts)[len(pcts)//2]
            medians.append((na, med, len(pcts)))
    medians = sorted(medians, key=lambda x: (-x[1], -x[2]))
    for na, med, n in medians[:20]:
        print(' ', na, 'median=', med, 'n=', n)
    if not medians:
        print('  no PC_GDP candidates')
