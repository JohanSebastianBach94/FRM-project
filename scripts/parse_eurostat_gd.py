#!/usr/bin/env python3
"""Extract Government Consolidated Gross Debt (GD) from euro_gov_10dd_edpt1_*.json files.
Filters: unit == 'MIO_EUR', sector == 'S13', na_item in ['GD','GD_F2','GD_F3','GD_F4'].
Writes level CSVs in currency units and .meta.json sidecars.
"""
import json
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'


def invert_index_map(idx_map):
    inv = {}
    for k, v in idx_map.items():
        try:
            inv[int(v)] = k
        except Exception:
            inv[int(v)] = k
    return inv


def decode_flat_index(flat_idx, sizes):
    idx = int(flat_idx)
    coords = [0] * len(sizes)
    for i in range(len(sizes)-1, -1, -1):
        if sizes[i] <= 0:
            coords[i] = 0
        else:
            coords[i] = idx % sizes[i]
            idx = idx // sizes[i]
    return coords


def process_file(p):
    raw = json.loads(p.read_text(encoding='utf-8'))
    iso = p.name.split('_')[-1].replace('.json','')
    print('Processing', p.name, '->', iso)
    dim_order = raw.get('id') or list(raw['dimension'].keys())
    sizes = raw.get('size') or [len(raw['dimension'][d]['category']['index']) for d in dim_order]

    pos_code_maps = {}
    label_maps = {}
    for d in dim_order:
        cat = raw['dimension'][d].get('category', {})
        idx_map = cat.get('index', {})
        lab_map = cat.get('label', {})
        pos_code_maps[d] = invert_index_map(idx_map)
        label_maps[d] = lab_map

    vals = raw.get('value', {})
    series = {}
    gd_codes = set(['GD','GD_F2','GD_F3','GD_F4','GD_F31','GD_F32','GD_F41','GD_F42'])
    found_codes = set()
    for flat_k, v in vals.items():
        coords = decode_flat_index(flat_k, sizes)
        coord_map = {}
        for i, d in enumerate(dim_order):
            pos = coords[i]
            code = pos_code_maps[d].get(pos)
            coord_map[d] = code
        if coord_map.get('unit') != 'MIO_EUR':
            continue
        if coord_map.get('sector') != 'S13':
            continue
        na = coord_map.get('na_item')
        if na not in gd_codes:
            continue
        # year label
        time_code = coord_map.get('time')
        try:
            year = int(label_maps['time'].get(time_code, time_code))
        except Exception:
            continue
        if v is None:
            continue
        # convert million->units
        series.setdefault(na, {})[year] = float(v) * 1_000_000.0
        found_codes.add(na)

    if not series:
        print('  no gross-debt entries found in', p.name)
        return

    # prefer 'GD' if present
    chosen = None
    if 'GD' in series:
        chosen = 'GD'
    else:
        # pick GD_F2/GD_F3/GD_F4 in that precedence
        for c in ['GD_F2','GD_F3','GD_F4','GD_F31','GD_F32','GD_F41','GD_F42']:
            if c in series:
                chosen = c
                break
    if not chosen:
        chosen = sorted(series.keys())[0]
    out_series = series[chosen]
    rows = [{'year':int(y), 'debt_level': out_series[y]} for y in sorted(out_series.keys())]
    out_csv = MACRO / f'general_government_gross_debt_level_{iso}.csv'
    pd.DataFrame(rows).to_csv(out_csv, index=False)
    meta = {
        'iso': iso,
        'source_file': p.name,
        'selected_na_item': chosen,
        'note': 'Filtered unit=MIO_EUR sector=S13; values converted MIO_EUR->units (x1e6)'
    }
    out_meta = out_csv.with_suffix('.meta.json')
    out_meta.write_text(json.dumps(meta, indent=2))
    print('  wrote', out_csv.name, 'selected_na_item=', chosen)


def main():
    files = sorted(MACRO.glob('euro_gov_10dd_edpt1_*.json'))
    if not files:
        print('No euro_gov_10dd_edpt1_*.json files')
        return
    for p in files:
        try:
            process_file(p)
        except Exception as e:
            print('Error', p.name, e)

if __name__ == '__main__':
    main()
