#!/usr/bin/env python3
"""Parse Eurostat SDMX-style JSON payloads saved under
`data_repository/raw/macro/euro_gov_10_gdp_{ISO}.json`.

This script decodes the flat `value` index into per-dimension coordinates
(using `raw['id']` and `raw['size']`) and extracts nominal debt levels
for `unit=='MIO_EUR'` and `sector=='S13'`.

It enumerates candidate `na_item` codes and picks the most plausible one by
comparing implied debt/GDP with World Bank GDP (`wb_NY.GDP.MKTP.CD_{ISO}.json`),
selecting the `na_item` whose median debt/GDP (2000-2020) falls in [10,200] percent
(or the closest candidate if none match exactly).

Output for each ISO:
- `data_repository/raw/macro/general_government_gross_debt_level_{ISO}.csv` (columns: year,debt_level)
- `.../.meta.json` with provenance and chosen na_item

"""
import json
from pathlib import Path
import math
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
MACRO.mkdir(parents=True, exist_ok=True)


def invert_index_map(idx_map):
    # idx_map: code -> position (int)
    # return pos -> code
    inv = {}
    for k, v in idx_map.items():
        try:
            inv[int(v)] = k
        except Exception:
            # if positions not ints, try convert
            inv[int(v)] = k
    return inv


def decode_flat_index(flat_idx, sizes):
    # sizes: list of ints for each dimension in order
    idx = int(flat_idx)
    coords = [0] * len(sizes)
    for i in range(len(sizes)-1, -1, -1):
        if sizes[i] <= 0:
            coords[i] = 0
        else:
            coords[i] = idx % sizes[i]
            idx = idx // sizes[i]
    return coords


def load_wb_annual_gdp(iso):
    p = MACRO / f'wb_NY.GDP.MKTP.CD_{iso}.json'
    if not p.exists():
        return {}
    raw = json.loads(p.read_text(encoding='utf-8'))
    # try standard WB structure
    if isinstance(raw, list) and len(raw) >= 2 and isinstance(raw[1], list):
        entries = raw[1]
        data = {int(e['date']): float(e['value']) for e in entries if 'date' in e and e.get('value') is not None}
        return data
    # fallback: try to extract year:number patterns
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


def select_best_na_item(candidates, wb_gdp):
    # candidates: dict na_item_code -> {year: value_in_currency_units}
    # wb_gdp: dict year->value (WB current USD)
    # compute median pct over 2000-2020 if possible; choose one in [10,200]
    scores = []
    for code, series in candidates.items():
        pcts = []
        for y, val in series.items():
            if y < 1995 or y > 2022:
                continue
            g = wb_gdp.get(y)
            if g and g > 0 and val is not None:
                pct = (val / g) * 100.0
                if not (math.isfinite(pct)):
                    continue
                pcts.append(pct)
        if pcts:
            median = sorted(pcts)[len(pcts)//2]
            scores.append((code, median))
    if not scores:
        # fallback: pick the na_item with largest 2019 value
        vals = [(code, series.get(2019, 0)) for code, series in candidates.items()]
        vals = sorted(vals, key=lambda x: (x[1] if x[1] is not None else 0), reverse=True)
        return vals[0][0] if vals else None
    # prefer median in 10..200
    within = [s for s in scores if 10 <= s[1] <= 200]
    if within:
        # pick median nearest to 60 (arbitrary center)
        return sorted(within, key=lambda x: abs(x[1]-60))[0][0]
    # else pick the one with median closest to 60
    return sorted(scores, key=lambda x: abs(x[1]-60))[0][0]


def process_file(p):
    raw = json.loads(p.read_text(encoding='utf-8'))
    iso = p.name.replace('euro_gov_10_gdp_','').replace('.json','')
    print('Processing', iso)
    # determine dimension order
    dim_order = raw.get('id')
    if not dim_order:
        # fallback to keys order (best-effort)
        dim_order = list(raw['dimension'].keys())
    sizes = raw.get('size') or [len(raw['dimension'][d]['category']['index']) for d in dim_order]

    # prepare pos->code maps
    pos_code_maps = {}
    label_maps = {}
    for d in dim_order:
        cat = raw['dimension'][d].get('category', {})
        idx_map = cat.get('index', {})
        lab_map = cat.get('label', {})
        pos_code_maps[d] = invert_index_map(idx_map)
        label_maps[d] = lab_map

    # collect candidates for na_item
    candidates = {}

    vals = raw.get('value', {})
    for flat_k, v in vals.items():
        coords = decode_flat_index(flat_k, sizes)
        coord_map = {}
        for i, d in enumerate(dim_order):
            pos = coords[i]
            code = pos_code_maps[d].get(pos)
            coord_map[d] = code
        # filter unit and sector
        if coord_map.get('unit') != 'MIO_EUR':
            continue
        if coord_map.get('sector') != 'S13':
            continue
        na = coord_map.get('na_item')
        time_code = coord_map.get('time')
        # time_code should be something like '1975'
        try:
            year = int(label_maps['time'].get(time_code, time_code))
        except Exception:
            try:
                year = int(time_code)
            except Exception:
                continue
        if na not in candidates:
            candidates[na] = {}
        # convert MIO_EUR -> currency units
        if v is None:
            continue
        candidates[na][year] = float(v) * 1_000_000.0

    # load WB GDP annual mapping
    wb_gdp = load_wb_annual_gdp(iso)

    chosen = select_best_na_item(candidates, wb_gdp)
    if not chosen:
        print('No na_item chosen for', iso)
        return

    series = candidates[chosen]
    # write CSV: year,debt_level (currency units)
    rows = []
    for y in sorted(series.keys()):
        rows.append({'year': int(y), 'debt_level': series[y]})
    out_csv = MACRO / f'general_government_gross_debt_level_{iso}.csv'
    pd.DataFrame(rows).to_csv(out_csv, index=False)

    meta = {
        'iso': iso,
        'source_file': p.name,
        'selected_na_item': chosen,
        'note': 'values converted from MIO_EUR -> units (multiplied by 1e6). Filtered by sector=S13 and unit=MIO_EUR.'
    }
    meta_path = out_csv.with_suffix('.meta.json')
    meta_path.write_text(json.dumps(meta, indent=2))
    print('Wrote', out_csv.name, 'selected_na_item=', chosen)


def main():
    files = sorted(MACRO.glob('euro_gov_10_gdp_*.json'))
    if not files:
        print('No euro_gov_10_gdp_*.json files found in', MACRO)
        return
    for p in files:
        try:
            process_file(p)
        except Exception as e:
            print('Error processing', p.name, e)


if __name__ == '__main__':
    main()
