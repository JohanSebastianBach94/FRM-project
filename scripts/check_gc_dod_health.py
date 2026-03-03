#!/usr/bin/env python3
"""
Check health of general government gross debt level and pct-of-GDP CSVs.
Produces a concise report per ISO found in `data_repository/raw/macro/`.
"""
import csv
import math
from pathlib import Path
from statistics import mean, stdev, median
import json
import re

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'

YEAR_RE = re.compile(r'(?P<year>19\d{2}|20\d{2})')
NUM_RE = re.compile(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+\.\d+')


def load_csv_series(path):
    vals = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            hdr = next(reader, None)
            for row in reader:
                if not row:
                    continue
                try:
                    y = int(row[0])
                    v = float(row[1])
                    vals[y] = v
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return vals


def find_year_number_pairs_in_text(text: str):
    pairs = {}
    for m in YEAR_RE.finditer(text):
        y = int(m.group('year'))
        window = text[m.end(): m.end() + 120]
        num_m = NUM_RE.search(window)
        if num_m:
            raw = num_m.group(0).replace(',', '')
            try:
                val = float(raw)
                pairs[y] = val
            except Exception:
                continue
    return pairs


def load_wb_gdp(iso):
    p = MACRO / f'wb_NY.GDP.MKTP.CD_{iso}.json'
    if not p.exists():
        return None
    txt = p.read_text(encoding='utf-8', errors='ignore')
    return find_year_number_pairs_in_text(txt)


def report_iso(iso):
    out = []
    level_p = MACRO / f'general_government_gross_debt_level_{iso}.csv'
    pct_p = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.csv'
    level = load_csv_series(level_p)
    pct = load_csv_series(pct_p)
    out.append(f'ISO: {iso}')
    out.append(f'  level file: {level_p.name} rows={len(level)}')
    out.append(f'  pct file:   {pct_p.name} rows={len(pct)}')

    def span_stats(d):
        if not d:
            return 'none'
        years = sorted(d.keys())
        gaps = [b - a for a, b in zip(years, years[1:])] if len(years) > 1 else []
        max_gap = max(gaps) if gaps else 0
        miss_between = sum(1 for y in range(years[0], years[-1] + 1) if y not in d)
        return f'first={years[0]} last={years[-1]} count={len(years)} missing_between={miss_between} max_gap={max_gap}'

    out.append('  level span: ' + span_stats(level))
    out.append('  pct span:   ' + span_stats(pct))

    # Basic value stats
    def val_stats(d):
        if not d:
            return 'none'
        vals = list(d.values())
        try:
            mn = min(vals); mx = max(vals); av = mean(vals)
            sd = stdev(vals) if len(vals) > 1 else 0.0
            return f'min={mn:.3g} max={mx:.3g} mean={av:.3g} sd={sd:.3g}'
        except Exception:
            return 'error'

    out.append('  level stats: ' + val_stats(level))
    out.append('  pct stats:   ' + val_stats(pct))

    # Overlap checks
    overlap_years = sorted(set(level.keys()).intersection(pct.keys()))
    out.append(f'  overlap years: {len(overlap_years)}')

    # If WB GDP present, compute expected level = pct/100 * gdp and compare
    gdp = load_wb_gdp(iso)
    if gdp:
        overlaps_with_gdp = sorted(set(pct.keys()).intersection(gdp.keys()))
        diffs = []
        for y in overlaps_with_gdp:
            g = gdp[y]
            p = pct[y]
            expected = p / 100.0 * g
            # find level if exists for year y
            l = level.get(y)
            if l is not None and g != 0:
                rel = abs(l - expected) / (abs(expected) + 1e-12)
                diffs.append(rel)
        if diffs:
            out.append(f'  GDP overlap years: {len(overlaps_with_gdp)}; level vs pct-derived mean_rel_error={mean(diffs):.3g} median={median(diffs):.3g} max={max(diffs):.3g}')
        else:
            out.append('  GDP present but no overlapping years with pct and level')
    else:
        out.append('  No WB GDP JSON found for ISO -> cannot validate pct->level consistency against WB GDP')

    # Implied GDP from level and pct where both exist
    implied_gdps = []
    for y in overlap_years:
        l = level.get(y)
        p = pct.get(y)
        if p is None or p == 0:
            continue
        implied = l / (p / 100.0)
        implied_gdps.append(implied)
    if implied_gdps:
        out.append(f'  implied GDP stats from level/pct: median={median(implied_gdps):.3g} mean={mean(implied_gdps):.3g} sd={stdev(implied_gdps) if len(implied_gdps)>1 else 0.0:.3g}')
    else:
        out.append('  No implied GDP could be calculated (no overlapping non-zero pct entries)')

    # Detect outliers in pct series (negative values, excessively large >1000%)
    outliers = [ (y,v) for y,v in pct.items() if (v < 0 or abs(v) > 1000) ]
    out.append(f'  pct outliers (neg or >1000%): {len(outliers)}')

    return '\n'.join(out)


def main():
    files = list(MACRO.glob('general_government_gross_debt_level_*.csv'))
    isos = [p.name.replace('general_government_gross_debt_level_','').replace('.csv','') for p in files]
    # also include any isos that only have pct files
    pct_files = list(MACRO.glob('general_government_gross_debt_pct_gdp_*.csv'))
    isos += [p.name.replace('general_government_gross_debt_pct_gdp_','').replace('.csv','') for p in pct_files]
    isos = sorted(set(isos))
    if not isos:
        print('No GC.DOD files found under', MACRO)
        return
    reports = []
    for iso in isos:
        reports.append(report_iso(iso))
    print('\n\n'.join(reports))

if __name__ == '__main__':
    main()
