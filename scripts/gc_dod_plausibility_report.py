#!/usr/bin/env python3
"""
Produce a plausibility report for newly computed GC.DOD pct CSVs and write a markdown report.
Also emit a small JSON summary per ISO for easy inclusion in config.
"""
import csv
from pathlib import Path
import json
import statistics

MACRO = Path(__file__).resolve().parents[1] / 'data_repository' / 'raw' / 'macro'
OUT = MACRO / 'gc_dod_plausibility_report.md'
SUMMARY = MACRO / 'gc_dod_plausibility_summary.json'
ISOS = ['DEU','FRA','ITA','ESP','USA']

report_lines = ["# GC.DOD Plausibility Report\n"]
summaries = {}

for iso in ISOS:
    csvp = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.csv'
    meta = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.meta.json'
    report_lines.append(f'## {iso}\n')
    if not csvp.exists():
        report_lines.append(f'- File missing: `{csvp.name}`\n')
        summaries[iso] = {'status':'missing'}
        continue
    rows = []
    qc_counts = {}
    values = []
    with open(csvp, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
            qc = r.get('qc_flag') or ''
            qc_counts[qc] = qc_counts.get(qc,0) + 1
            v = r.get('debt_pct')
            try:
                if v is not None and v != '':
                    val = float(v)
                    values.append(val)
            except Exception:
                pass
    count = len(rows)
    first = None
    last = None
    if count:
        # try to infer first/last from 'year' or 'period'
        keys = [r.get('year') or r.get('period') for r in rows]
        keys = [k for k in keys if k]
        if keys:
            first = keys[0]
            last = keys[-1]
    report_lines.append(f'- Rows: {count}\n')
    report_lines.append(f'- Range: {first} — {last}\n')
    report_lines.append(f'- QC flags: {json.dumps(qc_counts)}\n')
    if values:
        report_lines.append(f'- debt_pct stats: mean={statistics.mean(values):.3g}, median={statistics.median(values):.3g}, min={min(values):.3g}, max={max(values):.3g}\n')
    else:
        report_lines.append('- debt_pct stats: none\n')
    report_lines.append('\n')
    summaries[iso] = {
        'rows': count,
        'range_first': first,
        'range_last': last,
        'qc_counts': qc_counts,
        'pct_stats': {
            'mean': statistics.mean(values) if values else None,
            'median': statistics.median(values) if values else None,
            'min': min(values) if values else None,
            'max': max(values) if values else None,
        }
    }

OUT.write_text('\n'.join(report_lines), encoding='utf-8')
SUMMARY.write_text(json.dumps(summaries, indent=2), encoding='utf-8')
print('Wrote report to', OUT)
print('Wrote summary to', SUMMARY)
