#!/usr/bin/env python3
"""Scan saved BIS raw payloads for approved BIS series codes and propose candidate file+series_key matches.

Writes `analysis_outputs/bis_discovery_candidates.csv` with columns:
  file_path, flow, bis_series_code, occurrences, sample_line
"""
import csv
from pathlib import Path

RAW_DIR = Path('data_repository/raw/bis_api')
CAT = Path('data_repository/processed/BIS_catalog.csv')
OUT = Path('analysis_outputs/bis_discovery_candidates.csv')

if not CAT.exists():
    print('Missing catalog', CAT); raise SystemExit(1)
if not RAW_DIR.exists():
    print('Missing raw dir', RAW_DIR); raise SystemExit(1)

# load approved series
approved = set()
with CAT.open(newline='', encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        if (row.get('approval') or '').strip().upper() == 'A':
            approved.add((row.get('series') or '').strip())

if not approved:
    print('No approved series found'); raise SystemExit(1)

rows = []
for p in sorted(RAW_DIR.glob('*')):
    if p.suffix.lower() not in ('.xml', '.json', '.csv'):
        continue
    try:
        txt = p.read_text(encoding='utf-8', errors='ignore')
    except Exception:
        continue
    lower = txt.lower()
    for s in approved:
        if not s:
            continue
        slow = s.lower()
        if slow in lower:
            # find a sample line
            idx = lower.find(slow)
            start = max(0, idx-80)
            sample = txt[start: start+160].replace('\n',' ')[:200]
            rows.append({'file_path': str(p), 'flow': p.name, 'bis_series_code': s, 'occurrences': lower.count(slow), 'sample_line': sample})

OUT.parent.mkdir(parents=True, exist_ok=True)
with OUT.open('w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=['file_path','flow','bis_series_code','occurrences','sample_line'])
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print('Wrote discovery candidates to', OUT, 'found', len(rows), 'matches')
