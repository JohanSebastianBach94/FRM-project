#!/usr/bin/env python3
import csv
from collections import Counter
from pathlib import Path

P = Path('analysis_outputs') / 'bis_download_diagnostics.csv'
if not P.exists():
    print('No diagnostics file at', P)
    raise SystemExit(1)

rows = []
with P.open(newline='', encoding='utf-8') as f:
    r = csv.DictReader(f)
    for row in r:
        rows.append(row)

print('Total diagnostics rows:', len(rows))
cnt = Counter()
out_count = 0
obs_positive = 0
no_flow = 0
for row in rows:
    msg = (row.get('parsed_msg') or '').strip()
    cnt[msg] += 1
    if (row.get('out_csv') or '').strip():
        out_count += 1
    try:
        if int(row.get('obs_count') or 0) > 0:
            obs_positive += 1
    except Exception:
        pass
    if msg.startswith('no-flow') or msg == 'no-flow-or-key':
        no_flow += 1

print('\nparsed_msg counts:')
for k, v in cnt.most_common():
    print(' ', repr(k), ':', v)

print('\nrows with out_csv:', out_count)
print('rows with obs_count>0:', obs_positive)
print('rows no-flow-or-key:', no_flow)

print('\nSample diagnostics (first 10 rows):')
for r in rows[:10]:
    print(r)
