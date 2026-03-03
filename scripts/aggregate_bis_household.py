#!/usr/bin/env python3
"""Aggregate per-country BIS household files into unique period series.

Writes files alongside originals with suffix `_agg.csv` containing columns:
period,value

Usage:
  python scripts/aggregate_bis_household.py
"""
from pathlib import Path
import pandas as pd
import re

BASE = Path(__file__).resolve().parents[1]
PROC = BASE / 'data_repository' / 'processed'
files = list(PROC.glob('bis_lbs_household_*.csv'))
exclude = ['bis_lbs_household_matches.csv']
for f in files:
    if f.name in exclude:
        continue
    if f.name.endswith('_agg.csv'):
        continue
    # skip the aggregated output itself
    if f.name.endswith('_agg.csv'):
        continue
    # read
    try:
        df = pd.read_csv(f, dtype=str)
    except Exception as e:
        print('skip', f, 'read error', e)
        continue
    # expect columns 'period' and 'value' (some files include a leading reporting country column)
    cols = [c.lower() for c in df.columns]
    if 'period' not in cols or 'value' not in cols:
        # try heuristics: last two columns may be period,value
        if len(df.columns) >= 2:
            period_col = df.columns[-2]
            value_col = df.columns[-1]
        else:
            print('skip', f, 'unknown schema')
            continue
    else:
        period_col = df.columns[cols.index('period')]
        value_col = df.columns[cols.index('value')]
    df2 = df[[period_col, value_col]].copy()
    df2.columns = ['period', 'value']
    # coerce numeric
    df2['value'] = pd.to_numeric(df2['value'].astype(str).str.replace(',', ''), errors='coerce')
    df2 = df2.dropna(subset=['value'])
    agg = df2.groupby('period', as_index=False)['value'].sum()
    out = f.with_name(f.stem + '_agg.csv')
    agg = agg.sort_values('period')
    agg.to_csv(out, index=False)
    print('wrote', out, 'rows', len(agg))
print('done')
