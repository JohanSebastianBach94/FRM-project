#!/usr/bin/env python3
"""Inspect coverage and basic health of FRED CSVs under data_repository/raw/fred

Writes: outputs/fred_health_report.csv (summary per series)
        outputs/fred_health_summary.txt (human-readable)
"""
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_DIR = ROOT / 'data_repository' / 'raw' / 'fred'
OUT_DIR = ROOT / 'outputs'
OUT_DIR.mkdir(parents=True, exist_ok=True)

rows = []

files = sorted(IN_DIR.glob('*.csv'))
if not files:
    print('No FRED CSVs found in', IN_DIR)
    sys.exit(2)

for p in files:
    sid = p.stem
    try:
        # try to read with first column as date
        df = pd.read_csv(p, parse_dates=True)
        # locate date column: common names
        date_cols = [c for c in df.columns if c.lower() in ('date','observation_date','time')]
        if date_cols:
            df.set_index(pd.to_datetime(df[date_cols[0]]), inplace=True)
            # drop the date column if it's duplicated
            if date_cols[0] in df.columns:
                df = df.drop(columns=[date_cols[0]] )
        else:
            # assume first column is date-like
            df.set_index(pd.to_datetime(df.iloc[:,0]), inplace=True)
            df = df.iloc[:,1:]

        # coerce numeric values to a single series if many cols
        if df.shape[1] > 1:
            # try find common value column names
            for n in ('value','observations','obs','series'):
                if n in [c.lower() for c in df.columns]:
                    col = [c for c in df.columns if c.lower()==n][0]
                    s = pd.to_numeric(df[col], errors='coerce')
                    break
            else:
                # fallback: take first numeric column
                s = pd.to_numeric(df.iloc[:,0], errors='coerce')
        else:
            s = pd.to_numeric(df.iloc[:,0], errors='coerce')

        s = s.sort_index()
        n_obs = len(s)
        n_missing = int(s.isna().sum())
        pct_missing = float(n_missing)/n_obs if n_obs else 0.0
        start = s.index.min().date() if n_obs else None
        end = s.index.max().date() if n_obs else None
        try:
            inferred = pd.infer_freq(s.index)
        except Exception:
            inferred = None
        # compute max gap in days between consecutive valid observations
        idx = s.dropna().index
        max_gap_days = None
        large_gaps = 0
        if len(idx) >= 2:
            diffs = idx.to_series().diff().dt.days.dropna()
            max_gap_days = int(diffs.max())
            large_gaps = int((diffs > 90).sum())

        rows.append({
            'series_id': sid,
            'file': str(p.relative_to(ROOT)),
            'start': start,
            'end': end,
            'n_obs': n_obs,
            'n_missing': n_missing,
            'pct_missing': round(pct_missing, 4),
            'inferred_freq': inferred,
            'max_gap_days': max_gap_days,
            'large_gaps_gt_90d': large_gaps,
        })

    except Exception as e:
        rows.append({
            'series_id': sid,
            'file': str(p.relative_to(ROOT)),
            'error': str(e),
        })

out_csv = OUT_DIR / 'fred_health_report.csv'
pd.DataFrame(rows).to_csv(out_csv, index=False)

with open(OUT_DIR / 'fred_health_summary.txt', 'w', encoding='utf-8') as fh:
    fh.write('FRED health check summary\n')
    fh.write('Files scanned: %d\n\n' % len(files))
    for r in rows:
        fh.write('Series: %s\n' % r.get('series_id'))
        fh.write('  file: %s\n' % r.get('file'))
        if 'error' in r:
            fh.write('  ERROR: %s\n\n' % r['error'])
            continue
        fh.write('  observations: %s (missing %s, pct_missing=%s)\n' % (r.get('n_obs'), r.get('n_missing'), r.get('pct_missing')))
        fh.write('  start: %s  end: %s\n' % (r.get('start'), r.get('end')))
        fh.write('  inferred_freq: %s  max_gap_days: %s  large_gaps_gt_90d: %s\n' % (r.get('inferred_freq'), r.get('max_gap_days'), r.get('large_gaps_gt_90d')))
        fh.write('\n')

print('Wrote', out_csv, 'and fred_health_summary.txt')
