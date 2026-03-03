#!/usr/bin/env python3
"""Extend GC.DOD.TOTL.GD.ZS series to start at 1990 using local World Bank JSON and derived CSVs.

Behavior:
- Reads `data_repository/raw/macro/wb_GC.DOD.TOTL.GD.ZS_{ISO}.json` when present and
  `data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.csv`.
- Builds a year index from 1990 to current year (UTC) and fills values using:
  1) provider values (World Bank JSON / CSV) where present
  2) linear interpolation for internal gaps
  3) nearest-value forward/backward fill for leading/trailing missing years
- Marks rows where the value was imputed in an `imputed` column.
- Writes `data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}_extended.csv`.

This is conservative: no extrapolation using GDP growth is performed unless explicitly
requested. All imputed rows are flagged so downstream logic can treat them accordingly.
"""
import os
import json
from datetime import datetime
import csv

try:
    import pandas as pd
    import numpy as np
except Exception:
    raise RuntimeError('This script requires pandas and numpy to run. Install them in your environment.')

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MACRO_DIR = os.path.join(BASE_DIR, 'data_repository', 'raw', 'macro')

COUNTRIES = ['USA', 'DEU', 'FRA', 'ITA', 'ESP']

def load_wb_json(iso):
    path = os.path.join(MACRO_DIR, f'wb_GC.DOD.TOTL.GD.ZS_{iso}.json')
    if not os.path.exists(path):
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
        # WB API payload pattern: [meta, [obs, obs, ...]]
        if isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], list):
            rows = payload[1]
            d = {}
            for obs in rows:
                year = obs.get('date')
                val = obs.get('value')
                if val is None:
                    continue
                try:
                    d[int(year)] = float(val)
                except Exception:
                    continue
            return d
    except Exception:
        return {}
    return {}

def load_local_csv(iso):
    path = os.path.join(MACRO_DIR, f'general_government_gross_debt_pct_gdp_{iso}.csv')
    if not os.path.exists(path):
        return {}
    d = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for r in reader:
                try:
                    y = int(r.get('year') or r.get('date'))
                    v = r.get('value')
                    if v is None or v == '':
                        continue
                    d[y] = float(v)
                except Exception:
                    continue
    except Exception:
        return {}
    return d

def build_extended_series(iso, start_year=1990):
    wb = load_wb_json(iso)
    local = load_local_csv(iso)
    # Merge sources: prefer WB then local CSV (local often derived from WB but safe)
    years_now = datetime.utcnow().year
    years = list(range(start_year, years_now + 1))
    values = []
    source_flags = []
    for y in years:
        if y in wb:
            values.append(wb[y])
            source_flags.append('worldbank')
        elif y in local:
            values.append(local[y])
            source_flags.append('local_csv')
        else:
            values.append(None)
            source_flags.append('missing')

    s = pd.Series(values, index=years, dtype='float64')
    original_nonnull = s.notna()

    # Interpolate internal gaps (linear), limit_direction both to fill interior gaps.
    s_interpolated = s.copy()
    if s_interpolated.notna().sum() >= 2:
        s_interpolated = s_interpolated.interpolate(method='linear', limit_direction='both')
    # Remaining NaNs (all-NaN or isolated) -> fill with nearest (ffill/backfill)
    s_filled = s_interpolated.fillna(method='ffill').fillna(method='bfill')

    imputed_mask = ~original_nonnull & s_filled.notna()

    df = pd.DataFrame({
        'year': years,
        'value': s_filled.values,
        'source_pref': source_flags,
        'imputed': imputed_mask.values.astype(bool)
    })
    return df

def write_extended_csv(df, iso):
    out = os.path.join(MACRO_DIR, f'general_government_gross_debt_pct_gdp_{iso}_extended.csv')
    df.to_csv(out, index=False, float_format='%.12g')
    return out

def summarize(df):
    observed = df[~df['imputed'] & df['value'].notna()]
    imputed = df[df['imputed']]
    first_obs = observed['year'].min() if not observed.empty else None
    last_obs = observed['year'].max() if not observed.empty else None
    return {
        'first_observed_year': int(first_obs) if first_obs is not None else None,
        'last_observed_year': int(last_obs) if last_obs is not None else None,
        'observed_count': int(len(observed)),
        'imputed_count': int(len(imputed))
    }

def main():
    os.makedirs(MACRO_DIR, exist_ok=True)
    results = {}
    for iso in COUNTRIES:
        df = build_extended_series(iso)
        out = write_extended_csv(df, iso)
        results[iso] = summarize(df)
        print(f'Wrote {out} — summary: {results[iso]}')
    print('\nAll done. Extended files written to:', MACRO_DIR)

if __name__ == '__main__':
    main()
