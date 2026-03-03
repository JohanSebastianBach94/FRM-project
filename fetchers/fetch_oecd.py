"""
Fetch monthly House Price Index series from OECD via SDMX-JSON.

Behavior:
- Try a list of candidate OECD dataset IDs and query SDMX-JSON for data starting in 1990.
- Parse SDMX-JSON response into pandas DataFrames per series.
- For target countries (DE, FR, ES, IT) pick the best candidate series preferring monthly frequency and index-like units.
- Save cleaned `DATE,VALUE` CSVs into the project's Lehman enhanced output folder.

Notes:
- OECD dataset IDs vary between sources; this script tries several likely IDs and logs results.
- If the environment has `pandasdmx` or other SDMX helpers available, this script will still use raw SDMX-JSON to avoid extra dependencies.
"""
from pathlib import Path
import requests
import pandas as pd
import numpy as np
import time
import sys

OUT_DIR = Path(__file__).parent / '..' / 'DCC GARCH MODEL' / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
OUT_DIR = OUT_DIR.resolve()
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_COUNTRIES = {'DE': 'Germany', 'FR': 'France', 'ES': 'Spain', 'IT': 'Italy'}

# Candidate OECD dataset IDs to try. This is not exhaustive; the script will try each and report if successful.
CANDIDATES = [
    'HOUSE_PRICE_INDEX',
    'HOUSE_PRICES',
    'HPPI',
    'HOUSES_PRICES',
    'HPIDX',
    'HPI',
    'HOUSEPRICE',
]

# Helper: parse SDMX-JSON response into a list of (series_key, series_values, obs_times)
def parse_sdmx_json(js):
    # See SDMX-JSON structure: 'structure' contains dimensions, 'dataSets' contains series
    try:
        structure = js['structure']
        ds = js.get('dataSets', [])
        if not ds:
            return []
        ds0 = ds[0]
    except Exception:
        return []

    # observation time values (observation dimension)
    obs_dim = structure.get('dimensions', {}).get('observation', [])
    time_values = []
    if obs_dim:
        time_values = [v.get('id') for v in obs_dim[0].get('values', [])]

    # series dimension metadata
    series_dims = structure.get('dimensions', {}).get('series', [])
    series_dim_names = [d.get('id') for d in series_dims]
    series_values = [d.get('values', []) for d in series_dims]

    parsed = []
    series_dict = ds0.get('series', {})
    for s_key, s_obj in series_dict.items():
        # s_key like "0:1:2"
        key_parts = s_key.split(':')
        # map key parts to dimension values
        meta = {}
        for i, part in enumerate(key_parts):
            try:
                val_list = series_values[i]
                v = val_list[int(part)].get('id')
            except Exception:
                v = None
            meta[series_dim_names[i] if i < len(series_dim_names) else f'dim{i}'] = v
        # observations
        obs = s_obj.get('observations', {})
        # obs: index -> [value, ...]
        values = []
        times = []
        for obs_idx_str, obs_val in obs.items():
            try:
                obs_idx = int(obs_idx_str)
            except Exception:
                continue
            if obs_idx < len(time_values):
                times.append(time_values[obs_idx])
            else:
                # fallback: use index number
                times.append(str(obs_idx))
            if isinstance(obs_val, list) and len(obs_val) > 0:
                values.append(obs_val[0])
            else:
                values.append(None)
        parsed.append({'meta': meta, 'times': times, 'values': values})
    return parsed


def try_dataset(dataset_id):
    url = f'https://stats.oecd.org/SDMX-JSON/data/{dataset_id}/all?startTime=1990'
    print('Querying OECD dataset:', dataset_id)
    try:
        r = requests.get(url, timeout=60)
    except Exception as e:
        print('  Request failed:', e)
        return []
    if r.status_code != 200:
        print('  HTTP', r.status_code, 'for', dataset_id)
        return []
    try:
        js = r.json()
    except Exception as e:
        print('  JSON decode failed:', e)
        return []
    parsed = parse_sdmx_json(js)
    print('  Parsed', len(parsed), 'series from', dataset_id)
    return parsed


def normalize_time_label(s):
    s = str(s)
    # Try common formats: YYYY-MM, YYYY-MM-DD, YYYY-Qn, YYYY
    if '-' in s and len(s.split('-')[0]) == 4:
        # try YYYY-MM or YYYY-MM-DD
        try:
            if len(s.split('-')) == 2:
                return pd.to_datetime(s + '-01', format='%Y-%m-%d', errors='coerce')
            else:
                return pd.to_datetime(s, errors='coerce')
        except Exception:
            return pd.to_datetime(s, errors='coerce')
    if 'Q' in s or 'q' in s:
        # try pandas Period
        try:
            return pd.Period(s).to_timestamp()
        except Exception:
            return pd.to_datetime(s, errors='coerce')
    if s.isdigit() and len(s) == 4:
        return pd.to_datetime(s + '-01-01')
    return pd.to_datetime(s, errors='coerce')


def choose_best_series_for_country(parsed_series, country_code):
    # parsed_series: list of {'meta':..., 'times':..., 'values':...}
    candidates = []
    for s in parsed_series:
        meta = s['meta']
        # try to find country dimension key in meta
        found = False
        for k, v in meta.items():
            if v == country_code or (isinstance(v, str) and v.endswith('.' + country_code)):
                found = True
        # If not explicitly matched, still keep as possible (some datasets use other dim names)
        # Build DataFrame of times/values
        if not s['times']:
            continue
        df = pd.DataFrame({'TIME': s['times'], 'VALUE': s['values']})
        df['DATE'] = df['TIME'].apply(normalize_time_label)
        df = df.dropna(subset=['DATE']).copy()
        if df.empty:
            continue
        df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
        df = df.dropna(subset=['VALUE'])
        if df.empty:
            continue
        # estimate freq
        diffs = df['DATE'].diff().dt.days.dropna()
        median_days = int(diffs.median()) if len(diffs) > 0 else None
        freq = 'unknown'
        if median_days is not None:
            if median_days <= 40:
                freq = 'monthly'
            elif 60 <= median_days <= 110:
                freq = 'quarterly'
            elif 300 <= median_days <= 400:
                freq = 'annual'
            else:
                freq = f'~{median_days}d'
        candidates.append({'meta': meta, 'df': df.sort_values('DATE'), 'freq': freq, 'n': len(df), 'matched_country': found})
    if not candidates:
        return None
    # prefer matched_country True, then prefer monthly by freq, then max n
    candidates.sort(key=lambda x: (not x['matched_country'], 0 if x['freq']=='monthly' else 1, -x['n']))
    return candidates[0]


def main():
    results = {}
    for ds in CANDIDATES:
        parsed = try_dataset(ds)
        if not parsed:
            continue
        for cc in TARGET_COUNTRIES.keys():
            best = choose_best_series_for_country(parsed, cc)
            if best is None:
                continue
            # Save if better than existing
            prev = results.get(cc)
            if prev is None:
                results[cc] = {'dataset': ds, 'best': best}
            else:
                # compare preferences: prefer monthly and longer series
                prefer_new = False
                def score(x):
                    s = 0
                    if x['freq'] == 'monthly':
                        s += 10000
                    if x['matched_country']:
                        s += 5000
                    s += x['n']
                    return s
                if score(best) > score(prev['best']):
                    results[cc] = {'dataset': ds, 'best': best}
        # be polite
        time.sleep(0.5)

    # write outputs
    for cc, info in results.items():
        df = info['best']['df'][['DATE','VALUE']].drop_duplicates('DATE').sort_values('DATE')
        csv_out = OUT_DIR / f'oecd_{info["dataset"]}_{cc}.csv'
        df.to_csv(csv_out, index=False)
        print('Wrote', csv_out, 'rows', len(df), 'freq', info['best']['freq'], 'dataset', info['dataset'])

    if not results:
        print('No suitable OECD series found for target countries using candidate dataset list.')

if __name__ == '__main__':
    main()
