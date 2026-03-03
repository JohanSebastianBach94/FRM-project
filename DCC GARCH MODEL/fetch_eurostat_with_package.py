"""Use `eurostat` package to discover and download monthly HPI for DE/FR/ES/IT.

Steps:
- Search Eurostat for datasets mentioning "house price" or "house price index".
- For candidate datasets, fetch series filtered by geo (DE/FR/ES/IT) and frequency=M if available.
- Save CSV + small PNG into Lehman enhanced folder.
"""
from pathlib import Path
from datetime import datetime
import eurostat as es
import pandas as pd
import matplotlib.pyplot as plt

OUT_DIR = Path(__file__).parent / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = {'DE':'Germany','FR':'France','IT':'Italy','ES':'Spain'}

# 1) get table of contents and filter for house-price related datasets
print('Loading Eurostat table of contents (may take a few seconds)...')
# Prefer using eurostat.search which returns dataset codes and titles
print('Searching Eurostat catalog for "house price"...')
# First try the package search helper
try:
    search_df = es.search('house price')
    if isinstance(search_df, pd.DataFrame) and not search_df.empty and 'code' in search_df.columns:
        candidates = search_df['code'].tolist()
        print('Found candidate datasets via eurostat.search:', len(candidates))
        print(search_df[['code','title']].head(20).to_string(index=False))
    else:
        candidates = []
except Exception as e:
    print('eurostat.search failed:', e)
    candidates = []

# Fallback: scan table-of-contents
if not candidates:
    try:
        toc_df = es.get_toc_df()
        pattern = 'house price|house-price|house price index|house-price index|price index|property price'
        mask = pd.Series(False, index=toc_df.index)
        for col in toc_df.columns:
            if pd.api.types.is_string_dtype(toc_df[col]):
                try:
                    mask = mask | toc_df[col].str.contains(pattern, case=False, na=False)
                except Exception:
                    continue
        matches = toc_df[mask]
        print('Found candidate datasets (fallback):', len(matches))
        candidates = matches['code'].tolist() if 'code' in matches.columns else []
        if candidates:
            cols_to_show = [c for c in ['code','title'] if c in matches.columns]
            print(matches[cols_to_show].head(20).to_string(index=False))
    except Exception as e:
        print('TOC fallback failed:', e)

# Last resort
if not candidates:
    print('Falling back to known dataset codes list')
    candidates = ['prc_hpi_midx','prc_hpi']

fetched = {}
for code in candidates:
    print('\nTrying dataset', code)
    try:
        df = es.get_data_df(code)
    except Exception as e:
        print('Failed to get data for', code, e)
        continue
    # eurostat returns multiindex with geo/time columns depending; try to pivot
    # columns typically: geo, unit, time_period, values -> depending on version
    if df.empty:
        print('Dataset empty for', code)
        continue
    # Reshape wide tables (many time columns) into long form where appropriate
    df = df.reset_index()
    import re
    time_cols = [c for c in df.columns if re.search(r"\d{4}", str(c))]
    if len(time_cols) > 3:
        id_vars = [c for c in df.columns if c not in time_cols]
        melted = df.melt(id_vars=id_vars, value_vars=time_cols, var_name='TIME_PERIOD', value_name='value')
        geo_col = next((c for c in id_vars if 'geo' in str(c).lower()), None)
        if geo_col is None and len(id_vars) > 0:
            geo_col = id_vars[-1]
        if geo_col in melted.columns:
            melted = melted.rename(columns={geo_col: 'geo'})
        df = melted[['geo','TIME_PERIOD','value']].dropna(subset=['value'])

    # If df is long-form with 'geo','TIME_PERIOD','value', parse and save per country
    if set(['geo','TIME_PERIOD','value']).issubset(df.columns):
        # helper to parse many period formats (monthly YYYY-MM, quarterly YYYY-Qx, annual YYYY)
        def parse_time_label(s):
            s = str(s)
            # monthly e.g. 2005-01
            try:
                if re.match(r"^\d{4}-\d{2}$", s):
                    return pd.to_datetime(s, format="%Y-%m", errors='coerce')
                if 'Q' in s or re.match(r"^\d{4}Q\d$", s):
                    return pd.Period(s).to_timestamp()
                if re.match(r"^\d{4}$", s):
                    return pd.to_datetime(s + '-01-01')
                # fallback
                return pd.to_datetime(s, errors='coerce')
            except Exception:
                return pd.to_datetime(s, errors='coerce')

        # dimension-detection heuristics: prefer rows with index-like units/indicators
        preferred_pattern = re.compile(r"MIDX|I20|OBS_VALUE|INDEX|HPPI|HPI|PRICE|PRICE_INDEX|PRC", re.I)

        # find additional dimension columns (exclude geo/TIME_PERIOD/value)
        dim_cols = [c for c in df.columns if c not in ('geo','TIME_PERIOD','value')]

        for geo, gname in COUNTRIES.items():
            geo_df = df[df['geo'] == geo]
            if geo_df.empty:
                print('No data for', geo, 'in', code)
                continue

            # try to detect monthly TIME_PERIODs
            tp_strs = geo_df['TIME_PERIOD'].astype(str)
            is_monthly = tp_strs.str.match(r"^\d{4}-\d{2}$")

            # prefer rows where any dimension column matches the preferred pattern
            preferred_mask = pd.Series(False, index=geo_df.index)
            for c in dim_cols:
                try:
                    preferred_mask = preferred_mask | geo_df[c].astype(str).str.contains(preferred_pattern, na=False)
                except Exception:
                    continue

            # if monthly rows exist, focus on them
            if is_monthly.any():
                candidate = geo_df[is_monthly]
            else:
                candidate = geo_df

            # if preferred dimension selection yields rows, narrow to them
            if preferred_mask.any():
                candidate = candidate.loc[preferred_mask.reindex(candidate.index, fill_value=False)]

            # drop rows with missing or non-numeric values
            candidate = candidate.dropna(subset=['value']).copy()
            candidate['VALUE'] = pd.to_numeric(candidate['value'], errors='coerce')
            candidate = candidate.dropna(subset=['VALUE'])
            if candidate.empty:
                # fallback: try filtering by value magnitude (index values typically >10)
                alt_candidate = geo_df.dropna(subset=['value']).copy()
                alt_candidate['VALUE'] = pd.to_numeric(alt_candidate['value'], errors='coerce')
                alt_candidate = alt_candidate[alt_candidate['VALUE'].abs() > 10]
                if alt_candidate.empty:
                    print('No suitable numeric series for', geo, 'in', code)
                    continue
                candidate = alt_candidate

            # Aggregate if multiple dimension rows per TIME_PERIOD: take mean
            candidate['DATE'] = candidate['TIME_PERIOD'].apply(parse_time_label)
            candidate = candidate.dropna(subset=['DATE'])
            if candidate.empty:
                print('No parseable dates for', geo, 'in', code)
                continue
            out_df = candidate.groupby('DATE', sort=True)['VALUE'].mean().reset_index()

            # ensure we have monthly frequency if possible; otherwise keep as-is
            if out_df['DATE'].dt.to_period('M').nunique() >= 12:
                # looks like monthly or high-frequency; keep
                pass

            out_df = out_df.sort_values('DATE')
            if out_df.empty:
                print('No valid time series for', geo, 'in', code)
                continue

            csv_out = OUT_DIR / f'eurostat_{code}_{geo}.csv'
            png_out = OUT_DIR / f'eurostat_{code}_{geo}.png'
            out_df.to_csv(csv_out, index=False)
            plt.figure(figsize=(8,3))
            plt.plot(out_df['DATE'], out_df['VALUE'], '-o', markersize=3)
            plt.title(f'{gname} - {code}')
            plt.grid(alpha=0.2)
            plt.tight_layout()
            plt.savefig(png_out, dpi=150)
            plt.close()
            print('Wrote:', csv_out, 'rows:', len(out_df))
            fetched[(code,geo)] = csv_out

print('\nDone. Fetched files:', len(fetched))
