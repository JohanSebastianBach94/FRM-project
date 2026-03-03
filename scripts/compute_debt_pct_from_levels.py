#!/usr/bin/env python3
"""
Compute debt (% of GDP) from nominal debt level CSVs and GDP JSONs.
- Uses annual GDP if GDP is annual.
- Uses trailing 4-quarter sum if GDP is quarterly.
- Applies QC: if denominator <= 0 mark qc_flag 'invalid_denominator' and do not compute pct.
- Writes CSV: data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.csv
- Writes metadata sidecar: .meta.json with provenance and denom method.
"""
import json
from pathlib import Path
import pandas as pd
import math
import re

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / 'data_repository' / 'raw' / 'macro'
MACRO.mkdir(parents=True, exist_ok=True)


def _select_level_path(iso: str) -> Path | None:
    """Select the best available debt level file for an ISO.

    Prefer USD-denominated debt levels when available because World Bank GDP
    (NY.GDP.MKTP.CD) is in current USD.
    """
    usd = MACRO / f'general_government_gross_debt_level_usd_{iso}.csv'
    if usd.exists():
        return usd
    local = MACRO / f'general_government_gross_debt_level_{iso}.csv'
    if local.exists():
        return local
    return None

# detect WB JSON structure and extract date/value list
def load_wb_gdp_series(iso):
    p = MACRO / f'wb_NY.GDP.MKTP.CD_{iso}.json'
    if not p.exists():
        return None, None
    raw = json.loads(p.read_text(encoding='utf-8'))
    # World Bank API typically returns [metadata, [ { 'date': '2023', 'value': 123 }, ... ]]
    try:
        if isinstance(raw, list) and len(raw) >= 2 and isinstance(raw[1], list):
            entries = raw[1]
            data = {e['date']: e.get('value') for e in entries if 'date' in e}
            # determine if dates contain quarters
            sample_dates = list(data.keys())[:10]
            is_quarterly = any(('Q' in d or '-' in d and not d.isdigit()) for d in sample_dates)
            # convert to pandas Series
            # Normalize dates: if quarterly look for 'YYYY-Qn' or 'YYYYQn'
            if is_quarterly:
                ser = {}
                for k, v in data.items():
                    if v is None:
                        continue
                    # try several formats
                    # common WB won't have quarters - fallback
                    try:
                        if 'Q' in k:
                            period = pd.Period(k.replace('Q','Q'), freq='Q')
                        else:
                            # try YYYY-MM
                            period = pd.Period(k, freq='M').asfreq('Q', how='end')
                        ser[period] = float(v)
                    except Exception:
                        continue
                s = pd.Series(ser)
                s.index = pd.PeriodIndex(s.index, freq='Q')
                return s, 'quarterly'
            else:
                # annual
                ser = {}
                for k, v in data.items():
                    if v is None:
                        continue
                    try:
                        y = int(k)
                        ser[y] = float(v)
                    except Exception:
                        continue
                s = pd.Series(ser)
                s.index = pd.Index(sorted(ser.keys()))
                s = s.sort_index()
                return s, 'annual'
    except Exception:
        pass
    # fallback: attempt heuristic extraction of year:value pairs from file text
    txt = p.read_text(encoding='utf-8', errors='ignore')
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
    if pairs:
        s = pd.Series(pairs)
        s = s.sort_index()
        return s, 'annual'
    return None, None


def load_level_csv(iso):
    p = _select_level_path(iso)
    if p is None:
        return None
    df = pd.read_csv(p)
    if df.empty:
        return None
    # expect columns year,debt_level
    # try to coerce year to int
    df = df.rename(columns={df.columns[0]:'year', df.columns[1]:'level'})
    df = df[['year','level']]
    # drop nulls
    df = df.dropna()
    # coerce
    try:
        df['year'] = df['year'].astype(int)
        df['level'] = df['level'].astype(float)
        s = pd.Series(df['level'].values, index=pd.Index(df['year'].values))
        s = s.sort_index()
        return s
    except Exception:
        # try period-based index (quarterly)
        try:
            df['period'] = pd.to_datetime(df['year'], errors='coerce')
            df = df.dropna(subset=['period'])
            idx = pd.PeriodIndex(pd.to_datetime(df['period']), freq='Q')
            s = pd.Series(df['level'].values, index=idx)
            return s
        except Exception:
            return None


def compute_and_save(iso):
    debt_level_path = _select_level_path(iso)
    level = load_level_csv(iso)
    if level is None:
        print('No level file for', iso)
        return
    gdp, freq = load_wb_gdp_series(iso)
    if gdp is None:
        print('No WB GDP JSON for', iso, '— cannot compute reliable pct; skipping')
        return
    # Prepare dataframe
    # Determine if level index is years or periods
    denom_method = None
    df_out = None
    if isinstance(level.index, pd.PeriodIndex) or (hasattr(level.index, 'freq') and str(getattr(level.index, 'freq', ''))=='Q'):
        # level is quarterly
        if freq == 'quarterly':
            # compute trailing 4-quarter GDP sum
            gdp_q = gdp.copy()
            # ensure PeriodIndex quarterly
            if not isinstance(gdp_q.index, pd.PeriodIndex):
                # try convert
                gdp_q.index = pd.PeriodIndex(pd.to_datetime(gdp_q.index.astype(str)), freq='Q')
            denom = gdp_q.rolling(4, min_periods=4).sum()
            denom_method = 'trailing_4q_sum'
            # align on level index
            df = pd.DataFrame({'level': level})
            df = df.join(denom.rename('gdp_den'), how='left')
        else:
            # GDP is annual but level quarterly — fallback: use annual GDP of the year containing quarter
            denom_method = 'annual_gdp_align_quarter_to_year'
            # map quarter period to year
            df = pd.DataFrame({'level': level})
            years = df.index.year
            # gdp is Series indexed by year ints
            if isinstance(gdp.index, pd.Index):
                df['gdp_den'] = [gdp.get(int(y), None) for y in years]
            else:
                # attempt conversion
                df['gdp_den'] = [gdp.get(int(p.year), None) for p in df.index]
    else:
        # level is annual (index int years)
        if freq == 'annual':
            denom_method = 'annual'
            df = pd.DataFrame({'level': level})
            df = df.join(gdp.rename('gdp_den'), how='left')
        else:
            # GDP quarterly but level annual — sum GDP four quarters per year (annualize)
            denom_method = 'gdp_t4_annualized'
            # convert gdp quarterly to annual sum by year
            if isinstance(gdp.index, pd.PeriodIndex):
                gdp_q = gdp.copy()
                # sum by year
                gdp_annual = gdp_q.groupby(lambda p: p.year).sum()
                df = pd.DataFrame({'level': level})
                df = df.join(gdp_annual.rename('gdp_den'), how='left')
            else:
                # fallback
                df = pd.DataFrame({'level': level})
                df = df.join(gdp.rename('gdp_den'), how='left')

    # compute pct and QC
    df['debt_pct'] = None
    df['qc_flag'] = None
    for idx, row in df.iterrows():
        denom = row.get('gdp_den')
        lvl = row.get('level')
        if denom is None or (isinstance(denom, float) and math.isnan(denom)):
            df.at[idx, 'qc_flag'] = 'missing_denominator'
            continue
        try:
            if float(denom) <= 0:
                df.at[idx, 'qc_flag'] = 'invalid_denominator'
                continue
            # compute
            pct = float(lvl) / float(denom) * 100.0
            df.at[idx, 'debt_pct'] = pct
            # outlier flags
            if pct < 0:
                df.at[idx, 'qc_flag'] = 'negative_pct'
            elif pct > 500:
                df.at[idx, 'qc_flag'] = 'large_pct'
        except Exception:
            df.at[idx, 'qc_flag'] = 'compute_error'

    # Save CSV
    # normalize index to year integer or period string
    out_rows = []
    if isinstance(df.index, pd.PeriodIndex):
        for idx, row in df.iterrows():
            out_rows.append({'period': str(idx), 'debt_pct': row['debt_pct'], 'qc_flag': row['qc_flag']})
        out_csv = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.csv'
    else:
        for idx, row in df.iterrows():
            out_rows.append({'year': int(idx), 'debt_pct': row['debt_pct'], 'qc_flag': row['qc_flag']})
        out_csv = MACRO / f'general_government_gross_debt_pct_gdp_{iso}.csv'

    # write CSV
    pd.DataFrame(out_rows).to_csv(out_csv, index=False)
    # metadata
    meta = {
        'iso': iso,
        'denominator_method': denom_method,
        'provenance': {
            'debt_level_file': str(debt_level_path.name) if debt_level_path is not None else None,
            'gdp_source_file': str((MACRO / f'wb_NY.GDP.MKTP.CD_{iso}.json').name)
        }
    }
    meta_path = out_csv.with_suffix('.meta.json')
    meta_path.write_text(json.dumps(meta, indent=2))
    print('Wrote', out_csv.name, 'method=', denom_method)


def main():
    # find level files
    files = list(MACRO.glob('general_government_gross_debt_level_*.csv'))
    isos = []
    for p in files:
        name = p.name
        # Ignore USD numerator helpers; those are selected automatically.
        if name.startswith('general_government_gross_debt_level_usd_'):
            continue
        iso = name.replace('general_government_gross_debt_level_', '').replace('.csv', '')
        isos.append(iso)
    isos = sorted(set(isos))
    for iso in isos:
        compute_and_save(iso)

if __name__ == '__main__':
    main()
