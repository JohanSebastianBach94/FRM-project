"""
Fetch national HPI series using per-country handlers.

Strategy:
- First, try to use already-downloaded Eurostat/BIS files in the project's output folders (these exist from prior runs).
- If a monthly series is found locally, copy it to the canonical output filename `national_<CC>_hpi.csv`.
- If only a quarterly/annual series is available locally, save it and warn (file named `national_<CC>_hpi_quarterly.csv`).
- If no local fallback, attempt naive HTTP requests to known ONS endpoints (placeholders). These may need manual API keys or dataset IDs.
- Report earliest date and frequency for each country.

This script is conservative and non-destructive: it won't attempt aggressive interpolation.
"""
from pathlib import Path
import pandas as pd
import re
import requests
import sys

ROOT = Path(__file__).parent.parent.resolve()
OUT_DIR = ROOT / 'DCC GARCH MODEL' / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_COUNTRIES = {'DE': 'Germany', 'FR': 'France', 'ES': 'Spain', 'IT': 'Italy'}

# Candidate naive ONS endpoints (placeholders). In many cases these require exact table IDs or API keys;
# they are included as a best-effort attempt and will generally return 404 unless configured.
ONS_CANDIDATES = {
    'DE': [
        # Destatis GENESIS (requires query params / key)
        'https://www-genesis.destatis.de/genesis/online/data;jsessionid=?operation=download&format=csv',
        # known CSV page (non-API) - placeholder
        'https://www.destatis.de/EN/Themes/Economy/Prices/house-price-index.csv'
    ],
    'FR': [
        # INSEE BDM API (requires API key) placeholder
        'https://api.insee.fr/series/BDM/V1/data/serie/PRC_HPI_M?startPeriod=1990',
        'https://www.insee.fr/en/statistiques/table-download?code=PRC_HPI&format=csv'
    ],
    'IT': [
        # ISTAT SDMX (placeholder)
        'https://sdmx.istat.it/SDMXWS/rest/data/PRC_HPI_M/IT',
        'https://www.istat.it/en/archivio/hpi.csv'
    ],
    'ES': [
        # INE jaxi CSV pattern (placeholder; requires table id)
        'https://www.ine.es/jaxiT3/Tabla.htm?t=12345&L=0',
        'https://www.ine.es/jaxiT3/files/t/es/csv_tabla.csv'
    ]
}


def detect_freq(df):
    """Return 'monthly','quarterly','annual' or approximate days."""
    if df is None or df.empty:
        return None
    if 'DATE' not in df.columns:
        return None
    df = df.dropna(subset=['DATE']).sort_values('DATE')
    if df.shape[0] < 2:
        return None
    diffs = df['DATE'].diff().dt.days.dropna()
    if diffs.empty:
        return None
    median = int(diffs.median())
    if median <= 40:
        return 'monthly'
    if 60 <= median <= 110:
        return 'quarterly'
    if 300 <= median <= 400:
        return 'annual'
    return f'~{median}d'


def find_local_eurostat_or_bis(cc):
    """Search output folders for existing eurostat or fred/BIS files for country cc."""
    out = Path(ROOT) / 'DCC GARCH MODEL' / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
    files = list(out.glob(f'*_{cc}.csv'))
    # prefer files that contain 'PRC_HPI' or 'eurostat' or 'fred' or 'BIS' in name
    priority = sorted(files, key=lambda p: (0 if 'PRC_HPI' in p.name or 'eurostat' in p.name else 1, 0 if 'Q' in p.name else 1))
    for f in priority:
        try:
            df = pd.read_csv(f, parse_dates=['DATE'])
        except Exception:
            continue
        freq = detect_freq(df)
        return f, df, freq
    return None, None, None


def try_ons_urls(cc):
    candidates = ONS_CANDIDATES.get(cc, [])
    for url in candidates:
        try:
            r = requests.get(url, timeout=30)
        except Exception as e:
            print(f'  ONS request failed for {cc} url {url}:', e)
            continue
        if r.status_code != 200:
            print(f'  ONS url {url} returned HTTP', r.status_code)
            continue
        # try to parse CSV from content
        text = r.text
        # Heuristic: if csv-like, has commas and newline and a header containing DATE or Period
        if (',' in text or ';' in text) and ('DATE' in text or 'Period' in text or re.search(r'\d{4}-\d{2}', text)):
            # attempt to read
            from io import StringIO
            sep = ',' if ',' in text.splitlines()[0] else ';'
            try:
                df = pd.read_csv(StringIO(text), sep=sep)
                # normalize column names
                if 'DATE' not in df.columns:
                    # try to find date-like column
                    for c in df.columns:
                        if re.search(r'date|period|time', c, re.I):
                            df = df.rename(columns={c: 'DATE'})
                            break
                if 'DATE' in df.columns:
                    df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
                    if 'VALUE' not in df.columns:
                        # try to pick first numeric column
                        for c in df.columns:
                            if c == 'DATE':
                                continue
                            try:
                                df[c] = pd.to_numeric(df[c], errors='coerce')
                                df = df.rename(columns={c: 'VALUE'})
                                break
                            except Exception:
                                continue
                    return df
            except Exception as e:
                print('  CSV parse failed for', url, e)
                continue
        else:
            print('  ONS url content not CSV-like:', url[:120])
    return None


def save_canonical(df, cc, monthly_pref=True):
    if df is None or df.empty:
        return None
    df = df.dropna(subset=['DATE']).copy().sort_values('DATE')
    freq = detect_freq(df)
    if freq == 'monthly' or (not monthly_pref):
        out = OUT_DIR / f'national_{cc}_hpi.csv'
    else:
        out = OUT_DIR / f'national_{cc}_hpi_quarterly.csv'
    df_to_save = df[['DATE','VALUE']].dropna()
    df_to_save.to_csv(out, index=False)
    return out, freq, df_to_save['DATE'].min(), df_to_save['DATE'].max(), len(df_to_save)


def process_country(cc):
    print('\nProcessing', cc, TARGET_COUNTRIES.get(cc))
    found_file, df, freq = find_local_eurostat_or_bis(cc)
    if found_file is not None:
        print('  Found local file:', found_file.name, 'detected freq:', freq)
        # ensure df has DATE and VALUE
        if 'DATE' in df.columns and 'VALUE' not in df.columns:
            # attempt to find numeric column
            for c in df.columns:
                if c == 'DATE':
                    continue
                try:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
                    df = df.rename(columns={c: 'VALUE'})
                    break
                except Exception:
                    continue
        out = save_canonical(df, cc)
        if out:
            print('  Saved canonical:', out[0].name, 'freq:', out[1], 'rows:', out[4])
            return out
        else:
            print('  Failed to save canonical from local file')
    # try ONS naive urls
    print('  No suitable local HPI; trying national ONS endpoints...')
    df2 = try_ons_urls(cc)
    if df2 is not None:
        print('  ONS returned data; detecting frequency...')
        out = save_canonical(df2, cc)
        if out:
            print('  Saved canonical from ONS:', out[0].name, 'freq:', out[1])
            return out
    print('  No national ONS series found or parsed for', cc)
    return None


def main():
    report = {}
    for cc in TARGET_COUNTRIES.keys():
        res = process_country(cc)
        report[cc] = res
    print('\nSummary:')
    for cc, res in report.items():
        if res is None:
            print(' ', cc, 'no series found')
        else:
            out, freq, start, end, rows = res
            print(' ', cc, '->', out.name, 'freq:', freq, 'start:', start.date(), 'end:', end.date(), 'rows:', rows)

if __name__ == '__main__':
    main()
