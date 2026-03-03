"""Fetch selected BIS FRED property-price series for DEU/FRA/ITA/ESP and save CSV + PNG.

This re-uses the same fetch strategy as the project's main fetcher: try pandas_datareader, fall back to fredapi if available.
"""
from pathlib import Path
from datetime import datetime
import os

OUT_DIR = Path(__file__).parent / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
OUT_DIR.mkdir(parents=True, exist_ok=True)

SERIES = {
    'QDER628BIS': 'Real Residential Property Prices for Germany (QDER628BIS)',
    'QDEN628BIS': 'Residential Property Prices for Germany (QDEN628BIS)',
    'QFRR628BIS': 'Real Residential Property Prices for France (QFRR628BIS)',
    'QFRN628BIS': 'Residential Property Prices for France (QFRN628BIS)',
    'QITR628BIS': 'Real Residential Property Prices for Italy (QITR628BIS)',
    'QITN628BIS': 'Residential Property Prices for Italy (QITN628BIS)',
    'QESR628BIS': 'Real Residential Property Prices for Spain (QESR628BIS)',
    'QESN628BIS': 'Residential Property Prices for Spain (QESN628BIS)'
}

START = '1990-01-01'
END = datetime.today().strftime('%Y-%m-%d')


def fetch_with_pdr(series_id, start, end):
    try:
        from pandas_datareader import data as pdr
    except Exception:
        return None, 'pandas_datareader not installed'
    try:
        df = pdr.DataReader(series_id, 'fred', start, end)
        return df, None
    except Exception as e:
        return None, str(e)


def fetch_with_fredapi(series_id, start, end):
    try:
        from fredapi import Fred
    except Exception:
        return None, 'fredapi not installed'
    key = os.getenv('FRED_API_KEY')
    if not key:
        return None, 'FRED_API_KEY not set'
    try:
        fred = Fred(api_key=key)
        s = fred.get_series(series_id, observation_start=start, observation_end=end)
        df = s.to_frame(series_id)
        df.index.name = 'DATE'
        return df, None
    except Exception as e:
        return None, str(e)


def try_fetch(series_id, start=START, end=END):
    df, err = fetch_with_pdr(series_id, start, end)
    if df is not None:
        return df
    # fallback to fredapi
    df, err2 = fetch_with_fredapi(series_id, start, end)
    if df is not None:
        return df
    raise SystemExit(f'Failed to fetch {series_id}: pdr error="{err}"; fredapi error="{err2}"')


def try_plot(df, out_png, title):
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print('matplotlib not installed — skipping plot for', out_png)
        return
    plt.figure(figsize=(8,3))
    plt.plot(df.index, df.iloc[:,0], color='#2ca02c')
    plt.title(title)
    plt.grid(alpha=0.2)
    plt.tight_layout()
    plt.axvspan(datetime(2006,1,1), datetime(2008,12,31), color='red', alpha=0.08)
    plt.savefig(out_png, dpi=150)
    plt.close()


def main():
    results = {}
    for sid, name in SERIES.items():
        print('Fetching', sid)
        df = try_fetch(sid)
        df.columns = [sid]
        csv_out = OUT_DIR / f'fred_{sid}.csv'
        png_out = OUT_DIR / f'fred_{sid}.png'
        df.to_csv(csv_out)
        try_plot(df, png_out, name)
        print('Wrote:', csv_out)
        if png_out.exists():
            print('Wrote:', png_out)
        results[sid] = csv_out
    print('\nDone.')


if __name__ == '__main__':
    main()
