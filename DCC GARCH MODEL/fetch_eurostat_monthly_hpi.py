"""Fetch Eurostat monthly HPI CSV via Eurostat API (try CSV format first) for DE/FR/ES/IT.

This script tries dataset IDs in `DATASETS` and requests CSV output with `format=CSV`.
If successful it saves CSV + optional plot in the same `heatmaps_final/.../lehman_enhanced` folder.
"""
from pathlib import Path
from datetime import datetime
import requests
import sys

OUT_DIR = Path(__file__).parent / 'heatmaps_final' / 'narrative_spotlights' / 'lehman_enhanced'
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = {'DE':'Germany','FR':'France','IT':'Italy','ES':'Spain'}
# Candidate Eurostat datasets to try (common HPI dataset names)
DATASETS = ['prc_hpi_midx','prc_hpi']

session = requests.Session()
session.headers.update({'User-Agent': 'FRM-project-agent/1.0 (+https://example.local)'})

results = {}
for ds in DATASETS:
    for geo, name in COUNTRIES.items():
        out_csv = OUT_DIR / f'eurostat_{ds}_{geo}.csv'
        out_png = OUT_DIR / f'eurostat_{ds}_{geo}.png'
        if out_csv.exists():
            print('Already have', out_csv)
            results[(ds,geo)] = out_csv
            continue
        url = f'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{ds}?geo={geo}&format=CSV'
        try:
            r = session.get(url, timeout=30)
        except Exception as e:
            print('Request error', ds, geo, e)
            continue
        if r.status_code == 200 and r.text.strip():
            # save csv
            out_csv.write_text(r.text, encoding='utf-8')
            print('Wrote:', out_csv)
            # try plotting
            try:
                import pandas as pd
                import matplotlib.pyplot as plt
                from io import StringIO
                df = pd.read_csv(StringIO(r.text))
                # Eurostat CSV may come in several formats; try to pivot time/value
                if 'TIME_PERIOD' in df.columns and 'value' in df.columns:
                    df2 = df[['TIME_PERIOD','value']].dropna()
                    df2['TIME_PERIOD'] = pd.to_datetime(df2['TIME_PERIOD'], errors='coerce')
                    df2 = df2.sort_values('TIME_PERIOD')
                    plt.figure(figsize=(8,3))
                    plt.plot(df2['TIME_PERIOD'], df2['value'], '-o', markersize=3)
                    plt.title(f'{name} - {ds}')
                    plt.grid(alpha=0.2)
                    plt.tight_layout()
                    plt.savefig(out_png, dpi=150)
                    plt.close()
                    print('Wrote:', out_png)
                else:
                    print('CSV format unexpected for', ds, geo, '- saved raw CSV')
            except Exception as e:
                print('Plot skipped (pandas/matplotlib missing or parse error):', e)
            results[(ds,geo)] = out_csv
        else:
            print('No CSV returned for', ds, geo, 'status', r.status_code)

if not results:
    print('\nNo datasets returned CSV for tried dataset IDs. Consider installing `eurostat` Python package or providing dataset IDs.')
else:
    print('\nDone. Files written:', len(results))
