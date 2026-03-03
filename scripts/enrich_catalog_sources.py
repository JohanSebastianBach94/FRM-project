"""
Enrich catalog.csv rows with explicit provider, fetch_method and storage_path.

This script reads `catalog.csv`, heuristically maps the existing `source`,
`source_group` and `source_detail` columns to three explicit columns:
  - provider: canonical provider (fred, worldbank, manual, yfinance, panel, dnss, bis, imf, ecb, yahoo)
  - fetch_method: how to obtain the series (api, fredapi, worldbank_api, local_csv, yfinance, merged_panel, dnss_pipeline)
  - storage_path: the on-disk path or URL where the source is stored (when available)

It writes back an updated `catalog.csv` (backing up the original) and
also writes `analysis_outputs/catalog_enriched.csv` for review.

Usage:
    python scripts/enrich_catalog_sources.py
"""
from pathlib import Path
import pandas as pd
import re
import shutil
from datetime import datetime


ROOT = Path('.').resolve()
CATALOG = ROOT / 'catalog.csv'
OUT_DIR = ROOT / 'analysis_outputs'
OUT_DIR.mkdir(exist_ok=True)

if not CATALOG.exists():
    raise FileNotFoundError('catalog.csv not found in project root')

df = pd.read_csv(CATALOG, dtype=str).fillna('')

def infer_provider_and_method(row):
    src = row.get('source','') or ''
    src_group = row.get('source_group','') or ''
    src_detail = row.get('source_detail','') or ''
    provider = ''
    method = ''
    storage = ''

    lsrc = src.lower()
    ldetail = src_detail.lower()
    lgroup = src_group.lower()

    # Manual CSV entries (point to files)
    if 'manual' in lsrc or 'manual csv' in lsrc or ldetail.endswith('.csv') or 'data_repository' in ldetail:
        provider = 'manual'
        method = 'local_csv'
        storage = src_detail or ''
        return provider, method, storage

    # Panel / merged data
    if 'panel' in lsrc or 'panel derived' in lsrc or 'merged_panel' in ldetail or 'merged_panel' in src_detail:
        provider = 'panel'
        method = 'merged_panel'
        storage = 'data/stress_indicators_expanded.csv'
        return provider, method, storage

    # FRED
    if 'fred' in lsrc or 'fred' in lgroup or 'fred' in ldetail:
        provider = 'fred'
        method = 'fredapi'
        storage = ''
        return provider, method, storage

    # World Bank
    if 'world' in lsrc or 'world bank' in lsrc or ldetail.startswith('wb_') or 'worldbank' in ldetail:
        provider = 'worldbank'
        method = 'worldbank_api_or_json'
        storage = src_detail or ''
        return provider, method, storage

    # Yahoo / yfinance
    if 'yahoo' in lsrc or 'yfinance' in lgroup or 'yfinance' in ldetail:
        provider = 'yahoo'
        method = 'yfinance'
        storage = ''
        return provider, method, storage

    # DNSS / Derived pipelines
    if 'dnss' in lsrc or 'derived_dnss' in lgroup or 'dnss' in ldetail:
        provider = 'dnss'
        method = 'dnss_pipeline'
        storage = ''
        return provider, method, storage

    # BIS / IMF / ECB
    if 'bis' in lsrc or 'bis' in lgroup or 'bis' in ldetail:
        provider = 'bis'
        method = 'bis_api_or_csv'
        storage = src_detail or ''
        return provider, method, storage
    if 'imf' in lsrc or 'imf' in lgroup or 'imf' in ldetail:
        provider = 'imf'
        method = 'imf_sdmx'
        storage = src_detail or ''
        return provider, method, storage
    if 'ecb' in lsrc or 'ecb' in lgroup or 'ecb' in ldetail:
        provider = 'ecb'
        method = 'ecb_sdmx'
        storage = src_detail or ''
        return provider, method, storage

    # Default fallback: keep original source as provider and mark method unknown
    provider = src or lgroup or 'unknown'
    method = 'unknown'
    storage = src_detail or ''
    return provider, method, storage


providers = []
methods = []
storages = []

for _, r in df.iterrows():
    p,m,s = infer_provider_and_method(r)
    providers.append(p)
    methods.append(m)
    storages.append(s)

df['provider'] = providers
df['fetch_method'] = methods
df['storage_path'] = storages

# Backup original catalog
backup = CATALOG.with_suffix('.catalog.backup.' + datetime.utcnow().strftime('%Y%m%dT%H%M%S'))
shutil.copy(CATALOG, backup)

# Write enriched catalog and a copy to analysis_outputs for review
df.to_csv(CATALOG, index=False)
df.to_csv(OUT_DIR / 'catalog_enriched.csv', index=False)

print(f'Wrote enriched catalog to {CATALOG} (backup at {backup})')
print(f'Also wrote analysis_outputs/catalog_enriched.csv')
