"""Update `data_repository/processed/BIS_catalog.csv` using canonical values from `catalog.csv`.

For series present in both files, update BIS fields:
- `entity`, `country_code`, `instrument` (from `catalog.csv`)
- add `coverage_ratio` column populated from `catalog.csv` (if present)

Backs up original BIS file before writing.
"""
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
CAT = BASE / 'catalog.csv'
BIS = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'

def main():
    if not CAT.exists():
        print('Missing', CAT)
        return 1
    if not BIS.exists():
        print('Missing', BIS)
        return 1

    cat = pd.read_csv(CAT, dtype=str).fillna('')
    bis = pd.read_csv(BIS, dtype=str).fillna('')

    # ensure we can find rows by series
    bis_idx = {s: i for i, s in enumerate(bis['series'].astype(str))}

    updated = 0
    added_coverage = 0
    for _, crow in cat.iterrows():
        s = str(crow.get('series', ''))
        if s in bis_idx:
            i = bis_idx[s]
            # update entity, country_code, instrument
            entity = crow.get('entity', '')
            country = crow.get('country_code', '')
            instrument = crow.get('instrument', '')
            # Only update if non-empty to avoid overwriting useful BIS content with blanks
            if entity:
                bis.at[i, 'entity'] = entity
            if country:
                bis.at[i, 'country_code'] = country
            if instrument:
                bis.at[i, 'instrument'] = instrument
            # coverage_ratio: add column if missing
            if 'coverage_ratio' not in bis.columns:
                bis['coverage_ratio'] = ''
            cov = crow.get('coverage_ratio', '')
            if cov:
                bis.at[i, 'coverage_ratio'] = cov
                added_coverage += 1
            updated += 1

    # backup original
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    backup = BIS.parent / f'BIS_catalog.csv.bak_update_{ts}'
    BIS.rename(backup)

    # write updated BIS
    bis.to_csv(BIS, index=False)
    # also write a review copy
    review = BIS.parent / 'BIS_catalog.updated_review.csv'
    bis.to_csv(review, index=False)

    print('Backed up original to', backup)
    print('Wrote updated BIS to', BIS)
    print('Wrote review copy to', review)
    print('Rows updated:', updated)
    print('Coverage_ratio values written:', added_coverage)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
