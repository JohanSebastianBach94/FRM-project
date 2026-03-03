"""Generate normalized matches and write structured BIS catalog.

1. Read `analysis_outputs/bis_matches.csv` and `data_repository/processed/BIS_catalog.csv`.
2. Map `bis_index` to `bis_series` and compute per-catalog-series ranks.
3. Write `analysis_outputs/bis_matches_normalized.csv`.
4. For each BIS `series`, collect top-3 matching catalog entries and append structured
   columns to the BIS catalog; backup original file and write the updated CSV.
"""
import csv
from pathlib import Path
from datetime import datetime
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / 'analysis_outputs'
BIS_PATH = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'
MATCHS = OUT / 'bis_matches.csv'
NORM = OUT / 'bis_matches_normalized.csv'

def main():
    if not MATCHS.exists():
        print('Missing', MATCHS)
        return 1
    if not BIS_PATH.exists():
        print('Missing', BIS_PATH)
        return 1

    bis = pd.read_csv(BIS_PATH, dtype=str).fillna('')
    # ensure index mapping is consistent with previous runs
    bis = bis.reset_index(drop=True)

    raw = pd.read_csv(MATCHS, dtype=str).fillna('')
    # normalize column names (some runs used different names)
    raw_columns = [c.lower() for c in raw.columns]
    # expected raw columns: series (catalog_series), catalog_entity, bis_index, match_score

    # coerce bis_index to numeric where possible
    raw['bis_index_num'] = pd.to_numeric(raw.get('bis_index', ''), errors='coerce')
    # map to bis.series when bis_index present
    def map_bis_series(idx):
        try:
            if pd.isna(idx):
                return ''
            return str(bis.iloc[int(idx)].get('series',''))
        except Exception:
            return ''

    raw['bis_series'] = raw['bis_index_num'].apply(map_bis_series)
    raw['catalog_series'] = raw.get('series', '')
    raw['catalog_entity'] = raw.get('catalog_entity', '')
    raw['score'] = pd.to_numeric(raw.get('match_score', raw.get('score', '')), errors='coerce').fillna(0.0)

    # compute rank per catalog_series (descending score)
    raw['score_float'] = raw['score'].astype(float)
    raw = raw.sort_values(['catalog_series', 'score_float'], ascending=[True, False])
    raw['rank'] = raw.groupby('catalog_series').cumcount() + 1

    norm_cols = ['bis_series', 'catalog_series', 'catalog_entity', 'score', 'rank']
    norm = raw[norm_cols].copy()
    norm.to_csv(NORM, index=False)
    print('Wrote normalized matches', NORM, 'rows', len(norm))

    # build structured matches per BIS series (top 3 by score)
    # We'll take the best matches for each bis_series by score (rank within catalog_group isn't relevant here)
    top = norm.copy()
    top['score_float'] = pd.to_numeric(top['score'], errors='coerce').fillna(0.0)
    top = top.sort_values(['bis_series', 'score_float'], ascending=[True, False])
    grouped = top.groupby('bis_series')

    # prepare structured columns
    structured = {}
    for bis_series, g in grouped:
        rows = g.head(3).to_dict(orient='records')
        vals = {}
        for i in range(3):
            key_prefix = f'match{i+1}'
            if i < len(rows):
                vals[f'{key_prefix}_catalog_series'] = rows[i].get('catalog_series','')
                vals[f'{key_prefix}_catalog_entity'] = rows[i].get('catalog_entity','')
                vals[f'{key_prefix}_score'] = rows[i].get('score','')
                vals[f'{key_prefix}_rank'] = rows[i].get('rank','')
            else:
                vals[f'{key_prefix}_catalog_series'] = ''
                vals[f'{key_prefix}_catalog_entity'] = ''
                vals[f'{key_prefix}_score'] = ''
                vals[f'{key_prefix}_rank'] = ''
        structured[bis_series] = vals

    # backup original BIS file
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    backup = BIS_PATH.parent / f"BIS_catalog.csv.bak_{ts}"
    BIS_PATH.rename(backup)
    print('Backed up original BIS to', backup)

    # re-load backup (to preserve original order/columns) and append structured cols
    bis = pd.read_csv(backup, dtype=str).fillna('')
    # append columns
    for i in range(1,4):
        bis[f'match{i}_catalog_series'] = ''
        bis[f'match{i}_catalog_entity'] = ''
        bis[f'match{i}_score'] = ''
        bis[f'match{i}_rank'] = ''

    for idx, brow in bis.iterrows():
        key = str(brow.get('series',''))
        if key in structured:
            vals = structured[key]
            for k,v in vals.items():
                bis.at[idx, k] = v

    out_path = BIS_PATH
    bis.to_csv(out_path, index=False)
    print('Wrote structured BIS catalog', out_path)

    # also write a separate structured copy for review
    out_copy = BIS_PATH.parent / 'BIS_catalog.structured.csv'
    bis.to_csv(out_copy, index=False)
    print('Wrote review copy', out_copy)

    return 0

if __name__ == '__main__':
    raise SystemExit(main())
