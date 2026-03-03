from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
bis_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'
matches_path = BASE / 'analysis_outputs' / 'bis_matches.csv'
catalog_path = BASE / 'catalog.csv'
out_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.annotated.csv'

if not bis_path.exists():
    print('Missing BIS file:', bis_path)
    raise SystemExit(1)
if not matches_path.exists():
    print('Missing matches file:', matches_path)
    raise SystemExit(1)
if not catalog_path.exists():
    print('Missing catalog file:', catalog_path)
    raise SystemExit(1)

bis = pd.read_csv(bis_path, dtype=str).fillna('')
matches = pd.read_csv(matches_path, dtype=str).fillna('')
catalog = pd.read_csv(catalog_path, dtype=str).fillna('')

# ensure match_score numeric
if 'match_score' in matches.columns:
    matches['match_score'] = pd.to_numeric(matches['match_score'], errors='coerce').fillna(0.0)
else:
    matches['match_score'] = 0.0

# older code expected an integer bis index; current matches CSV provides `bis_series` keys
# so we'll map by `bis_series` (or derive it from `bis_row_sample`) below.

# Build mapping: bis_series (code) -> list of (catalog_series, catalog_entity, score)
grouped = {}
for _, r in matches.iterrows():
    bi = (r.get('bis_series') or '').strip()
    if not bi:
        # try bis_row_sample field which often contains code at start
        br = (r.get('bis_row_sample') or '')
        bi = br.split('|',1)[0].strip()
    if not bi:
        continue
    grouped.setdefault(bi, []).append((r.get('catalog_series') or r.get('catalog_series'), r.get('catalog_entity',''), float(r.get('match_score',0) or 0)))

# For each BIS row, build top-3 where country matches between catalog and bis
out_rows = []
out_rows_ids = []
for idx, brow in bis.reset_index().iterrows():
    entries = []
    entries_ids = []
    bis_cc = str(brow.get('country_code','')).strip()
    # find candidates by BIS series code (use `series` column as bis code)
    bis_code_key = str(brow.get('series','')).strip()
    candidates = grouped.get(bis_code_key, [])
    # sort candidates by score desc
    candidates.sort(key=lambda x: x[2], reverse=True)
    rank = 1
    for s, ent, score in candidates:
        # find catalog row for s to get country
        crow = catalog[catalog['series'] == s]
        crow_cc = ''
        crow_ent = ent
        if not crow.empty:
            crow_cc = str(crow.iloc[0].get('country_code','')).strip()
            crow_ent = str(crow.iloc[0].get('entity', ent))
        # include only if country matches (non-empty and equal)
        if bis_cc and crow_cc and bis_cc.upper() == crow_cc.upper():
            # include the BIS series code (if present) from the BIS row
            bis_code = str(brow.get('bis_internal_id','')).strip()
            entries.append(f"{s}|{crow_ent}|{score:.4f}|{rank}|{bis_code}")
            entries_ids.append(bis_code)
            rank += 1
        if rank > 3:
            break
    out_rows.append(';'.join(entries))
    out_rows_ids.append(';'.join([e for e in entries_ids if e]))

# attach new column
# attach new columns
bis['matched_series_top3'] = out_rows
bis['matched_series_top3_bis_ids'] = out_rows_ids

# write annotated file (do not overwrite original)
bis.to_csv(out_path, index=False)
print('Wrote annotated BIS file:', out_path)
