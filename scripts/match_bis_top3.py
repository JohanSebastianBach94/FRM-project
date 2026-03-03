from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
in_path = BASE / 'analysis_outputs' / 'bis_matches.csv'
out_path = BASE / 'analysis_outputs' / 'bis_matches_top3.csv'

if not in_path.exists():
    print('Input not found:', in_path)
    raise SystemExit(1)

df = pd.read_csv(in_path, dtype=str).fillna('')
df['match_score'] = pd.to_numeric(df['match_score'], errors='coerce').fillna(0.0)

rows = []
for series, g in df.groupby('series'):
    g2 = g.sort_values('match_score', ascending=False).head(3).reset_index(drop=True)
    out = {'series': series}
    # copy catalog_entity from first row if available
    out['catalog_entity'] = g2.loc[0, 'catalog_entity'] if len(g2) > 0 else ''
    for i in range(3):
        if i < len(g2):
            r = g2.loc[i]
            suffix = f'_{i+1}'
            out[f'bis_index{suffix}'] = r.get('bis_index','')
            out[f'bis_row_sample{suffix}'] = r.get('bis_row_sample','')
            out[f'match_score{suffix}'] = r.get('match_score',0)
            out[f'matched_tokens_count{suffix}'] = r.get('matched_tokens_count','')
            out[f'matched_tokens{suffix}'] = r.get('matched_tokens','')
        else:
            suffix = f'_{i+1}'
            out[f'bis_index{suffix}'] = ''
            out[f'bis_row_sample{suffix}'] = ''
            out[f'match_score{suffix}'] = ''
            out[f'matched_tokens_count{suffix}'] = ''
            out[f'matched_tokens{suffix}'] = ''
    rows.append(out)

out_df = pd.DataFrame(rows)
out_df.to_csv(out_path, index=False)
print('Wrote', out_path, 'rows', len(out_df))
