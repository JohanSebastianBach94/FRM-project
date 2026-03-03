from pathlib import Path
import pandas as pd
import datetime

BASE = Path(__file__).resolve().parents[1]
norm_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'
annot_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.annotated.csv'
matches_path = BASE / 'analysis_outputs' / 'bis_matches.csv'
backup_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.bak_norm_20251218T133145Z.csv'
out_dir = BASE / 'analysis_outputs'
ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
out_path = out_dir / f'diagnose_match_without_flow_{ts}.txt'

df = pd.read_csv(norm_path, dtype=str).fillna('')
annot = pd.read_csv(annot_path, dtype=str).fillna('') if annot_path.exists() else None
matches = pd.read_csv(matches_path, dtype=str).fillna('') if matches_path.exists() else None

mask = (df['match'].str.strip() != '') & (df['bis_flow'].str.strip() == '')
bad = df[mask]

with open(out_path, 'w', encoding='utf-8') as fh:
    fh.write(f'Diagnostic: matches present but bis_flow empty\nGenerated: {ts}\n')
    fh.write(f'Total rows in normalized catalog: {len(df)}\n')
    fh.write(f'Rows with match and no bis_flow: {len(bad)}\n\n')

    sample = bad.head(30)
    for i, r in sample.iterrows():
        fh.write('---\n')
        fh.write(f'index: {i}\n')
        fh.write(f"series: {r['series']}\n")
        fh.write(f"entity: {r['entity']}\n")
        fh.write(f"country_code: {r['country_code']}\n")
        fh.write(f"match: {r['match']}\n")
        fh.write(f"coverage: {r['coverage']}\n")
        fh.write(f"file_name: {r['file_name']}\n")

        # provenance: annotated
        prov = []
        if annot is not None:
            # find row in annotated by series
            arows = annot[annot.get('series','') == r['series']]
            if not arows.empty:
                prov.append('annotated: ' + str(arows.iloc[0].get('matched_series_top3','')))

        if matches is not None:
            # find rows in matches where catalog_series == match OR bis_row_sample startswith series
            mrows = matches[(matches.get('catalog_series','') == r['match']) | (matches.get('bis_row_sample','').str.startswith(r['series']))]
            if not mrows.empty:
                # include top 3 matching rows by score if present
                if 'match_score' in mrows.columns:
                    mrows['match_score_num'] = pd.to_numeric(mrows['match_score'], errors='coerce').fillna(0)
                    mrows = mrows.sort_values('match_score_num', ascending=False)
                prov.append('matches_rows_count: ' + str(len(mrows)))
                prov.append('matches_sample: ' + str(mrows.head(3).to_dict(orient='records')))

        if not prov:
            prov_text = 'provenance: NONE FOUND'
        else:
            prov_text = '\n'.join(prov)
        fh.write(prov_text + '\n')

    fh.write('\nEnd of report\n')

print('Diagnostic written to', out_path)
