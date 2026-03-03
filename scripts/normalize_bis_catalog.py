from pathlib import Path
import pandas as pd
import datetime
import shutil

BASE = Path(__file__).resolve().parents[1]
bis_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'
annot_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.annotated.csv'
matches_path = BASE / 'analysis_outputs' / 'bis_matches.csv'
bis_folder = BASE / 'data_repository' / 'BIS'
out_path = bis_path
report_dir = BASE / 'analysis_outputs'

ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

if not bis_path.exists():
    print('Missing BIS catalog:', bis_path)
    raise SystemExit(1)

df = pd.read_csv(bis_path, dtype=str).fillna('')

# Try to load annotated catalog if present for matched_series_top3
annot = None
if annot_path.exists():
    try:
        annot = pd.read_csv(annot_path, dtype=str).fillna('')
    except Exception:
        annot = None

# Also load matches file if annotated not available
matches = None
if annot is None and matches_path.exists():
    try:
        matches = pd.read_csv(matches_path, dtype=str).fillna('')
    except Exception:
        matches = None

def first_match_from_annot_entry(s):
    # annotated `matched_series_top3` format: entries separated by ';', each entry: series|entity|score|rank|bis_code
    if not s:
        return ''
    parts = str(s).split(';')
    if not parts:
        return ''
    first = parts[0]
    fields = first.split('|')
    if len(fields) >= 1:
        return fields[0]
    return ''

# helpers for column detection
def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

# Determine key columns on source df
series_col = pick_col(df, ['series','series_key','series_key_norm','series_id']) or df.columns[0]
entity_col = pick_col(df, ['entity','entity_name','entity_long','entity_id'])
country_col = pick_col(df, ['country_code','country','cc','iso2'])
bis_flow_col = pick_col(df, ['bis_flow','sector','flow','bis_sector'])
coverage_col = pick_col(df, ['coverage','coverage_ratio','coverage_pct','bis_coverage'])
frequency_col = pick_col(df, ['frequency','freq','series_frequency'])
file_col = pick_col(df, ['file_name','filename','file','csv_file'])

if entity_col is None:
    # try to infer entity from catalog or leave blank
    entity_col = ''
if country_col is None:
    country_col = ''

# Build file name lookup in BIS folder (map stems and lowercased names)
file_map = {}
if bis_folder.exists() and bis_folder.is_dir():
    for p in bis_folder.glob('*.csv'):
        file_map[p.stem.lower()] = p.name
        file_map[p.name.lower()] = p.name

# Prepare output DataFrame
out_cols = ['series','entity','country_code','bis_flow','match','coverage','frequency','file_name']
out = pd.DataFrame(columns=out_cols)

total = len(df)
found_match = 0
found_file = 0
coverage_nonempty = 0
freq_counts = {}

for i, row in df.iterrows():
    series = str(row.get(series_col,'')) if series_col in df.columns else str(row.iloc[0])
    entity = str(row.get(entity_col,'')) if entity_col and entity_col in df.columns else str(row.get('entity','') or '')
    country = str(row.get(country_col,'')) if country_col and country_col in df.columns else str(row.get('country_code','') or '')
    bis_flow = str(row.get(bis_flow_col,'')) if bis_flow_col and bis_flow_col in df.columns else ''
    coverage = str(row.get(coverage_col,'')) if coverage_col and coverage_col in df.columns else ''
    freq = str(row.get(frequency_col,'')) if frequency_col and frequency_col in df.columns else ''

    # Determine top-1 match
    top1 = ''
    # Prefer annotated top3 col
    if annot is not None and 'matched_series_top3' in annot.columns:
        arow = annot[annot.get(series_col, '') == series]
        if not arow.empty:
            mm = arow.iloc[0].get('matched_series_top3','')
            top1 = first_match_from_annot_entry(mm)
    # fallback: if matches table is available and has bis_series field equal to this series, choose highest score
    if not top1 and matches is not None:
        # matches rows may have bis_series and catalog_series
        mrows = matches[(matches.get('bis_series','') == series) | (matches.get('bis_row_sample','').str.startswith(series))]
        if not mrows.empty:
            # pick highest score
            if 'match_score' in mrows.columns:
                mrows['match_score_num'] = pd.to_numeric(mrows['match_score'], errors='coerce').fillna(0)
                mrows = mrows.sort_values('match_score_num', ascending=False)
            top1 = str(mrows.iloc[0].get('catalog_series','') or '')

    if top1:
        found_match += 1

    # Coverage presence
    if coverage:
        coverage_nonempty += 1

    # Frequency mapping: normalize to Y, M, D (prefer Y then M then D)
    f = ''
    ff = freq.strip().upper()
    if ff in ['A','ANNUAL','YEAR','YEARLY','Y']:
        f = 'Y'
    elif ff in ['Q','QUARTERLY']:
        # no Q in target set; map quarterly to M as monthly-level representative
        f = 'M'
    elif ff in ['M','MONTHLY']:
        f = 'M'
    elif ff in ['D','DAILY']:
        f = 'D'
    else:
        # try to infer from coverage or existing columns
        f = ''
    if f:
        freq_counts[f] = freq_counts.get(f,0) + 1

    # Find file name: check explicit file col, then lookup by series stem
    fname = ''
    if file_col and file_col in df.columns and row.get(file_col):
        fname = row.get(file_col,'')
    else:
        # try stems
        fname = file_map.get(series.lower(), '')
        if not fname:
            # try series_key_norm
            sk = row.get('series_key_norm','')
            if sk:
                fname = file_map.get(sk.lower(), '')
    if fname:
        found_file += 1

    out.loc[len(out)] = {
        'series': series,
        'entity': entity,
        'country_code': country,
        'bis_flow': bis_flow,
        'match': top1,
        'coverage': coverage,
        'frequency': f,
        'file_name': fname,
    }

# Backup original catalog
bak = bis_path.with_name(f"BIS_catalog.bak_norm_{ts}.csv")
shutil.copy2(bis_path, bak)

# Write normalized catalog (overwrite original)
out.to_csv(out_path, index=False)

# Write diagnostics
report = {
    'total_rows': total,
    'matches_found': found_match,
    'coverage_nonempty': coverage_nonempty,
    'files_found': found_file,
    'frequency_counts': freq_counts,
    'backup_file': str(bak.name),
    'written_file': str(out_path.name),
}

rep_path = report_dir / f'normalize_bis_catalog_report_{ts}.txt'
with open(rep_path, 'w', encoding='utf-8') as fh:
    fh.write('Normalize BIS Catalog Report\n')
    fh.write('Generated: ' + ts + '\n')
    for k,v in report.items():
        fh.write(f"{k}: {v}\n")

print('Normalization complete.')
print('Report written to', rep_path)
print('Backup of original catalog at', bak)
