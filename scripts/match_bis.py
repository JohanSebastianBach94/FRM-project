"""Match catalog series to BIS catalog and write normalized matches.

Outputs:
- `analysis_outputs/bis_matches.csv` (detailed raw matches)
- `analysis_outputs/bis_matches_normalized.csv` (normalized table with one row per bis-catalog match)
- `analysis_outputs/bis_matches_summary.csv` (best score per catalog_series)

The matcher is a lightweight combination of token overlap, n-gram (character) Jaccard,
and a SequenceMatcher fuzzy ratio. The output includes provenance (git rev + timestamp).
"""

import subprocess
from datetime import datetime
from pathlib import Path
import re
import pandas as pd
from difflib import SequenceMatcher


BASE = Path(__file__).resolve().parents[1]
cat_path = BASE / 'catalog.csv'
bis_path = BASE / 'data_repository' / 'processed' / 'BIS_catalog.csv'
out_dir = BASE / 'analysis_outputs'
out_dir.mkdir(exist_ok=True)


def load_csv(path: Path):
    if not path.exists():
        print(f"Missing file: {path}")
        return None
    return pd.read_csv(path, dtype=str).fillna('')


cat = load_csv(cat_path)
if cat is None:
    raise SystemExit(1)
bis = load_csv(bis_path)
if bis is None:
    raise SystemExit(1)


bis['__text'] = bis.astype(str).agg(' '.join, axis=1).str.lower()


def token_list_from_row(r):
    fields = ['series', 'entity', 'instrument', 'short_name', 'description', 'unit', 'source', 'topic_keywords', 'extra_keywords']
    parts = []
    for f in fields:
        if f in r and pd.notna(r.get(f, '')):
            parts.append(str(r.get(f, '')).lower())
    text = ' '.join(parts)
    toks = [t for t in re.split(r"[^a-z0-9]+", text) if t and len(t) > 1]
    kws = []
    for f in ['topic_keywords', 'extra_keywords']:
        if f in r and pd.notna(r.get(f, '')):
            kws += [t.strip().lower() for t in str(r.get(f, '')).split(',') if t.strip()]
    toks = list(dict.fromkeys(toks + kws))
    return toks


def ngram_set(s, n=3):
    s = re.sub(r"\s+", ' ', s)
    s = re.sub(r"[^a-z0-9 ]", '', s)
    s = s.replace(' ', '_')
    return set([s[i:i + n] for i in range(max(0, len(s) - n + 1))])


def match_score(tokens, bis_text):
    tokens = [t for t in tokens if t]
    if not tokens:
        return 0.0, []
    matched = [t for t in tokens if t in bis_text]
    token_overlap = len(matched) / max(1, len(tokens))

    fuzzy_max = 0.0
    for t in tokens:
        r = SequenceMatcher(None, t, bis_text).ratio()
        if r > fuzzy_max:
            fuzzy_max = r

    token_blob = ' '.join(tokens)
    a = ngram_set(token_blob[:1000], n=3)
    b = ngram_set(bis_text[:2000], n=3)
    jaccard = 0.0
    if a or b:
        jaccard = len(a & b) / max(1, len(a | b))

    score = 3.0 * token_overlap + 1.5 * jaccard + 1.0 * fuzzy_max
    return float(score), matched


# provenance
try:
    git_rev = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=BASE, stderr=subprocess.DEVNULL).decode().strip()
except Exception:
    git_rev = ''

now = datetime.utcnow().isoformat()

rows = []
norm_rows = []

for _, crow in cat.iterrows():
    tokens = token_list_from_row(crow)
    candidates = []
    # iterate BIS rows with access to their fields so we can prefer country/instrument matches
    for j, brow in bis.iterrows():
        brow_text = brow.get('__text', '')
        s, matched = match_score(tokens, brow_text)
        if s <= 0:
            continue
        # prefer exact country matches and penalize obvious country mismatches
        bis_cc = str(brow.get('country_code', '')).strip()
        cat_cc = str(crow.get('country_code', '')).strip()
        country_match = (bis_cc != '' and cat_cc != '' and bis_cc.upper() == cat_cc.upper())
        country_mismatch = (bis_cc != '' and cat_cc != '' and bis_cc.upper() != cat_cc.upper())
        # stronger preference for same-country matches; larger penalty for mismatch
        if country_match:
            s += 5.0
        elif country_mismatch:
            s -= 4.0

        # prefer frequency match
        bis_freq = str(brow.get('bis_freq', '')).strip().lower()
        cat_freq = str(crow.get('frequency_label', '')).strip().lower()
        if bis_freq and cat_freq and bis_freq == cat_freq:
            s += 1.0

        # simple instrument match: if catalog instrument token appears in BIS row text
        inst = str(crow.get('instrument', '')).lower()
        inst_match = False
        if inst and inst in brow_text:
            inst_match = True
            s += 1.0

        # ensure non-negative
        if s < 0:
            s = 0.0

        if s > 0:
            candidates.append((s, j, matched))

    if candidates:
        candidates.sort(reverse=True, key=lambda x: x[0])
        for rank, (s, j, matched) in enumerate(candidates[:5], start=1):
            brow = bis.iloc[j]
            sample = ' | '.join([str(brow.get(c, '')) for c in bis.columns[:4]])
            rows.append({
                'catalog_series': crow.get('series', ''),
                'catalog_entity': crow.get('entity', ''),
                'bis_series': brow.get('series', ''),
                'bis_row_sample': sample[:400],
                'score': round(s, 6),
                'matched_tokens_count': len(matched),
                'matched_tokens': ';'.join(matched)
            })

            bis_cc = str(brow.get('country_code', '')).strip()
            cat_cc = str(crow.get('country_code', '')).strip()
            country_match = (bis_cc != '' and cat_cc != '' and bis_cc.upper() == cat_cc.upper())

            bis_start = pd.to_datetime(brow.get('bis_start', ''), errors='coerce')
            bis_end = pd.to_datetime(brow.get('bis_end', ''), errors='coerce')
            catalog_last = pd.to_datetime(crow.get('last_observation', ''), errors='coerce')

            bis_duration_years = ''
            catalog_last_in_bis_window = False
            if pd.notna(bis_start) and pd.notna(bis_end):
                try:
                    bis_duration_years = (bis_end - bis_start).days / 365.0
                except Exception:
                    bis_duration_years = ''
                if pd.notna(catalog_last):
                    catalog_last_in_bis_window = (catalog_last >= bis_start) and (catalog_last <= bis_end)

            bis_freq = str(brow.get('bis_freq', '')).strip().lower()
            cat_freq = str(crow.get('frequency_label', '')).strip().lower()
            frequency_match = (bis_freq != '' and cat_freq != '' and bis_freq == cat_freq)

            norm_rows.append({
                'bis_series': brow.get('series', ''),
                'catalog_series': crow.get('series', ''),
                'catalog_entity': crow.get('entity', ''),
                'score': round(s, 6),
                'rank': rank,
                'country_match': country_match,
                'bis_start': str(brow.get('bis_start', '')),
                'bis_end': str(brow.get('bis_end', '')),
                'bis_duration_years': bis_duration_years,
                'catalog_last_observation': str(crow.get('last_observation', '')),
                'catalog_last_in_bis_window': catalog_last_in_bis_window,
                'frequency_match': frequency_match,
                'match_method': 'token+ngram+jaccard+fuzzy',
                'match_date': now,
                'matcher_version': git_rev,
            })
    else:
        rows.append({'catalog_series': crow.get('series', ''), 'catalog_entity': crow.get('entity', ''), 'bis_series': '', 'bis_row_sample': '', 'score': 0.0, 'matched_tokens_count': 0, 'matched_tokens': ''})


out_df = pd.DataFrame(rows)
out_path = out_dir / 'bis_matches.csv'
out_df.to_csv(out_path, index=False)
print('Wrote', out_path, 'rows', len(out_df))

norm_df = pd.DataFrame(norm_rows)
norm_path = out_dir / 'bis_matches_normalized.csv'
norm_df.to_csv(norm_path, index=False)
print('Wrote normalized matches', norm_path, 'rows', len(norm_df))

summary = out_df.groupby('catalog_series')['score'].max().reset_index().rename(columns={'score': 'best_score'})
summary_path = out_dir / 'bis_matches_summary.csv'
summary.to_csv(summary_path, index=False)
print('Wrote summary', summary_path)
