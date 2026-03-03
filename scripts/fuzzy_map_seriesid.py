#!/usr/bin/env python3
"""
Fuzzy map extracted BIS series IDs to existing `.meta.json` files using token overlap.

Writes a report and (optionally) updates meta files and catalog when confident.
Run with no flags to do a dry-run; set WRITE=1 at top to apply changes.
"""
from pathlib import Path
import csv, json, datetime, shutil, re

WRITE = 1
THRESHOLD = 0.6  # fraction overlap to consider confident


def tokens(s):
    if not s: return set()
    return set([t.strip().lower() for t in re.split(r'[^A-Za-z0-9]+', s) if t.strip()])


def find_latest_extracted():
    p = Path('analysis_outputs')
    files = sorted(p.glob('bis_extracted_titles_strong_*.csv'))
    return files[-1] if files else None


def load_extracted(f):
    rows = []
    with f.open('r', encoding='utf-8', newline='') as fh:
        r = csv.DictReader(fh)
        for row in r:
            sk = (row.get('series_key_norm') or '').strip()
            sid = (row.get('series_id') or '').strip()
            cand = (row.get('candidate_title') or '').strip()
            rows.append({'series_key_norm': sk, 'series_id': sid, 'candidate_title': cand})
    return rows


def main():
    ex = find_latest_extracted()
    if not ex:
        print('no extracted file'); return 1
    extracted = load_extracted(ex)

    meta_dir = Path('data_repository/BIS')
    metas = sorted(meta_dir.glob('*.meta.json'))
    report = []
    updated = 0
    for m in metas:
        try:
            j = json.loads(m.read_text(encoding='utf-8'))
        except Exception:
            continue
        sk_meta = (j.get('series_key_norm') or '').strip()
        best = None
        best_score = 0.0
        tok_meta = tokens(sk_meta)
        for row in extracted:
            sk = row.get('series_key_norm')
            if not sk: continue
            tok_ex = tokens(sk)
            if not tok_meta and not tok_ex:
                continue
            inter = tok_meta.intersection(tok_ex)
            union = tok_meta.union(tok_ex)
            score = (len(inter)/len(union)) if union else 0.0
            if score > best_score:
                best_score = score
                best = row
        if best and best_score >= THRESHOLD:
            # confident mapping
            old = j.get('bis_internal_id')
            if not old or old.strip()=='' or old!=best['series_id']:
                report.append({'meta_file': str(m.name), 'meta_series_key': sk_meta, 'mapped_series_id': best['series_id'], 'score': best_score})
                if WRITE:
                    j['bis_internal_id'] = best['series_id']
                    j['bis_internal_id_source'] = 'fuzzy_extracted'
                    m.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding='utf-8')
                    updated += 1

    # merge into catalog similar to previous script
    catalog_p = Path('data_repository/processed/BIS_catalog.csv')
    if catalog_p.exists() and WRITE and updated>0:
        ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        backup = catalog_p.parent / f"{catalog_p.stem}.bak_fuzzy_map_{ts}{catalog_p.suffix}"
        shutil.copy2(catalog_p, backup)
        with catalog_p.open('r', encoding='utf-8', newline='') as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)
            fns = reader.fieldnames[:]
        if 'bis_internal_id' not in fns:
            fns.append('bis_internal_id')
        # build mapping from meta files
        mapping = {}
        for m in metas:
            try:
                j = json.loads(m.read_text(encoding='utf-8'))
            except Exception:
                continue
            key = j.get('catalog_series') or j.get('series') or ''
            if j.get('bis_internal_id'):
                mapping[key] = j.get('bis_internal_id')
        updated_rows = 0
        for r in rows:
            if r.get('bis_internal_id'):
                continue
            key = r.get('series')
            if key and key in mapping:
                r['bis_internal_id'] = mapping[key]
                updated_rows += 1
        with catalog_p.open('w', encoding='utf-8', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=fns)
            w.writeheader()
            for r in rows:
                w.writerow(r)
    else:
        backup = None
        updated_rows = 0

    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    out = Path('analysis_outputs') / f'bis_fuzzy_map_report_{ts}.csv'
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open('w', encoding='utf-8', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=['meta_file','meta_series_key','mapped_series_id','score'])
        w.writeheader()
        for r in report:
            w.writerow(r)
    print({'report': str(out), 'mapped_meta_files': len(report), 'updated_meta_files': updated, 'catalog_backup': str(backup) if backup else '', 'catalog_rows_updated': updated_rows})
    return 0


if __name__ == '__main__':
    main()
