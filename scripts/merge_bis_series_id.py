#!/usr/bin/env python3
"""
Merge BIS series IDs (series_id) extracted into .meta.json files and catalog.

Finds the latest `analysis_outputs/bis_extracted_titles_strong_*.csv`, uses
`series_id` and `series_key_norm` to populate `bis_internal_id` in metadata
and `BIS_catalog.csv` where missing.

Usage: python scripts/merge_bis_series_id.py
"""
from pathlib import Path
import csv, json, datetime, shutil


def find_latest_extracted():
    p = Path('analysis_outputs')
    files = sorted(p.glob('bis_extracted_titles_strong_*.csv'))
    return files[-1] if files else None


def load_extracted(f):
    d = {}
    with f.open('r', encoding='utf-8', newline='') as fh:
        r = csv.DictReader(fh)
        for row in r:
            sid = (row.get('series_id') or '').strip()
            sk = (row.get('series_key_norm') or '').strip()
            if sid:
                d.setdefault(sk, []).append(sid)
                d.setdefault(sid, []).append(sid)
    return d


def main():
    extracted = find_latest_extracted()
    if not extracted:
        print('No extracted titles CSV found in analysis_outputs'); return 1
    mapping = load_extracted(extracted)

    meta_dir = Path('data_repository/BIS')
    meta_files = sorted(meta_dir.glob('*.meta.json'))
    updated_meta = 0
    for m in meta_files:
        try:
            j = json.loads(m.read_text(encoding='utf-8'))
        except Exception:
            continue
        # try match on series_key_norm in meta
        sk = (j.get('series_key_norm') or '').strip()
        sid_candidates = []
        if sk and sk in mapping:
            sid_candidates = mapping[sk]
        # else try catalog_series
        series = j.get('catalog_series') or j.get('series')
        if series and series in mapping and not sid_candidates:
            sid_candidates = mapping[series]

        sid = None
        if sid_candidates:
            # pick first candidate
            sid = sid_candidates[0]

        if sid and (not j.get('bis_internal_id')):
            j['bis_internal_id'] = sid
            j['bis_internal_id_source'] = 'extracted_payload'
            m.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding='utf-8')
            updated_meta += 1

    # merge into catalog
    catalog_p = Path('data_repository/processed/BIS_catalog.csv')
    if not catalog_p.exists():
        print('No catalog to merge into');
        print({'updated_meta_files': updated_meta});
        return 0

    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    backup = catalog_p.parent / f"{catalog_p.stem}.bak_merge_seriesid_{ts}{catalog_p.suffix}"
    shutil.copy2(catalog_p, backup)

    with catalog_p.open('r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        fns = reader.fieldnames[:]

    if 'bis_internal_id' not in fns:
        fns.append('bis_internal_id')

    updated_rows = 0
    for r in rows:
        cur = (r.get('bis_internal_id') or '').strip()
        if cur:
            continue
        sk = (r.get('series_key_norm') or '').strip()
        sid = None
        if sk and sk in mapping:
            sid = mapping[sk][0]
        elif r.get('series') and r.get('series') in mapping:
            sid = mapping[r.get('series')][0]
        if sid:
            r['bis_internal_id'] = sid
            updated_rows += 1

    with catalog_p.open('w', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=fns)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    report = Path('analysis_outputs') / f'bis_merge_seriesid_report_{ts}.csv'
    report.parent.mkdir(parents=True, exist_ok=True)
    with report.open('w', encoding='utf-8', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['catalog_backup', str(backup), 'updated_meta_files', updated_meta, 'catalog_rows_updated', updated_rows])

    print({'extracted_file': str(extracted), 'updated_meta_files': updated_meta, 'catalog_backup': str(backup), 'catalog_rows_updated': updated_rows, 'report': str(report)})
    return 0


if __name__ == '__main__':
    main()
