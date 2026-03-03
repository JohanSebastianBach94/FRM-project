#!/usr/bin/env python3
"""
Clean synthetic BIS titles created earlier and merge them into the catalog.

Usage:
  python scripts/clean_synth_titles.py --meta-dir data_repository/BIS --catalog data_repository/processed/BIS_catalog.csv

This will back up the catalog, update `.meta.json` files where synthetic titles were found,
and merge cleaned titles into the catalog, producing a merge report.
"""
from pathlib import Path
import json, csv, shutil, datetime, re, sys


def is_synth(j):
    t = (j.get('bis_internal_title') or '')
    return '[SYNTH]' in t or j.get('bis_internal_title_source','').startswith('SYNTH')


def clean_text(s):
    if not s:
        return s
    # remove weird replacement chars and normalize spaces
    s = s.replace('\ufffd', ' ')  # replacement char
    s = s.replace('�', ' ')
    s = re.sub(r'\s+', ' ', s)
    s = s.strip()
    return s


def freq_map(tok):
    tok = (tok or '').lower()
    if tok in ('d','daily'):
        return 'Daily'
    if tok in ('m','monthly'):
        return 'Monthly'
    if tok in ('q','quarterly'):
        return 'Quarterly'
    if tok in ('a','y','annual'):
        return 'Annual'
    return None


def build_clean_title(j, catalog_row):
    # prefer bis_dsd_components
    comps = j.get('bis_dsd_components') or []
    country = None
    indicator = None
    freq = None
    # inspect components
    for c in comps:
        dim = (c.get('dim') or '').lower()
        code = c.get('code')
        label = c.get('label')
        if not country and ('cty' in dim or 'country' in dim or (code and len(code)==3 and code.isupper())):
            country = label or code
        elif not freq and ('freq' in dim or (code and code.lower() in ('d','m','q','a'))):
            freq = freq_map(label) or freq_map(code) or label
        elif not indicator:
            indicator = label or code

    # fallbacks
    if not country and catalog_row:
        country = catalog_row.get('entity') or catalog_row.get('country_code')
    if not freq and catalog_row:
        freq = catalog_row.get('bis_freq')
    if not indicator:
        # try to salvage from existing bis_internal_title
        existing = j.get('bis_internal_title') or ''
        # strip [SYNTH]
        existing = existing.replace('[SYNTH]', '').strip()
        indicator = existing

    parts = [p for p in [country, indicator, freq] if p]
    parts = [clean_text(p) for p in parts]
    # deduplicate adjacent
    out = ' — '.join(parts)
    out = re.sub(r'\s+—\s+—\s+', ' — ', out)
    out = out.strip(' —')
    if out:
        out = f"{out}"
    return out


def main():
    meta_dir = Path('data_repository/BIS')
    catalog_p = Path('data_repository/processed/BIS_catalog.csv')
    meta_files = sorted(meta_dir.glob('*.meta.json'))
    if not meta_files:
        print('No meta files found')
        return 1

    # load catalog mapping by series
    catalog_map = {}
    if catalog_p.exists():
        with catalog_p.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for r in reader:
                catalog_map[r.get('series')] = r

    changed = []
    for m in meta_files:
        try:
            j = json.loads(m.read_text(encoding='utf-8'))
        except Exception:
            continue
        if not is_synth(j):
            continue
        series = j.get('catalog_series') or j.get('series') or m.stem.replace('.meta','')
        catalog_row = catalog_map.get(series)
        new_title = build_clean_title(j, catalog_row)
        if not new_title:
            continue
        old = j.get('bis_internal_title')
        if old and old.strip() == new_title.strip():
            continue
        j['bis_internal_title_old'] = old
        j['bis_internal_title'] = new_title
        j['bis_internal_title_source'] = 'SYNTH_CLEANED'
        m.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding='utf-8')
        changed.append({'meta_file': str(m.name), 'old': old, 'new': new_title})

    # write back to catalog (non-destructive merge)
    if catalog_p.exists():
        # backup
        ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        backup = catalog_p.parent / f"{catalog_p.stem}.bak_synth_clean_{ts}{catalog_p.suffix}"
        shutil.copy2(catalog_p, backup)

        # read, update
        with catalog_p.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames

        if 'bis_internal_title' not in fieldnames:
            fieldnames.append('bis_internal_title')
        if 'bis_internal_id' not in fieldnames:
            fieldnames.append('bis_internal_id')

        mapping = {}
        for m in meta_files:
            try:
                j = json.loads(m.read_text(encoding='utf-8'))
            except Exception:
                continue
            key = j.get('catalog_series') or j.get('series') or m.stem.replace('.meta','')
            mapping[key] = {'bis_internal_id': j.get('bis_internal_id'), 'bis_internal_title': j.get('bis_internal_title'), 'meta_file': str(m)}

        updated = 0
        report_rows = []
        for r in rows:
            series = r.get('series')
            old_id = r.get('bis_internal_id','')
            old_title = r.get('bis_internal_title','')
            mm = mapping.get(series)
            if mm:
                new_id = mm.get('bis_internal_id')
                new_title = mm.get('bis_internal_title')
                changed_flag = False
                if new_id and (not old_id or old_id.strip()==''):
                    r['bis_internal_id'] = new_id; changed_flag = True
                if new_title and (not old_title or old_title.strip()=='' or '[SYNTH]' in old_title or 'Private non-financial sector' in old_title):
                    r['bis_internal_title'] = new_title; changed_flag = True
                if changed_flag:
                    updated += 1
                report_rows.append({'series': series, 'old_title': old_title, 'new_title': r.get('bis_internal_title',''), 'meta_file': mm.get('meta_file')})
            else:
                report_rows.append({'series': series, 'old_title': r.get('bis_internal_title',''), 'new_title': r.get('bis_internal_title',''), 'meta_file': ''})

        # write catalog
        with catalog_p.open('w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        # write report
        rep = Path('analysis_outputs') / f'bis_catalog_synth_clean_{ts}.csv'
        rep.parent.mkdir(parents=True, exist_ok=True)
        with rep.open('w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(f, fieldnames=['series','old_title','new_title','meta_file'])
            w.writeheader()
            for rr in report_rows:
                w.writerow(rr)

        print({'changed_meta_files': len(changed), 'catalog_backup': str(backup), 'catalog_rows_updated': updated, 'report': str(rep)})
    else:
        print('No catalog found to merge into')

    return 0


if __name__ == '__main__':
    sys.exit(main())
