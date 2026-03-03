#!/usr/bin/env python3
"""
Resolve BIS series titles using SDMX DSD codelists and Series elements.

Usage:
  python scripts/resolve_bis_titles.py --meta-dir data_repository/BIS --raw-dir data_repository/raw/bis_api --catalog data_repository/processed/BIS_catalog.csv [--dry-run]

Outputs:
  - updates `*.meta.json` with `bis_internal_title`, `bis_internal_title_source`, and `bis_dsd_components` (unless --dry-run)
  - backup of catalog before merge (when not dry-run)
  - merge report in `analysis_outputs/bis_sdmx_dsd_lookup_<TS>.csv`
  - optional review CSV for ambiguous cases

This is designed to be robust to namespaces and multiple SDMX encodings.
"""
import argparse
from pathlib import Path
import xml.etree.ElementTree as ET
import json
import csv
import shutil
import datetime
import re
import sys


def local(tag):
    return tag.split('}')[-1] if '}' in tag else tag


def find_all(root_el, local_name):
    for el in root_el.iter():
        if local(el.tag).lower() == local_name.lower():
            yield el


def parse_codelists(root_el):
    codelists = {}
    labels_by_code = {}
    code_to_codelists = {}
    for cl in find_all(root_el, 'codelist'):
        cl_id = cl.attrib.get('id') or cl.attrib.get('Codelist') or cl.attrib.get('codeList') or ''
        if not cl_id:
            cl_id = cl.attrib.get('agencyID') or cl.attrib.get('name') or ''
        mapping = {}
        for code_el in cl:
            if local(code_el.tag).lower() not in ('code', 'codelistitem'):
                continue
            code_val = code_el.attrib.get('value') or code_el.attrib.get('id') or code_el.attrib.get('code')
            if not code_val:
                continue
            label = None
            # prefer english name/label
            for child in code_el:
                ctag = local(child.tag).lower()
                if ctag in ('name', 'label', 'description'):
                    lang = child.attrib.get('{http://www.w3.org/XML/1998/namespace}lang', '')
                    if lang.startswith('en'):
                        label = (child.text or '').strip()
                        break
                    if not label and (child.text and child.text.strip()):
                        label = child.text.strip()
            if not label:
                label = code_el.attrib.get('label') or code_el.attrib.get('name')
            mapping[code_val] = label
            code_to_codelists.setdefault(code_val, set()).add(cl_id)
            if code_val not in labels_by_code and label:
                labels_by_code[code_val] = label
        if mapping:
            codelists[cl_id] = mapping
    return codelists, code_to_codelists, labels_by_code


def resolve_code(code_val, dim_name, codelists, code_to_codelists, labels_by_code):
    if not code_val:
        return None
    dim_low = (dim_name or '').lower()
    if dim_low:
        for clid, mapping in codelists.items():
            if dim_low in clid.lower() or clid.lower() in dim_low:
                if code_val in mapping and mapping[code_val]:
                    return mapping[code_val]
    if code_val in labels_by_code:
        return labels_by_code[code_val]
    for clid, mapping in codelists.items():
        if code_val in mapping and mapping[code_val]:
            return mapping[code_val]
    return None


def extract_series_key_values(series_el):
    vals = []
    # gather Value children anywhere under series
    for child in series_el.iter():
        if local(child.tag).lower() == 'value' and child.attrib.get('value'):
            dim = child.attrib.get('concept') or child.attrib.get('id') or child.attrib.get('name') or ''
            code = child.attrib.get('value')
            vals.append((dim, code))
    return vals


def extract_explicit_labels(series_el):
    labels = []
    for child in series_el:
        ctag = local(child.tag).lower()
        if ctag in ('name', 'label', 'title', 'description') and (child.text and child.text.strip()):
            labels.append(child.text.strip())
        for sub in child.iter():
            stag = local(sub.tag).lower()
            if stag in ('name', 'label', 'title', 'text', 'description') and (sub.text and sub.text.strip()):
                labels.append(sub.text.strip())
    # prefer english-like entries if possible by simple heuristic
    dedup = []
    seen = set()
    for l in labels:
        nl = ' '.join(l.split())
        if nl and nl not in seen:
            seen.add(nl)
            dedup.append(nl)
    return dedup


def normalize_tokens(s):
    parts = re.split(r"[^0-9A-Za-z]+", (s or '').lower())
    return [p for p in parts if p]


def assemble_title_from_components(components):
    # components: list of (dim, code, label) - prefer labels when present
    parts = []
    for dim, code, label in components:
        if label:
            parts.append(label)
        else:
            parts.append(code)
    return ' — '.join(parts) if parts else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--meta-dir', required=True)
    p.add_argument('--raw-dir', required=True)
    p.add_argument('--catalog', required=True)
    p.add_argument('--dry-run', action='store_true')
    args = p.parse_args()

    meta_dir = Path(args.meta_dir)
    raw_dir = Path(args.raw_dir)
    catalog_p = Path(args.catalog)

    meta_files = sorted(meta_dir.glob('*.meta.json'))
    if not meta_files:
        print('No meta files found in', meta_dir)
        return 1

    # load and parse each referenced raw xml and build codelist maps
    xml_cache = {}
    codelists_by_file = {}
    labels_by_file = {}
    codeindex_by_file = {}

    for m in meta_files:
        try:
            j = json.loads(m.read_text(encoding='utf-8'))
        except Exception:
            continue
        raw = j.get('raw_source_file')
        if not raw:
            continue
        pth = Path(raw)
        if not pth.exists():
            pth = raw_dir / pth.name
        if not pth.exists():
            # try relative
            pth = Path(raw)
        if not pth.exists():
            print('raw file not found for', m, raw)
            continue
        if str(pth) in xml_cache:
            continue
        try:
            tree = ET.parse(str(pth))
            root_el = tree.getroot()
        except Exception as e:
            print('Failed parse', pth, e)
            xml_cache[str(pth)] = None
            continue
        xml_cache[str(pth)] = root_el
        codelists, codeindex, labels_by_code = parse_codelists(root_el)
        codelists_by_file[str(pth)] = codelists
        codeindex_by_file[str(pth)] = codeindex
        labels_by_file[str(pth)] = labels_by_code

    report_rows = []
    ambiguous = []
    changed_meta = 0

    for m in meta_files:
        try:
            j = json.loads(m.read_text(encoding='utf-8'))
        except Exception:
            continue
        raw = j.get('raw_source_file')
        if not raw:
            continue
        pth = Path(raw)
        if not pth.exists():
            pth = raw_dir / pth.name
        if not pth.exists():
            pth = Path(raw)
        root_el = xml_cache.get(str(pth))
        if root_el is None:
            report_rows.append({'meta_file': str(m.name), 'reason': 'no_raw'})
            continue

        series_key_norm = j.get('series_key_norm') or j.get('series_key') or ''
        key_tokens = normalize_tokens(series_key_norm)
        catalog_title = (j.get('catalog_bis_title') or '').strip()

        best_score = -999
        best_label = None
        best_components = None

        # iterate Series elements
        for s in root_el.iter():
            if local(s.tag).lower() != 'series':
                continue
            explicit = extract_explicit_labels(s)
            values = extract_series_key_values(s)
            sig = []
            for dim, code in values:
                if dim: sig.append(dim.lower())
                if code: sig.append(code.lower())
            score = 0
            # match key tokens
            if key_tokens:
                for kt in key_tokens:
                    if kt in sig:
                        score += 3
            # explicit label boost
            if explicit:
                score += 10
            # catalog title substring
            for e in explicit:
                if catalog_title and catalog_title.lower() in e.lower():
                    score += 5
            # resolve code labels
            codelists = codelists_by_file.get(str(pth), {})
            codeindex = codeindex_by_file.get(str(pth), {})
            labels_index = labels_by_file.get(str(pth), {})
            comp_labels = []
            for dim, code in values:
                resolved = resolve_code(code, dim, codelists, codeindex, labels_index)
                comp_labels.append((dim, code, resolved))
                if resolved:
                    score += 1

            # choose candidate label
            if explicit:
                candidate = explicit[0]
            else:
                candidate = assemble_title_from_components(comp_labels)

            if candidate and score > best_score:
                best_score = score
                best_label = candidate
                best_components = comp_labels

        # if nothing matched well, try code-based lookup from tokens
        if not best_label and key_tokens:
            labels_index = labels_by_file.get(str(pth), {})
            found = []
            for kt in key_tokens:
                lab = labels_index.get(kt.upper()) or labels_index.get(kt)
                if lab:
                    found.append(lab)
            if found:
                best_label = ' — '.join(found)
                best_components = []

        old_title = (j.get('bis_internal_title') or '').strip()
        chosen_source = None
        if best_label and (not old_title or old_title.strip() != best_label.strip()):
            chosen_source = 'SDMX_DSD_RESOLVED' if not extract_explicit_labels(s) else 'SDMX_EXPLICIT'
            j['bis_internal_title'] = best_label
            j['bis_internal_title_source'] = chosen_source
            j['bis_dsd_components'] = [{'dim': d, 'code': c, 'label': l} for (d, c, l) in (best_components or [])]
            if not j.get('bis_internal_id'):
                j['bis_internal_id'] = f"DSD::{pth.name}::{abs(hash(best_label))%1000000}"
            if not args.dry_run:
                m.write_text(json.dumps(j, ensure_ascii=False, indent=2), encoding='utf-8')
            changed_meta += 1
            report_rows.append({'meta_file': str(m.name), 'old_title': old_title, 'new_title': best_label, 'source': chosen_source})
        else:
            report_rows.append({'meta_file': str(m.name), 'old_title': old_title, 'new_title': old_title, 'source': 'UNCHANGED'})

    # merge into catalog non-destructively (only when not dry-run)
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    report_p = Path('analysis_outputs') / f'bis_sdmx_dsd_lookup_{ts}.csv'
    report_p.parent.mkdir(parents=True, exist_ok=True)
    with report_p.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['meta_file', 'old_title', 'new_title', 'source'])
        writer.writeheader()
        for r in report_rows:
            writer.writerow(r)

    if not args.dry_run:
        # backup catalog
        backup = Path(catalog_p.parent) / f"{catalog_p.stem}.bak_dsd_{ts}{catalog_p.suffix}"
        shutil.copy2(catalog_p, backup)
        # now merge
        with catalog_p.open('r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames
        if 'bis_internal_title' not in fieldnames:
            fieldnames.append('bis_internal_title')
        if 'bis_internal_id' not in fieldnames:
            fieldnames.append('bis_internal_id')

        # build mapping from meta files
        mapping = {}
        for m in meta_files:
            try:
                j = json.loads(m.read_text(encoding='utf-8'))
            except Exception:
                continue
            key = j.get('catalog_series') or j.get('series') or Path(m).stem.replace('.meta', '')
            mapping[key] = {'bis_internal_id': j.get('bis_internal_id'), 'bis_internal_title': j.get('bis_internal_title'), 'meta_file': str(m)}

        updated = 0
        merge_rows = []
        for r in rows:
            series_key = r.get('series')
            old_id = r.get('bis_internal_id', '')
            old_title = r.get('bis_internal_title', '')
            mm = mapping.get(series_key)
            if mm:
                new_id = mm.get('bis_internal_id')
                new_title = mm.get('bis_internal_title')
                changed = False
                if new_id and (not old_id or old_id.strip() == ''):
                    r['bis_internal_id'] = new_id
                    changed = True
                if new_title and (not old_title or old_title.strip() == '' or old_title.strip() == 'no-internal-found'):
                    r['bis_internal_title'] = new_title
                    changed = True
                if changed:
                    updated += 1
                merge_rows.append({'series': series_key, 'old_bis_internal_id': old_id, 'old_bis_internal_title': old_title, 'new_bis_internal_id': r.get('bis_internal_id',''), 'new_bis_internal_title': r.get('bis_internal_title',''), 'meta_file': mm.get('meta_file')})
            else:
                merge_rows.append({'series': series_key, 'old_bis_internal_id': r.get('bis_internal_id',''), 'old_bis_internal_title': r.get('bis_internal_title',''), 'new_bis_internal_id': r.get('bis_internal_id',''), 'new_bis_internal_title': r.get('bis_internal_title',''), 'meta_file': ''})

        # write updated catalog
        with catalog_p.open('w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        # write merge report
        merge_p = Path('analysis_outputs') / f'bis_sdmx_dsd_lookup_merge_{ts}.csv'
        with merge_p.open('w', encoding='utf-8', newline='') as f:
            rn = ['series', 'old_bis_internal_id', 'old_bis_internal_title', 'new_bis_internal_id', 'new_bis_internal_title', 'meta_file']
            w = csv.DictWriter(f, fieldnames=rn)
            w.writeheader()
            for rr in merge_rows:
                w.writerow(rr)

        print(f'Merge complete. catalog backup: {backup} rows updated: {updated} merge_report: {merge_p}')
    else:
        print(f'Dry-run complete. report: {report_p} meta_changed: {changed_meta}')

    return 0


if __name__ == '__main__':
    sys.exit(main())
