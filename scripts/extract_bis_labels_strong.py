#!/usr/bin/env python3
"""
Extract series labels and codelist labels from saved BIS SDMX XML payloads.

Writes a CSV to `analysis_outputs/bis_extracted_titles_strong_<ts>.csv` with candidate
titles and provenance to help build stronger title detection heuristics.
"""
from pathlib import Path
import xml.etree.ElementTree as ET
import json, csv, datetime, re


def ns_strip(tag):
    return tag.split('}',1)[-1] if '}' in tag else tag


def parse_codelists(root):
    # returns {codelist_id: {code: label}}
    cl_map = {}
    for cl in root.findall('.//{*}CodeList'):
        cid = cl.get('id') or cl.get('{http://www.w3.org/XML/1998/namespace}id') or cl.get('{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message}id')
        if not cid:
            cid = cl.get('agencyID') or 'unknown'
        codes = {}
        for code in cl.findall('.//{*}Code'):
            v = code.get('value') or code.get('id') or code.get('code')
            label = None
            name = code.find('./{*}Name')
            if name is not None and (name.text or '').strip():
                label = name.text.strip()
            else:
                # try annotated value
                for n in code.findall('.//{*}Name'):
                    if n.text and n.text.strip():
                        label = n.text.strip(); break
            if v:
                codes[v] = label
        cl_map[cid] = codes
    return cl_map


def extract_from_series(series_el, codelists):
    # series_el: Element <Series>
    info = {}
    # try Series id
    sid = series_el.get('id') or series_el.get('seriesKey')
    info['series_id'] = sid
    # Names
    names = []
    for nm in series_el.findall('.//{*}Name'):
        txt = (nm.text or '').strip()
        lang = nm.get('{http://www.w3.org/XML/1998/namespace}lang') or nm.get('lang')
        if txt:
            names.append({'lang': lang, 'text': txt})
    info['names'] = names

    # SeriesKey values
    comps = []
    for v in series_el.findall('.//{*}SeriesKey/{*}Value') + series_el.findall('.//{*}SeriesKey/{*}Value[@value]'):
        # attributes 'id' or 'value' or 'code'
        code = v.get('value') or v.get('id') or v.get('code')
        dim = v.get('id') or v.get('concept') or v.get('codelist') or ''
        comps.append({'dim': dim, 'code': code})
    # fallback: also try SeriesKey/Value children any depth
    if not comps:
        for vv in series_el.findall('.//{*}Value'):
            code = vv.get('value') or vv.get('id') or vv.get('code') or (vv.text or '').strip()
            dim = vv.get('id') or vv.get('concept') or ''
            if code:
                comps.append({'dim': dim, 'code': code})

    info['components'] = comps

    # assemble candidate title using names or mapping codes via codelists
    title_source = None
    candidate = None
    # prefer english name
    for n in names:
        if n.get('lang','').startswith('en'):
            candidate = n['text']; title_source = 'Name_en'; break
    if not candidate and names:
        candidate = names[0]['text']; title_source = 'Name_any'

    if not candidate and comps:
        parts = []
        for c in comps:
            code = c.get('code')
            dim = c.get('dim')
            lab = None
            # try map via any codelist that contains code
            for cid, cmap in codelists.items():
                if code in cmap and cmap[code]:
                    lab = cmap[code]; break
            if not lab:
                lab = code
            parts.append(lab)
        candidate = ' — '.join([p for p in parts if p])
        title_source = 'AssembledFromCodes'

    # normalize whitespace
    if candidate:
        candidate = re.sub(r'\s+', ' ', candidate).strip()

    return {'series_id': info.get('series_id'), 'candidate': candidate, 'title_source': title_source, 'components': info.get('components'), 'names': info.get('names')}


def main():
    raw_dir = Path('data_repository/raw/bis_api')
    files = sorted(raw_dir.glob('*.xml'))
    out_rows = []
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    outp = Path('analysis_outputs') / f'bis_extracted_titles_strong_{ts}.csv'
    outp.parent.mkdir(parents=True, exist_ok=True)

    if not files:
        print('No raw XML files in', raw_dir); return 1

    for f in files:
        try:
            tree = ET.parse(f)
            root = tree.getroot()
        except Exception as e:
            print('parse error', f, e); continue

        # parse codelists
        codelists = parse_codelists(root)

        # find all Series elements
        series_nodes = root.findall('.//{*}Series')
        if not series_nodes:
            # try alt path
            series_nodes = root.findall('.//{*}SeriesKey/..')

        for s in series_nodes:
            res = extract_from_series(s, codelists)
            series_key_norm = None
            comps = res.get('components') or []
            if comps:
                vals = [c.get('code') or '' for c in comps]
                series_key_norm = '.'.join(vals)
            out_rows.append({'file': str(f.name), 'series_id': res.get('series_id') or '', 'series_key_norm': series_key_norm or '', 'candidate_title': res.get('candidate') or '', 'title_source': res.get('title_source') or '', 'components': json.dumps(res.get('components') or []), 'names': json.dumps(res.get('names') or [])})

    # write CSV
    with outp.open('w', encoding='utf-8', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=['file','series_id','series_key_norm','candidate_title','title_source','components','names'])
        w.writeheader()
        for r in out_rows:
            w.writerow(r)

    print({'files_scanned': len(files), 'series_candidates': len(out_rows), 'output': str(outp)})
    return 0


if __name__ == '__main__':
    main()
