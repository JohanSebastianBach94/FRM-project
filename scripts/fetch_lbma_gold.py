#!/usr/bin/env python3
"""Scrape LBMA prices/downloads page to find current gold price CSV/XLSX links and download them.

This script attempts to find files linked from LBMA's prices pages and save them to
`data_repository/raw/lbma/` with a `LBMA_gold.meta.json` sidecar describing attempts.
"""
import os
from pathlib import Path
from urllib.request import urlopen
from urllib.parse import urljoin
import json
import re

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data_repository' / 'raw' / 'lbma'
OUT.mkdir(parents=True, exist_ok=True)
META = OUT / 'LBMA_gold.meta.json'

# LBMA pages to inspect for downloads (landing pages may change; start at root)
start_pages = [
    'https://www.lbma.org.uk/prices-and-data'
]

def find_links(html, base_url):
    matches = re.findall(r'href=["\\']([^"\\']+)["\\']', html, flags=re.IGNORECASE)
    links = []
    for m in matches:
        full = urljoin(base_url, m)
        links.append(full)
    # dedupe while preserving order
    seen = set(); uniq = []
    for u in links:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq

meta = {'start_pages': start_pages, 'pages_visited': [], 'attempted': [], 'found_links': [], 'success': False}

# Crawl depth-2: get links from start page, then fetch each subpage and search for downloadable files
max_subpages = 40
for page in start_pages:
    try:
        with urlopen(page, timeout=30) as r:
            html = r.read().decode('utf-8', errors='replace')
    except Exception as e:
        meta.setdefault('page_errors', []).append({'page': page, 'error': str(e)})
        continue
    meta['pages_visited'].append(page)
    links = find_links(html, page)
    # filter to same domain and likely pages
    subpages = [l for l in links if 'lbma.org.uk' in l and len(l) < 300]
    subpages = subpages[:max_subpages]

    for sp in subpages:
        try:
            with urlopen(sp, timeout=30) as r2:
                html2 = r2.read().decode('utf-8', errors='replace')
        except Exception as e:
            meta.setdefault('subpage_errors', []).append({'page': sp, 'error': str(e)})
            continue
        meta['pages_visited'].append(sp)
        # find candidate file links (csv/xls/xlsx)
        candidates = [l for l in find_links(html2, sp) if any(ext in l.lower() for ext in ['.csv', '.xlsx', '.xls'])]
        # also consider links that contain 'download' or 'daily' or 'prices'
        if not candidates:
            candidates = [l for l in find_links(html2, sp) if any(k in l.lower() for k in ['download', 'daily', 'historical', 'prices'])]
        for link in candidates:
            if link not in meta['found_links']:
                meta['found_links'].append(link)

# Try HEAD or small GET for each found link to follow redirects and confirm content-type
for link in meta.get('found_links', []):
    meta['attempted'].append(link)
    try:
        # attempt download
        with urlopen(link, timeout=30) as r:
            content = r.read()
            name = os.path.basename(link.split('?')[0]) or 'lbma_download'
            outp = OUT / name
            with open(outp, 'wb') as f:
                f.write(content)
            meta['success'] = True
            meta['downloaded'] = str(outp)
            break
    except Exception as e:
        meta.setdefault('errors', []).append({'url': link, 'error': str(e)})

with open(META, 'w', encoding='utf-8') as f:
    json.dump(meta, f, indent=2)

print('LBMA crawler attempted; meta written to', META)
