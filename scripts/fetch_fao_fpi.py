"""
Fetch FAO Food Price Index (FPI).
Strategy:
 - Try known API endpoints (fenixservices) if available.
 - Fall back to scraping FAO's Food Price Index page and downloading linked CSV.
 - Save CSV to `data_repository/raw/faostat/FAO_FPI.csv` and a `.meta.json` sidecar with provenance.
"""
import os
import sys
import requests
import json
import re
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
OUT_DIR = os.path.join(PROJECT_ROOT, 'data_repository', 'raw', 'faostat')
OUT_DIR = os.path.normpath(OUT_DIR)
os.makedirs(OUT_DIR, exist_ok=True)

candidates = [
    # FENIX / FAOSTAT API candidate (may or may not exist for FPI).
    "https://fenixservices.fao.org/faostat/api/v1/en/data/FPI?detail=1",
    "https://fenixservices.fao.org/faostat/api/v1/en/data/FOOD_PRICES?detail=1",
    # FAO Food Price Index landing page (we will scrape it for CSV links)
    "https://www.fao.org/worldfoodsituation/foodprices/foodprices-index/en/",
]

session = requests.Session()
headers = {"User-Agent": "FRM-project-data-fetcher/1.0 (+https://example.invalid)"}

def try_json_url(url):
    try:
        r = session.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            ctype = r.headers.get('Content-Type','')
            if 'application/json' in ctype or r.text.strip().startswith('{'):
                return r.json(), url
    except Exception:
        print('try_json_url error for', url)
        # don't raise; return None to continue
    return None, None


def try_csv_url(url):
    try:
        r = session.get(url, headers=headers, timeout=30)
        if r.status_code == 200:
            ctype = r.headers.get('Content-Type','')
            if 'text/csv' in ctype or url.lower().endswith('.csv') or ',' in r.text[:200]:
                return r.content, url
    except Exception:
        print('try_csv_url error for', url)
    return None, None


def scrape_for_csv(page_url):
    try:
        r = session.get(page_url, headers=headers, timeout=30)
        if r.status_code != 200:
            return None
        text = r.text
        # look for hrefs ending in .csv or containing 'download' and 'csv'
        matches = re.findall(r'href=["\']([^"\']+\.csv)["\']', text, flags=re.IGNORECASE)
        # also look for data-download links
        if not matches:
            matches = re.findall(r'href=["\']([^"\']+)["\']', text, flags=re.IGNORECASE)
            matches = [m for m in matches if '.csv' in m.lower() or 'download' in m.lower() and 'csv' in m.lower()]
        # normalize relative links
        urls = []
        from urllib.parse import urljoin
        for m in matches:
            full = urljoin(page_url, m)
            urls.append(full)
        # uniq preserve order
        seen = set()
        uniq = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        return uniq
    except Exception:
        print('scrape_for_csv error for', page_url)
        return None


def save_file(content_bytes, target_path):
    with open(target_path, 'wb') as f:
        f.write(content_bytes)


def write_meta(meta, meta_path):
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)


def main():
    meta = {
        'fetched_at': datetime.utcnow().isoformat() + 'Z',
        'candidates_tried': [],
        'success': False,
    }

    # Try JSON API endpoints first
    for url in candidates:
        meta['candidates_tried'].append({'url': url, 'time': datetime.utcnow().isoformat()+'Z'})
        j, src = try_json_url(url)
        if j is not None:
            # try to find reasonable table within json
            meta['success'] = True
            meta['source_url'] = src
            # save JSON as CSV-ish text if possible
            out_csv = os.path.join(OUT_DIR, 'FAO_FPI_from_api.json')
            with open(out_csv, 'w', encoding='utf-8') as f:
                json.dump(j, f, indent=2, ensure_ascii=False)
            meta['note'] = 'Saved JSON API response; review and convert to canonical CSV if needed.'
            write_meta(meta, os.path.join(OUT_DIR, 'FAO_FPI.meta.json'))
            print('Saved JSON response to', out_csv)
            return

    # Try scraping the FAO page for CSV links
    page = candidates[-1]
    links = scrape_for_csv(page)
    if links:
        meta['scraped_links'] = links
        for link in links:
            meta['candidates_tried'].append({'url': link, 'time': datetime.utcnow().isoformat()+'Z'})
            content, src = try_csv_url(link)
            if content is not None:
                # Save CSV
                out_csv = os.path.join(OUT_DIR, 'FAO_FPI.csv')
                save_file(content, out_csv)
                meta['success'] = True
                meta['source_url'] = src
                meta['note'] = 'Downloaded CSV scraped from FAO page.'
                write_meta(meta, os.path.join(OUT_DIR, 'FAO_FPI.meta.json'))
                print('Saved CSV to', out_csv)
                return

    # If we reach here, we failed to fetch
    meta['error'] = 'No usable FAO FPI endpoint or CSV link found.'
    write_meta(meta, os.path.join(OUT_DIR, 'FAO_FPI.meta.json'))
    print('Failed to fetch FAO FPI; wrote meta only to indicate attempts. See', os.path.join(OUT_DIR, 'FAO_FPI.meta.json'))

if __name__ == '__main__':
    main()
