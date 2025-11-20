#!/usr/bin/env python3
"""Extended trial fetcher for Phase 3 structural series.

Attempts World Bank, ECB SDW and IMF SDMX example endpoints and writes results
to `data/raw/`. Creates/updates `data/raw/structural_metadata.csv` with provenance.

Features:
 - dependency-free (urllib)
 - optional proxy via HTTP_PROXY/HTTPS_PROXY env vars
 - retries with backoff
 - graceful logging and metadata recording
"""
import os
import sys
import time
import csv
import json
import urllib.request
import urllib.error
import urllib.parse
import urllib
from datetime import datetime
import argparse
import socket

# CLI defaults
PROVIDER_HOSTS = {
    'ecb': 'sdw-wsrest.ecb.europa.eu',
    'imf': 'dataservices.imf.org',
    'bis': 'stats.bis.org'
}

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_REPO = os.path.join(BASE_DIR, 'data_repository')
RAW_DIR = os.path.join(DATA_REPO, 'raw')
MACRO_DIR = os.path.join(RAW_DIR, 'macro')
STRUCTURAL_DIR = os.path.join(RAW_DIR, 'structural')
CATALOG_CSV = os.path.join(DATA_REPO, 'catalog.csv')
METADATA_CSV = os.path.join(STRUCTURAL_DIR, 'structural_metadata.csv')

for path in (DATA_REPO, RAW_DIR, MACRO_DIR, STRUCTURAL_DIR):
    os.makedirs(path, exist_ok=True)

COUNTRIES = ['FRA', 'DEU', 'ITA', 'ESP', 'USA', 'GBR', 'CHE']

# Provider-guided configs (sourced from public documentation)
# ECB SDW REST guide: https://sdw-wsrest.ecb.europa.eu/help/
ECB_SERIES = [
    {
        'dataset': 'BSI',
        # Loans to NFCs, euro area aggregate (per ECB BSI code builder)
        'key': 'M.U2.N.A.A20.A.1.U2.3000.Z01.E',
        'query': 'detail=dataonly&startPeriod=2018-01&format=sdmx-json',
        'series_id': 'BSI:M.U2.N.A.A20.A.1.U2.3000.Z01.E',
        'notes': 'ECB BSI - Loans to NFCs (Euro area), SDMX guideline syntax',
        'frequency': 'monthly'
    },
    {
        'dataset': 'BSI',
        # Household loans total
        'key': 'M.U2.N.A.A20.A.1.U2.1000.Z01.E',
        'query': 'detail=dataonly&startPeriod=2018-01&format=sdmx-json',
        'series_id': 'BSI:M.U2.N.A.A20.A.1.U2.1000.Z01.E',
        'notes': 'ECB BSI - Loans to households (Euro area)',
        'frequency': 'monthly'
    }
]

# IMF SDMX endpoints documented at https://dataservices.imf.org/REST/SDMX_JSON.svc/
IMF_SERIES = [
    {
        'dataset': 'IFS',
        'key': 'USA.NGDP_R',  # GDP, real terms
        'series_id': 'IFS:USA.NGDP_R',
        'notes': 'IMF IFS nominal GDP (guideline CompactData syntax)',
        'startPeriod': '2015',
        'endPeriod': '',
        'frequency': 'annual'
    },
    {
        'dataset': 'IFS',
        'key': 'ITA.NGDP_R',
        'series_id': 'IFS:ITA.NGDP_R',
        'notes': 'IMF IFS nominal GDP (Italy)',
        'startPeriod': '2015',
        'endPeriod': '',
        'frequency': 'annual'
    }
]

# BIS/CDIS public CSV endpoints (documented under https://stats.bis.org/api)
BIS_DOWNLOADS = [
    {
        'series_id': 'BIS:LBS_D_PUB',
        'url': 'https://stats.bis.org/api/views/LBS_D_PUB/CSV?downloadfilename=LBS_D_PUB.csv',
        'filename': 'bis_lbs_d_pub.csv',
        'notes': 'BIS Locational Banking Statistics (public CSV)',
        'frequency': 'quarterly'
    },
    {
        'series_id': 'CDIS:CDIS_D_PUB',
        'url': 'https://stats.bis.org/api/views/CDIS_D_PUB/CSV?downloadfilename=CDIS_D_PUB.csv',
        'filename': 'bis_cdis_d_pub.csv',
        'notes': 'IMF/BIS Coordinated Direct Investment Survey (public CSV)',
        'frequency': 'annual'
    }
]

CATALOG_FIELDS = [
    'dataset_name', 'category', 'frequency', 'coverage', 'source',
    'source_url', 'storage_path', 'refresh_method', 'last_updated', 'notes'
]

def write_metadata_row(row):
    exists = os.path.exists(METADATA_CSV)
    with open(METADATA_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(['source','series_id','country','frequency','last_fetch','status','filepath','notes'])
        writer.writerow(row)


def upsert_catalog_entry(entry):
    rows = []
    if os.path.exists(CATALOG_CSV):
        with open(CATALOG_CSV, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    names = [row.get('dataset_name') for row in rows]
    if entry['dataset_name'] in names:
        idx = names.index(entry['dataset_name'])
        rows[idx].update(entry)
    else:
        rows.append(entry)
    with open(CATALOG_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CATALOG_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, '') for field in CATALOG_FIELDS})

def fetch_url(url, outpath, retries=1, timeout=15):
    # Honor proxies from environment if present
    proxies = urllib.request.getproxies()
    if proxies:
        proxy_handler = urllib.request.ProxyHandler(proxies)
        opener = urllib.request.build_opener(proxy_handler)
        urllib.request.install_opener(opener)

    last_err = None
    max_attempts = max(1, retries + 1)
    for attempt in range(1, max_attempts + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'FRM-extend-fetch/1.0'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            # ensure directory exists
            os.makedirs(os.path.dirname(outpath), exist_ok=True)
            with open(outpath, 'wb') as f:
                f.write(data)
            print(f"    fetched {url} -> {outpath} ({len(data)} bytes)")
            return True, len(data), None
        except Exception as e:
            last_err = e
            # Exponential backoff with ceiling
            wait = min(10.0, 1.8 ** attempt)
            print(f"  attempt {attempt}/{max_attempts} failed: {e}; retrying in {wait:.1f}s")
            time.sleep(wait)
    return False, 0, str(last_err)


def try_pandasdmx_fetch(provider, url, outpath):
    """Best-effort: try pandasdmx to fetch SDMX/CompactData from provider.
    Returns (ok, msg). If False, msg contains the error; on True msg is a short note.
    This is optional - failure here should not block the urllib fallback.
    """
    try:
        import pandasdmx
    except Exception as e:
        return False, f'pandasdmx not available: {e}'
    try:
        # Use provider code where possible (e.g., 'IMF' or 'ECB') to let pandasdmx choose endpoints
        req = pandasdmx.Request(provider)
        # pandasdmx.Request.get may accept a full URL or resource id; try both patterns
        try:
            resp = req.get(url)
        except Exception:
            # try to strip base if provider known
            resp = req.get(url.split('://', 1)[-1])

        # Attempt to serialize response content; this is best-effort and may vary by provider
        try:
            data_bytes = resp.write()
            if isinstance(data_bytes, str):
                data_bytes = data_bytes.encode('utf-8')
        except Exception:
            # Fallback: string representation
            data_bytes = str(resp).encode('utf-8')

        os.makedirs(os.path.dirname(outpath), exist_ok=True)
        with open(outpath, 'wb') as f:
            f.write(data_bytes)
        return True, 'fetched via pandasdmx'
    except Exception as e:
        return False, f'pandasdmx fetch failed: {e}'

def fetch_worldbank():
    indicator = 'GC.DOD.TOTL.GD.ZS'
    successes = []
    for c in COUNTRIES:
        url = f'https://api.worldbank.org/v2/country/{c}/indicator/{indicator}?format=json&per_page=2000'
        out = os.path.join(MACRO_DIR, f'wb_{indicator}_{c}.json')
        print('Fetching World Bank', c)
        ok, size, err = fetch_url(url, out)
        status = 'OK' if ok else 'FAIL'
        if ok:
            successes.append(c)
        write_metadata_row(['WorldBank', indicator, c, 'annual', datetime.utcnow().isoformat(), status, out if ok else err, 'general government gross debt % GDP'])
    if successes:
        upsert_catalog_entry({
            'dataset_name': 'World Bank General Government Debt (% GDP)',
            'category': 'macro',
            'frequency': 'annual',
            'coverage': '1990-present (country-dependent)',
            'source': 'World Bank WDI via API',
            'source_url': 'https://api.worldbank.org/v2/country/{ISO}/indicator/GC.DOD.TOTL.GD.ZS',
            'storage_path': 'data_repository/raw/macro/wb_GC.DOD.TOTL.GD.ZS_<ISO>.json',
            'refresh_method': 'scripts/extend_fetch_structural_data.py',
            'last_updated': datetime.utcnow().date().isoformat(),
            'notes': f"Countries covered: {', '.join(successes)}"
        })

def fetch_ecb_bsi_sample():
    hosts = [
        'https://sdw-wsrest.ecb.europa.eu/service',
        'https://sdw.ecb.europa.eu/service'
    ]
    for series in ECB_SERIES:
        last_err = 'not attempted'
        filepath = ''
        success = False
        # Try pandasdmx first (optional); pass dataset/key as resource id
        fname = f"ecb_{series['key'].replace('.', '_')}.json"
        out = os.path.join(STRUCTURAL_DIR, fname)
        try_pd, pd_msg = try_pandasdmx_fetch('ECB', f"{series['dataset']}/{series['key']}", out)
        if try_pd:
            print(f"    pandasdmx: {pd_msg}")
            success = True
            filepath = out
        else:
            # proceed with URL-based attempts
            for host in hosts:
                url = f"{host}/data/{series['dataset']}/{series['key']}?{series['query']}"
                fname = f"ecb_{series['key'].replace('.', '_')}.json"
                out = os.path.join(STRUCTURAL_DIR, fname)
                print(f"Fetching ECB series {series['key']} via {host}...")
                ok, size, err = fetch_url(url, out)
                if ok:
                    success = True
                    filepath = out
                    break
                last_err = err
        status = 'OK' if success else 'FAIL'
        notes = series['notes'] if success else f"{series['notes']} (last error: {last_err})"
        write_metadata_row(['ECB', series['series_id'], '', series['frequency'], datetime.utcnow().isoformat(), status, filepath if success else last_err, notes])
        if success:
            upsert_catalog_entry({
                'dataset_name': f"ECB {series['series_id']}",
                'category': 'structural',
                'frequency': series['frequency'],
                'coverage': '2018-01-present',
                'source': 'ECB SDW REST',
                'source_url': 'https://sdw-wsrest.ecb.europa.eu/service/data/BSI',
                'storage_path': f"data_repository/raw/structural/{fname}",
                'refresh_method': 'scripts/extend_fetch_structural_data.py',
                'last_updated': datetime.utcnow().date().isoformat(),
                'notes': series['notes']
            })

def fetch_imf_sd_ex():
    base = 'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData'
    for series in IMF_SERIES:
        params = []
        if series.get('startPeriod'):
            params.append(f"startPeriod={series['startPeriod']}")
        if series.get('endPeriod'):
            params.append(f"endPeriod={series['endPeriod']}")
        query = '&'.join(params)
        url = f"{base}/{series['dataset']}/{series['key']}"
        if query:
            url = f"{url}?{query}"
        fname = f"imf_{series['key'].replace('.', '_')}.json"
        out = os.path.join(STRUCTURAL_DIR, fname)
        print('Fetching IMF series', series['key'])
        # Try pandasdmx first (optional)
        try_pd, pd_msg = try_pandasdmx_fetch('IMF', f"{series['dataset']}/{series['key']}", out)
        if try_pd:
            print(f"    pandasdmx: {pd_msg}")
            ok = True
            size = os.path.getsize(out) if os.path.exists(out) else 0
            err = None
        else:
            ok, size, err = fetch_url(url, out)
        status = 'OK' if ok else 'FAIL'
        write_metadata_row(['IMF', series['series_id'], series['key'].split('.')[0] if '.' in series['key'] else '', series['frequency'], datetime.utcnow().isoformat(), status, out if ok else err, series['notes']])
        if ok:
            upsert_catalog_entry({
                'dataset_name': f"IMF {series['series_id']}",
                'category': 'structural',
                'frequency': series['frequency'],
                'coverage': f"{series.get('startPeriod', 'TBD')}-present",
                'source': 'IMF SDMX JSON',
                'source_url': 'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData',
                'storage_path': f"data_repository/raw/structural/{fname}",
                'refresh_method': 'scripts/extend_fetch_structural_data.py',
                'last_updated': datetime.utcnow().date().isoformat(),
                'notes': series['notes']
            })

def fetch_bis_cdis_downloads():
    for item in BIS_DOWNLOADS:
        out = os.path.join(STRUCTURAL_DIR, item['filename'])
        print('Fetching BIS/CDIS file', item['filename'])
        # Try the canonical URL and a couple of conservative fallbacks
        tried_urls = []
        ok = False
        size = 0
        err = None
        candidates = [item['url']]
        # fallback: try without the query parameter
        if 'api/views' in item['url']:
            base = item['url'].split('?')[0]
            candidates.append(base)
            # try switching to a simpler /views/.../csv pattern
            candidates.append(base.replace('/api/views/', '/views/').rstrip('/') + '/csv')
            # additional conservative permutations
            candidates.append(base.rstrip('/') + '/CSV')
            candidates.append(base.rstrip('/') + '/csv')
        for url in candidates:
            tried_urls.append(url)
            ok, size, err = fetch_url(url, out)
            if ok:
                break
        status = 'OK' if ok else 'FAIL'
        # If we got a 404, provide an actionable hint
        notes = item['notes']
        if not ok and err and '404' in err:
            notes = f"{item['notes']} (HTTP 404 on {tried_urls[0]}; tried: {', '.join(tried_urls)}; check BIS view identifier or the public download URL)"
            print('  Actionable: Received 404. Verify the view id or the API URL on https://stats.bis.org/')
        write_metadata_row(['BIS/CDIS', item['series_id'], '', item['frequency'], datetime.utcnow().isoformat(), status, out if ok else err, notes])
        if ok:
            upsert_catalog_entry({
                'dataset_name': item['series_id'].replace(':', ' '),
                'category': 'structural',
                'frequency': item['frequency'],
                'coverage': 'Provider default',
                'source': 'BIS Statistics API',
                'source_url': item['url'],
                'storage_path': f"data_repository/raw/structural/{item['filename']}",
                'refresh_method': 'scripts/extend_fetch_structural_data.py',
                'last_updated': datetime.utcnow().date().isoformat(),
                'notes': item['notes']
            })

def main():
    parser = argparse.ArgumentParser(description='Extended fetcher for structural series')
    parser.add_argument('--only', help='Comma-separated providers to run: worldbank,ecb,imf,bis', default='worldbank,ecb,imf,bis')
    parser.add_argument('--attach', help='Directory of pre-downloaded files to ingest into catalog (no download).', default=None)
    parser.add_argument('--proxy', help='Set HTTP/HTTPS proxy for this run (format http://host:port)', default=None)
    parser.add_argument('--skip-preflight', help='Skip preflight connectivity checks', action='store_true')
    args = parser.parse_args()

    if args.proxy:
        os.environ['HTTP_PROXY'] = args.proxy
        os.environ['HTTPS_PROXY'] = args.proxy

    providers = [p.strip().lower() for p in args.only.split(',') if p.strip()]

    print('Extended fetch started at', datetime.utcnow().isoformat())

    # Preflight connectivity checks
    if not args.skip_preflight:
        preflight_results = preflight_connectivity(providers)
        # If any critical provider shows DNS failure / TCP failure, warn and continue but note likely failures
        for prov, res in preflight_results.items():
            if not res['dns_ok']:
                print(f"WARNING: DNS lookup failed for {prov} ({PROVIDER_HOSTS.get(prov,'unknown')}). Requests to this provider will fail until DNS is fixed or a proxy is used.")
            elif not res['tcp_ok']:
                print(f"WARNING: TCP/HTTPS to {prov} ({res['addr']}) failed. Outbound HTTPS may be blocked or the host filtered.")

    # Attach-mode: ingest files without downloading
    if args.attach:
        process_attach(args.attach)

    if 'worldbank' in providers:
        fetch_worldbank()
    if 'ecb' in providers:
        fetch_ecb_bsi_sample()
    if 'imf' in providers:
        fetch_imf_sd_ex()
    if 'bis' in providers:
        fetch_bis_cdis_downloads()

    print('Done. See', METADATA_CSV)


def preflight_connectivity(providers):
    """Check DNS resolution and TCP connect (443) for selected providers.
    Returns dict of results { provider: {dns_ok:bool, tcp_ok:bool, addr:ip or ''} }
    """
    results = {}
    for prov in providers:
        host = PROVIDER_HOSTS.get(prov)
        res = {'dns_ok': False, 'tcp_ok': False, 'addr': ''}
        if not host:
            results[prov] = res
            continue
        try:
            # DNS resolution
            infos = socket.getaddrinfo(host, 443)
            if infos:
                res['dns_ok'] = True
                addr = infos[0][4][0]
                res['addr'] = addr
                # TCP connect test
                try:
                    sock = socket.create_connection((addr, 443), timeout=5)
                    sock.close()
                    res['tcp_ok'] = True
                except Exception:
                    res['tcp_ok'] = False
        except Exception:
            res['dns_ok'] = False
        results[prov] = res
    return results


def process_attach(dirpath):
    """Ingest files from a local directory into the catalog and metadata as 'attached' sources.
    The routine is conservative: it copies files into `data_repository/raw/structural/` if not already present,
    writes a metadata row and upserts a minimal catalog entry.
    """
    if not os.path.isdir(dirpath):
        print('Attach directory not found:', dirpath)
        return
    for fname in os.listdir(dirpath):
        src = os.path.join(dirpath, fname)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(STRUCTURAL_DIR, fname)
        if os.path.abspath(src) != os.path.abspath(dst):
            try:
                # copy file
                with open(src, 'rb') as r, open(dst, 'wb') as w:
                    w.write(r.read())
            except Exception as e:
                print('Failed copying attach file', fname, e)
                continue
        # write metadata row
        write_metadata_row(['LOCAL_ATTACH', fname, '', 'unknown', datetime.utcnow().isoformat(), 'OK', dst, 'attached by user'])
        # upsert catalog minimal entry
        upsert_catalog_entry({
            'dataset_name': fname,
            'category': 'structural',
            'frequency': 'unknown',
            'coverage': 'attached',
            'source': 'local-attach',
            'source_url': '',
            'storage_path': f'data_repository/raw/structural/{fname}',
            'refresh_method': 'local-attach',
            'last_updated': datetime.utcnow().date().isoformat(),
            'notes': 'Attached file ingested into canonical repository'
        })

if __name__ == '__main__':
    main()
