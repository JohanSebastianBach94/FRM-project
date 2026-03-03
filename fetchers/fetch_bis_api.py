#!/usr/bin/env python3
"""Utility to explore and download data from the BIS SDMX RESTful API.

The API documented under `bis-stats-api-latest.yaml` exposes SDMX endpoints that we can
probe in order to fill the remaining BIS risk factors (LBS, DSR, CDIS, NEER, etc.).

Usage examples:
  python fetchers/fetch_bis_api.py list-flows
  python fetchers/fetch_bis_api.py fetch \
      --flow "BIS,WS_EER,1.0" --key all --start-period 2000 --end-period 2025
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
BIS_API_BASE = 'https://stats.bis.org/api/v1'
OUT_DIR = BASE_DIR / 'data_repository' / 'raw' / 'bis_api'
METADATA_FILE = OUT_DIR / 'bis_api_metadata.csv'
CONFIG_FILE = OUT_DIR / 'fetcher_config.json'
HEADERS = {'Accept': 'application/json', 'User-Agent': 'FRM-bis-api-fetcher/1.0'}
NS_MAP = {
    'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'
}


def ensure_output_dir() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def slugify(value: str) -> str:
    candidate = re.sub(r"[^A-Za-z0-9]+", '_', value or '')
    return (candidate.strip('_') or 'value').lower()


def fetch_url(path: str, params: dict[str, list[str] | str] | None = None) -> tuple[bytes, str, str]:
    query = ''
    if params:
        query = urllib.parse.urlencode(params, doseq=True)
    url = BIS_API_BASE + path
    if query:
        url = f"{url}?{query}"
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
        content_type = (resp.getheader('Content-Type') or '').split(';')[0]
    return data, content_type, url


def list_dataflows() -> None:
    try:
        data, content_type, _ = fetch_url('/dataflow')
    except Exception as exc:  # pragma: no cover - best effort inspection
        raise SystemExit(f"Failed to list flows: {exc}")

    flows = []
    try:
        if content_type and 'json' in content_type.lower():
            payload = json.loads(data)
            flows = payload.get('structure', {}).get('dataflows', [])
        elif content_type and 'xml' in content_type.lower():
            flows = parse_dataflow_xml(data)
        else:
            flows = parse_dataflow_xml(data)
    except Exception as exc:  # pragma: no cover - parsing fallback
        raise SystemExit(f"Unable to parse dataflow list: {exc}")

    if not flows:
        print('No dataflows returned. Check the API endpoint or your connection.')
        return

    for flow in flows:
        flow_id = flow.get('id')
        name = flow.get('name') or flow.get('id')
        version = flow.get('version') or flow.get('version_id') or 'latest'
        print(f"{flow_id} ({version}) | {name}")


def parse_dataflow_xml(data: bytes) -> list[dict[str, str]]:
    tree = ET.fromstring(data)
    dataflows = []
    for node in tree.findall('.//str:Dataflow', NS_MAP):
        flow_id = node.get('id')
        version = node.get('version') or node.get('versionID')
        name_el = node.find('str:Name', NS_MAP)
        name = name_el.text if name_el is not None and name_el.text else flow_id
        dataflows.append({'id': flow_id, 'name': name, 'version': version})
    return dataflows


def parse_components(component_args: list[str]) -> dict[str, list[str]]:
    components: dict[str, list[str]] = {}
    for comp in component_args:
        if '=' not in comp:
            raise SystemExit("--component values must use the form DIM=VALUE")
        dim, val = comp.split('=', 1)
        key = f"c[{dim}]"
        components.setdefault(key, []).append(val)
    return components


def record_metadata(row: dict[str, str]) -> None:
    fieldnames = ['timestamp', 'flow', 'key', 'status', 'params', 'content_type', 'file_path', 'size', 'note']
    exists = METADATA_FILE.exists()
    with open(METADATA_FILE, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, '') for field in fieldnames})


def write_payload(flow: str, key: str, data: bytes, content_type: str) -> Path:
    ext = '.json'
    if 'xml' in content_type:
        ext = '.xml'
    elif 'csv' in content_type:
        ext = '.csv'
    safe_flow = slugify(flow)
    safe_key = slugify(key)
    timestamp = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    filename = f"bis_api_{safe_flow}_{safe_key}_{timestamp}{ext}"
    out_path = OUT_DIR / filename
    with open(out_path, 'wb') as fh:
        fh.write(data)
    return out_path


def load_config() -> dict:
    ensure_output_dir()
    if not CONFIG_FILE.exists():
        return {}
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_config(cfg: dict) -> None:
    ensure_output_dir()
    with open(CONFIG_FILE, 'w', encoding='utf-8') as fh:
        json.dump(cfg, fh)


def fetch_data(args: argparse.Namespace) -> None:
    ensure_output_dir()
    flow_seg = urllib.parse.quote(args.flow, safe=',*._+')
    key_seg = urllib.parse.quote(args.key, safe=',*._+')
    path = f"/data/{flow_seg}/{key_seg}/all"

    params: dict[str, list[str] | str] = {}
    if args.start_period:
        params['startPeriod'] = args.start_period
    if args.end_period:
        params['endPeriod'] = args.end_period
    if args.mode:
        params['mode'] = args.mode
    if args.references:
        params['references'] = ','.join(args.references)
    if args.detail:
        params['detail'] = args.detail
    if args.components:
        params.update(parse_components(args.components))

    # If no explicit FREQ component provided and key is generic, try preferred frequencies
    provided_freq = any(k.lower().startswith('c[freq]') for k in params.keys()) or ('FREQ:' in (args.key or '') or 'FREQ=' in (args.key or ''))
    tried_freqs = []
    if not provided_freq:
        cfg = load_config()
        pref = cfg.get('prefer_freq') if isinstance(cfg.get('prefer_freq'), list) else None
        if not pref:
            pref = ['D', 'M', 'Q']
        # Only attempt preference when key is wildcard or generic
        if args.key in ('*', 'all', '') or '*' in (args.key or ''):
            for freq in pref:
                tried_freqs.append(freq)
                temp_params = dict(params)
                # add freq as component
                temp_params.update(parse_components([f"FREQ={freq}"]))
                try:
                    raw, content_type, url = fetch_url(path, temp_params)
                    if raw and len(raw) > 0:
                        key_for_save = args.key if args.key and args.key != '*' else f"FREQ:{freq}"
                        out_path = write_payload(args.flow, key_for_save, raw, content_type)
                        metadata = {
                            'timestamp': datetime.utcnow().isoformat(),
                            'flow': args.flow,
                            'key': key_for_save,
                            'status': 'OK',
                            'params': urllib.parse.urlencode(temp_params, doseq=True),
                            'content_type': content_type,
                            'file_path': str(out_path),
                            'size': str(len(raw)),
                            'note': f"preferred-freq:{freq}; {url}",
                        }
                        record_metadata(metadata)
                        print(f"Wrote {out_path} ({len(raw)} bytes) [freq={freq}]")
                        return
                except urllib.error.HTTPError as httpex:
                    metadata = {
                        'timestamp': datetime.utcnow().isoformat(),
                        'flow': args.flow,
                        'key': args.key,
                        'status': f"HTTP {httpex.code}",
                        'params': urllib.parse.urlencode(temp_params, doseq=True),
                        'content_type': '',
                        'file_path': '',
                        'size': '0',
                        'note': f"attempted-freq:{freq}; {str(httpex)}",
                    }
                    record_metadata(metadata)
                    # try next freq
                except Exception as exc:  # pragma: no cover - continue attempts
                    metadata = {
                        'timestamp': datetime.utcnow().isoformat(),
                        'flow': args.flow,
                        'key': args.key,
                        'status': 'ERROR',
                        'params': urllib.parse.urlencode(temp_params, doseq=True),
                        'content_type': '',
                        'file_path': '',
                        'size': '0',
                        'note': f"attempted-freq:{freq}; {str(exc)}",
                    }
                    record_metadata(metadata)
                    # continue to next freq

    # Fallback: single attempt with given key/params
    try:
        raw, content_type, url = fetch_url(path, params)
        out_path = write_payload(args.flow, args.key, raw, content_type)
        metadata = {
            'timestamp': datetime.utcnow().isoformat(),
            'flow': args.flow,
            'key': args.key,
            'status': 'OK',
            'params': urllib.parse.urlencode(params, doseq=True),
            'content_type': content_type,
            'file_path': str(out_path),
            'size': str(len(raw)),
            'note': url,
        }
        record_metadata(metadata)
        print(f"Wrote {out_path} ({len(raw)} bytes)")
    except urllib.error.HTTPError as httpex:
        metadata = {
            'timestamp': datetime.utcnow().isoformat(),
            'flow': args.flow,
            'key': args.key,
            'status': f"HTTP {httpex.code}",
            'params': urllib.parse.urlencode(params, doseq=True),
            'content_type': '',
            'file_path': '',
            'size': '0',
            'note': str(httpex),
        }
        record_metadata(metadata)
        raise SystemExit(f"Request failed: {httpex}")
    except Exception as exc:  # pragma: no cover - CLI exit path
        metadata = {
            'timestamp': datetime.utcnow().isoformat(),
            'flow': args.flow,
            'key': args.key,
            'status': 'ERROR',
            'params': urllib.parse.urlencode(params, doseq=True),
            'content_type': '',
            'file_path': '',
            'size': '0',
            'note': str(exc),
        }
        record_metadata(metadata)
        raise SystemExit(f"Failed to fetch data: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fetch BIS SDMX data for missing risk factors')
    subparsers = parser.add_subparsers(dest='command', required=True)

    subparsers.add_parser('list-flows', help='List BIS dataflows (inspection only)')

    fetch_parser = subparsers.add_parser('fetch', help='Download a BIS SDMX data slice')
    fetch_parser.add_argument('--flow', required=True, help='Dataflow or context (e.g. BIS,WS_EER,1.0)')
    fetch_parser.add_argument('--key', default='*', help='Series key (wildcards allowed). Default: all')
    fetch_parser.add_argument('--start-period', dest='start_period', help='Inclusive start period (ISO or SDMX notation)')
    fetch_parser.add_argument('--end-period', dest='end_period', help='Inclusive end period (ISO or SDMX notation)')
    fetch_parser.add_argument('--component', dest='components', action='append', default=[],
                              help='Component filter expressed as DIM=VALUE (repeatable). Example: --component FREQ=M')
    fetch_parser.add_argument('--mode', choices=['exact', 'available'], help='Content constraint mode')
    fetch_parser.add_argument('--references', nargs='+', choices=['none', 'all', 'datastructure', 'conceptscheme', 'codelist',
                                                               'dataproviderscheme', 'dataflow'],
                              help='References to include in the response (comma separated)')
    fetch_parser.add_argument('--detail', choices=['full', 'allstubs', 'referencepartial', 'allcompletestubs',
                                                  'referencecompletestubs', 'raw'],
                              help='Detail level for structural metadata returned alongside the data')
    # configuration commands
    cfg_set = subparsers.add_parser('set-preference', help='Set persistent fetcher frequency preference (comma-separated)')
    cfg_set.add_argument('--order', required=True, help='Comma-separated list of frequency codes in preferred order, e.g. D,M,Q')
    subparsers.add_parser('get-preference', help='Show current fetcher frequency preference')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == 'list-flows':
        list_dataflows()
    elif args.command == 'fetch':
        fetch_data(args)
    elif args.command == 'set-preference':
        # called as: python fetchers/fetch_bis_api.py set-preference --order D,M,Q
        order = [s.strip().upper() for s in args.order.split(',') if s.strip()]
        if not order:
            print('No frequencies provided')
            return
        cfg = load_config()
        cfg['prefer_freq'] = order
        save_config(cfg)
        print('Saved preference:', order)
    elif args.command == 'get-preference':
        cfg = load_config()
        pref = cfg.get('prefer_freq') or ['D', 'M', 'Q']
        print('Current preference:', pref)


if __name__ == '__main__':
    main()
