#!/usr/bin/env python3
"""Confirm BIS series downloads from discovery candidates and auto-save them."""

from __future__ import annotations

import csv
import json
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'analysis_outputs'
CATALOG = ROOT / 'data_repository' / 'processed' / 'BIS_catalog.csv'
TARGET_DIR = ROOT / 'data_repository' / 'BIS'
RAW_DIR = ROOT / 'data_repository' / 'raw' / 'bis_api'
METADATA = RAW_DIR / 'bis_api_metadata.csv'
FETCHER = ROOT / 'fetchers' / 'fetch_bis_api.py'
SRC_CANDIDATES = OUT_DIR / 'bis_discovery_candidates_scored.csv'
DIAG = OUT_DIR / 'bis_auto_confirm_diag.csv'


def load_candidates() -> List[Dict[str, str]]:
    if not SRC_CANDIDATES.exists():
        raise FileNotFoundError(f"Missing scored candidates: {SRC_CANDIDATES}")
    with SRC_CANDIDATES.open(newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        rows = sorted(reader, key=lambda r: (r.get('bis_series_code'), int(r.get('rank') or 0)))
    return rows


def normalize_key(attr_string: str) -> Optional[str]:
    if not attr_string:
        return None
    parts = re.split(r'\||\|', attr_string)
    comps = []
    for part in parts:
        text = part.strip()
        if not text:
            continue
        if ':' in text:
            key, value = text.split(':', 1)
        elif '=' in text:
            key, value = text.split('=', 1)
        else:
            continue
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        if key.upper().startswith('TITLE') or key.upper() == 'DECIMALS':
            continue
        value = value.replace('|', ',')
        comps.append(f"{key}:{value}")
    return '|'.join(comps) if comps else None


def run_fetch(flow: str, key: str) -> Tuple[Optional[Path], List[str]]:
    cmd = [sys.executable, str(FETCHER), 'fetch', '--flow', flow, '--key', key, '--detail', 'raw']
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    except Exception as exc:
        return None, [f'fetch-error:{exc}']
    if result.returncode != 0:
        return None, [result.stdout.strip(), result.stderr.strip()]

    if not METADATA.exists():
        return None, ['no-metadata']
    last: Optional[Dict[str, str]] = None
    with METADATA.open(newline='', encoding='utf-8') as meta_fh:
        reader = csv.DictReader(meta_fh)
        for row in reader:
            if row.get('flow') == flow and row.get('key') == key and row.get('status','').upper().startswith('OK'):
                last = row
    if last and last.get('file_path'):
        path = Path(last['file_path'])
        if path.exists():
            return path, ['ok']
    return None, ['no-ok-entry']


def parse_series(file_path: Path, key_tokens: Dict[str, str]) -> Tuple[Optional[List[Tuple[str, float]]], Optional[str]]:
    try:
        context = ET.iterparse(str(file_path), events=('end',))
    except ET.ParseError:
        return None, None
    matched_rows: Optional[List[Tuple[str, float]]] = None
    matched_title: Optional[str] = None
    for event, elem in context:
        tag = elem.tag
        if not tag.lower().endswith('series'):
            elem.clear()
            continue
        attrs = {k.upper(): v for k, v in elem.attrib.items()}
        matches = True
        for key, val in key_tokens.items():
            actual = attrs.get(key)
            if not actual or val not in actual.lower():
                matches = False
                break
        if not matches:
            elem.clear()
            continue
        matched_title = attrs.get('TITLE_TS') or attrs.get('ID') or matched_title
        obs = []
        for child in elem:
            tagc = child.tag
            if not tagc.lower().endswith('obs'):
                continue
            period = child.get('TIME_PERIOD') or child.get('PERIOD') or child.get('TIME') or child.get('T')
            value = child.get('OBS_VALUE') or child.get('OBS') or child.get('VALUE')
            if not period or not value:
                continue
            try:
                num = float(value)
            except ValueError:
                continue
            obs.append((period, num))
        elem.clear()
        if obs:
            matched_rows = obs
            break
    return matched_rows, matched_title


def write_series(series: str, title: str, rows: List[Tuple[str, float]], extra: Dict[str, str]) -> Path:
    safe_left = re.sub(r'[^A-Za-z0-9_]+', '_', extra.get('catalog_series') or series).strip('_') or series
    series_dir = TARGET_DIR / safe_left
    series_dir.mkdir(parents=True, exist_ok=True)
    name = title or series
    safe_name = re.sub(r'[\\/:*?"<>|]+', ' ', name).strip().replace(' ', '_')[:120]
    out_csv = series_dir / f"{safe_name}.csv"
    with out_csv.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        writer.writerow(['period', 'value'])
        for period, value in rows:
            writer.writerow([period, f"{value:.6f}"])
    meta_path = out_csv.with_suffix(out_csv.suffix + '.meta.json')
    meta = {**extra, 'written_at': datetime.utcnow().isoformat() + 'Z'}
    with meta_path.open('w', encoding='utf-8') as mf:
        json.dump(meta, mf, ensure_ascii=False, indent=2)
    return out_csv


def main() -> None:
    candidates = load_candidates()
    groups: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    for row in candidates:
        groups[row['bis_series_code']].append(row)

    diag_rows = []
    successes = []
    for series, rows in groups.items():
        for candidate in rows:
            key = normalize_key(candidate.get('series_key', ''))
            if not key:
                diag_rows.append({'series': series, 'candidate': candidate.get('series_key'), 'status': 'skip-no-key'})
                continue
            flow = candidate.get('bis_flow', '') or ''
            path, notes = run_fetch(flow, key)
            if not path:
                diag_rows.append({'series': series, 'candidate': key, 'status': 'fetch-fail', 'note': '|'.join(notes)})
                continue
            token_map = {part.split(':')[0].upper(): part.split(':', 1)[1].lower() for part in key.split('|') if ':' in part}
            rows_found, title = parse_series(path, token_map)
            if not rows_found:
                diag_rows.append({'series': series, 'candidate': key, 'status': 'no-rows', 'note': f'file={path}'})
                continue
            out_csv = write_series(series, title or series, rows_found, {
                'catalog_series': series,
                'bis_series': series,
                'bis_title': title or '',
                'series_key': key,
                'raw_file': str(path),
            })
            diag_rows.append({'series': series, 'candidate': key, 'status': 'ok', 'file': str(out_csv)})
            successes.append({'series': series, 'flow': flow, 'series_key': key})
            break
        else:
            diag_rows.append({'series': series, 'candidate': '', 'status': 'all-failed'})

    if successes:
        backup = CATALOG.with_suffix(f'.bak_autoapply_{datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")}.csv')
        CATALOG.replace(backup)
        with backup.open(newline='', encoding='utf-8') as fh:
            reader = list(csv.DictReader(fh))
        fieldnames = list(reader[0].keys())
        if 'series_key' not in fieldnames:
            fieldnames.append('series_key')
        with CATALOG.open('w', newline='', encoding='utf-8') as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                for hit in successes:
                    if row.get('series') == hit['series']:
                        row['series_key'] = hit['series_key']
                        if not row.get('bis_flow'):
                            row['bis_flow'] = hit['flow']
                writer.writerow(row)

    with DIAG.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=['series', 'candidate', 'status', 'note', 'file'])
        writer.writeheader()
        for d in diag_rows:
            writer.writerow(d)

    print(f"Completed. Successes: {len(successes)}, diagnostics: {DIAG}")


if __name__ == '__main__':
    main()