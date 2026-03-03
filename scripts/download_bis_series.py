#!/usr/bin/env python3
"""Download BIS series for matches with score > 2.0 and save per-series CSVs + diagnostics.

Behavior:
- Reads `analysis_outputs/bis_matches_normalized.csv` and selects matches with `score` > 2.0
- Maps `bis_series` to `data_repository/processed/BIS_catalog.csv` rows to get `bis_flow`, `series_key`, `bis_start`, `bis_end`, `bis_source_file`
- For each candidate: if raw BIS API file exists, attempt to extract series; otherwise call `fetchers/fetch_bis_api.py fetch` to retrieve the slice.
- Attempts to parse SDMX XML/JSON responses into a simple `period,value` CSV saved at `data_repository/BIS/{bis_series}.csv`.
- Produces `analysis_outputs/bis_download_diagnostics.csv` summarizing health and coverage.

This script is conservative: if parsing fails it still saves raw payload path and records failure in diagnostics.
"""

from __future__ import annotations
import csv
import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'analysis_outputs'
OUT.mkdir(parents=True, exist_ok=True)
MATCHES = OUT / 'bis_matches_normalized.csv'
BIS_CAT = ROOT / 'data_repository' / 'processed' / 'BIS_catalog.csv'
RAW_BIS = ROOT / 'data_repository' / 'raw' / 'bis_api'
TARGET_DIR = ROOT / 'data_repository' / 'BIS'
TARGET_DIR.mkdir(parents=True, exist_ok=True)
FETCHER = ROOT / 'fetchers' / 'fetch_bis_api.py'
METADATA = RAW_BIS / 'bis_api_metadata.csv'
DIAG = OUT / 'bis_download_diagnostics.csv'


def read_matches(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        print('Missing matches file:', path, file=sys.stderr)
        sys.exit(1)
    with path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader]
    return rows


def read_bis_catalog(path: Path) -> Dict[str, Dict[str, str]]:
    by_series = {}
    if not path.exists():
        print('Missing BIS catalog:', path, file=sys.stderr)
        sys.exit(1)
    with path.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            series = r.get('series') or r.get('catalog_series') or r.get('bis_series')
            if not series:
                continue
            by_series[series] = r
    return by_series


def unique_bis_candidates(matches: List[Dict[str, str]], threshold: float=2.0) -> List[Tuple[str,float]]:
    # collect best score per bis_series
    best: Dict[str,float] = {}
    for r in matches:
        try:
            s = float(r.get('score', r.get('score_num','0') or 0))
        except Exception:
            s = 0.0
        if s <= threshold:
            continue
        bis = r.get('bis_series') or r.get('bis_series')
        if not bis:
            continue
        if bis not in best or s > best[bis]:
            best[bis] = s
    items = sorted(best.items(), key=lambda kv: kv[0])
    return items


def call_fetcher(flow: str, key: str, start: Optional[str], end: Optional[str]) -> Optional[Path]:
    cmd = [sys.executable, str(FETCHER), 'fetch', '--flow', flow, '--key', key]
    if start:
        cmd += ['--start-period', start]
    if end:
        cmd += ['--end-period', end]
    # request raw structural metadata to get full series
    cmd += ['--detail', 'raw']
    print('Running fetch:', ' '.join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print('Fetcher failed for', flow, key, e, file=sys.stderr)
        return None
    # read metadata to find last matching entry
    if not METADATA.exists():
        return None
    latest = None
    with METADATA.open('r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('flow') == flow and row.get('key') == key:
                latest = row
    if latest and latest.get('file_path'):
        return Path(latest['file_path'])
    return None


def parse_sdmx_xml_extract(file_path: Path, series_identifier: Optional[str]=None) -> Tuple[Optional[List[Tuple[str,float]]], str, Optional[str]]:
    """Attempt to extract first matching series obs from an SDMX XML dump.
    Returns (rows, message, series_title). rows is list of (period, value) or None on failure.
    series_title is the BIS database series name extracted from the SDMX payload when available.
   """
    try:
        context = ET.iterparse(str(file_path), events=("end",))
    except Exception as e:
        return None, f'parse-error:{e}'
    rows = None
    series_title = None
    # helper to normalize attrib join
    def make_series_key(attrib: Dict[str,str]) -> str:
        return '|'.join(f"{k}:{v}" for k,v in sorted(attrib.items()))
    for event, elem in context:
        tag = elem.tag
        if tag.endswith('Series') or tag.lower().endswith('series'):
            # try to identify
            attrib = {k:v for k,v in elem.attrib.items()}
            key_str = make_series_key(attrib)
            matches = False
            if series_identifier:
                if series_identifier in key_str or series_identifier in (attrib.get('ID') or '') or series_identifier in (attrib.get('TITLE_TS') or ''):
                    matches = True
            # if no identifier, accept the first series
            if series_identifier is None:
                matches = True
            if not matches:
                elem.clear()
                continue
            obs = []
            # attempt to get a human-readable title from attributes or subelements
            # common attribute names: TITLE_TS, ID; or child <Name> inside Series
            if not series_title:
                series_title = elem.attrib.get('TITLE_TS') or elem.attrib.get('ID')
                if not series_title:
                    name_el = elem.find('.//{http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure}Name')
                    if name_el is None:
                        # fallback: try any Name tag without namespace
                        for child in elem:
                            if child.tag.lower().endswith('name') and child.text:
                                series_title = child.text
                                break
            for child in elem:
                ctag = child.tag
                if not (ctag.endswith('Obs') or ctag.lower().endswith('obs')):
                    continue
                period = child.get('TIME_PERIOD') or child.get('PERIOD') or child.get('TIME') or child.get('T')
                value = child.get('OBS_VALUE') or child.get('OBS') or child.get('VALUE')
                if period is None or value is None:
                    continue
                try:
                    num = float(value)
                except Exception:
                    continue
                obs.append((period, num))
            if obs:
                rows = obs
                elem.clear()
                break
            elem.clear()
    if rows is None:
        return None, 'no-series-found', None
    return rows, f'parsed-rows:{len(rows)}', (series_title or None)


def _slugify_filename(value: str, maxlen: int = 120) -> str:
    # remove or replace characters not allowed in Windows filenames
    if not value:
        return 'value'
    # replace slashes and backslashes and other illegal characters
    import re
    s = re.sub(r'[\\/:*?"<>|]+', ' ', value)
    s = re.sub(r'\s+', ' ', s).strip()
    # truncate
    if len(s) > maxlen:
        s = s[:maxlen].rstrip()
    # replace spaces with underscores for readability
    s = s.replace(' ', '_')
    return s or 'value'


def write_series_csv(series_id: str, rows: List[Tuple[str,float]], title: Optional[str]=None, catalog_series: Optional[str]=None, metadata: Optional[Dict[str,str]] = None) -> Path:
    """Write per-series CSV directly under `TARGET_DIR` with a slugified basename."""
    left = (catalog_series or series_id).strip()
    safe_left = _slugify_filename(left, maxlen=120)
    filename = f"{safe_left}.csv"
    outp = TARGET_DIR / filename
    with outp.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['period', 'value'])
        for p, v in rows:
            writer.writerow([p, f"{v:.6f}"])

    # write companion JSON metadata file next to CSV
    try:
        import json
        meta = metadata or {}
        # ensure some canonical fields
        meta.setdefault('catalog_series', catalog_series or series_id)
        meta.setdefault('bis_series', series_id)
        meta.setdefault('bis_title', title or '')
        meta.setdefault('written_at', datetime.utcnow().isoformat() + 'Z')
        meta_path = outp.with_suffix(outp.suffix + '.meta.json')
        with meta_path.open('w', encoding='utf-8') as mf:
            json.dump(meta, mf, ensure_ascii=False, indent=2)
    except Exception:
        pass

    return outp


def infer_expected_count(start: str, end: str, freq: str) -> Optional[int]:
    # naive expected count by freq code
    try:
        if not start or not end:
            return None
        if freq.upper().startswith('Q') or freq.upper()=='Q':
            # start like YYYY-QN or YYYY-QN
            def to_q_index(s):
                s = s.replace('Q','-Q') if 'Q' in s and '-' not in s else s
                parts = s.split('-')
                y = int(parts[0])
                q = 1
                for token in parts:
                    if token.startswith('Q'):
                        q = int(token.replace('Q',''))
                return y*4+ (q-1)
            si = to_q_index(start)
            ei = to_q_index(end)
            return ei - si + 1
        if freq.upper().startswith('M') or freq.upper()=='M':
            # YYYY-MM
            syear, smonth = map(int, start.split('-')[:2])
            eyear, emonth = map(int, end.split('-')[:2])
            return (eyear - syear)*12 + (emonth - smonth) + 1
        if freq.upper().startswith('A') or freq.upper() in ('Y','A'):
            syear = int(start.split('-')[0])
            eyear = int(end.split('-')[0])
            return eyear - syear + 1
        if freq.upper().startswith('D') or freq.upper()=='D':
            # daily — fallback: count days between iso dates
            from datetime import datetime as dt
            si = dt.fromisoformat(start)
            ei = dt.fromisoformat(end)
            return (ei - si).days + 1
    except Exception:
        return None
    return None


def main() -> None:
    matches = read_matches(MATCHES)
    bis_map = read_bis_catalog(BIS_CAT)
    candidates = unique_bis_candidates(matches, threshold=2.0)
    print(f'Found {len(candidates)} unique BIS candidates with score > 2.0')

    diagnostics = []
    for bis_series, best_score in candidates:
        entry = bis_map.get(bis_series, {})
        # skip if catalog entry not approved
        if (entry.get('approval') or '').strip().upper() != 'A':
            print(f"Skipping {bis_series}: not approved in catalog")
            continue
        flow = entry.get('bis_flow') or entry.get('bis_dataset') or entry.get('bis_series_key','')
        # try authoritative series_key fields, fallback to bis_series code if missing
        series_key = entry.get('series_key') or entry.get('bis_series_key') or entry.get('bis_series_title') or ''
        if not series_key:
            series_key = bis_series
        start = entry.get('bis_start') or ''
        end = entry.get('bis_end') or ''
        freq = entry.get('bis_freq') or entry.get('bis_frequency') or ''
        source_file = entry.get('bis_source_file') or ''
        raw_saved = ''
        parsed_rows = None
        parsed_msg = ''
        out_csv = ''
        # prefer extracting from existing raw file
        if source_file:
            candidate_path = RAW_BIS / source_file
            if candidate_path.exists():
                # prefer matching by series_key if available
                key_to_match = series_key or bis_series
                parsed_rows, parsed_msg, parsed_title = parse_sdmx_xml_extract(candidate_path, series_identifier=key_to_match)
                if parsed_rows:
                    # prefer the BIS title from the catalog when available
                    title_choice = entry.get('bis_title') or entry.get('bis_series_title') or parsed_title
                    catalog_series_val = entry.get('series') or entry.get('catalog_series') or bis_series
                    metadata = {
                        'bis_internal_id': entry.get('bis_internal_id',''),
                        'bis_internal_title': entry.get('bis_internal_title',''),
                        'series_key': entry.get('series_key',''),
                        'series_key_norm': entry.get('series_key_norm',''),
                        'bis_source_file': entry.get('bis_source_file',''),
                    }
                    outp = write_series_csv(bis_series, parsed_rows, title=title_choice, catalog_series=catalog_series_val, metadata=metadata)
                    out_csv = str(outp)
                    raw_saved = str(candidate_path)
        # if not parsed, call fetcher
        if parsed_rows is None:
            if flow and series_key:
                fetched = call_fetcher(flow, series_key, start, end)
                if fetched:
                    raw_saved = str(fetched)
                    key_to_match = series_key or bis_series
                    parsed_rows, parsed_msg, parsed_title = parse_sdmx_xml_extract(fetched, series_identifier=key_to_match)
                    if parsed_rows:
                        title_choice = entry.get('bis_title') or entry.get('bis_series_title') or parsed_title
                        catalog_series_val = entry.get('series') or entry.get('catalog_series') or bis_series
                        metadata = {
                            'bis_internal_id': entry.get('bis_internal_id',''),
                            'bis_internal_title': entry.get('bis_internal_title',''),
                            'series_key': entry.get('series_key',''),
                            'series_key_norm': entry.get('series_key_norm',''),
                            'bis_source_file': entry.get('bis_source_file',''),
                        }
                        outp = write_series_csv(bis_series, parsed_rows, title=title_choice, catalog_series=catalog_series_val, metadata=metadata)
                        out_csv = str(outp)
                else:
                    parsed_msg = 'fetch-failed'
            else:
                parsed_msg = 'no-flow-or-key'
        # record diag
        obs_count = len(parsed_rows) if parsed_rows else 0
        expected = infer_expected_count(start, end, freq) if freq else None
        pct = None
        if expected and expected>0:
            pct = round(100.0 * obs_count / expected, 2)
        diagnostics.append({
            'bis_series': bis_series,
            'best_score': best_score,
            'flow': flow,
            'series_key': series_key,
            'source_file': source_file,
            'raw_saved': raw_saved,
            'out_csv': out_csv,
            'parsed_msg': parsed_msg,
            'obs_count': obs_count,
            'freq': freq,
            'bis_start': start,
            'bis_end': end,
            'expected_count': expected or '',
            'pct_expected': pct or '',
        })
    # write diagnostics
    fieldnames = ['bis_series','best_score','flow','series_key','source_file','raw_saved','out_csv','parsed_msg','obs_count','freq','bis_start','bis_end','expected_count','pct_expected']
    with DIAG.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in diagnostics:
            writer.writerow(d)
    print(f'Wrote diagnostics to {DIAG}')


if __name__ == '__main__':
    main()
