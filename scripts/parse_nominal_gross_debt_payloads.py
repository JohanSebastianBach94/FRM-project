#!/usr/bin/env python3
"""
Parse saved Eurostat JSON and OECD SDMX XML payloads into canonical CSVs.

Usage (after you've run the fetch script on a connected host):
  python scripts\parse_nominal_gross_debt_payloads.py

It expects raw files in `data_repository/raw/macro/` named like:
  - euro_gov_10_gdp_{ISO}.json
  - oecd_gov_debt_{ISO}.xml

Outputs (per ISO):
  - data_repository/raw/macro/general_government_gross_debt_level_{ISO}.csv
  - data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.csv  (if GDP available)

This parser is intentionally conservative: it attempts multiple heuristics and logs results for manual review.
"""
import json
import re
import csv
import sys
from pathlib import Path
from typing import Dict, Optional

try:
    from lxml import etree
except Exception:
    etree = None

BASE_DIR = Path(__file__).resolve().parents[1]
MACRO_DIR = BASE_DIR / 'data_repository' / 'raw' / 'macro'
MACRO_DIR.mkdir(parents=True, exist_ok=True)

COUNTRIES = {
    'DEU': {'iso2': 'DE'},
    'FRA': {'iso2': 'FR'},
    'ITA': {'iso2': 'IT'},
    'ESP': {'iso2': 'ES'},
    'USA': {'iso2': 'US'},
}

YEAR_RE = re.compile(r'(?P<year>19\d{2}|20\d{2})')
NUM_RE = re.compile(r'-?\d{1,3}(?:,\d{3})*(?:\.\d+)?|-?\d+\.\d+')


def find_year_number_pairs_in_text(text: str) -> Dict[int, float]:
    # crude heuristic: find a year, then look nearby for a number
    pairs = {}
    for m in YEAR_RE.finditer(text):
        y = int(m.group('year'))
        # search within next 80 chars for a number
        window = text[m.end(): m.end() + 120]
        num_m = NUM_RE.search(window)
        if num_m:
            raw = num_m.group(0).replace(',', '')
            try:
                val = float(raw)
                pairs[y] = val
            except Exception:
                continue
    return pairs


def parse_eurostat_json(path: Path) -> Dict[int, float]:
    text = path.read_text(encoding='utf-8', errors='ignore')
    try:
        data = json.loads(text)
    except Exception:
        # fallback to heuristic text search
        return find_year_number_pairs_in_text(text)

    # Try structured parsing typical of Eurostat "dissemination" JSON
    # structure: 'value' dict + 'dimension' with 'time' categories
    if isinstance(data, dict) and 'value' in data and 'dimension' in data:
        values = {}
        try:
            time_dim = data['dimension']['time']['category']['label']
            # 'value' keys might be flattened indices; we need to map indexes to times
            # Safe approach: look for 'id' -> list mapping
            time_index = data['dimension']['time']['category'].get('index')
            if isinstance(time_index, dict):
                # index maps year string -> position (string)
                # invert it
                inv = {v: k for k, v in time_index.items()}
                for k, v in data['value'].items():
                    # k may be like '0:0:3' -> last segment is time index
                    seg = k.split(':')[-1]
                    year = inv.get(seg)
                    if year is None:
                        continue
                    try:
                        values[int(year)] = float(v)
                    except Exception:
                        continue
                if values:
                    return values
        except Exception:
            pass

    # Last resort: text heuristic
    return find_year_number_pairs_in_text(text)


def parse_oecd_sdmx(path: Path) -> Dict[int, float]:
    if etree is None:
        print('lxml not installed; cannot parse OECD SDMX XML precisely. Falling back to regex.')
        return find_year_number_pairs_in_text(path.read_text(encoding='utf-8', errors='ignore'))

    tree = etree.parse(str(path))
    ns = {k: v for k, v in tree.getroot().nsmap.items() if k}
    # Look for observation entries
    values = {}
    # Try common SDMX compact structure: <Obs><Time>YEAR</Time><ObsValue value="..."/></Obs>
    obs_nodes = tree.findall('.//{*}Obs')
    if not obs_nodes:
        # Try generic search for Time and ObsValue tags
        time_nodes = tree.findall('.//{*}Time')
        for t in time_nodes:
            parent = t.getparent()
            if parent is None:
                continue
            val_node = parent.find('.//{*}ObsValue')
            if val_node is None:
                # maybe value is attribute in <Obs value="..."/>
                if parent.tag.endswith('Obs') and 'value' in parent.attrib:
                    try:
                        values[int(t.text)] = float(parent.attrib['value'])
                    except Exception:
                        continue
                continue
            val = val_node.attrib.get('value')
            try:
                values[int(t.text)] = float(val)
            except Exception:
                continue
        return values

    for obs in obs_nodes:
        t = obs.find('.//{*}Time')
        v = obs.find('.//{*}ObsValue')
        if t is not None and v is not None:
            try:
                year = int(t.text)
                val = float(v.attrib.get('value'))
                values[year] = val
            except Exception:
                continue
    return values


def save_level_csv(iso: str, series: Dict[int, float], out_dir: Path):
    if not series:
        print(f'  No level data for {iso}')
        return None
    rows = sorted(series.items())
    out_path = out_dir / f'general_government_gross_debt_level_{iso}.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['year', 'debt_level'])
        for y, v in rows:
            writer.writerow([y, v])
    print(f'  Wrote levels: {out_path} ({len(rows)} rows)')
    return out_path


def try_load_wb_gdp(iso: str) -> Optional[Dict[int, float]]:
    # looks for existing WB GDP JSON named wb_NY.GDP.MKTP.CD_{ISO}.json in macro dir
    path = MACRO_DIR / f'wb_NY.GDP.MKTP.CD_{iso}.json'
    if not path.exists():
        return None
    try:
        obj = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None
    # simple heuristic to extract year:value pairs from the JSON text
    return find_year_number_pairs_in_text(path.read_text(encoding='utf-8', errors='ignore'))


def save_pct_csv(iso: str, debt_series: Dict[int, float], gdp_series: Dict[int, float], out_dir: Path):
    # compute pct = debt / gdp * 100
    rows = []
    for y, debt in debt_series.items():
        gdp = gdp_series.get(y)
        if gdp and gdp != 0:
            pct = debt / gdp * 100.0
            rows.append((y, pct))
    if not rows:
        print(f'  No overlapping GDP years to compute pct for {iso}')
        return None
    rows.sort()
    out_path = out_dir / f'general_government_gross_debt_pct_gdp_{iso}.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['year', 'debt_pct_gdp'])
        for y, v in rows:
            writer.writerow([y, v])
    print(f'  Wrote pct-of-gdp: {out_path} ({len(rows)} rows)')
    return out_path


def main():
    print('Parsing saved Eurostat / OECD payloads...')
    for iso in COUNTRIES.keys():
        print('--', iso)
        euro_path = MACRO_DIR / f'euro_gov_10_gdp_{iso}.json'
        oecd_path = MACRO_DIR / f'oecd_gov_debt_{iso}.xml'

        level_series = {}
        if euro_path.exists():
            try:
                s = parse_eurostat_json(euro_path)
                if s:
                    level_series.update(s)
                    print(f'  Eurostat parsed {len(s)} points')
                else:
                    print('  Eurostat parser found no points')
            except Exception as e:
                print('  Eurostat parse error:', e)

        if oecd_path.exists():
            try:
                s = parse_oecd_sdmx(oecd_path)
                if s:
                    # prefer OECD values (often levels) if present
                    level_series.update(s)
                    print(f'  OECD parsed {len(s)} points')
                else:
                    print('  OECD parser found no points')
            except Exception as e:
                print('  OECD parse error:', e)

        if not level_series:
            print(f'  No nominal-level series parsed for {iso} — check raw payloads manually')
            continue

        levels_out = save_level_csv(iso, level_series, MACRO_DIR)

        # try to compute pct-of-gdp using local WB GDP JSON if present
        gdp = try_load_wb_gdp(iso)
        if gdp:
            save_pct_csv(iso, level_series, gdp, MACRO_DIR)
        else:
            print('  No WB GDP JSON found for', iso)

    print('Parsing complete. Review outputs in data_repository/raw/macro/')


if __name__ == '__main__':
    main()
