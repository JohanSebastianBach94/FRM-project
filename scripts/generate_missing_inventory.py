#!/usr/bin/env python3
"""Generate missing series inventory from `config/country_blocks_extended.yaml`.
Writes `outputs/missing_series_inventory.csv` with country, block, series_key, missing_status, local_path_if_any.
"""
import os
from pathlib import Path
import csv
import sys
try:
    import yaml
except Exception:
    print('Missing dependency: pyyaml required. Install with pip install pyyaml')
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[1]
CFG = ROOT / 'config' / 'country_blocks_extended.yaml'
OUT_DIR = ROOT / 'outputs'
OUT_DIR.mkdir(exist_ok=True)
OUT_CSV = OUT_DIR / 'missing_series_inventory.csv'

if not CFG.exists():
    print('Extended config not found:', CFG)
    sys.exit(1)

with open(CFG, 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

rows = []

def walk(node, country=None):
    if isinstance(node, list):
        for item in node:
            walk(item, country)
        return
    if not isinstance(node, dict):
        return
    if 'country' in node and 'blocks' in node:
        country = node.get('country')
        for b in node.get('blocks', []):
            process_block(b, country)
        return
    for v in node.values():
        walk(v, country)

def process_block(block, country_name):
    key = block.get('key','')
    missing = block.get('missing_series', {}) or {}
    local = block.get('local_series_files', {}) or {}
    # For each missing_series key, check if a local file exists with a standard name
    for s_key, status in missing.items():
        found_path = ''
        exists = False
        # If the block defines a local_series_files mapping, prefer that path
        if s_key in local:
            candidate = Path(local[s_key])
            # resolve relative to project root
            candidate_abs = ROOT / candidate
            if candidate_abs.exists():
                found_path = str(candidate_abs.relative_to(ROOT))
                exists = True
            else:
                # still record the provided mapping even if file missing
                found_path = str(candidate)
                exists = False
        else:
            # Try to detect a local file: look for any file under data_repository/raw or data/raw containing the series_key
            search_dirs = [ROOT / 'data_repository' / 'raw', ROOT / 'data' / 'raw', ROOT / 'data_repository' / 'processed', ROOT / 'data' / 'processed']
            for d in search_dirs:
                if not d.exists():
                    continue
                for p in d.rglob('*'):
                    if p.is_file() and s_key.lower() in p.name.lower():
                        found_path = str(p.relative_to(ROOT))
                        exists = True
                        break
                if exists:
                    break
        rows.append({
            'country': country_name,
            'block': key,
            'series_key': s_key,
            'missing_status': status,
            'exists': exists,
            'found_path': found_path,
        })
    # Also include missing series implied in top-level missing_series_overview? Skip for now

walk(cfg)

with open(OUT_CSV, 'w', newline='', encoding='utf-8') as f:
    fieldnames = ['country','block','series_key','missing_status','exists','found_path']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print(f'Wrote inventory to {OUT_CSV} with {len(rows)} rows')

if __name__ == '__main__':
    pass
