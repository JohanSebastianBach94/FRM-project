#!/usr/bin/env python3
"""Check presence of `local_series_files` referenced in `config/country_blocks_extended.yaml`.

Writes a CSV report to `outputs/local_series_presence_report.csv` and prints a short summary.
"""
import os
import sys
from pathlib import Path
import csv
try:
    import yaml
except Exception:
    print("Missing dependency: pyyaml is required. Install with `pip install pyyaml`.")
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[1]
CFG = ROOT / 'config' / 'country_blocks_extended.yaml'
OUT_DIR = ROOT / 'outputs'
OUT_DIR.mkdir(exist_ok=True)
OUT_CSV = OUT_DIR / 'local_series_presence_report.csv'

if not CFG.exists():
    print(f"Config file not found: {CFG}")
    sys.exit(1)

with open(CFG, 'r', encoding='utf-8') as f:
    cfg = yaml.safe_load(f)

rows = []

def process_block(block, country_name=None):
    key = block.get('key','')
    local = block.get('local_series_files', {}) or {}
    for series_key, path in local.items():
        p = Path(path)
        exists = (ROOT / path).exists() if not p.is_absolute() else p.exists()
        rows.append({
            'country': country_name or '',
            'block': key,
            'series_key': series_key,
            'path': path,
            'exists': exists,
        })
    # nested country_blocks
    if 'country_blocks' in block and isinstance(block['country_blocks'], list):
        for sub in block['country_blocks']:
            walk(sub)

def walk(node):
    # node can be a list or dict root or top-level dict
    if isinstance(node, list):
        for item in node:
            walk(item)
        return
    if not isinstance(node, dict):
        return
    # if node looks like a country block
    if 'country' in node and 'blocks' in node:
        country_name = node.get('country')
        for b in node.get('blocks', []):
            process_block(b, country_name)
        return
    # else traverse values
    for v in node.values():
        walk(v)

walk(cfg)

missing = [r for r in rows if not r['exists']]

with open(OUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['country','block','series_key','path','exists']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow(r)

print(f"Checked {len(rows)} local_series_files entries.")
print(f"Missing: {len(missing)} entries. Report written to {OUT_CSV}")
if len(missing) > 0:
    print("Examples of missing files:")
    for r in missing[:10]:
        print(f" - {r['country']} | {r['block']} | {r['series_key']} -> {r['path']}")

sys.exit(0)
