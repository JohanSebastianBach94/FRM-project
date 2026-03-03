import os
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / 'config' / 'country_blocks_extended.yaml'
PROCESSED = ROOT / 'data_repository' / 'processed'
OUT_REPORT = ROOT / 'reports' / 'bis_presence_report.csv'
OUT_PREVIEW = ROOT / 'proposed_patches' / 'country_blocks_removed_tokens.yaml'

os.makedirs(ROOT / 'reports', exist_ok=True)
os.makedirs(ROOT / 'proposed_patches', exist_ok=True)

# Patterns to remove
REMOVE_PATTERNS = [re.compile(r"\bBIS_LBS_Total_Loans\b"),
                   re.compile(r"\bIRLTLT01"),
                   re.compile(r"\bIRLTLT02"),
                   re.compile(r"\bTERM_SPREAD\b")]

# Helper
def should_remove(line):
    for p in REMOVE_PATTERNS:
        if p.search(line):
            return True
    return False

# Read config and perform lightweight parsing
with CONFIG.open('r', encoding='utf-8') as f:
    lines = f.readlines()

# Produce preview content (remove matching lines only)
preview_lines = []
for ln in lines:
    if should_remove(ln):
        # skip the line (removal)
        continue
    preview_lines.append(ln)

# Write preview YAML (does not modify original)
with OUT_PREVIEW.open('w', encoding='utf-8') as f:
    f.writelines(preview_lines)

# Now extract BIS tokens and local mappings per country (lightweight scan)
entries = []  # list of dicts: country, iso, token, mapping_path, present
current_country = None
current_iso = None
in_local_map = False

# regexes
country_re = re.compile(r"^\s*-\s*country:\s*(.+)\s*$")
iso_re = re.compile(r"^\s*iso_code:\s*(\S+)\s*$")
list_item_re = re.compile(r"^\s*-\s*(\S.*)\s*$")
kv_map_re = re.compile(r"^\s*(BIS_LBS_[A-Za-z0-9_]+)\s*:\s*(\S.*)\s*$")

# Collect per-country local_series_files mapping
local_maps = {}  # (iso -> {token: path})
collected_tokens = []  # tuples (country, iso, token, context)
current_block_key = None
for ln in lines:
    # detect country start
    m = country_re.match(ln)
    if m:
        current_country = m.group(1).strip()
        current_iso = None
        in_local_map = False
        current_block_key = None
        continue
    m = iso_re.match(ln)
    if m:
        current_iso = m.group(1).strip()
        continue
    # detect start of local_series_files
    if ln.strip().startswith('local_series_files:'):
        in_local_map = True
        continue
    # detect entering a new block key
    bk = re.match(r"^\s*-\s*key:\s*(\S+)", ln)
    if bk:
        current_block_key = bk.group(1).strip()
        in_local_map = False
        continue
    if in_local_map:
        km = kv_map_re.match(ln)
        if km and current_iso:
            tok = km.group(1).strip()
            path = km.group(2).strip()
            local_maps.setdefault(current_iso, {})[tok] = path
        continue
    # find series_codes and optional_series_codes items
    if re.search(r"series_codes\s*:|optional_series_codes\s*:", ln):
        # next lines will include - entries; just continue
        continue
    m = list_item_re.match(ln)
    if m and current_iso:
        item = m.group(1).strip()
        # if item looks like a BIS token
        if item.startswith('BIS_LBS_'):
            collected_tokens.append((current_country or '', current_iso, item, 'series'))

# Now cross-check presence
processed_files = list(map(str, PROCESSED.glob('**/*')))
processed_set = set(Path(p).name for p in processed_files)

rows = ["country,iso,token,expected_path,present,found_path\n"]
for country, iso, token, ctx in collected_tokens:
    expected = ''
    found = ''
    present = False
    # If explicit local mapping exists for this ISO and token
    if iso in local_maps and token in local_maps[iso]:
        expected = local_maps[iso][token]
        if (ROOT / expected).exists():
            present = True
            found = expected
    else:
        # heuristics for expected filename
        if token == 'BIS_LBS_Household_Loans':
            fn = f'BIS_LBS_Household_Loans_{iso}.csv'
            expected = str(PROCESSED / fn)
            if fn in processed_set:
                present = True
                found = str(PROCESSED / fn)
        elif token.startswith('BIS_LBS_Private_NFC'):
            # could be generic or with suffix
            # prefer Loans file
            fn1 = f'BIS_LBS_Private_NFC_Loans_{iso}.csv'
            fn2 = f'BIS_LBS_Private_NFC_Total_{iso}.csv'
            if fn1 in processed_set:
                present = True
                found = str(PROCESSED / fn1)
                expected = str(PROCESSED / fn1)
            elif fn2 in processed_set:
                present = True
                found = str(PROCESSED / fn2)
                expected = str(PROCESSED / fn2)
            else:
                # try generic country file names (e.g., country names)
                # skip for now
                expected = str(PROCESSED / f'BIS_LBS_Private_NFC_Loans_{iso}.csv')
        else:
            # fallback: look for any processed file starting with token
            matches = [p for p in processed_files if Path(p).name.startswith(token)]
            if matches:
                present = True
                found = matches[0]
                expected = matches[0]
            else:
                expected = ''
    rows.append(f'{country},{iso},{token},{expected},{present},{found}\n')

with OUT_REPORT.open('w', encoding='utf-8') as f:
    f.writelines(rows)

print('Report written to', OUT_REPORT)
print('Preview YAML written to', OUT_PREVIEW)
print('Collected', len(collected_tokens), 'BIS token instances from YAML')
