#!/usr/bin/env python3
"""Auto-approve exact matches, disapprove the rest, clean BIS folder,
filter matches to approved bis_series, run downloader, and stop before merging.

Usage: python scripts/prepare_and_download_approved.py
"""
from pathlib import Path
import csv
import shutil
import datetime
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
BIS_CAT = ROOT / 'data_repository' / 'processed' / 'BIS_catalog.csv'
BIS_DIR = ROOT / 'data_repository' / 'BIS'
MATCHES = ROOT / 'analysis_outputs' / 'bis_matches_normalized.csv'
MATCHES_BACKUP = ROOT / 'analysis_outputs'
DOWNLOAD_SCRIPT = ROOT / 'scripts' / 'download_bis_series.py'

def backup_file(p: Path) -> Path:
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    b = p.with_name(p.name + f'.bak_prep_{ts}')
    shutil.copy2(p, b)
    return b

def update_approvals():
    if not BIS_CAT.exists():
        print('Missing catalog:', BIS_CAT); sys.exit(1)
    bak = backup_file(BIS_CAT)
    print('Catalog backed up to', bak)

    rows = []
    with BIS_CAT.open('r', newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        fns = reader.fieldnames[:]
        for r in reader:
            rows.append(r)

    if 'approval' not in fns:
        # insert approval after match if present
        try:
            idx = fns.index('match') + 1
        except ValueError:
            idx = len(fns)
        fns.insert(idx, 'approval')
        for r in rows:
            r['approval'] = ''

    updated = 0
    for r in rows:
        series = (r.get('series') or '').strip()
        match = (r.get('match') or '').strip()
        cur = (r.get('approval') or '').strip()
        # auto-approve exact match
        if series and match and series == match:
            if cur != 'A':
                r['approval'] = 'A'
                updated += 1
        else:
            # leave existing approvals alone; others will be set to D later
            pass

    # set remaining empty approvals to 'D'
    disapproved = 0
    for r in rows:
        if not (r.get('approval') or '').strip():
            r['approval'] = 'D'
            disapproved += 1

    with BIS_CAT.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fns)
        writer.writeheader()
        writer.writerows(rows)

    print(f'Approvals updated: auto-approved {updated}, disapproved {disapproved} rows')

def clean_bis_folder():
    if not BIS_DIR.exists():
        print('No BIS folder to clean:', BIS_DIR)
        return
    # remove all children inside BIS_DIR
    removed = 0
    for child in BIS_DIR.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed += 1
        except Exception as e:
            print('Failed to remove', child, e)
    print('Cleaned BIS folder, removed', removed, 'items')

def filter_matches_to_approved():
    if not MATCHES.exists():
        print('No matches file found at', MATCHES); return None
    bak = backup_file(MATCHES)
    print('Matches backed up to', bak)

    # read approved bis_series set from catalog
    approved = set()
    with BIS_CAT.open('r', newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for r in reader:
            if (r.get('approval') or '').strip() == 'A':
                m = (r.get('match') or '').strip()
                if m:
                    approved.add(m)

    # filter matches
    kept = []
    with MATCHES.open('r', newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        fns = reader.fieldnames[:]
        for r in reader:
            if (r.get('bis_series') or '').strip() in approved:
                kept.append(r)

    # overwrite MATCHES with filtered set (safe because we backed up)
    with MATCHES.open('w', newline='', encoding='utf-8') as fh:
        writer = csv.DictWriter(fh, fieldnames=fns)
        writer.writeheader()
        for r in kept:
            writer.writerow(r)

    print(f'Filtered matches: kept {len(kept)} rows for {len(approved)} approved bis_series')
    return bak

def run_downloader():
    # invoke the existing download script
    cmd = [sys.executable, str(DOWNLOAD_SCRIPT)]
    print('Running downloader:', ' '.join(cmd))
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print('Downloader failed', e); return False
    return True

def restore_matches_backup(bak_path: Path):
    if bak_path and bak_path.exists():
        shutil.copy2(bak_path, MATCHES)
        print('Restored original matches from', bak_path)

def main():
    update_approvals()
    clean_bis_folder()
    bak = filter_matches_to_approved()
    ok = run_downloader()
    if not ok:
        print('Downloader reported failure; restoring matches and exiting')
        restore_matches_backup(bak)
        sys.exit(1)
    # restore matches so other workflows are unaffected
    restore_matches_backup(bak)
    print('Download step completed. Diagnostics available at analysis_outputs/bis_download_diagnostics.csv')

if __name__ == '__main__':
    main()
