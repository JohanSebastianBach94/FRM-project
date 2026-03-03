import csv
import shutil
import argparse
from datetime import datetime
from pathlib import Path


def backup_file(path: Path) -> Path:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(path.name + f".bak_review_{ts}")
    shutil.copy2(path, backup)
    return backup


def review(catalog_path: Path, start_row: int = 1, auto_approve_exact: bool = False):
    backup = backup_file(catalog_path)
    print(f"Backup created: {backup}")

    with catalog_path.open("r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)

    if 'approval' not in fieldnames:
        fieldnames.insert(fieldnames.index('match') + 1 if 'match' in fieldnames else len(fieldnames), 'approval')
        for r in rows:
            r['approval'] = ''

    print("Instructions: type 'A' to approve, 'D' to disapprove, 'S' to skip, 'Q' to quit and save progress.")
    print(f"Starting review at row {start_row}. Auto-approve exact matches: {auto_approve_exact}")

    changed = False
    total = len(rows)
    for i, row in enumerate(rows):
        # skip rows before start
        if (i + 1) < start_row:
            continue

        # show a concise row summary
        series = (row.get('series', '') or row.get('bis_series', '') or '')
        entity = row.get('entity', '')
        match = row.get('match', '')
        current = row.get('approval', '')

        # display with clearer formatting and indicate exact match
        series_disp = series if series else '<empty>'
        match_disp = match if match else '<empty>'
        exact = (series == match) and series != ''
        exact_marker = ' (EXACT)' if exact else ''

        print(f"\nRow {i+1}/{total}: series='{series_disp}' | match='{match_disp}'{exact_marker} | entity='{entity}' | approval={current}")

        # auto-approve exact matches if requested
        if auto_approve_exact and exact:
            if current != 'A':
                row['approval'] = 'A'
                changed = True
                print("Auto-approved (exact match).")
            else:
                print("Already approved.")
            continue

        while True:
            resp = input("Set approval [A/D/S/Q]: ").strip().upper()
            if resp in ('A', 'D', 'S', 'Q'):
                break
            print("Invalid input. Enter 'A', 'D', 'S', or 'Q'.")

        if resp == 'Q':
            print("Quitting and saving progress...")
            break
        if resp == 'S':
            continue

        # set approval
        row['approval'] = resp
        changed = True

    if changed:
        with catalog_path.open("w", newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote updates to {catalog_path}")
    else:
        print("No changes made.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Interactive BIS catalog reviewer')
    parser.add_argument('--catalog', '-c', default=r"c:\Users\frank\Documents\FRM project\data_repository\processed\BIS_catalog.csv", help='Path to BIS catalog CSV')
    parser.add_argument('--start', '-s', type=int, default=1, help='1-based row number to start reviewing from')
    parser.add_argument('--auto-approve-exact', action='store_true', help='Automatically approve rows where match equals series exactly')
    args = parser.parse_args()

    catalog = Path(args.catalog)
    review(catalog, start_row=max(1, args.start), auto_approve_exact=bool(args.auto_approve_exact))
