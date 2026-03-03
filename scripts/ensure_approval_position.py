import csv
import shutil
from datetime import datetime
from pathlib import Path


def ensure_position(catalog_path: Path, col_name: str = "approval", after_col: str = "match"):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup = catalog_path.with_name(catalog_path.name + f".bak_pos_{ts}")
    shutil.copy2(catalog_path, backup)

    with catalog_path.open("r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if col_name not in fieldnames:
        # nothing to do
        print(f"Column '{col_name}' not present; no change made.")
        return

    # remove existing col and re-insert after after_col
    fieldnames = [c for c in fieldnames if c != col_name]
    try:
        idx = fieldnames.index(after_col)
    except ValueError:
        # append at end
        fieldnames.append(col_name)
    else:
        fieldnames.insert(idx + 1, col_name)

    # Ensure rows have the column
    for r in rows:
        if col_name not in r:
            r[col_name] = ""

    with catalog_path.open("w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Rewrote {catalog_path} with '{col_name}' after '{after_col}' (backup at {backup})")


if __name__ == '__main__':
    catalog = Path(r"c:\Users\frank\Documents\FRM project\data_repository\processed\BIS_catalog.csv")
    ensure_position(catalog, "approval", "match")
