import csv
import shutil
from datetime import datetime
from pathlib import Path


def add_approval_column(catalog_path: Path, col_name: str = "approval"):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    backup = catalog_path.with_name(catalog_path.name + f".bak_addcol_{ts}")
    shutil.copy2(catalog_path, backup)
    with catalog_path.open("r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if col_name in fieldnames:
        print(f"Column '{col_name}' already present in {catalog_path}")
        return

    fieldnames.append(col_name)
    for r in rows:
        r[col_name] = ""

    with catalog_path.open("w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Added column '{col_name}' to {catalog_path} (backup at {backup})")


if __name__ == '__main__':
    catalog = Path(r"c:\Users\frank\Documents\FRM project\data_repository\processed\BIS_catalog.csv")
    add_approval_column(catalog, "approval")
