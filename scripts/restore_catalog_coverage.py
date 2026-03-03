import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
CATALOG = BASE_DIR / "catalog.csv"
BACKUP = BASE_DIR / "catalog.csv.bak"
COPY_FIELDS = ["coverage_ratio", "median_gap_days", "last_observation", "window_obs", "total_obs"]

with BACKUP.open(newline="") as backup_fp:
    backup = {row["series"]: row for row in csv.DictReader(backup_fp)}

with CATALOG.open(newline="") as cat_fp:
    reader = csv.DictReader(cat_fp)
    rows = list(reader)
    fieldnames = reader.fieldnames

if fieldnames is None:
    raise SystemExit("Catalog file appears empty or malformed")

updated = False
for row in rows:
    src = backup.get(row["series"])
    if not src:
        continue
    for field in COPY_FIELDS:
        if row.get(field, "") != src.get(field, ""):
            row[field] = src.get(field, "")
            updated = True

if not updated:
    print("No coverage updates needed")
else:
    with CATALOG.open("w", newline="") as cat_fp:
        writer = csv.DictWriter(cat_fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Coverage stats synchronized from backup")
