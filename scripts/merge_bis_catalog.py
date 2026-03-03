from __future__ import annotations

import csv
from pathlib import Path

BASE_FILE = Path("data_repository/processed/BIS_catalog.csv.bak_prep_20251218T162534Z")
NORM_FILE = Path("data_repository/processed/BIS_catalog.bak_norm_20251218T133145Z.csv")
OUT_FILE = Path("data_repository/processed/BIS_catalog.csv")

EXPECTED_FIELDS = [
    "series",
    "entity",
    "country_code",
    "bis_flow",
    "match",
    "approval",
    "coverage",
    "frequency",
    "file_name",
    "bis_source_file",
    "bis_title",
    "bis_freq",
    "bis_start",
    "bis_end",
    "bis_observations",
    "series_key",
]

if not BASE_FILE.exists():
    raise SystemExit(f"Missing base catalog file: {BASE_FILE}")
if not NORM_FILE.exists():
    raise SystemExit(f"Missing normalized catalog file: {NORM_FILE}")

with BASE_FILE.open("r", newline="", encoding="utf-8") as bf:
    base_rows = list(csv.DictReader(bf))

with NORM_FILE.open("r", newline="", encoding="utf-8") as nf:
    norm_map = {row.get("series", ""): row for row in csv.DictReader(nf)}

with OUT_FILE.open("w", newline="", encoding="utf-8") as outf:
    writer = csv.DictWriter(outf, fieldnames=EXPECTED_FIELDS)
    writer.writeheader()
    for row in base_rows:
        merged = {field: row.get(field, "") for field in EXPECTED_FIELDS[:9]}
        norm = norm_map.get(row.get("series", ""), {})
        merged.update(
            {field: norm.get(field, "") for field in EXPECTED_FIELDS[9:]}
        )
        writer.writerow(merged)

print(f"Merged {len(base_rows)} catalog rows into {OUT_FILE}")
