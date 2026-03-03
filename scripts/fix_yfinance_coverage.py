import csv
from pathlib import Path

SERIES_TO_FIX = {
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
}

CATALOG_PATH = Path(__file__).resolve().parents[1] / "catalog.csv"

with CATALOG_PATH.open(newline="") as catalog_fp:
    reader = csv.reader(catalog_fp)
    header = next(reader)
    rows = [list(row) for row in reader]

updated = False
for idx, row in enumerate(rows):
    series = row[0]
    if series not in SERIES_TO_FIX:
        continue
    try:
        float(row[1])
        continue
    except ValueError:
        pass

    window_str = row[10]
    total_str = row[11]
    try:
        window = float(window_str)
        total = float(total_str)
    except ValueError:
        raise SystemExit(f"Unexpected non-numeric window/total for {series}: {window_str}/{total_str}")
    if window == 0:
        raise SystemExit(f"Window observations is zero for {series}")

    ratio = total / window
    ratio_str = f"{ratio:.6f}"

    old = row.copy()
    new_row = row.copy()
    new_row[1] = ratio_str
    mapping = [
        (7, 1),
        (8, 7),
        (9, 8),
        (10, 9),
        (11, 10),
        (12, 11),
        (13, 12),
        (14, 13),
        (15, 14),
        (16, 15),
        (17, 16),
        (18, 17),
        (19, 18),
    ]
    for target, source in mapping:
        new_row[target] = old[source]
    rows[idx] = new_row
    updated = True

if not updated:
    print("No YFinance coverage fixes needed")
else:
    with CATALOG_PATH.open("w", newline="") as catalog_fp:
        writer = csv.writer(catalog_fp)
        writer.writerow(header)
        writer.writerows(rows)
    print("Updated YFinance sector coverage ratios")
