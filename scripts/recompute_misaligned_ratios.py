import csv
from pathlib import Path

threshold = 0.003
path = Path(__file__).resolve().parents[1] / "catalog.csv"
updated = False
with path.open(newline="") as fp:
    reader = csv.DictReader(fp)
    fieldnames = reader.fieldnames
    rows = list(reader)
if fieldnames is None:
    raise SystemExit("Missing header")
for row in rows:
    ratio = row.get("coverage_ratio", "").strip()
    window = row.get("window_obs", "").strip()
    total = row.get("total_obs", "").strip()
    if not ratio or not window or not total:
        continue
    try:
        ratio_val = float(ratio)
    except ValueError:
        continue
    try:
        window_val = float(window)
        total_val = float(total)
    except ValueError:
        continue
    if window_val < 1e-6:
        continue
    expected = total_val / window_val
    if abs(expected - ratio_val) <= threshold:
        continue
    row["coverage_ratio"] = f"{expected:.6f}"
    updated = True
if updated:
    with path.open("w", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Recomputed mismatch ratios")
else:
    print("Ratios already match totals/window")
