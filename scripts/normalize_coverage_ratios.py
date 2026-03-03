import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE_DIR / "catalog.csv"
COPY_FIELDS = [
    "coverage_bucket",
    "frequency_label",
    "median_gap_days",
    "last_observation",
    "window_obs",
    "total_obs",
    "source",
    "source_group",
    "source_detail",
    "has_data",
    "provider",
    "fetch_method",
    "storage_path",
]

FREQUENCY_FIELDS = {"daily", "weekly", "monthly", "quarterly", "annual"}

updated = False

with CATALOG_PATH.open(newline="") as catalog_fp:
    reader = csv.DictReader(catalog_fp)
    fieldnames = reader.fieldnames
    if fieldnames is None:
        raise SystemExit("Catalog header missing")
    rows = []
    for row in reader:
        row = dict(row)
        try:
            float(row["coverage_ratio"])
            rows.append(row)
            continue
        except ValueError:
            pass
        original = row.copy()

        ratio = ""
        window_value = original["window_obs"].strip()
        total_value = original["total_obs"].strip()
        shift_needed = False
        freq_value = original["frequency_label"].strip()
        if freq_value:
            try:
                float(freq_value)
                shift_needed = True
            except ValueError:
                shift_needed = False

        bucket_value = original["coverage_ratio"].strip()

        if shift_needed:
            window_value = original["last_observation"].strip()
            total_value = original["window_obs"].strip()

        if window_value and total_value:
            try:
                ratio = f"{float(total_value) / float(window_value):.6f}"
            except ZeroDivisionError:
                ratio = ""
            except ValueError:
                ratio = ""

        row["coverage_ratio"] = ratio
        row["coverage_bucket"] = bucket_value

        if shift_needed:
            row["frequency_label"] = original["coverage_bucket"].strip()
            row["median_gap_days"] = original["frequency_label"].strip()
            row["last_observation"] = original["median_gap_days"].strip()
            row["window_obs"] = original["last_observation"].strip()
            row["total_obs"] = original["window_obs"].strip()
            row["source"] = original["total_obs"].strip()
            row["source_group"] = original["source"].strip()
            row["source_detail"] = original["source_group"].strip()
            row["has_data"] = original["source_detail"].strip()
            row["provider"] = original["has_data"].strip()
            row["fetch_method"] = original["provider"].strip()
            row["storage_path"] = original["fetch_method"].strip()
        updated = True
        rows.append(row)

if not updated:
    print("No normalization needed")
else:
    with CATALOG_PATH.open("w", newline="") as catalog_fp:
        writer = csv.DictWriter(catalog_fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Normalized coverage_ratio and realigned coverage fields")
