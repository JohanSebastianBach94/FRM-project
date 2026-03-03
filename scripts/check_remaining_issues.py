import csv
from pathlib import Path

targets = {
    "BAMLH0A0CM",
    "GC.DOD.TOTL.GD.ZS_DEU",
}
path = Path(__file__).resolve().parents[1] / "catalog.csv"
with path.open(newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["series"] in targets:
            print(
                row["series"],
                repr(row["coverage_ratio"]),
                repr(row["coverage_bucket"]),
                repr(row["window_obs"]),
                repr(row["total_obs"]),
                repr(row["frequency_label"]),
            )
