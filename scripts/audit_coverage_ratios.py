import csv
from pathlib import Path

threshold = 0.003
path = Path(__file__).resolve().parents[1] / "catalog.csv"
issues = []
with path.open(newline="") as fp:
    reader = csv.DictReader(fp)
    for row in reader:
        series = row.get("series")
        ratio = row.get("coverage_ratio", "")
        if not ratio:
            continue
        try:
            ratio_val = float(ratio)
        except ValueError:
            continue
        window = row.get("window_obs", "")
        total = row.get("total_obs", "")
        try:
            window_val = float(window)
            total_val = float(total)
        except ValueError:
            continue
        if window_val < 1e-6:
            continue
        expected = total_val / window_val
        if abs(expected - ratio_val) > threshold:
            issues.append((series, ratio_val, expected))
print("issues", len(issues))
for series, ratio_val, expected in issues:
    print(series, ratio_val, expected)
