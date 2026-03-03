import csv
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DATE = datetime(1990, 2, 1)
CATALOG_PATH = Path(__file__).resolve().parents[1] / "catalog.csv"
FREQUENCY_PERIOD_MAP = {
    "weekly": "W",
    "monthly": "M",
    "quarterly": "Q",
    "annual": "Y",
}

def trading_days_between(end_date: datetime) -> int:
    if end_date < BASE_DATE:
        return 0
    return len(pd.bdate_range(start=BASE_DATE, end=end_date, freq="B"))

def expected_observations(end_date: datetime, frequency_label: str) -> int:
    freq = frequency_label.strip().lower()
    if freq == "daily":
        return trading_days_between(end_date)
    period_alias = FREQUENCY_PERIOD_MAP.get(freq)
    if not period_alias:
        return 0
    try:
        start_period = pd.Period(BASE_DATE, freq=period_alias)
        end_period = pd.Period(end_date, freq=period_alias)
    except ValueError:
        return 0
    if end_period < start_period:
        return 0
    return end_period.ordinal - start_period.ordinal + 1

with CATALOG_PATH.open(newline="") as catalog_fp:
    reader = csv.DictReader(catalog_fp)
    rows = list(reader)
    fieldnames = reader.fieldnames

if fieldnames is None:
    raise SystemExit("Missing catalog header")

updated = False
for row in rows:
    freq_label = row.get("frequency_label", "")
    last_obs = row.get("last_observation", "").split("T")[0]
    if not last_obs:
        continue
    try:
        last_date = datetime.fromisoformat(last_obs)
    except ValueError:
        continue
    denominators = expected_observations(last_date, freq_label)
    if denominators == 0:
        continue
    try:
        total_obs = float(row.get("total_obs", ""))
    except ValueError:
        continue
    new_ratio = total_obs / denominators
    old_ratio = row.get("coverage_ratio", "")
    if not old_ratio:
        pass
    else:
        try:
            if abs(float(old_ratio) - new_ratio) < 1e-6:
                continue
        except ValueError:
            pass
    row["coverage_ratio"] = f"{new_ratio:.6f}"
    updated = True

if updated:
    with CATALOG_PATH.open("w", newline="") as catalog_fp:
        writer = csv.DictWriter(catalog_fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print("Rebased coverage ratios to 1990 trading window")
else:
    print("No coverage ratios required rebasing")
