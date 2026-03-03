#!/usr/bin/env python3
"""List hole-prone drivers from the feed."""
from pathlib import Path
import pandas as pd

HOLE_COLUMNS = (
    "missing_required_list",
    "missing_optional_list",
    "insufficient_required_list",
    "insufficient_optional_list",
    "insufficient_required_details",
    "insufficient_optional_details",
)

feed = pd.read_csv(Path("analysis_outputs") / "risk_factor_holes_feed.csv", dtype=str)
holes = set()
for column in HOLE_COLUMNS:
    if column not in feed:
        continue
    for value in feed[column].fillna(""):
        for entry in (item.strip() for item in value.split("|") if item.strip()):
            name = entry.split(" (", 1)[0].strip()
            if name:
                holes.add(name)

print(f"{len(holes)} unique hole-prone drivers:")
for driver in sorted(holes):
    print(driver)
