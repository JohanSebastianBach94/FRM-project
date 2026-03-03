#!/usr/bin/env python3
"""Convert the BIS DSR XML dump into per-country time series for the selected five economies."""
import csv
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data_repository" / "raw" / "bis_api"
INFILE = RAW / "bis_api_bis_ws_dsr_1_0_all_20251216T142436Z.xml"
OUT_DIR = BASE / "data_repository" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

COUNTRY_ISO3_MAP = {
    "US": "USA",
    "IT": "ITA",
    "FR": "FRA",
    "DE": "DEU",
    "ES": "ESP",
}
TARGET_COUNTRIES = set(COUNTRY_ISO3_MAP)
BORROWER_LABELS = {
    "P": "PrivateNonFinancial",
    "H": "HouseholdsNPISH",
    "N": "NonFinancialCorporations",
}

if not INFILE.exists():
    raise FileNotFoundError(f"Missing BIS DSR file at {INFILE}")

series_rows = defaultdict(list)
series_titles = {}
series_catalog = []


def strip_ns(tag: str) -> str:
    if '}' in tag:
        tag = tag.split('}', 1)[1]
    if ':' in tag:
        tag = tag.split(':', 1)[1]
    return tag

print(f"Parsing {INFILE.name} for {', '.join(sorted(TARGET_COUNTRIES))}...", flush=True)
context = ET.iterparse(INFILE, events=("end",))
for event, elem in context:
    tag = strip_ns(elem.tag)
    if tag != "Series":
        if tag != "Obs":
            elem.clear()
        continue
    country_code = elem.get("BORROWERS_CTY")
    if country_code not in TARGET_COUNTRIES:
        elem.clear()
        continue
    country_iso = COUNTRY_ISO3_MAP.get(country_code)
    borrowers = elem.get("DSR_BORROWERS")
    title = elem.get("TITLE_TS") or ""
    rows = []
    first_period = None
    last_period = None
    for child in elem:
        if strip_ns(child.tag) != "Obs":
            continue
        period = child.get("TIME_PERIOD") or child.get("PERIOD")
        value = child.get("OBS_VALUE")
        if not period or not value:
            continue
        try:
            numeric = float(value)
        except ValueError:
            continue
        rows.append((period, numeric))
        if first_period is None:
            first_period = period
        last_period = period
    obs_added = len(rows)
    series_catalog.append({
        "country_iso3": country_iso,
        "country_iso2": country_code,
        "borrowers": borrowers or "",
        "borrower_label": BORROWER_LABELS.get(borrowers, ""),
        "freq": elem.get("FREQ") or "",
        "title": title,
        "observations": obs_added,
        "start": first_period or "",
        "end": last_period or "",
        "series_key": "|".join(f"{k}={v}" for k, v in sorted(elem.attrib.items())),
        "source_file": INFILE.name,
    })
    if borrowers not in BORROWER_LABELS:
        elem.clear()
        continue
    key = (country_iso, borrowers)
    series_titles[key] = title
    for period, numeric in rows:
        series_rows[key].append((period, numeric))
    print(f"  {country_iso} ({BORROWER_LABELS[borrowers]}): captured {obs_added} rows", flush=True)
    elem.clear()

metadata_path = OUT_DIR / "BIS_DSR_metadata.csv"
metadata_writer = csv.writer(metadata_path.open("w", newline=""))
metadata_writer.writerow([
    "country",
    "borrowers",
    "label",
    "title",
    "observations",
    "start",
    "end",
    "filename",
])

for (country, borrowers), rows in series_rows.items():
    if not rows:
        continue
    label = BORROWER_LABELS[borrowers]
    rows.sort(key=lambda item: item[0])
    file_name = f"BIS_DSR_{label}_{country}.csv"
    output_path = OUT_DIR / file_name
    with output_path.open("w", newline="") as out_csv:
        writer = csv.writer(out_csv)
        writer.writerow(["period", "value"])
        for period, value in rows:
            writer.writerow([period, f"{value:.6f}"])
    metadata_writer.writerow([
        country,
        borrowers,
        label,
        series_titles.get((country, borrowers), ""),
        len(rows),
        rows[0][0],
        rows[-1][0],
        file_name,
    ])
    print(f"Wrote {output_path.name} ({len(rows)} quarters from {rows[0][0]} to {rows[-1][0]})", flush=True)

print(f"Metadata summary saved to {metadata_path.name}")

series_catalog_path = OUT_DIR / "BIS_DSR_series_catalog_corefive.csv"
series_catalog_writer = csv.writer(series_catalog_path.open("w", newline=""))
series_catalog_writer.writerow([
    "dataset",
    "country_iso3",
    "country_iso2",
    "borrowers",
    "borrower_label",
    "freq",
    "title",
    "series_key",
    "observations",
    "start",
    "end",
    "source_file",
])
for entry in series_catalog:
    series_catalog_writer.writerow([
        "BIS DSR",
        entry["country_iso3"],
        entry["country_iso2"],
        entry["borrowers"],
        entry["borrower_label"],
        entry["freq"],
        entry["title"],
        entry["series_key"],
        entry["observations"],
        entry["start"],
        entry["end"],
        entry["source_file"],
    ])
print(f"Series catalog saved to {series_catalog_path.name}")
