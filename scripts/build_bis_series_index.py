import csv
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data_repository" / "raw" / "bis_api"
INDEX_PATH = BASE / "data_repository" / "processed" / "bis_series_index.csv"

FLOW_MAP_PATH = RAW_DIR / "bis_api_metadata.csv"
COUNTRY_DIMENSIONS = (
    "REF_AREA",
    "REF_AREA1",
    "REF_AREA2",
    "COUNTRY",
    "COUNTRY_CODE",
    "BORROWERS_CTY",
)
BORROWER_DIMENSIONS = ("BORROWERS", "MODE", "DEBTOR")
TITLE_ATTRIBUTES = ("TITLE", "TITLE_TS", "TITLE_TS_EN", "TITLE_GROUP")

FIELDNAMES = [
    "bis_flow",
    "bis_source_file",
    "bis_freq",
    "country_code",
    "borrower_type",
    "bis_title",
    "bis_start",
    "bis_end",
    "bis_observations",
    "series_key",
    "keywords",
]


def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def load_flow_map() -> dict[str, str]:
    if not FLOW_MAP_PATH.exists():
        return {}
    flow_map: dict[str, str] = {}
    with FLOW_MAP_PATH.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            file_name = Path(row.get("file_path", "")).name
            flow_map[file_name] = row.get("flow", "")
    return flow_map


def extract_country(dimensions: dict[str, str]) -> str:
    for key in COUNTRY_DIMENSIONS:
        value = dimensions.get(key, "")
        if value:
            return value
    return ""


def extract_borrower(dimensions: dict[str, str]) -> str:
    for key in BORROWER_DIMENSIONS:
        if value := dimensions.get(key, ""):
            return value
    return ""


def build_series_key(dimensions: dict[str, str]) -> str:
    if not dimensions:
        return ""
    pairs = sorted(f"{key}:{value}" for key, value in dimensions.items())
    return "|".join(pairs)


def keywords_from_entry(flow: str, title: str, dimensions: dict[str, str]) -> str:
    parts = [flow, title] if title else [flow]
    parts.extend(dimensions.values())
    return " ".join(part for part in parts if part)


def pick_title(attributes: dict[str, str]) -> str:
    for key in TITLE_ATTRIBUTES:
        if value := attributes.get(key):
            return value
    return ""


def parse_xml(file_path: Path, flow_name: str, writer: csv.DictWriter) -> None:
    context = ET.iterparse(file_path, events=("start", "end"))
    inside_series = False
    inside_series_key = False
    inside_series_attrs = False
    current_series: dict[str, Any] | None = None

    for event, elem in context:
        tag = strip_ns(elem.tag)
        if event == "start" and tag == "Series":
            inside_series = True
            current_series = {
                "dimensions": {},
                "attributes": {},
                "first_period": None,
                "last_period": None,
                "observations": 0,
                "freq": elem.attrib.get("FREQ", ""),
            }
            # copy Series attributes so we capture dimensions without SeriesKey
            current_series["dimensions"].update(elem.attrib)
            current_series["attributes"].update(elem.attrib)
        elif inside_series and event == "start" and tag == "SeriesKey":
            inside_series_key = True
        elif inside_series and event == "start" and tag == "SeriesAttributes":
            inside_series_attrs = True
        elif inside_series and event == "end" and tag == "SeriesKey":
            inside_series_key = False
        elif inside_series and event == "end" and tag == "SeriesAttributes":
            inside_series_attrs = False
        elif inside_series and event == "end" and tag == "Value":
            current = current_series
            if not current or "id" not in elem.attrib:
                continue
            target = None
            if inside_series_key:
                target = current["dimensions"]
            elif inside_series_attrs:
                target = current["attributes"]
            if target is None:
                continue
            value = elem.attrib.get("value", "")
            if value:
                target[elem.attrib["id"]] = value
        elif inside_series and event == "end" and tag == "Obs":
            assert current_series is not None
            period = elem.attrib.get("TIME_PERIOD") or elem.attrib.get("TIME")
            if not period:
                obs_dim = elem.find(".//ObsDimension")
                period = obs_dim.attrib.get("value") if obs_dim is not None else None
            if period:
                if current_series["first_period"] is None:
                    current_series["first_period"] = period
                current_series["last_period"] = period
            if elem.attrib.get("OBS_VALUE"):
                current_series["observations"] += 1
        elif event == "end" and tag == "Series" and current_series is not None:
            entry = {
                "bis_flow": flow_name,
                "bis_source_file": file_path.name,
                "bis_freq": current_series.get("freq", ""),
                "country_code": extract_country(current_series["dimensions"]),
                "borrower_type": extract_borrower(current_series["dimensions"]),
                "bis_title": pick_title(current_series["attributes"]),
                "bis_start": current_series["first_period"] or "",
                "bis_end": current_series["last_period"] or "",
                "bis_observations": current_series["observations"],
                "series_key": build_series_key(current_series["dimensions"]),
                "keywords": keywords_from_entry(
                    flow_name,
                    current_series["attributes"].get("TITLE", ""),
                    current_series["dimensions"],
                ),
            }
            writer.writerow(entry)
            elem.clear()
            inside_series = False
            current_series = None


def main() -> None:
    flow_map = load_flow_map()
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    with INDEX_PATH.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        for xml_file in sorted(RAW_DIR.glob("bis_api_*.xml")):
            flow_name = flow_map.get(xml_file.name, xml_file.stem)
            print(f"Indexing {xml_file.name} ({flow_name})")
            parse_xml(xml_file, flow_name, writer)


if __name__ == "__main__":
    main()
