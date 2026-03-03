import csv
import xml.etree.ElementTree as ET
import re
from pathlib import Path
from typing import Iterable

BASE = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE / "catalog.csv"
BIS_DSR_CATALOG = BASE / "data_repository" / "processed" / "BIS_DSR_series_catalog_corefive.csv"
BIS_EER_XML = BASE / "data_repository" / "raw" / "bis_api" / "bis_api_bis_ws_eer_1_0_all_20251216T142721Z.xml"
BIS_CATALOG_OUTPUT = BASE / "data_repository" / "processed" / "BIS_catalog.csv"


def strip_ns(tag: str) -> str:
    if "}" in tag:
        tag = tag.split("}", 1)[1]
    if ":" in tag:
        tag = tag.split(":", 1)[1]
    return tag


def normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()


def load_catalog_rows() -> Iterable[dict]:
    with CATALOG_PATH.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            yield row


def load_bis_dsr_entries() -> list[dict]:
    entries = []
    if not BIS_DSR_CATALOG.exists():
        raise FileNotFoundError(f"Missing DSR catalog at {BIS_DSR_CATALOG}")
    with BIS_DSR_CATALOG.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.append({
                "dataset": "BIS DSR",
                "title": row.get("title", ""),
                "freq": row.get("freq", ""),
                "start": row.get("start", ""),
                "end": row.get("end", ""),
                "observations": row.get("observations", ""),
                "source_file": row.get("source_file", ""),
                "series_key": row.get("series_key", ""),
            })
    return entries


def parse_eer_entries() -> list[dict]:
    entries = []
    if not BIS_EER_XML.exists():
        raise FileNotFoundError(f"Missing EER XML at {BIS_EER_XML}")
    context = ET.iterparse(BIS_EER_XML, events=("end",))
    for event, elem in context:
        if strip_ns(elem.tag) != "Series":
            elem.clear()
            continue
        title = elem.get("TITLE_TS", "")
        freq = elem.get("FREQ", "")
        ref_area = elem.get("REF_AREA", "")
        eer_type = elem.get("EER_TYPE", "")
        eer_basket = elem.get("EER_BASKET", "")
        series_key = "|".join(f"{k}={v}" for k, v in sorted(elem.attrib.items()))
        first_period = None
        last_period = None
        valid_obs = 0
        for child in elem:
            if strip_ns(child.tag) != "Obs":
                continue
            period = child.get("TIME_PERIOD")
            value = child.get("OBS_VALUE")
            if not period or not value:
                continue
            try:
                float(value)
            except ValueError:
                continue
            if first_period is None:
                first_period = period
            last_period = period
            valid_obs += 1
        entries.append({
            "dataset": "BIS EER",
            "title": title or f"{ref_area} EER {eer_type} {eer_basket}",
            "freq": freq,
            "start": first_period or "",
            "end": last_period or "",
            "observations": valid_obs,
            "source_file": BIS_EER_XML.name,
            "series_key": series_key,
        })
        elem.clear()
    return entries


def best_match(entity: str, entries: Iterable[dict]) -> tuple[dict | None, int]:
    normalized_entity = normalize(entity)
    if not normalized_entity:
        return None, 0
    tokens = normalized_entity.split()
    best = None
    best_score = 0
    for entry in entries:
        title_norm = normalize(entry.get("title", ""))
        score = sum(1 for token in tokens if token and token in title_norm)
        if score > best_score:
            best = entry
            best_score = score
    return best, best_score


def build_bis_catalog() -> None:
    bis_entries = load_bis_dsr_entries() + parse_eer_entries()
    with BIS_CATALOG_OUTPUT.open("w", newline="", encoding="utf-8") as outfile:
        fieldnames = [
            "catalog_series",
            "entity",
            "relationship",
            "match_reason",
            "bis_dataset",
            "bis_series_title",
            "bis_frequency",
            "bis_start",
            "bis_end",
            "bis_observations",
            "bis_source_file",
            "bis_series_key",
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in load_catalog_rows():
            series = row.get("series", "")
            if not series or "_beta" in series.lower():
                continue
            entity = row.get("entity", "").strip()
            match, score = best_match(entity, bis_entries)
            if match:
                relationship = "same" if score >= len(entity.split()) and entity else "similar"
                match_reason = (
                    "title contains entity tokens"
                    if score > 0 and entity
                    else "matched by heuristic"
                )
                writer.writerow({
                    "catalog_series": series,
                    "entity": entity,
                    "relationship": relationship,
                    "match_reason": match_reason,
                    "bis_dataset": match.get("dataset", ""),
                    "bis_series_title": match.get("title", ""),
                    "bis_frequency": match.get("freq", ""),
                    "bis_start": match.get("start", ""),
                    "bis_end": match.get("end", ""),
                    "bis_observations": match.get("observations", ""),
                    "bis_source_file": match.get("source_file", ""),
                    "bis_series_key": match.get("series_key", ""),
                })
            else:
                writer.writerow({
                    "catalog_series": series,
                    "entity": entity,
                    "relationship": "missing",
                    "match_reason": "no BIS match",
                    "bis_dataset": "",
                    "bis_series_title": "",
                    "bis_frequency": "",
                    "bis_start": "",
                    "bis_end": "",
                    "bis_observations": "",
                    "bis_source_file": "",
                    "bis_series_key": "",
                })
    print(f"Wrote {BIS_CATALOG_OUTPUT.name} with BIS matches for catalog entities.")


def main() -> None:
    build_bis_catalog()


if __name__ == "__main__":
    main()
