#!/usr/bin/env python3
"""Extract BIS components from cached raw flow payloads for matching series."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data_repository" / "raw" / "bis_api"
OUT_CSV = ROOT / "analysis_outputs" / "bis_series_components.csv"
KEY_ATTRS = ["FREQ", "BORROWERS_CTY", "DSR_BORROWERS"]


def flow_slug_from_path(path: Path) -> str | None:
    name = path.stem
    if not name.startswith("bis_api_"):
        return None
    return name[len("bis_api_"):]


def slug_to_flow(slug: str) -> str:
    parts = slug.split("_")
    if len(parts) <= 5:
        return slug
    base = "_".join(parts[:-1])
    base_parts = base.split("_")
    if len(base_parts) < 5:
        return slug
    agency = base_parts[0].upper()
    flow_id = f"{base_parts[1].upper()}_{base_parts[2].upper()}"
    version = f"{base_parts[3]}.{base_parts[4]}"
    return f"{agency},{flow_id},{version}"


def component_string(attrs: dict[str, str]) -> str:
    parts: list[str] = []
    for key in KEY_ATTRS:
        val = attrs.get(key)
        if val:
            parts.append(f"{key}={val}")
    return "|".join(parts)


def component_flags(attrs: dict[str, str]) -> str:
    parts = []
    for key in KEY_ATTRS:
        val = attrs.get(key)
        if val:
            parts.append(f"--component {key}={val}")
    return " ".join(parts)


def parse_series(file_path: Path, flow: str, flow_slug: str) -> Iterable[dict[str, str]]:
    context = ET.iterparse(str(file_path), events=("end",))
    for _, elem in context:
        if not (elem.tag or "").lower().endswith("series"):
            elem.clear()
            continue
        attrs = {k: v for k, v in elem.attrib.items()}
        obs_count = 0
        for child in elem:
            if (child.tag or "").lower().endswith("obs"):
                obs_count += 1
        comp = component_string(attrs)
        if not comp:
            elem.clear()
            continue
        row = {
            "flow": flow,
            "flow_slug": flow_slug,
            "file_path": str(file_path),
            "title_ts": attrs.get("TITLE_TS", ""),
            "freq": attrs.get("FREQ", ""),
            "borrowers_cty": attrs.get("BORROWERS_CTY", ""),
            "dsr_borrowers": attrs.get("DSR_BORROWERS", ""),
            "decimals": attrs.get("DECIMALS", ""),
            "obs_count": str(obs_count),
            "component_string": comp,
            "component_flags": component_flags(attrs),
        }
        elem.clear()
        yield row


def main() -> None:
    if not RAW_DIR.exists():
        raise SystemExit(f"Missing raw BIS payload directory at {RAW_DIR}")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for xml_file in sorted(RAW_DIR.glob("bis_api_*.xml")):
        slug = flow_slug_from_path(xml_file)
        if not slug:
            continue
        flow = slug_to_flow(slug)
        for row in parse_series(xml_file, flow, slug):
            key = (row["flow"], row["title_ts"], row["component_string"])
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    rows.sort(key=lambda r: (r["flow"], r["title_ts"], r["component_string"]))
    fieldnames = [
        "flow",
        "flow_slug",
        "file_path",
        "title_ts",
        "freq",
        "borrowers_cty",
        "dsr_borrowers",
        "decimals",
        "obs_count",
        "component_string",
        "component_flags",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Extracted {len(rows)} series/component rows to {OUT_CSV}")


if __name__ == "__main__":
    main()