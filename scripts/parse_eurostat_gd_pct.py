#!/usr/bin/env python3
"""Extract Government consolidated gross debt as % of GDP from Eurostat GOV_10DD_EDPT1 JSON dumps.

Input files:
- data_repository/raw/macro/euro_gov_10dd_edpt1_{ISO}.json

Filter:
- unit == 'PC_GDP'
- sector == 'S13' (General government)
- na_item == 'GD' (Government consolidated gross debt)

Output:
- data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.csv
- data_repository/raw/macro/general_government_gross_debt_pct_gdp_{ISO}.meta.json

This is used as a practical, high-coverage proxy for government debt (% GDP)
when the World Bank GC.DOD series is sparse.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parents[1]
MACRO = BASE / "data_repository" / "raw" / "macro"


def invert_index_map(idx_map: dict) -> dict[int, str]:
    inv: dict[int, str] = {}
    for code, pos in (idx_map or {}).items():
        inv[int(pos)] = code
    return inv


def decode_flat_index(flat_idx: int | str, sizes: list[int]) -> list[int]:
    idx = int(flat_idx)
    coords = [0] * len(sizes)
    for i in range(len(sizes) - 1, -1, -1):
        size = int(sizes[i]) if sizes[i] else 0
        if size <= 0:
            coords[i] = 0
            continue
        coords[i] = idx % size
        idx //= size
    return coords


def process_file(path: Path) -> Path | None:
    raw = json.loads(path.read_text(encoding="utf-8"))
    iso = path.name.split("_")[-1].replace(".json", "")

    dim_order = raw.get("id") or list(raw.get("dimension", {}).keys())
    if not dim_order:
        print("No dimension order in", path.name)
        return None

    sizes = raw.get("size") or [len(raw["dimension"][d]["category"]["index"]) for d in dim_order]

    pos_code_maps: dict[str, dict[int, str]] = {}
    label_maps: dict[str, dict] = {}
    for d in dim_order:
        cat = raw["dimension"][d].get("category", {})
        pos_code_maps[d] = invert_index_map(cat.get("index", {}))
        label_maps[d] = cat.get("label", {})

    values = raw.get("value", {}) or {}
    series: dict[int, float] = {}

    for flat_k, v in values.items():
        if v is None:
            continue
        coords = decode_flat_index(flat_k, sizes)
        coord_map: dict[str, str | None] = {}
        for i, d in enumerate(dim_order):
            coord_map[d] = pos_code_maps[d].get(coords[i])

        if coord_map.get("unit") != "PC_GDP":
            continue
        if coord_map.get("sector") != "S13":
            continue
        if coord_map.get("na_item") != "GD":
            continue

        time_code = coord_map.get("time")
        try:
            year = int(label_maps["time"].get(time_code, time_code))
        except Exception:
            continue

        try:
            series[year] = float(v)
        except Exception:
            continue

    if not series:
        print("No PC_GDP GD/S13 series found in", path.name)
        return None

    out_csv = MACRO / f"general_government_gross_debt_pct_gdp_{iso}.csv"
    df = pd.DataFrame(
        {
            "year": sorted(series.keys()),
            "debt_pct": [series[y] for y in sorted(series.keys())],
            "qc_flag": [""] * len(series),
        }
    )
    df.to_csv(out_csv, index=False)

    meta = {
        "iso": iso,
        "source_file": path.name,
        "filters": {"unit": "PC_GDP", "sector": "S13", "na_item": "GD"},
        "note": "Eurostat GOV_10DD_EDPT1, GD (general government consolidated gross debt) as % of GDP",
    }
    out_csv.with_suffix(".meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print("Wrote", out_csv.name, "obs=", len(df), "range=", df["year"].min(), "-", df["year"].max())
    return out_csv


def main() -> None:
    files = sorted(MACRO.glob("euro_gov_10dd_edpt1_*.json"))
    if not files:
        print("No euro_gov_10dd_edpt1_*.json files found under", MACRO)
        return

    for p in files:
        try:
            process_file(p)
        except Exception as exc:
            print("Error processing", p.name, exc)


if __name__ == "__main__":
    main()
