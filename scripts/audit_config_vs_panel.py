"""Audit configured block series vs merged panel columns.

Goal:
- For each (country, block) in config/country_blocks_extended.yaml,
  list which configured series_codes are NOT present as columns in the
  merged stress panel (stress_indicators_expanded.csv).

This answers: which configured series aren’t making it into the merged panel.

Outputs:
- analysis_outputs/diagnostics/config_vs_panel_missing_series.csv
- analysis_outputs/diagnostics/config_vs_panel_missing_series.md

Usage:
    python scripts/audit_config_vs_panel.py
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import csv
import sys

import pandas as pd
import yaml


ROOT = Path(__file__).resolve().parents[1]

BLOCKS_PATH = ROOT / "config" / "country_blocks_extended.yaml"
PANEL_CANDIDATES = [
    ROOT / "stress_indicators_expanded.csv",
    ROOT / "data" / "stress_indicators_expanded.csv",
    ROOT / "data_pipeline" / "data" / "stress_indicators_expanded.csv",
]

DIAG_DIR = ROOT / "analysis_outputs" / "diagnostics"
DIAG_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = DIAG_DIR / "config_vs_panel_missing_series.csv"
OUT_MD = DIAG_DIR / "config_vs_panel_missing_series.md"


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _pick_panel_path() -> Path:
    for candidate in PANEL_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "No stress_indicators_expanded.csv found. Tried: "
        + ", ".join(str(p) for p in PANEL_CANDIDATES)
    )


def _read_panel_columns(path: Path) -> set[str]:
    # Read just the header cheaply.
    header = pd.read_csv(path, nrows=0).columns.tolist()
    cols = {str(c).strip() for c in header if str(c).strip()}
    cols.discard("Date")
    return cols


def _as_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(x) for x in value if str(x).strip()]
    return [str(value)]


def main() -> int:
    payload = _load_yaml(BLOCKS_PATH)
    panel_path = _pick_panel_path()
    panel_cols = _read_panel_columns(panel_path)

    countries = payload.get("country_blocks") or []
    if not isinstance(countries, list) or not countries:
        print("No country_blocks found in YAML.")
        return 2

    rows: list[dict[str, str]] = []

    for country in countries:
        if not isinstance(country, dict):
            continue
        iso = str(country.get("iso_code") or "").strip()
        name = str(country.get("country") or iso).strip()
        region = str(country.get("region") or "").strip()

        blocks = country.get("blocks") or []
        if not isinstance(blocks, list):
            continue

        for block in blocks:
            if not isinstance(block, dict):
                continue
            block_key = str(block.get("key") or "").strip()
            series_codes = _as_list(block.get("series_codes"))
            local_files = block.get("local_series_files") or {}
            if not isinstance(local_files, dict):
                local_files = {}

            for code in series_codes:
                code_str = str(code).strip()
                if not code_str:
                    continue
                if code_str in panel_cols:
                    continue

                rows.append(
                    {
                        "iso_code": iso,
                        "country": name,
                        "region": region,
                        "block": block_key,
                        "series_code": code_str,
                        "local_series_file": str(local_files.get(code_str, "")),
                    }
                )

    # Write CSV
    fieldnames = [
        "iso_code",
        "country",
        "region",
        "block",
        "series_code",
        "local_series_file",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Write Markdown summary
    lines: list[str] = []
    lines.append("# Config vs Panel: Missing Series\n")
    lines.append(f"Panel: {panel_path.as_posix()}\n")
    lines.append(f"Missing configured series_codes: {len(rows)}\n")

    if not rows:
        lines.append("All configured series_codes are present in the merged panel columns.\n")
        OUT_MD.write_text("\n".join(lines), encoding="utf-8")
        print(f"[OK] No missing series. Wrote: {OUT_MD}")
        print(f"[OK] CSV: {OUT_CSV}")
        return 0

    # Group for readability
    rows_sorted = sorted(rows, key=lambda r: (r["iso_code"], r["block"], r["series_code"]))

    current_iso = None
    current_block = None
    for row in rows_sorted:
        iso = row["iso_code"]
        block = row["block"]
        if iso != current_iso:
            lines.append(f"## {iso} ({row['country']})\n")
            current_iso = iso
            current_block = None
        if block != current_block:
            lines.append(f"### {block}\n")
            current_block = block
        file_hint = row.get("local_series_file") or ""
        if file_hint:
            lines.append(f"- {row['series_code']} (file: {file_hint})")
        else:
            lines.append(f"- {row['series_code']}")

    OUT_MD.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    print(f"[SAVED] {OUT_CSV}")
    print(f"[SAVED] {OUT_MD}")
    if rows:
        # Non-zero exit to make it noticeable in CI, but still writes outputs.
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
