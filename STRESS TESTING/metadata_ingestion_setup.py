"""Support script for Phase 3 point 0 (Metadata & ingestion setup)."""

from __future__ import annotations

import csv
import textwrap
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
REGRESSION_CSV = PROJECT_ROOT / "data_final" / "regression_long.csv"
COUNTRY_BLOCKS = PROJECT_ROOT / "config" / "country_blocks_extended.yaml"
METADATA_YAML = PROJECT_ROOT / "config" / "series_metadata.yaml"


def read_regression_series(csv_path: Path) -> set[str]:
    """Collect the column headers from the regression csv to seed metadata."""

    if not csv_path.exists():
        raise FileNotFoundError(f"Missing regression file: {csv_path}")

    with csv_path.open() as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader, [])

    return {col.strip() for col in header if col}


def read_country_block_series(blocks_path: Path) -> set[str]:
    """Collect all series codes declared in the country block registry."""

    if not blocks_path.exists():
        raise FileNotFoundError(f"Missing country blocks: {blocks_path}")

    with blocks_path.open() as file:
        data = yaml.safe_load(file)

    series_codes: set[str] = set()
    for block in data.get("blocks", []):
        series_codes.update(block.get("series_codes", []))
        series_codes.update(block.get("extra_series", []))

    return {code for code in series_codes if code}


def load_existing_metadata(path: Path) -> dict:
    if not path.exists():
        return {}

    with path.open() as file:
        return yaml.safe_load(file) or {}


def build_default_record(series_name: str) -> dict[str, str]:
    return {
        "source": "unknown",
        "frequency": "monthly",
        "transform": "identity",
        "coverage": "unreviewed",
        "notes": "Add explicit details when available.",
    }


def persist_metadata(metadata: dict[str, dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            metadata,
            file,
            sort_keys=True,
            allow_unicode=False,
        )


def main() -> None:
    regression_series = read_regression_series(REGRESSION_CSV)
    block_series = read_country_block_series(COUNTRY_BLOCKS)
    metadata = load_existing_metadata(METADATA_YAML)

    all_series = sorted(regression_series | block_series)
    changed = False

    for series in all_series:
        if series not in metadata:
            metadata[series] = build_default_record(series)
            changed = True

    if changed:
        persist_metadata(metadata, METADATA_YAML)

    print(textwrap.dedent(
        f"""
        Updated {METADATA_YAML.relative_to(PROJECT_ROOT)} with {len(all_series)} entries.
        Please document source/frequency/transform/coverage for any record that still lists 'unknown' or 'unreviewed'.
        """
    ).strip())


if __name__ == "__main__":
    main()