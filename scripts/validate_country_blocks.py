"""Validate country block configuration for governance rules.

Primary check: "no double usage" (a series_code should not appear in multiple
blocks within the same country) unless you explicitly allow it.

This is intended as a lightweight preflight for the pipeline.

Usage:
  python scripts/validate_country_blocks.py
  python scripts/validate_country_blocks.py --strict
  python scripts/validate_country_blocks.py --config config/country_blocks_extended.yaml

Exit codes:
  0 = OK (or warnings only)
  2 = Validation failed (strict mode)
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import yaml


def _load_config(path: Path) -> dict:
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError:
        payload = yaml.safe_load(path.read_text(encoding="utf-8", errors="replace"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected mapping at top-level of {path}, got {type(payload).__name__}")
    return payload


def find_duplicates_by_iso(country_blocks: List[dict]) -> Dict[str, Dict[str, List[str]]]:
    dups_by_iso: Dict[str, Dict[str, List[str]]] = {}
    for entry in country_blocks:
        if not isinstance(entry, dict):
            continue
        iso = entry.get("iso_code") or entry.get("country") or "<unknown>"
        blocks = entry.get("blocks") or []
        usage: dict[str, list[str]] = defaultdict(list)
        for block in blocks:
            if not isinstance(block, dict):
                continue
            block_key = str(block.get("key") or "<missing_key>")
            for s in (block.get("series_codes") or []):
                if s is None:
                    continue
                usage[str(s)].append(block_key)
        dups = {s: keys for s, keys in usage.items() if len(keys) > 1}
        if dups:
            dups_by_iso[str(iso)] = dups
    return dups_by_iso


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate config/country_blocks_extended.yaml governance rules")
    parser.add_argument(
        "--config",
        default="config/country_blocks_extended.yaml",
        help="Path to blocks YAML (default: config/country_blocks_extended.yaml)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail (exit code 2) if duplicates are detected.",
    )
    args = parser.parse_args()

    path = Path(args.config)
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    payload = _load_config(path)
    country_blocks = payload.get("country_blocks")
    if not isinstance(country_blocks, list) or not country_blocks:
        raise ValueError(f"No country_blocks declared in {path}")

    dups_by_iso = find_duplicates_by_iso(country_blocks)

    if not dups_by_iso:
        print(f"OK: no duplicate series_codes across blocks (checked {len(country_blocks)} countries)")
        return 0

    print(f"WARN: found duplicate series_codes across blocks for {len(dups_by_iso)}/{len(country_blocks)} countries")
    for iso, dups in sorted(dups_by_iso.items()):
        print(f"\n{iso}: {len(dups)} duplicates")
        for series, blocks in sorted(dups.items()):
            print(f"  {series} => {', '.join(blocks)}")

    if args.strict:
        print("\nSTRICT mode enabled: failing validation.")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
