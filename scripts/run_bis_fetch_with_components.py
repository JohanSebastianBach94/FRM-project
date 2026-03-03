#!/usr/bin/env python3
"""Launch BIS data downloads using components from the extracted CSV."""

from __future__ import annotations

import argparse
import csv
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "analysis_outputs" / "bis_series_components.csv"
FETCHER = ROOT / "fetchers" / "fetch_bis_api.py"


def load_components() -> list[dict[str, str]]:
    if not CSV_PATH.exists():
        raise SystemExit(f"Component inventory not found: {CSV_PATH}")
    with CSV_PATH.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def matching_rows(rows: Iterable[dict[str, str]], flow: str | None, flow_slug: str | None,
                  title_contains: str | None) -> list[dict[str, str]]:
    matches = []
    for row in rows:
        if flow and row["flow"] != flow:
            continue
        if flow_slug and row["flow_slug"] != flow_slug:
            continue
        if title_contains and title_contains.lower() not in row["title_ts"].lower():
            continue
        matches.append(row)
    return matches


def build_command(row: dict[str, str], args: argparse.Namespace) -> list[str]:
    fragment = row.get(args.component_column)
    if not fragment:
        raise SystemExit(f"Missing component fragment in column '{args.component_column}' for {row.get('title_ts')} ({row.get('flow')})")
    cmd = [sys.executable, str(FETCHER), "fetch", "--flow", row["flow"], "--key", args.key]
    if args.start_period:
        cmd.extend(["--start-period", args.start_period])
    if args.end_period:
        cmd.extend(["--end-period", args.end_period])
    if args.mode:
        cmd.extend(["--mode", args.mode])
    if args.detail:
        cmd.extend(["--detail", args.detail])
    if args.references:
        cmd.extend(["--references", ",".join(args.references)])
    if args.overrides:
        cmd.extend(shlex.split(args.overrides))
    cmd.extend(shlex.split(fragment))
    return cmd


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch BIS flow slices via pre-calculated components")
    parser.add_argument("--flow", help="Flow identifier (e.g. BIS,WS_DSR,1.0)")
    parser.add_argument("--flow-slug", help="Slug for the cached file (e.g. bis_ws_dsr_1_0_all_20251216T142436Z)")
    parser.add_argument("--title-contains", help="Match rows whose TITLE_TS contains this substring")
    parser.add_argument("--key", default="all", help="Series key passed to the BIS fetcher")
    parser.add_argument("--start-period", help="Inclusive start period (ISO/SDMX)")
    parser.add_argument("--end-period", help="Inclusive end period (ISO/SDMX)")
    parser.add_argument("--mode", choices=["exact", "available"], help="Content constraint mode")
    parser.add_argument("--references", nargs="+",
                        choices=["none", "all", "datastructure", "conceptscheme", "codelist",
                                 "dataproviderscheme", "dataflow"], help="References to include")
    parser.add_argument("--detail", choices=["full", "allstubs", "referencepartial", "allcompletestubs",
                                               "referencecompletestubs", "raw"], help="Metadata detail level")
    parser.add_argument("--overrides", help="Additional CLI fragments appended after the CSV components")
    parser.add_argument("--dry-run", action="store_true", help="Show commands instead of executing them")
    parser.add_argument("--limit", type=int, help="Maximum number of matching rows to fetch")
    parser.add_argument("--component-column", default="component_flags",
                        help="Column containing the CLI fragments to reuse")
    args = parser.parse_args()
    if not (args.flow or args.flow_slug or args.title_contains):
        parser.error('At least one of --flow, --flow-slug or --title-contains is required to pick rows')

    rows = load_components()
    matches = matching_rows(rows, args.flow, args.flow_slug, args.title_contains)
    if not matches:
        raise SystemExit("No matching series found in the component inventory")
    if args.limit and len(matches) > args.limit:
        matches = matches[: args.limit]

    for row in matches:
        cmd = build_command(row, args)
        info = f"title='{row['title_ts']}' freq='{row['freq']}' country='{row['borrowers_cty']}'"
        print(f"=> {info}")
        if args.dry_run:
            print(" ".join(shlex.quote(token) for token in cmd))
            continue
        subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()