"""Report the pruned series that still met the coverage threshold."""

from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path
import re

ENTRY_PATTERN = re.compile(r"-\s+([^(]+?)\s+\((0\.\d+)\)")


def parse_pruned_markdown(markdown_path: Path):
    iso = None
    for raw_line in markdown_path.read_text().splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("## "):  # keep track of the current ISO section
            iso = line[3:].strip()
            continue
        match = ENTRY_PATTERN.match(line)
        if not match:
            continue
        series, ratio_text = match.groups()
        yield iso or "<unknown>", series.strip(), float(ratio_text)


def build_parser():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        type=Path,
        nargs="?",
        default=Path("analysis_outputs/coverage_optimizer/pruned_series_summary_0.85.md"),
        help="Markdown file listing pruned series",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.85,
        help="Minimum coverage ratio to include in the report",
    )
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    if not args.source.exists():
        raise FileNotFoundError(f"Missing report: {args.source}")

    survivors = [
        (iso, series, ratio)
        for iso, series, ratio in parse_pruned_markdown(args.source)
        if ratio >= args.threshold
    ]

    if not survivors:
        print(
            f"No pruned entries have coverage >= {args.threshold:.2f}; see {args.source} for details."
        )
    else:
        print(f"Pruned series with coverage >= {args.threshold:.2f}:")
        for iso, series, ratio in sorted(survivors):
            print(f"{iso}: {series} ({ratio:.6f})")
