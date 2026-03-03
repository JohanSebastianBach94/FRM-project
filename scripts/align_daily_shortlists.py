#!/usr/bin/env python3
"""Align daily factor shortlists across ISOs by taking a union of features."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

def align_daily_shortlists(
    shortlist_dir: Path,
    output_dir: Path,
    date_column: str | None = "date",
) -> list[str]:
    output_dir = output_dir.resolve()
    if not shortlist_dir.is_dir():
        raise FileNotFoundError(f"Daily shortlist directory missing: {shortlist_dir}")

    iso_paths = sorted(shortlist_dir.glob("*_factors_daily_shortlist.csv"))
    if not iso_paths:
        raise FileNotFoundError(f"No daily shortlists found under {shortlist_dir}")

    feature_sets = []
    data_frames: dict[str, pd.DataFrame] = {}

    for path in iso_paths:
        iso = path.stem.split("_")[0].upper()
        df = pd.read_csv(path)
        if date_column and date_column not in df.columns:
            raise ValueError(f"Expected column {date_column!r} missing in {path}")
        features = [col for col in df.columns if col != date_column]
        data_frames[iso] = df
        feature_sets.append(set(features))

    union_features = set.union(*feature_sets)
    keep_features = sorted(union_features)
    if not keep_features:
        raise ValueError("No features remain in the union of shortlists")

    output_dir.mkdir(parents=True, exist_ok=True)
    for path in iso_paths:
        iso = path.stem.split("_")[0].upper()
        df = data_frames[iso]
        missing = [feature for feature in keep_features if feature not in df.columns]
        for feature in missing:
            df[feature] = pd.NA
        columns = [date_column] + keep_features if date_column else keep_features
        aligned = df.loc[:, columns].dropna(axis=1, how="all")
        aligned.to_csv(output_dir / path.name, index=False)

    print(f"Aligned {len(keep_features)} drivers across {len(iso_paths)} ISOs.")
    return keep_features


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Align daily shortlists across ISOs")
    parser.add_argument(
        "--shortlist-dir",
        type=Path,
        default=Path("analysis_outputs") / "factors_daily_shortlist",
        help="Directory containing the raw daily shortlists",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("analysis_outputs") / "factors_daily_shortlist_aligned",
        help="Where aligned shortlists are written",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    align_daily_shortlists(
        shortlist_dir=args.shortlist_dir,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
