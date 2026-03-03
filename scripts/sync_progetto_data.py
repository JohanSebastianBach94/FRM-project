"""Utility to refresh the `progetto frm` bond and investing data from canonical sources."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def copy_bond_data(source_dir: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(source_dir.glob("BOND_*.csv"))
    if not files:
        print(f"⚠️ No BOND_*.csv files found in {source_dir}")
        return

    for src in files:
        dest = dest_dir / src.name
        shutil.copy2(src, dest)
        print(f"   Copied {src.name} to {dest}")


def copy_investing_data(source_dir: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(source_dir.glob("*.csv"))
    if not files:
        print(f"⚠️ No CSV files found in {source_dir}")
        return

    for src in files:
        dest_name = f"Government - {src.name}"
        dest = dest_dir / dest_name
        shutil.copy2(src, dest)
        print(f"   Copied {src.name} to {dest_name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync the canonical bond and investing sources to `progetto frm`."
    )
    parser.add_argument(
        "--bond-source",
        type=Path,
        default=Path("data/trial data folder"),
        help="Source directory containing BOND_*.csv files.",
    )
    parser.add_argument(
        "--investing-source",
        type=Path,
        default=Path("Investing bond"),
        help="Source directory containing Investing CSV files.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("progetto frm"),
        help="Root directory where BOND_Data and Investing live.",
    )

    args = parser.parse_args()

    bond_dest = args.output_root / "BOND_Data"
    investing_dest = args.output_root / "Investing"

    print(f"Syncing BOND data from {args.bond_source} into {bond_dest}")
    copy_bond_data(args.bond_source, bond_dest)

    print(f"Syncing Investing data from {args.investing_source} into {investing_dest}")
    copy_investing_data(args.investing_source, investing_dest)


def _main() -> None:
    main()


if __name__ == "__main__":
    _main()