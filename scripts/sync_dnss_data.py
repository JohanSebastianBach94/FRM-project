"""Utility to keep the DNSS data folders in sync with trusted sources."""

from pathlib import Path
import shutil


def copy_csv_files(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for file_path in src.glob("*.csv"):
        if file_path.is_file():
            shutil.copy2(file_path, dst / file_path.name)


def copy_investing_files(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for file_path in src.glob("*.csv"):
        if not file_path.is_file():
            continue
        name = file_path.name
        if "Government" not in name:
            name = f"Government {name}"
        shutil.copy2(file_path, dst / name)


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    source_bond = root / "data" / "trial data folder"
    source_investing = root / "Investing bond"
    target_bond = root / "progetto frm" / "BOND_Data"
    target_investing = root / "progetto frm" / "Investing"

    copy_csv_files(source_bond, target_bond)
    copy_investing_files(source_investing, target_investing)

    print("Synced BOND_Data and Investing folders with trusted sources.")