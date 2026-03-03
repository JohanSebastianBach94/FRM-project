from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ISOS = ["USA", "DEU", "FRA", "ITA", "ESP"]

FACTOR_DIR = ROOT / "analysis_outputs" / "factor_preparation_daily"
SHORTLIST_DIR = ROOT / "analysis_outputs" / "factors_daily_shortlist"

OUT_DIR = ROOT / "analysis_outputs" / "diagnostics"
OUT_PATH = OUT_DIR / "daily_driver_counts.csv"


def count_csv_columns(path: Path) -> int:
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        header = next(reader)
    return len(header)


def list_csv_columns(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as fp:
        reader = csv.reader(fp)
        header = next(reader)
    return header


def main() -> None:
    rows: list[dict] = []
    union_shortlist: set[str] = set()

    for iso in ISOS:
        factor_path = FACTOR_DIR / f"{iso}_factors_daily.csv"
        shortlist_path = SHORTLIST_DIR / f"{iso}_factors_daily_shortlist.csv"

        factor_cols = list_csv_columns(factor_path) if factor_path.exists() else []
        shortlist_cols = list_csv_columns(shortlist_path) if shortlist_path.exists() else []

        factor_features = [c for c in factor_cols if c.lower() != "date"]
        shortlist_features = [c for c in shortlist_cols if c.lower() != "date"]

        union_shortlist |= set(shortlist_features)

        rows.append(
            {
                "iso": iso,
                "factor_preparation_daily_cols": len(factor_cols),
                "factor_preparation_daily_features": len(factor_features),
                "daily_shortlist_cols": len(shortlist_cols),
                "daily_shortlist_features": len(shortlist_features),
            }
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # Write CSV
    import pandas as pd

    df = pd.DataFrame(rows).sort_values("iso")
    df["union_daily_shortlist_features_across_isos"] = len(union_shortlist)
    df.to_csv(OUT_PATH, index=False)

    print(df.to_string(index=False))
    print(f"\nUnion of daily shortlist features across {len(ISOS)} ISOs: {len(union_shortlist)}")
    print(f"Wrote {OUT_PATH}")


if __name__ == "__main__":
    main()
