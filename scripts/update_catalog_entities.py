import csv
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE / "catalog.csv"


def is_beta_series(series: str) -> bool:
    return "_beta" in series.lower()


def derive_entity(row: dict[str, str]) -> str:
    if is_beta_series(row.get("series", "")):
        return ""
    storage_path = row.get("storage_path", "")
    if storage_path:
        return Path(storage_path).stem.replace("_", " ")
    note = row.get("source_detail", "")
    if note and not note.startswith("data_repository/raw"):
        return note.replace("_", " ")
    if note:
        return Path(note).stem.replace("_", " ")
    group = row.get("source_group", "")
    if group:
        return group.replace("_", " ")
    source = row.get("source", "")
    if source:
        return source
    return row.get("series", "").replace("_", " ")


def main() -> None:
    with CATALOG_PATH.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        original_fields = reader.fieldnames or []
        if "entity" in original_fields:
            print("Entity column already present; no changes made.")
            return
        fieldnames = ["series", "entity"] + [f for f in original_fields if f != "series"]
        rows = []
        for row in reader:
            row["entity"] = derive_entity(row)
            rows.append(row)

    with CATALOG_PATH.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated {CATALOG_PATH.name} with {len(rows)} rows including entity labels.")


if __name__ == "__main__":
    main()
