import csv
import re
from pathlib import Path
from typing import Iterable, Sequence

BASE = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE / "catalog.csv"
ISO_ALIAS = {
    "DE": "DEU",
    "DEU": "DEU",
    "FR": "FRA",
    "FRA": "FRA",
    "ES": "ESP",
    "ESP": "ESP",
    "IT": "ITA",
    "ITA": "ITA",
    "US": "USA",
    "USA": "USA",
    "GB": "GBR",
    "GBR": "GBR",
    "EU": "EUR",
    "EUR": "EUR",
}
INSTRUMENT_KEYS = (
    ("credit spread", "Credit spread"),
    ("cds", "Credit default swap"),
    ("mortgage", "Mortgage rate"),
    (("oil", "brent", "wti"), "Oil price"),
    ("gold", "Gold"),
    (("cpi", "inflation"), "Inflation"),
    (("sofr", "term note"), "SOFR"),
    (("swaption", "swap"), "Swaption"),
    ("fx", "FX"),
    ("equity", "Equity index"),
    (("housing", "hpi", "price_to_income", "rent"), "Housing"),
    ("spread", "Spread"),
)


class CatalogRow(dict):
    @property
    def series_tokens(self) -> list[str]:
        return list(filter(None, re.split(r"[^A-Za-z0-9]+", self.get("series", ""))))


def normalize_tokens(text: str) -> list[str]:
    return [token.lower() for token in re.findall(r"[A-Za-z0-9]+", text)]


def guess_country(series: str, storage_path: str, source_detail: str) -> str:
    candidates = []
    candidates.extend(reversed([token.upper() for token in re.split(r"[^A-Za-z0-9]+", series) if token]))
    candidates.extend(reversed([token.upper() for token in re.split(r"[^A-Za-z0-9]+", storage_path) if token]))
    candidates.extend(reversed([token.upper() for token in re.split(r"[^A-Za-z0-9]+", source_detail) if token]))
    for candidate in candidates:
        if candidate in ISO_ALIAS:
            return ISO_ALIAS[candidate]
    return ""


def classify_instrument(tokens: Sequence[str]) -> str:
    normalized = " ".join(tokens).lower()
    for keyword, label in INSTRUMENT_KEYS:
        if isinstance(keyword, tuple):
            if any(key in normalized for key in keyword):
                return label
        elif keyword in normalized:
            return label
    return ""


def build_keywords(row: CatalogRow) -> str:
    pieces = []
    for field in ("entity", "source", "source_group", "source_detail", "storage_path"):
        pieces.append(row.get(field, ""))
    return " ".join(part for part in pieces if part)


def enrich_rows(rows: list[CatalogRow]) -> None:
    for row in rows:
        if row.get("series", "") == "":
            continue
        row["country_code"] = guess_country(row.get("series", ""), row.get("storage_path", ""), row.get("source_detail", ""))
        row["instrument"] = classify_instrument(row.series_tokens)
        row["topic_keywords"] = ",".join(sorted(set(normalize_tokens(row.get("entity", "")) + normalize_tokens(row.get("series", "")))))
        row["extra_keywords"] = build_keywords(row)


def main() -> None:
    with CATALOG_PATH.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        original_fields = reader.fieldnames or []
        rows = [CatalogRow(row) for row in reader]

    new_columns = ["country_code", "instrument", "topic_keywords", "extra_keywords"]
    if all(col in original_fields for col in new_columns) and len(original_fields) >= len(new_columns):
        print("Catalog already has enriched columns. Rewriting to refresh values.")

    def append_unique(container: list[str], value: str) -> None:
        if value not in container:
            container.append(value)

    fieldnames: list[str] = []
    inserted = False
    for field in original_fields:
        append_unique(fieldnames, field)
        if field == "entity" and not inserted:
            for col in new_columns:
                append_unique(fieldnames, col)
            inserted = True
    for col in new_columns:
        append_unique(fieldnames, col)
    enrich_rows(rows)

    with CATALOG_PATH.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Enriched catalog.csv with {len(rows)} entity rows.")


if __name__ == "__main__":
    main()
