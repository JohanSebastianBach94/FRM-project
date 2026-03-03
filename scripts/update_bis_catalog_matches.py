import csv
import re
from pathlib import Path
from typing import Iterable

BASE = Path(__file__).resolve().parents[1]
CATALOG_PATH = BASE / "catalog.csv"
INDEX_PATH = BASE / "data_repository" / "processed" / "bis_series_index.csv"
OUTPUT_PATH = BASE / "data_repository" / "processed" / "BIS_catalog.csv"

RE_TOKEN = re.compile(r"[A-Za-z0-9]+")

OUTPUT_FIELDS = [
    "series",
    "entity",
    "country_code",
    "instrument",
    "bis_flow",
    "bis_source_file",
    "relationship",
    "match_score",
    "matched_keywords",
    "country_match",
    "instrument_match",
    "bis_title",
    "bis_freq",
    "bis_start",
    "bis_end",
    "bis_observations",
    "series_key",
]


def normalize_tokens(value: str) -> set[str]:
    return {token.lower() for token in RE_TOKEN.findall(value or "")}


def load_index() -> list[dict]:
    entries = []
    with INDEX_PATH.open("r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            entry = dict(row)
            entry["country_code"] = entry.get("country_code", "").upper()
            entry["keyword_tokens"] = normalize_tokens(entry.get("keywords", ""))
            entries.append(entry)
    return entries


def build_row_tokens(row: dict[str, str]) -> set[str]:
    source_parts = " ".join(filter(None, [row.get("topic_keywords", ""), row.get("extra_keywords", ""), row.get("source_detail", ""), row.get("storage_path", "")]))
    base = f"{row.get('entity', '')} {row.get('series', '')} {source_parts}"
    return normalize_tokens(base)


def score_candidate(row_tokens: set[str], row_instrument: str, row_country: str, entry: dict) -> tuple[int, bool, bool, set[str]]:
    score = 0
    country_match = False
    instrument_match = False
    entry_country = entry.get("country_code", "")
    if row_country and entry_country:
        if row_country == entry_country:
            score += 4
            country_match = True
        elif row_country[:2] == entry_country[:2]:
            score += 1
    if row_instrument:
        instrument_tokens = normalize_tokens(row_instrument)
        if instrument_tokens & entry["keyword_tokens"]:
            score += 2
            instrument_match = True
    overlap = row_tokens & entry["keyword_tokens"]
    score += min(len(overlap), 3)
    if "bis" in row_tokens and "bis" in entry["keyword_tokens"]:
        score += 1
    return score, country_match, instrument_match, overlap


def choose_best_match(row: dict[str, str], entries: list[dict]) -> dict:
    row_country = (row.get("country_code") or "").upper()
    row_tokens = build_row_tokens(row)
    row_instrument = row.get("instrument", "")
    filtered = entries
    if row_country:
        filtered = [entry for entry in entries if entry.get("country_code") == row_country]
        if not filtered:
            filtered = [entry for entry in entries if entry.get("country_code", "").startswith(row_country[:2])]
    if not filtered:
        filtered = entries
    best = None
    best_score = -1
    for entry in filtered:
        score, country_match, instrument_match, overlap = score_candidate(row_tokens, row_instrument, row_country, entry)
        if score > best_score:
            best_score = score
            best = {
                "entry": entry,
                "score": score,
                "country_match": country_match,
                "instrument_match": instrument_match,
                "matched_keywords": ",".join(sorted(overlap)),
            }
    if not best or best_score <= 0:
        return {"entry": {}, "score": 0, "country_match": False, "instrument_match": False, "matched_keywords": ""}
    return best


def determine_relationship(score: int) -> str:
    if score >= 6:
        return "same"
    if score >= 3:
        return "similar"
    return "missing"


def main() -> None:
    entries = load_index()
    with CATALOG_PATH.open("r", newline="", encoding="utf-8") as infile, OUTPUT_PATH.open("w", newline="", encoding="utf-8") as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in reader:
            match = choose_best_match(row, entries)
            entry = match["entry"] or {}
            relationship = determine_relationship(match["score"])
            writer.writerow({
                "series": row.get("series", ""),
                "entity": row.get("entity", ""),
                "country_code": row.get("country_code", ""),
                "instrument": row.get("instrument", ""),
                "bis_flow": entry.get("bis_flow", ""),
                "bis_source_file": entry.get("bis_source_file", ""),
                "relationship": relationship,
                "match_score": match["score"],
                "matched_keywords": match.get("matched_keywords", ""),
                "country_match": match["country_match"],
                "instrument_match": match["instrument_match"],
                "bis_title": entry.get("bis_title", ""),
                "bis_freq": entry.get("bis_freq", ""),
                "bis_start": entry.get("bis_start", ""),
                "bis_end": entry.get("bis_end", ""),
                "bis_observations": entry.get("bis_observations", ""),
                "series_key": entry.get("series_key", ""),
            })


if __name__ == "__main__":
    main()
