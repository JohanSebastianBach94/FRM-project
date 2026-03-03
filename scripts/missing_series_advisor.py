"""Generate guidance for filling the missing panel series listed in missing_targets.json."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
MISSING_PATH = BASE_DIR / "analysis_outputs" / "feature_contributions" / "missing_targets.json"
PANEL_PATH = BASE_DIR / "data" / "cleaned_monthly_panel.parquet"
FILLED_DIR = BASE_DIR / "data" / "panel_filled"
FILLED_DIR.mkdir(parents=True, exist_ok=True)
SUGGESTED_SOURCES = {
    "USA": "FRED/BIS/Fed statistical releases",
    "DEU": "Eurostat/ECB/Bundesbank",
    "FRA": "Eurostat/ECB/INSEE",
    "ITA": "ECB/ISTAT/Bank of Italy",
    "ESP": "ECB/INE/Bank of Spain",
}


def load_missing_targets() -> Dict[str, List[str]]:
    if not MISSING_PATH.exists():
        return {}
    with MISSING_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def load_panel_columns() -> List[str]:
    if not PANEL_PATH.exists():
        return []
    panel = pd.read_parquet(PANEL_PATH)
    return sorted(panel.columns.tolist())


def tokenize(name: str) -> List[str]:
    return [token.lower() for token in re.split(r"[^A-Za-z0-9]+", name) if token]


def find_analogs(series: str, columns: Iterable[str]) -> List[str]:
    tokens = set(tokenize(series))
    scores: List[tuple[int, str]] = []
    for column in columns:
        score = sum(1 for token in tokens if token in column.lower())
        if score:
            scores.append((score, column))
    scores.sort(key=lambda item: (-item[0], item[1]))
    return [column for _, column in scores[:3]]


def build_imputation_plan(series: str, analogs: List[str]) -> str:
    if analogs:
        return (
            f"Regress {series} on {', '.join(analogs)} (in-sample weights) and upscale to monthly path; keep diagnostics." 
            "Cross-panel factor regressions can back-fill remaining gaps."
        )
    return (
        f"Source {series} from primary provider (FRED/ECB/BIS) or build regression from cross-country peers; document assumptions." 
        "Impute only after validating covariance with DNSS factors."
    )


def build_missing_reports() -> None:
    missing = load_missing_targets()
    columns = load_panel_columns()
    for iso, series_list in missing.items():
        rows = []
        source_hint = SUGGESTED_SOURCES.get(iso, "FRED/BIS/ECB")
        for series in series_list:
            analogs = find_analogs(series, columns)
            rows.append(
                {
                    "series": series,
                    "suggested_source": source_hint,
                    "analog_candidates": ", ".join(analogs) if analogs else "none",
                    "imputation_plan": build_imputation_plan(series, analogs),
                }
            )
        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(FILLED_DIR / f"{iso}_missing_targets.csv", index=False)


def main() -> None:
    build_missing_reports()


if __name__ == "__main__":
    main()
