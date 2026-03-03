"""Summarize health and units of the series referenced in country_blocks_extended.yaml."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import re

import pandas as pd
import yaml
from dateutil import parser as dateutil_parser

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "country_blocks_extended.yaml"
OUTPUT_PATH = REPO_ROOT / "analysis_outputs" / "series_health_summary.csv"


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def collect_series_files(config: dict[str, Any]) -> dict[str, str]:
    files: dict[str, str] = {}
    for block in config.get("country_blocks", []):
        for entry in block.get("blocks", []):
            local = entry.get("local_series_files", {})
            for code, rel_path in local.items():
                files[code] = rel_path
    return files


def _guess_annotation(code: str, sample: pd.Series) -> str:
    if sample.dropna().empty:
        return "value"
    name = code.lower()
    if "ratio" in name or "beta" in name:
        return "ratio (unitless)"
    if any(token in name for token in ("spread", "cds", "comm_paper")):
        return "basis points"
    if any(token in name for token in ("rate", "yield", "return")):
        return "percent"
    if any(token in name for token in ("debt", "gdp", "loan", "value")):
        return "percent (of GDP) / millions"
    if sample.dropna().between(0, 1).all():
        return "fraction (0-1)"
    if sample.dropna().between(-10, 200).all():
        return "index / level"
    return "value"


QUARTER_RE = re.compile(r"^(\d{4})-Q([1-4])$")


def _parse_date_column(values: pd.Series) -> pd.Series:
    def _parse(value: Any) -> datetime | pd.NaT:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return pd.NaT
        if isinstance(value, datetime):
            return value
        quart = QUARTER_RE.match(str(value).strip()) if isinstance(value, str) else None
        if quart:
            year, quarter = quart.groups()
            return pd.Period(int(year), freq="Q").asfreq("D").start_time
        try:
            return dateutil_parser.parse(str(value))
        except (ValueError, OverflowError):
            return pd.NaT

    parsed = values.map(_parse)
    return pd.to_datetime(parsed, errors="coerce")


def assess_file(code: str, rel_path: str) -> dict[str, Any]:
    file_path = REPO_ROOT / rel_path
    if not file_path.exists():
        return {
            "series_code": code,
            "file": rel_path,
            "status": "missing",
            "rows": 0,
            "start_date": None,
            "end_date": None,
            "nan_ratio": None,
            "annotation": "unknown",
        }
    try:
        df = pd.read_csv(file_path)
    except Exception as exc:
        return {
            "series_code": code,
            "file": rel_path,
            "status": f"read_error ({exc})",
            "rows": 0,
            "start_date": None,
            "end_date": None,
            "nan_ratio": None,
            "annotation": "unknown",
        }
    df = df.rename(columns={df.columns[0]: "date"})
    df["date"] = _parse_date_column(df["date"])
    df = df.dropna(subset=["date"]).set_index("date")
    value_col = df.columns[0]
    series = df[value_col]
    series = pd.to_numeric(series, errors="coerce")
    rows = len(series)
    missing_pct = series.isna().mean() * 100
    cleaned = series.dropna()
    annotation = _guess_annotation(code, cleaned)
    start_date = cleaned.index.min().isoformat() if not cleaned.empty else None
    end_date = cleaned.index.max().isoformat() if not cleaned.empty else None
    status = "ok" if rows else "empty"
    return {
        "series_code": code,
        "file": rel_path,
        "status": status,
        "rows": rows,
        "start_date": start_date,
        "end_date": end_date,
        "nan_ratio": round(missing_pct, 2),
        "annotation": annotation,
    }


def main() -> None:
    config = load_config(CONFIG_PATH)
    series_files = collect_series_files(config)
    results = [assess_file(code, path) for code, path in sorted(series_files.items())]
    df = pd.DataFrame(results)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Wrote summary to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
