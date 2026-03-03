"""Coverage vs Pipeline Difference report.

Compares the catalog coverage universe vs the frozen pipeline series list
(`outputs/country_block_definition.json`). Writes a Markdown report to
`analysis_outputs/coverage_vs_pipeline_diff.md`.

Usage:
    python scripts/coverage_vs_pipeline_diff.py
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

CATALOG_PATH = ROOT / "catalog.csv"
THRESHOLD_CONFIG_PATH = ROOT / "analysis_outputs" / "coverage_threshold_config.json"
BLOCKS_PATH = ROOT / "outputs" / "country_block_definition.json"
OUT_PATH = ROOT / "analysis_outputs" / "coverage_vs_pipeline_diff.md"


def _load_series_threshold(default: float = 0.62) -> float:
    if not THRESHOLD_CONFIG_PATH.exists():
        return default
    try:
        payload = json.loads(THRESHOLD_CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        return default

    value = payload.get("series_threshold", payload.get("threshold", default))
    try:
        out = float(value)
    except Exception:
        out = default
    return out


def _load_catalog() -> pd.DataFrame:
    if not CATALOG_PATH.exists():
        raise FileNotFoundError(f"missing catalog: {CATALOG_PATH}")

    df = pd.read_csv(CATALOG_PATH, dtype=str)
    if "series" not in df.columns:
        raise ValueError("catalog.csv must contain a 'series' column")

    if "coverage_ratio" in df.columns:
        df["coverage_ratio"] = pd.to_numeric(df["coverage_ratio"], errors="coerce")
    else:
        df["coverage_ratio"] = pd.NA

    if "do_not_use" in df.columns:
        df["do_not_use"] = df["do_not_use"].fillna("").astype(str)
    else:
        df["do_not_use"] = ""

    df["series"] = df["series"].fillna("").astype(str).str.strip()
    df = df[df["series"] != ""].copy()

    return df


def _load_pipeline_series() -> set[str]:
    if not BLOCKS_PATH.exists():
        raise FileNotFoundError(f"missing blocks json: {BLOCKS_PATH}")

    payload = json.loads(BLOCKS_PATH.read_text(encoding="utf-8"))

    if isinstance(payload, dict):
        country_entries = list(payload.values())
    elif isinstance(payload, list):
        country_entries = payload
    else:
        country_entries = []

    out: set[str] = set()
    for country_entry in country_entries:
        blocks = country_entry.get("blocks", []) if isinstance(country_entry, dict) else []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            for key in ("series_codes", "optional_series_codes"):
                codes = block.get(key, [])
                if isinstance(codes, list):
                    for c in codes:
                        if isinstance(c, str) and c.strip():
                            out.add(c.strip())
    return out


def _is_do_not_use(value: str) -> bool:
    return str(value or "").strip().upper() == "DO NOT USE"


def main() -> None:
    series_threshold = _load_series_threshold(default=0.62)
    df_cat = _load_catalog()
    pipeline_series = _load_pipeline_series()

    df_cat = df_cat[~df_cat["do_not_use"].apply(_is_do_not_use)].copy()

    cov = df_cat.set_index("series")["coverage_ratio"].to_dict()

    above_threshold = {
        row["series"]
        for _, row in df_cat.iterrows()
        if pd.notna(row.get("coverage_ratio")) and float(row["coverage_ratio"]) >= series_threshold
    }

    above_not_in_pipeline = sorted(
        [s for s in above_threshold if s not in pipeline_series],
        key=lambda s: (-(cov.get(s) or 0.0), s),
    )

    pipeline_below_threshold = sorted(
        [s for s in pipeline_series if (cov.get(s) is not None and float(cov.get(s) or 0.0) < series_threshold)],
        key=lambda s: (cov.get(s) or 0.0, s),
    )

    lines: list[str] = []
    lines.append("# Coverage vs Pipeline Difference\n")
    lines.append(f"Generated: {ROOT}\n")
    lines.append(f"Series threshold: {series_threshold:.2f}\n")
    lines.append("\n## Above-threshold catalog series not present in country_block_definition.json\n")
    lines.append(f"Count: {len(above_not_in_pipeline)}\n")
    for s in above_not_in_pipeline:
        lines.append(f"- {s} (coverage: {float(cov.get(s) or 0.0):.3f})\n")

    lines.append("\n## Pipeline series below the coverage threshold\n")
    lines.append(f"Count: {len(pipeline_below_threshold)}\n")
    for s in pipeline_below_threshold:
        lines.append(f"- {s} (coverage: {float(cov.get(s) or 0.0):.3f})\n")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text("".join(lines), encoding="utf-8")

    timestamp = datetime.utcnow().isoformat(timespec="seconds")
    print(f"Wrote {OUT_PATH} at {timestamp}Z")


if __name__ == "__main__":
    main()
