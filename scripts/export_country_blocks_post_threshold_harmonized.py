"""Export post-threshold + post-harmonization country block series lists.

This reads the *final* block definitions produced by the pipeline
(typically written by Step 2.1 country block definer) and outputs:

- analysis_outputs/country_block_lists_post_threshold_harmonized.md
- analysis_outputs/country_block_lists_post_threshold_harmonized_flat.csv

The exported lists are based on each block's `series_codes` field, which is
mutated by `scripts/prepare_country_blocks.py` after applying the coverage
threshold and weakest-ISO harmonization cutoff.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def _read_series_threshold(project_root: Path) -> float | None:
    plan = project_root / "data_collection_plan.json"
    if not plan.exists():
        return None
    try:
        payload = json.loads(plan.read_text(encoding="utf-8"))
    except Exception:
        return None
    try:
        return float(payload.get("series_threshold"))
    except Exception:
        return None


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    blocks_path = project_root / "outputs" / "country_block_definition.json"
    if not blocks_path.exists():
        raise FileNotFoundError(f"Missing {blocks_path}")

    data = json.loads(blocks_path.read_text(encoding="utf-8"))
    threshold = _read_series_threshold(project_root)

    out_md = project_root / "analysis_outputs" / "country_block_lists_post_threshold_harmonized.md"
    out_csv = project_root / "analysis_outputs" / "country_block_lists_post_threshold_harmonized_flat.csv"
    out_md.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    lines.append("# Country Block Lists (Post-Threshold + Post-Harmonization)\n")
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
    lines.append(f"Source: {blocks_path.as_posix()}\n")
    if threshold is not None:
        lines.append(f"Coverage threshold: {threshold}\n")
    lines.append(f"Countries: {len(data)}\n")

    rows: list[str] = ["iso,block_key,series_code"]

    for iso in sorted(data.keys()):
        country = data[iso]
        lines.append(f"## {iso} ({country.get('country')})\n")

        for block in country.get("blocks", []) or []:
            key = str(block.get("key") or "UNKNOWN_BLOCK")
            series = [str(s) for s in (block.get("series_codes") or []) if str(s).strip()]

            # One-line list per block (easy to diff/scan)
            joined = ", ".join(series)
            lines.append(f"- {key}: {joined}")
            for s in series:
                rows.append(f"{iso},{key},{s}")

        lines.append("")

    out_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    out_csv.write_text("\n".join(rows) + "\n", encoding="utf-8")

    print(f"Wrote {out_md}")
    print(f"Wrote {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
