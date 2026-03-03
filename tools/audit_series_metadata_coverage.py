"""Audit coverage of config/series_metadata.yaml against factor constituents.

Governance goal: every raw series appearing in any <ISO>_<block>_pca_loadings.csv
or <ISO>_<block>_factor_constituents.csv must have a frequency label in
series_metadata.yaml.

Outputs:
- analysis_outputs/factor_preparation/series_metadata_coverage_report.csv
- analysis_outputs/factor_preparation/series_metadata_coverage_missing.csv

Exit code:
- 0 if no missing series
- 2 if missing series are found
"""

from __future__ import annotations

import csv
from pathlib import Path


def _parse_iso_block_from_stem(stem: str, *, suffix_tokens: int) -> tuple[str, str]:
    # <ISO>_<block>_<suffix...>
    parts = (stem or "").split("_")
    iso = parts[0] if parts else ""
    block = "_".join(parts[1:-suffix_tokens]) if len(parts) >= (1 + suffix_tokens + 1) else ""
    return iso, block


def _load_series_frequency_map(series_metadata_path: Path) -> dict[str, str]:
    if not series_metadata_path.exists():
        return {}

    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(series_metadata_path.read_text(encoding="utf-8"))
        meta = (payload or {}).get("series_metadata") or {}
        if not isinstance(meta, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in meta.items():
            if not k or not isinstance(v, dict):
                continue
            freq = str(v.get("frequency") or "").strip().lower()
            if freq:
                out[str(k).strip()] = freq
        return out
    except Exception:
        return {}


def _iter_pca_loading_files(pca_dir: Path):
    for path in sorted(pca_dir.glob("*_pca_loadings.csv")):
        yield path


def _iter_factor_constituent_files(pca_dir: Path):
    for path in sorted(pca_dir.glob("*_factor_constituents.csv")):
        yield path


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    series_metadata_path = root / "config" / "series_metadata.yaml"
    pca_dir = root / "analysis_outputs" / "factor_preparation"

    series_freq = _load_series_frequency_map(series_metadata_path)

    report_path = pca_dir / "series_metadata_coverage_report.csv"
    missing_path = pca_dir / "series_metadata_coverage_missing.csv"

    # Track coverage per (iso, block)
    report_rows: list[dict[str, object]] = []
    missing_rows: list[dict[str, str]] = []

    def _emit_rows(*, iso: str, block: str, series_names: list[str], source_type: str, source_file: str) -> None:
        uniq = sorted({str(s).strip() for s in (series_names or []) if str(s).strip()})
        total = int(len(uniq))
        with_freq = 0
        missing_series: list[str] = []
        for s in uniq:
            if s in series_freq:
                with_freq += 1
            else:
                missing_series.append(s)

        row = {
            "iso": iso,
            "block": block,
            "pca_loadings_file": source_file if source_type == "pca_loadings" else "",
            "factor_constituents_file": source_file if source_type == "factor_constituents" else "",
            "source_type": source_type,
            "source_file": source_file,
            "n_constituents": total,
            "n_with_frequency": int(with_freq),
            "n_missing_frequency": int(len(missing_series)),
            "missing_share": (len(missing_series) / total) if total else 0.0,
        }
        report_rows.append(row)

        for s in missing_series:
            missing_rows.append(
                {
                    "iso": iso,
                    "block": block,
                    "series": s,
                    "pca_loadings_file": source_file if source_type == "pca_loadings" else "",
                    "factor_constituents_file": source_file if source_type == "factor_constituents" else "",
                    "source_type": source_type,
                    "source_file": source_file,
                }
            )

    for p in _iter_pca_loading_files(pca_dir):
        iso, block = _parse_iso_block_from_stem(p.stem, suffix_tokens=2)  # _pca_loadings
        try:
            # First column is the index (series name)
            with p.open("r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    continue
                series_names = []
                for row in reader:
                    if not row:
                        continue
                    series = (row[0] or "").strip()
                    if series:
                        series_names.append(series)
        except Exception:
            continue

        _emit_rows(iso=iso, block=block, series_names=series_names, source_type="pca_loadings", source_file=p.name)

    for p in _iter_factor_constituent_files(pca_dir):
        iso, block = _parse_iso_block_from_stem(p.stem, suffix_tokens=2)  # _factor_constituents
        try:
            with p.open("r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header is None:
                    continue
                try:
                    idx = [h.strip().lower() for h in header].index("series")
                except Exception:
                    continue

                series_names = []
                for row in reader:
                    if not row:
                        continue
                    if idx >= len(row):
                        continue
                    s = (row[idx] or "").strip()
                    if s:
                        series_names.append(s)
        except Exception:
            continue

        _emit_rows(
            iso=iso,
            block=block,
            series_names=series_names,
            source_type="factor_constituents",
            source_file=p.name,
        )

    # Write outputs
    pca_dir.mkdir(parents=True, exist_ok=True)

    with report_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "iso",
            "block",
            "pca_loadings_file",
            "factor_constituents_file",
            "source_type",
            "source_file",
            "n_constituents",
            "n_with_frequency",
            "n_missing_frequency",
            "missing_share",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(report_rows)

    with missing_path.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "iso",
            "block",
            "series",
            "pca_loadings_file",
            "factor_constituents_file",
            "source_type",
            "source_file",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(missing_rows)

    print("Wrote:", report_path)
    print("Wrote:", missing_path)

    if missing_rows:
        print(f"Missing frequency for {len(missing_rows)} series.")
        return 2

    print("OK: all constituents have frequency labels.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
