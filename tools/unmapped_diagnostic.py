import json
from pathlib import Path

import pandas as pd


def build_reverse_map(block_def_path: Path) -> dict[str, dict[str, set[str]]]:
    raw = json.loads(block_def_path.read_text(encoding="utf-8"))
    out: dict[str, dict[str, set[str]]] = {}
    for iso, payload in raw.items():
        blocks = payload.get("blocks") or []
        m: dict[str, set[str]] = {}
        for b in blocks:
            bk = str(b.get("key") or "")
            for s in (b.get("series_codes") or []):
                m.setdefault(str(s), set()).add(bk)
        out[str(iso)] = m
    return out


def load_dt_factors(run_dir: Path, iso: str) -> list[str]:
    dt = run_dir / "inputs" / iso / "covariance" / f"{iso}_Dt_daily.csv"
    cols = list(pd.read_csv(dt, nrows=0).columns)
    return [c for c in cols if c not in {"date", "Rt_daily"}]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    run_dir = root / "analysis_outputs" / "scenarios" / "latest"
    block_def = root / "analysis_outputs" / "literature_factors" / "country_block_definition.within_block.json"

    rev_map = build_reverse_map(block_def)

    checks = [
        ("DEU", "V2X"),
        ("ESP", "V2X"),
        ("FRA", "V2X"),
        ("ITA", "V2X"),
        ("USA", "VIXCLS"),
    ]

    out = []
    for iso, series in checks:
        factors = load_dt_factors(run_dir, iso)
        out.append(
            {
                "iso": iso,
                "series": series,
                "in_dt_factors": series in factors,
                "in_block_def": series in (rev_map.get(iso) or {}),
                "mapped_blocks": sorted(list((rev_map.get(iso) or {}).get(series, set()))),
            }
        )

    dest = root / "SRESS TEST PIPELINE" / "MC scenario plots" / "alternative2" / "_unmapped_diagnostic.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
