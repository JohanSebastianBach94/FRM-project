import json
from pathlib import Path
import importlib.util


def load_module_from_path(path: Path):
    spec = importlib.util.spec_from_file_location("step12_1", str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module spec from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    step_path = root / "SRESS TEST PIPELINE" / "12.1_monte_carlo_scenario_plots.py"
    block_def = root / "analysis_outputs" / "literature_factors" / "country_block_definition.within_block.json"

    mod = load_module_from_path(step_path)
    block_defs = mod._load_block_definitions(block_def)
    rev_map, dup_rows = mod._reverse_block_map(block_defs)

    checks = [
        ("DEU", "V2X"),
        ("ESP", "V2X"),
        ("FRA", "V2X"),
        ("ITA", "V2X"),
        ("USA", "VIXCLS"),
    ]

    out = {
        "block_def_path": str(block_def),
        "rev_map_has": [],
        "rev_map_sample_keys": {},
        "dup_rows_n": len(dup_rows),
    }

    for iso, series in checks:
        iso_rev = rev_map.get(iso) or {}
        out["rev_map_has"].append(
            {
                "iso": iso,
                "series": series,
                "present": series in iso_rev,
                "mapped_blocks": sorted(list(iso_rev.get(series, set()))),
            }
        )
        # Include a small sample of keys for debugging string mismatches
        keys = sorted(list(iso_rev.keys()))
        out["rev_map_sample_keys"][iso] = {
            "n_keys": len(keys),
            "first_30": keys[:30],
            "contains_V2X_like": [k for k in keys if "V2" in k or "VIX" in k][:30],
        }

    dest = root / "SRESS TEST PIPELINE" / "MC scenario plots" / "alternative2" / "_rev_map_step12_1_diagnostic.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
