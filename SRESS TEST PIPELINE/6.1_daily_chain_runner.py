"""Run the numbered daily pipeline scripts (factor prep → Lasso → shortlist)."""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
STEP_ONE = PIPELINE_DIR / "daily_factor_preparation.py"
STEP_TWO = PIPELINE_DIR / "daily_elasticnet_mapping.py"
STEP_THREE = PIPELINE_DIR / "daily_shortlist_builder.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the daily factor/Lasso/shortlist flow")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO codes to process")
    parser.add_argument("--lasso-target", type=str, default="Rt_daily", help="Target column for Lasso stage")
    parser.add_argument("--shortlist-max", type=int, default=12, help="Max shortlist size per ISO")
    return parser.parse_args()


def run_script(script_path: Path, args: list[str] | None = None) -> None:
    argv = [str(script_path)]
    if args:
        argv.extend(args)
    prior = sys.argv
    try:
        sys.argv = argv
        runpy.run_path(script_path, run_name="__main__")
    finally:
        sys.argv = prior


def main() -> None:
    args = parse_args()
    print("Running daily factor prep (step-one)")
    iso_args = ["--isos", *args.isos]
    run_script(STEP_ONE, iso_args)

    print(f"Running daily ElasticNet mapping for {args.isos} (step-two)")
    run_script(STEP_TWO, iso_args + ["--target", args.lasso_target])

    print("Building shortlists (step-three)")
    run_script(STEP_THREE, iso_args + ["--max-features", str(args.shortlist_max)])


if __name__ == "__main__":  # pragma: no cover
    main()
