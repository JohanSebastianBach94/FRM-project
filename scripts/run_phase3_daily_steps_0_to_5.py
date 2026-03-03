#!/usr/bin/env python3
"""Proxy that runs the new daily chain runner inside SRESS TEST PIPELINE."""

from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path
from typing import Sequence

TARGET = Path(__file__).resolve().parent.parent / "SRESS TEST PIPELINE" / "6.1_daily_chain_runner.py"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proxy invoking the numbered Phase 3 daily runner.")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO codes to process")
    parser.add_argument(
        "--lasso-target",
        type=str,
        default="Rt_daily",
        help="Target column for the daily Lasso stage (defaults to Rt_daily)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    run_args = [str(TARGET)]
    if args.isos:
        run_args.extend(["--isos", *args.isos])
    run_args.extend(["--lasso-target", args.lasso_target])

    prior_argv = sys.argv
    try:
        sys.argv = run_args
        runpy.run_path(TARGET, run_name="__main__")
    finally:
        sys.argv = prior_argv


if __name__ == "__main__":  # pragma: no cover
    main()
