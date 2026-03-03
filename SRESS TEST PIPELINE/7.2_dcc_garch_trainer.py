"""Wrapper for Step 7 – DCC GARCH fit (original: DCC GARCH MODEL/fit_dcc_garch.py)"""
from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE/fit_dcc_garch.py"

def main() -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--force-adcc",
        action="store_true",
        help="Run full ADCC estimation (slower). Default is DCC-only for stability/speed.",
    )
    args, passthrough = parser.parse_known_args(sys.argv[1:])

    argv = [str(TARGET)]
    if (not args.force_adcc) and ("--skip-adcc" not in passthrough):
        argv.append("--skip-adcc")
    argv.extend(passthrough)
    sys.argv = argv
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
