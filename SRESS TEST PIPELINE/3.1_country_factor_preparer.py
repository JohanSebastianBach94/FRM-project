"""Wrapper for Step 3 – factor prep (original: scripts/prepare_country_factors.py)"""
from __future__ import annotations

import os
import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "scripts/prepare_country_factors.py"

def main() -> None:
    argv = [str(TARGET)]

    # Literature mode wiring (optional): allows the orchestrator to generate
    # block-factor panels used by Step 7.2 without changing CLI invocation.
    if os.environ.get("DCC_LITERATURE", "0") == "1":
        argv.append("--literature")
        mode = os.environ.get("DCC_LITERATURE_MODE")
        if mode:
            argv.extend(["--literature-mode", str(mode)])
        freq = os.environ.get("DCC_LITERATURE_FREQ")
        if freq:
            argv.extend(["--literature-freq", str(freq)])
        if os.environ.get("DCC_LITERATURE_DAILY", "0") == "1":
            argv.append("--literature-expand-to-daily")
        max_f = os.environ.get("DCC_LITERATURE_MAX_FACTORS_PER_BLOCK")
        if max_f:
            argv.extend(["--literature-max-factors-per-block", str(max_f)])
        dedupe = os.environ.get("DCC_LITERATURE_DEDUPE_CORR")
        if dedupe:
            argv.extend(["--literature-dedupe-corr", str(dedupe)])

    sys.argv = argv
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
