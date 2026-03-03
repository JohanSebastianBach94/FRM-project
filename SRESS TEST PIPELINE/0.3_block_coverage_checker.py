"""Wrapper for Step 0 – check block coverage (original: scripts/check_block_coverage.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PIPELINE_DIR / "check_block_coverage.py"

def main() -> None:
    sys.argv = [str(TARGET)]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
