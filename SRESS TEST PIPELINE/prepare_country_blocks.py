#!/usr/bin/env python3
"""Pipeline-local copy of Step 2 core script.

This exists to satisfy the repository rule that pipeline-called scripts live
under `SRESS TEST PIPELINE/`.

Source of truth for the implementation remains the same logic as the root-level
script. This file is intentionally kept as a thin wrapper to avoid code drift.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET = PROJECT_ROOT / "scripts" / "prepare_country_blocks.py"


def main() -> None:
    sys.argv = [str(TARGET)]
    runpy.run_path(str(TARGET), run_name="__main__")


if __name__ == "__main__":
    main()
