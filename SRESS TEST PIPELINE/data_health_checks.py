#!/usr/bin/env python3
"""Pipeline-local entrypoint for Step 1 data health checks.

This lives inside SRESS TEST PIPELINE so that pipeline-called "core" code is
discoverable in one place, while forwarding to the canonical implementation in
scripts/data_health_checks.py.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET = PROJECT_ROOT / "scripts" / "data_health_checks.py"


def main() -> None:
    argv = [str(TARGET), *sys.argv[1:]]
    prior = sys.argv
    try:
        sys.argv = argv
        runpy.run_path(TARGET, run_name="__main__")
    finally:
        sys.argv = prior


if __name__ == "__main__":
    main()
