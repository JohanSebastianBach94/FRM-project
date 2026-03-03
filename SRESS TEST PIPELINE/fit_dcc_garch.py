#!/usr/bin/env python3
"""Pipeline-local entrypoint for the DCC-GARCH fit.

This exists so the Phase 3 pipeline only calls scripts located under
`SRESS TEST PIPELINE/`.

Implementation lives in `DCC GARCH MODEL/fit_dcc_garch.py`.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET = PROJECT_ROOT / "DCC GARCH MODEL" / "fit_dcc_garch.py"


def main() -> None:
    sys.argv = [str(TARGET), *sys.argv[1:]]
    runpy.run_path(str(TARGET), run_name="__main__")


if __name__ == "__main__":
    main()
