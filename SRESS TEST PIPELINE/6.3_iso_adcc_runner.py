"""Wrapper for Step 6 – ISO ADCC diagnostics (Step 6.3)."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE" / "iso_adcc_diagnostics.py"

def main() -> None:
    sys.argv = [str(TARGET), *sys.argv[1:]]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
