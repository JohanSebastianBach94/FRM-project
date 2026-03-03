"""Wrapper for Step 1 – run data health checks (original: scripts/data_health_checks.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE" / "data_health_checks.py"

def main() -> None:
    sys.argv = [str(TARGET), *sys.argv[1:]]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
