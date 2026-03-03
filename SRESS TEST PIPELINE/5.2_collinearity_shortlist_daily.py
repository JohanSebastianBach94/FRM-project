"""Wrapper for Step 5 – daily shortlist collinearity (original: scripts/step5_shortlist_collinearity_daily.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "scripts/step5_shortlist_collinearity_daily.py"

def main() -> None:
    sys.argv = [str(TARGET), *sys.argv[1:]]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
