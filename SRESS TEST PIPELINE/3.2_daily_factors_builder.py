"""Wrapper for Step 3 – build daily factors (governed path).

Historically this wrapper executed `scripts/build_daily_factors_for_st.py`. The
governed daily pipeline lives in `SRESS TEST PIPELINE/daily_factor_preparation.py`
and prefers the frozen Step-2 blocks + coverage contract policies.
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE" / "daily_factor_preparation.py"

def main() -> None:
    sys.argv = [str(TARGET)]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
