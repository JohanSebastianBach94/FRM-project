"""Wrapper for Step 3 – daily panel builder (governed alias).

The legacy implementation routed through `scripts/run_daily_for_st_pipeline.py`.
The governed daily pipeline is `SRESS TEST PIPELINE/daily_factor_preparation.py`.

Note: `run_phase3_pipeline.py` currently runs both 3.2 and 3.3. To avoid running
the daily factor build twice, this wrapper is idempotent: it only executes the
governed pipeline when daily outputs are missing.
"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE" / "daily_factor_preparation.py"

DAILY_FACTOR_DIR = PROJECT_ROOT / "analysis_outputs" / "factor_preparation_daily"


def _daily_outputs_exist() -> bool:
    if not DAILY_FACTOR_DIR.exists():
        return False
    # Treat any per-ISO output as evidence the governed daily build ran.
    return any(DAILY_FACTOR_DIR.glob("*_factors_daily.csv"))

def main() -> None:
    if _daily_outputs_exist():
        print("[Step 3.3] Daily governed factors already present; skipping rebuild.")
        return
    sys.argv = [str(TARGET)]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
