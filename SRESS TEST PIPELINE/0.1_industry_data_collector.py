"""Wrapper for Step 0 – collect industry data (original: collect_industry_data.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = PIPELINE_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PIPELINE_DIR / "collect_industry_data.py"


def _refresh_local_derived_series(project_root: Path) -> None:
    """Refresh local derived drivers (SOFR/swaption proxies) before collection.

    This avoids stale/stepwise manual-series artifacts (e.g., SWAPTION_VOL_USA)
    without requiring manual terminal parameters.
    """

    script_path = project_root / "scripts" / "generate_sofr_and_swaption_series.py"
    if not script_path.exists():
        return
    sys.argv = [str(script_path)]
    runpy.run_path(script_path, run_name="__main__")

def main() -> None:
    _refresh_local_derived_series(PROJECT_ROOT)
    sys.argv = [str(TARGET)]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
