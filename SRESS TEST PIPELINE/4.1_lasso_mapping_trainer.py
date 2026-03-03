"""Wrapper for Step 4 – lasso mappings (original: 4.0_lasso_pipeline.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
TARGET = Path(__file__).resolve().parent / "4.0_lasso_pipeline.py"

def main() -> None:
    # Keep Step 4 compute tractable for full-pipeline runs.
    # Permutation p-values and bootstrap stability selection are diagnostics-only and
    # can be very expensive; set to 0 to skip.
    sys.argv = [
        str(TARGET),
        "--macro-feature-source",
        "daily_shortlist",
        "--permutation-trials",
        "0",
        "--stability-bootstraps",
        "0",
    ]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
