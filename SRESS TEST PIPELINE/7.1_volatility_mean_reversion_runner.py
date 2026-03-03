"""Wrapper for Step 7 – volatility mean reversion pipeline (original: Volatility_MeanReversion/run_pipeline.py)"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "Volatility_MeanReversion/run_pipeline.py"

def main() -> None:
    sys.argv = [str(TARGET)]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
