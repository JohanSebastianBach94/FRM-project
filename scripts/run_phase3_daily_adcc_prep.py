#!/usr/bin/env python3
"""Thin wrapper for the daily volatility/correlation phase.

Source of truth lives in `SRESS TEST PIPELINE/daily_adcc_prep.py`.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_DIR = PROJECT_ROOT / "SRESS TEST PIPELINE"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from daily_adcc_prep import main  # noqa: E402


if __name__ == "__main__":  # pragma: no cover
    main()
