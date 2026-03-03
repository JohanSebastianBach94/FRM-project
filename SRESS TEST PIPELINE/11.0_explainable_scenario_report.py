#!/usr/bin/env python3
"""Compatibility wrapper.

The explainable scenario report generator belongs to the deterministic scenario phase.
The canonical script is now:
  python "SRESS TEST PIPELINE/10.3_explainable_scenario_report.py" --run-id <run_id>

This wrapper remains so older commands that reference Step 11.0 keep working.
"""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> int:
    target = Path(__file__).resolve().parent / "10.3_explainable_scenario_report.py"
    runpy.run_path(str(target), run_name="__main__")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
