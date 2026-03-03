"""Wrapper for Step 6 – daily ADCC preparation (Step 6.2)."""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TARGET = PROJECT_ROOT / "SRESS TEST PIPELINE" / "daily_adcc_prep.py"

# When this wrapper is executed via `runpy.run_path` (e.g., through VS Code's
# marker runner), Python does not reliably add this script's directory to
# sys.path. The underlying Step 6 code imports sibling modules (e.g.
# `iso_adcc_diagnostics`) as top-level modules, so we must ensure the pipeline
# directory is explicitly importable.
PIPELINE_DIR = TARGET.parent
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

def main() -> None:
    # Forward any wrapper args to the underlying script (e.g., --force).
    sys.argv = [str(TARGET)] + sys.argv[1:]
    runpy.run_path(TARGET, run_name="__main__")


if __name__ == "__main__":
    main()
