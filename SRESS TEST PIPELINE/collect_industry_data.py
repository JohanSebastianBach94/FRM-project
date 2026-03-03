"""Core Step 0 entrypoint (kept inside SRESS TEST PIPELINE).

This file exists so all pipeline-called entrypoints live under SRESS TEST PIPELINE.
Implementation logic remains in the repository-root script to avoid breaking
existing tooling; this entrypoint simply executes it.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    target = project_root / "collect_industry_data.py"
    sys.argv = [str(target)]
    runpy.run_path(target, run_name="__main__")


if __name__ == "__main__":
    main()
