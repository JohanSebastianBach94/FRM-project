"""Wrapper to run Step 6 ISO ADCC diagnostics with exception logging.

Any unhandled exception is written to `logs/phase3_last_error.log`.

This wrapper intentionally delegates CLI parsing to the underlying implementation
to avoid argument drift.
"""
from __future__ import annotations

import sys
import traceback
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PIPELINE_DIR = PROJECT_ROOT / "SRESS TEST PIPELINE"
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from iso_adcc_diagnostics import main as phase3_main  # noqa: E402


def main() -> int:
    log_path = Path("logs") / "phase3_last_error.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Delegate to the existing CLI by mimicking argv
        # phase3_iso_adcc.main() already parses its own arguments from sys.argv,
        # so we just call it and let it handle options the usual way.
        phase3_main()
        # Clear previous error on success
        if log_path.exists():
            log_path.write_text("", encoding="utf-8")
        return 0
    except Exception:  # noqa: BLE001
        log_path.write_text(traceback.format_exc(), encoding="utf-8")
        raise


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
