"""Step 1.2 – record the base coverage threshold before the optimizer runs later."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

DEFAULT_THRESHOLD = 0.62
DEFAULT_WINDOW_YEARS = 10
DEFAULT_TRADING_CALENDAR_START = "1990-02-01"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "analysis_outputs" / "coverage_threshold_config.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist the bootstrap coverage threshold for Phase 3.")
    parser.add_argument(
        "--initial-threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Baseline coverage fraction that later optimizers will start from",
    )
    parser.add_argument(
        "--coverage-window-years",
        type=int,
        default=DEFAULT_WINDOW_YEARS,
        help="Trailing window (in years) used when computing series coverage ratios",
    )
    parser.add_argument(
        "--trading-calendar-start",
        type=str,
        default=DEFAULT_TRADING_CALENDAR_START,
        help="Start date for the business-day trading calendar used by coverage checks (YYYY-MM-DD)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "series_threshold": args.initial_threshold,
        "block_threshold": args.initial_threshold,
        "coverage_window_years": int(args.coverage_window_years),
        "trading_calendar_start": str(args.trading_calendar_start),
        "daily_upsampling_policy": {
            "default_method": "step_hold",
            "allow_backfill": False,
            "interpolate_opt_in_series": [],
            "interpolate_opt_in_blocks": [],
            "interpolate_opt_in_block_series": [],
        },
    }
    CONFIG_PATH.write_text(
        json.dumps(
            payload,
            indent=2,
        )
    )
    print(f"[1.2] Recorded initial coverage threshold {args.initial_threshold:.2f} in {CONFIG_PATH}")


if __name__ == "__main__":
    main()
