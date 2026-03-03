"""Step 7.0 – run the coverage optimizer once diagnostics already exist."""
from __future__ import annotations

import argparse
import json
import logging
import math
import subprocess
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ISO_LIST = ["DEU", "ESP", "FRA", "ITA", "USA"]
PROJECT_ROOT = Path(__file__).resolve().parent.parent
TARGET_SCRIPT = PROJECT_ROOT / "scripts" / "coverage_threshold_optimizer.py"
SUMMARY_CSV = PROJECT_ROOT / "analysis_outputs" / "coverage_optimizer" / "coverage_threshold_summary.csv"
CONFIG_PATH = PROJECT_ROOT / "analysis_outputs" / "coverage_threshold_config.json"
DEFAULT_THRESHOLD = 0.62
DEFAULT_TARGET_PERSISTENCE = 0.82
DEFAULT_MIN_EIGEN = 0.04
DEFAULT_STEP = 0.02
DEFAULT_RETRIES = 3
DEFAULT_MIN_THRESHOLD = 0.5
DEFAULT_MAX_THRESHOLD = 0.95

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Guard the coverage threshold using diagnostics from the optimizer.")
    parser.add_argument(
        "--initial-threshold",
        type=float,
        default=None,
        help="Override the stored threshold before running the optimizer",
    )
    parser.add_argument(
        "--target-persistence",
        type=float,
        default=DEFAULT_TARGET_PERSISTENCE,
        help="Persistence level the optimizer should aim for",
    )
    parser.add_argument(
        "--min-eigen",
        type=float,
        default=DEFAULT_MIN_EIGEN,
        help="Minimum acceptable eigenvalue returned by the diagnostics",
    )
    parser.add_argument(
        "--step",
        type=float,
        default=DEFAULT_STEP,
        help="Step size to adjust the threshold when diagnostics are out of range",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_RETRIES,
        help="Maximum attempts at tuning the threshold before proceeding",
    )
    parser.add_argument(
        "--min-threshold",
        type=float,
        default=DEFAULT_MIN_THRESHOLD,
        help="Lower bound for the adjusted threshold",
    )
    parser.add_argument(
        "--max-threshold",
        type=float,
        default=DEFAULT_MAX_THRESHOLD,
        help="Upper bound for the adjusted threshold",
    )
    parser.add_argument("--grid-a", type=float, default=0.01, help="Grid A used by the optimizer")
    parser.add_argument("--grid-b", type=float, default=0.9, help="Grid B used by the optimizer")
    parser.add_argument("--grid-g", type=float, default=0.0, help="Grid G used by the optimizer")
    parser.add_argument(
        "--fit-method",
        choices=["opt", "grid"],
        default=None,
        help="Optional: force ADCC/DCC parameter fitting method for the underlying iso_adcc_diagnostics runs",
    )
    parser.add_argument(
        "--opt-maxiter",
        type=int,
        default=None,
        help="Optional: max iterations for optimizer when --fit-method=opt",
    )
    parser.add_argument("--shrinkage", type=float, default=0.2, help="Shrinkage fallback for ADCC")
    parser.add_argument("--dichotomic-tolerance", type=float, default=0.002, help="Tolerance for dichotomic refinements")
    parser.add_argument("--max-iterations", type=int, default=5, help="Max dichotomic iterations")
    parser.add_argument("--persist-summaries", action="store_true", help="Keep a log per optimizer run")
    return parser.parse_args()


def load_stored_threshold() -> Optional[float]:
    if not CONFIG_PATH.exists():
        return None
    try:
        payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        value = payload.get("series_threshold")
        if value is None:
            value = payload.get("block_threshold")
        if value is None:
            # Backwards compatibility for older configs.
            value = payload.get("threshold")
        if value is None:
            return None
        return float(value)
    except (ValueError, TypeError):
        return None


def persist_threshold(value: float) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        json.dumps(
            {
                # Backwards compatibility key.
                "threshold": value,
                "series_threshold": value,
                "block_threshold": value,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def build_optimizer_command(threshold: float, args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        str(TARGET_SCRIPT),
        "--thresholds",
        f"{threshold:.6f}",
        "--shrinkage",
        str(args.shrinkage),
        "--grid-a",
        str(args.grid_a),
        "--grid-b",
        str(args.grid_b),
        "--grid-g",
        str(args.grid_g),
    ]
    if args.fit_method:
        command += ["--fit-method", str(args.fit_method)]
    if args.opt_maxiter is not None:
        command += ["--opt-maxiter", str(int(args.opt_maxiter))]
    command += [
        "--target-persistence",
        str(args.target_persistence),
        "--dichotomic-tolerance",
        str(args.dichotomic_tolerance),
        "--max-iterations",
        str(args.max_iterations),
    ]
    if args.persist_summaries:
        command.append("--persist-summaries")
    return command


def run_optimizer(threshold: float, args: argparse.Namespace) -> None:
    command = build_optimizer_command(threshold, args)
    logger.info("Running coverage optimizer with threshold %.3f", threshold)
    subprocess.run(command, check=True)


def summary_row_count() -> int:
    if not SUMMARY_CSV.exists():
        return 0
    try:
        return pd.read_csv(SUMMARY_CSV).shape[0]
    except pd.errors.EmptyDataError:
        return 0


def fetch_recent_records(start: int) -> pd.DataFrame:
    if not SUMMARY_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(SUMMARY_CSV)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    return df.iloc[start:].reset_index(drop=True)


def extract_final_metrics(df: pd.DataFrame) -> tuple[float, float, float]:
    if len(df) < len(ISO_LIST):
        raise RuntimeError("Coverage optimizer did not emit enough rows for the diagnostics")
    final_slice = df.iloc[-len(ISO_LIST) :]
    thresholds = final_slice["threshold"].dropna().unique()
    if len(thresholds) == 0:
        raise RuntimeError("Missing threshold information in the summary file")
    final_threshold = float(thresholds[0])
    persistence_vals = final_slice["persistence"].dropna()
    persistence_mean = float(persistence_vals.mean()) if not persistence_vals.empty else math.nan
    min_eigen = float(final_slice["min_eigen"].min(skipna=True))
    return final_threshold, persistence_mean, min_eigen


def diagnostics_ok(persistence: float, min_eigen: float, args: argparse.Namespace) -> bool:
    if math.isnan(persistence) or math.isnan(min_eigen):
        return False
    return persistence >= args.target_persistence and min_eigen >= args.min_eigen


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    base_threshold = args.initial_threshold or load_stored_threshold() or DEFAULT_THRESHOLD
    current_threshold = clamp(base_threshold, args.min_threshold, args.max_threshold)
    prev_rows = summary_row_count()

    for attempt in range(1, args.max_retries + 1):
        logger.info("Optimizer attempt %d with threshold %.3f", attempt, current_threshold)
        run_optimizer(current_threshold, args)
        records = fetch_recent_records(prev_rows)
        prev_rows += len(records)
        if records.empty:
            raise RuntimeError("No new coverage optimizer results were produced")
        final_threshold, persistence, min_eigen = extract_final_metrics(records)
        persist_threshold(final_threshold)
        logger.info(
            "Attempt %d -> persistence=%.3f, min eigen=%.3f, threshold=%.3f",
            attempt,
            persistence,
            min_eigen,
            final_threshold,
        )
        if diagnostics_ok(persistence, min_eigen, args):
            logger.info("Diagnostics look healthy; continuing with threshold %.3f", final_threshold)
            return
        if attempt == args.max_retries:
            logger.warning(
                "Reached max retries but diagnostics still out of range; proceeding with threshold %.3f",
                final_threshold,
            )
            return
        next_threshold = clamp(final_threshold - args.step, args.min_threshold, args.max_threshold)
        if math.isclose(next_threshold, final_threshold):
            logger.warning("Threshold adjustment hit bounds (%.3f); stopping retries", final_threshold)
            return
        current_threshold = next_threshold


if __name__ == "__main__":
    main()