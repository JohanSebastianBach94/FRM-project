"""Tune coverage thresholds for the Rt/ADCC pipeline."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

LOG = logging.getLogger(__name__)

ISO_LIST = ["DEU", "ESP", "FRA", "ITA", "USA"]
OUTPUT_DIR = Path("analysis_outputs/coverage_optimizer")
OUTPUT_DIR.mkdir(exist_ok=True)
SUMMARY_CSV = OUTPUT_DIR / "coverage_threshold_summary.csv"
RT_HEADERS_ROOT = OUTPUT_DIR / "rt_headers"
RT_HEADERS_ROOT.mkdir(exist_ok=True)

PERSISTENCE_PATTERN = "ADCC done (persistence="


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run unified ISO ADCC diagnostics for multiple coverage thresholds and collect diagnostics."
    )
    parser.add_argument("--thresholds", nargs="+", type=float, default=[0.8, 0.85, 0.9],
                        help="Coverage thresholds to evaluate in ascending order.")
    parser.add_argument("--shrinkage", type=float, default=0.2, help="Shrinkage fallback used by the ADCC pipeline.")
    parser.add_argument("--grid-a", type=float, default=0.01)
    parser.add_argument("--grid-b", type=float, default=0.9)
    parser.add_argument("--grid-g", type=float, default=0.0)
    parser.add_argument(
        "--fit-method",
        choices=["opt", "grid"],
        default=None,
        help="Optional: force ADCC/DCC parameter fitting method in iso_adcc_diagnostics.py",
    )
    parser.add_argument(
        "--opt-maxiter",
        type=int,
        default=None,
        help="Optional: maximum iterations for optimizer when --fit-method=opt",
    )
    parser.add_argument("--target-persistence", type=float,
                        help="If provided, refine the threshold via a dichotomic search to approach this persistence (average across ISOs).")
    parser.add_argument("--dichotomic-tolerance", type=float, default=0.002,
                        help="Stop dichotomic search when the persistence difference is within this tolerance (avg).")
    parser.add_argument("--max-iterations", type=int, default=5,
                        help="Maximum iterations for the dichotomic refinements after the initial sweep.")
    parser.add_argument("--persist-summaries", action="store_true",
                        help="Keep a copy of each run's CLI output to output/{threshold}.log for traceability.")
    return parser.parse_args()


def run_pipeline(threshold: float, args: argparse.Namespace) -> tuple[str, dict[str, float]]:
    command = [sys.executable, "SRESS TEST PIPELINE/iso_adcc_diagnostics.py",
               "--coverage-threshold", str(threshold),
               "--shrinkage", str(args.shrinkage),
               "--grid-a", str(args.grid_a),
               "--grid-b", str(args.grid_b),
               "--grid-g", str(args.grid_g)]
    if args.fit_method:
        command += ["--fit-method", str(args.fit_method)]
    if args.opt_maxiter is not None:
        command += ["--opt-maxiter", str(int(args.opt_maxiter))]
    LOG.info("Running ADCC for coverage %.3f", threshold)
    process = subprocess.run(command, capture_output=True, text=True)
    output = process.stdout + process.stderr
    if args.persist_summaries:
        log_path = OUTPUT_DIR / f"run_{threshold:.3f}.log"
        log_path.write_text(output)
    persistences = {}
    for line in output.splitlines():
        if PERSISTENCE_PATTERN in line:
            # expected format "ISO ADCC done (persistence=0.9100, ..."
            try:
                prefix, rest = line.split("ADCC done (", 1)
                iso = prefix.strip().split()[0]
                parts = rest.split("persistence=")[1].split(",", 1)
                persistence = float(parts[0])
                persistences[iso] = persistence
                LOG.debug("Captured persistence %s=%.4f", iso, persistence)
            except (IndexError, ValueError):
                LOG.warning("Could not parse persistence line: %s", line)
    return output, persistences


def record_rt_columns(threshold: float, iso: str, columns: list[str]) -> None:
    dest = RT_HEADERS_ROOT / f"{threshold:.3f}"
    dest.mkdir(exist_ok=True)
    header_file = dest / f"{iso}_Rt_columns.json"
    header_file.write_text(json.dumps(columns, ensure_ascii=False, indent=2))


def collect_diagnostics(threshold: float, persistences: dict[str, float]) -> list[dict[str, str | float]]:
    records: list[dict[str, str | float]] = []
    for iso in ISO_LIST:
        sigma_path = Path("analysis_outputs/diag_corr") / f"{iso}_Sigma_eigenvalues.csv"
        if not sigma_path.exists():
            LOG.warning("Missing eigenvalue file for %s after threshold %.3f", iso, threshold)
            continue
        df_sigma = pd.read_csv(sigma_path, parse_dates=["date"]).sort_values("date")
        last = df_sigma.iloc[-1]
        rt_path = Path("analysis_outputs/diag_corr") / f"{iso}_Rt.csv"
        retained = 0
        if rt_path.exists():
            rt_df = pd.read_csv(rt_path, nrows=0)
            retained = len(rt_df.columns) - 1
            record_rt_columns(threshold, iso, rt_df.columns.tolist())
        records.append({
            "threshold": threshold,
            "iso": iso,
            "persistence": persistences.get(iso, math.nan),
            "min_eigen": last["min_eigen"],
            "max_eigen": last["max_eigen"],
            "retained_series": retained,
            "last_date": last["date"].strftime("%Y-%m-%d")
        })
    return records


def append_summary(records: Sequence[dict[str, str | float]]) -> None:
    headers = ["threshold", "iso", "persistence", "min_eigen", "max_eigen", "retained_series", "last_date"]
    write_header = not SUMMARY_CSV.exists()
    with SUMMARY_CSV.open("a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        if write_header:
            writer.writeheader()
        for record in records:
            writer.writerow(record)


def averaged_persistence(records: Sequence[dict[str, str | float]]) -> float:
    values = [r["persistence"] for r in records if not math.isnan(r["persistence"])]
    if not values:
        return math.nan
    return sum(values) / len(values)


def run_threshold_sweep(args: argparse.Namespace) -> list[dict[str, str | float]]:
    all_records: list[dict[str, str | float]] = []
    for threshold in sorted(set(args.thresholds)):
        _, persistences = run_pipeline(threshold, args)
        records = collect_diagnostics(threshold, persistences)
        append_summary(records)
        avg = averaged_persistence(records)
        LOG.info("Threshold %.3f -> avg persistence %.4f", threshold, avg)
        all_records.extend(records)
    return all_records


def dichotomic_search(args: argparse.Namespace) -> None:
    if args.target_persistence is None:
        return
    lower, upper = min(args.thresholds), max(args.thresholds)
    iteration = 0
    while iteration < args.max_iterations and lower + 1e-6 < upper:
        middle = (lower + upper) / 2
        _, persistences = run_pipeline(middle, args)
        records = collect_diagnostics(middle, persistences)
        append_summary(records)
        avg = averaged_persistence(records)
        LOG.info("Dichotomic trial %.6f -> avg persistence %.4f", middle, avg)
        if math.isnan(avg):
            break
        if abs(avg - args.target_persistence) <= args.dichotomic_tolerance:
            LOG.info("Reached target persistence %.4f within tolerance at threshold %.6f", args.target_persistence, middle)
            break
        # assume higher coverage increases persistence; adjust bounds accordingly
        if avg > args.target_persistence:
            lower = middle
        else:
            upper = middle
        iteration += 1


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    run_threshold_sweep(args)
    if args.target_persistence is not None:
        dichotomic_search(args)
    LOG.info("Coverage optimizer summary written to %s", SUMMARY_CSV)


if __name__ == "__main__":
    main()
