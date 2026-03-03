"""Optimize the smoothing-vs-noise trade-off by sweeping config overrides.

Each experiment:
1. Writes overrides to a temporary JSON file.
2. Calls `fit_dcc_garch.py` with the overrides to re-estimate parameters.
3. Runs `validate_model.py` with a bespoke persistence threshold.
4. Captures fit metrics, Ljung-Box summary, and forecast improvements.
5. Copies the produced results to a snapshot directory for later comparison.

Run this script when the team wants to collect comparative performance data across smoothing settings.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR / "results"
VALIDATION_DIR = BASE_DIR / "validation_results"
SNAPSHOT_ROOT = RESULTS_DIR / "tradeoff_snapshots"
VALIDATION_SNAPSHOT_ROOT = VALIDATION_DIR / "tradeoff_snapshots"
PYTHON_EXECUTABLE = sys.executable
SUMMARY_CSV = BASE_DIR / "tradeoff_results_summary.csv"

TRADEOFF_GRID = [
    {
        "name": "heavy_smooth",
        "description": "High shrinkage, strong winsorization, strict cleaning",
        "config": {
            "preprocess": {
                "winsorize": True,
                "winsorize_lower": 2.0,
                "winsorize_upper": 98.0
            },
            "cleaning": {
                "ffill_limit": 3,
                "second_ffill_limit": 6,
                "row_missing_threshold": 60
            },
            "dcc": {
                "dcc_reg_penalty": 1400.0,
                "dcc_stationarity_target": 0.92,
                "dcc_max_a_plus_b": 0.95
            }
        },
        "persistence_threshold": 0.0018
    },
    {
        "name": "balanced",
        "description": "Default smoothing with moderate winsorization",
        "config": {
            "preprocess": {
                "winsorize": True,
                "winsorize_lower": 0.5,
                "winsorize_upper": 99.5
            },
            "cleaning": {
                "ffill_limit": 5,
                "second_ffill_limit": 10,
                "row_missing_threshold": 50
            },
            "dcc": {
                "dcc_reg_penalty": 800.0,
                "dcc_stationarity_target": 0.9,
                "dcc_max_a_plus_b": 0.97
            }
        },
        "persistence_threshold": 0.0015
    },
    {
        "name": "light_smooth",
        "description": "Looser regularization, minimal winsorization/trimming",
        "config": {
            "preprocess": {
                "winsorize": False
            },
            "cleaning": {
                "ffill_limit": 7,
                "second_ffill_limit": 14,
                "row_missing_threshold": 80
            },
            "dcc": {
                "dcc_reg_penalty": 100.0,
                "dcc_stationarity_target": 0.88,
                "dcc_max_a_plus_b": 0.99
            }
        },
        "persistence_threshold": 0.0012
    }
]

def run_fit(overrides_path: Path) -> None:
    env = os.environ.copy()
    env["DCC_SKIP_ADCC_CORR_EXPORT"] = "1"
    subprocess.run(
        [PYTHON_EXECUTABLE, str(BASE_DIR / "fit_dcc_garch.py"), "--config-overrides", str(overrides_path)],
        check=True,
        env=env
    )


def run_validation(persistence_threshold: float) -> None:
    env = os.environ.copy()
    env["DCC_PERSISTENCE_THRESHOLD"] = str(persistence_threshold)
    subprocess.run(
        [PYTHON_EXECUTABLE, str(BASE_DIR / "validate_model.py")],
        check=True,
        env=env
    )


def snapshot_outputs(name: str) -> None:
    dest = SNAPSHOT_ROOT / name
    dest.mkdir(parents=True, exist_ok=True)
    for fname in [
        "dcc_garch_parameters.csv",
        "dcc_parameters.csv",
        "correlation_time_series.csv",
        "adcc_correlation_time_series.csv",
        "adcc_parameters.csv",
        "standardized_residuals.csv",
        "fit_metrics.json"
    ]:
        src = RESULTS_DIR / fname
        if src.exists():
            shutil.copy(src, dest / fname)
    val_dest = VALIDATION_SNAPSHOT_ROOT / name
    val_dest.mkdir(parents=True, exist_ok=True)
    for fname in [
        "out_of_sample_forecast_test.csv",
        "dcc_vs_adcc_correlation_diff.csv",
        "ljungbox_summary.json"
    ]:
        src = VALIDATION_DIR / fname
        if src.exists():
            shutil.copy(src, val_dest / fname)


def collect_metrics(spec: dict) -> dict:
    metrics = {
        "name": spec["name"],
        "description": spec.get("description", ""),
        "persistence_threshold": spec.get("persistence_threshold", 0.0015)
    }
    fit_metrics_path = RESULTS_DIR / "fit_metrics.json"
    if fit_metrics_path.exists():
        with open(fit_metrics_path, "r", encoding="utf-8") as fh:
            metrics.update(json.load(fh))
    forecast_file = VALIDATION_DIR / "out_of_sample_forecast_test.csv"
    if forecast_file.exists():
        df = pd.read_csv(forecast_file)
        if not df.empty:
            metrics["avg_improvement"] = df["improvement_pct"].mean()
            metrics["persistence_rate"] = (df["fallback"] == "persistence").mean()
    lb_summary = VALIDATION_DIR / "ljungbox_summary.json"
    if lb_summary.exists():
        with open(lb_summary, "r", encoding="utf-8") as fh:
            lb_data = json.load(fh)
            metrics["ljungbox_fail_rate"] = lb_data.get("fail_rate")
            metrics["ljungbox_tested"] = lb_data.get("tested")
    metrics.update(spec.get("config", {}).get("dcc", {}))
    return metrics

def run_experiment(spec):
    print(f"\n=== Running trade-off experiment: {spec['name']} ===")
    overrides_payload = spec.get("config", {})
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(overrides_payload, tmp)
        tmp.flush()
        overrides_path = Path(tmp.name)
    try:
        run_fit(overrides_path)
        run_validation(spec.get("persistence_threshold", 0.0015))
        snapshot_outputs(spec["name"])
        return collect_metrics(spec)
    except subprocess.CalledProcessError as exc:
        print(f"[ERROR] Experiment {spec['name']} failed: {exc}")
        return None
    finally:
        overrides_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Run trade-off experiments for DCC/ADCC smoothing")
    parser.add_argument("--spec", choices=[s["name"] for s in TRADEOFF_GRID], help="Run a single tradeoff config")
    parser.add_argument("--reset", action="store_true", help="Reset the summary file before running")
    args = parser.parse_args()

    if args.reset and SUMMARY_CSV.exists():
        SUMMARY_CSV.unlink()

    existing = []
    if SUMMARY_CSV.exists():
        existing = pd.read_csv(SUMMARY_CSV).to_dict(orient="records")

    specs_to_run = [s for s in TRADEOFF_GRID if args.spec is None or s["name"] == args.spec]
    results = existing

    for spec in specs_to_run:
        result = run_experiment(spec)
        if result is None:
            continue
        results = [r for r in results if r.get("name") != spec["name"]]
        results.append(result)
        pd.DataFrame(results).to_csv(SUMMARY_CSV, index=False)
        print(f"Metrics saved to {SUMMARY_CSV}")

    if args.spec is None:
        print("\nTrade-off experiments complete. Review snapshots/summary for comparison.")

if __name__ == "__main__":
    main()
