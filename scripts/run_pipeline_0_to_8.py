"""Run stress-test pipeline steps 0.1 through 8.0 with robust UTF-8 logging.

This is the full end-to-end runner for the wrapper scripts under
`SRESS TEST PIPELINE/`.

It avoids PowerShell redirection quirks (UTF-16) and makes long runs easy to
monitor via log files.

Usage:
  python scripts/run_pipeline_0_to_8.py

Outputs:
  analysis_outputs/runlogs/pipeline_0_to_8/latest/step*.log
  analysis_outputs/runlogs/pipeline_0_to_8/latest/status.json

Notes:
- Each step is executed with the current Python interpreter (sys.executable).
- stdout/stderr are captured into per-step UTF-8 log files.
- The runner stops at the first non-zero exit code.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class StepResult:
    name: str
    script: str
    started_at: str
    finished_at: str
    returncode: int
    log_path: str


def _utc_now() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def run_step(project_root: Path, script_path: Path, log_path: Path) -> StepResult:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = _utc_now()
    with log_path.open("w", encoding="utf-8", newline="\n") as log:
        log.write(f"[{started_at}] START {script_path}\n")
        log.flush()

        env = os.environ.copy()
        # Force UTF-8 stdio in child processes. This prevents Windows default
        # codepages (e.g., cp1252) from crashing on unicode prints.
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUNBUFFERED", "1")

        proc = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
            env=env,
        )

        finished_at = _utc_now()
        log.write(f"\n[{finished_at}] END rc={proc.returncode}\n")
        log.flush()

    return StepResult(
        name=script_path.name,
        script=str(script_path),
        started_at=started_at,
        finished_at=finished_at,
        returncode=int(proc.returncode),
        log_path=str(log_path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline steps 0.1 through 8.0 with robust UTF-8 logging")
    parser.add_argument(
        "--start-step",
        type=int,
        default=1,
        help="1-based step index to start from (default: 1). Useful for resuming after a failure.",
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    pipeline_dir = project_root / "SRESS TEST PIPELINE"

    base_dir = project_root / "analysis_outputs" / "runlogs" / "pipeline_0_to_8" / "latest"
    base_dir.mkdir(parents=True, exist_ok=True)

    # Preflight: validate block config governance rules ("no double usage")
    # without changing the existing step numbering.
    if os.environ.get("PIPELINE_SKIP_PREFLIGHT", "0") != "1":
        preflight_log = base_dir / "00_preflight_validate_country_blocks.log"
        strict = os.environ.get("PIPELINE_STRICT_NO_DOUBLE_USAGE", "0") == "1"
        script = project_root / "scripts" / "validate_country_blocks.py"
        env = os.environ.copy()
        env.setdefault("PYTHONUTF8", "1")
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUNBUFFERED", "1")
        with preflight_log.open("w", encoding="utf-8", newline="\n") as log:
            started_at = _utc_now()
            log.write(f"[{started_at}] START preflight {script}\n")
            log.flush()
            argv = [sys.executable, str(script)]
            if strict:
                argv.append("--strict")
            proc = subprocess.run(
                argv,
                cwd=str(project_root),
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
                env=env,
            )
            finished_at = _utc_now()
            log.write(f"\n[{finished_at}] END rc={proc.returncode}\n")
            log.flush()
        if proc.returncode != 0:
            return int(proc.returncode)

    steps = [
        pipeline_dir / "0.1_industry_data_collector.py",
        pipeline_dir / "0.2_industry_data_merger.py",
        pipeline_dir / "0.3_block_coverage_checker.py",
        pipeline_dir / "1.0_clean_monthly_panel.py",
        pipeline_dir / "1.1_data_health_diagnostics.py",
        pipeline_dir / "1.2_coverage_threshold_optimizer.py",
        pipeline_dir / "2.1_refresh_catalog_and_health.py",
        pipeline_dir / "2.1_country_block_definer.py",
        pipeline_dir / "3.1_clean_panel.py",
        pipeline_dir / "3.1_country_factor_preparer.py",
        pipeline_dir / "3.2_daily_factors_builder.py",
        pipeline_dir / "3.3_daily_panel_builder.py",
        pipeline_dir / "4.0_lasso_pipeline.py",
        pipeline_dir / "5.1_collinearity_shortlist.py",
        pipeline_dir / "5.2_collinearity_shortlist_daily.py",
        pipeline_dir / "6.1_daily_chain_runner.py",
        pipeline_dir / "6.2_backtest_daily_runner.py",
        pipeline_dir / "6.3_iso_adcc_runner.py",
        pipeline_dir / "6.4_verification_with_logging.py",
        pipeline_dir / "7.0_coverage_threshold_watchdog.py",
        pipeline_dir / "7.1_volatility_mean_reversion_runner.py",
        pipeline_dir / "7.2_dcc_garch_trainer.py",
        pipeline_dir / "8.0_postfit_model_diagnostics.py",
    ]

    missing = [str(p) for p in steps if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing pipeline scripts:\n" + "\n".join(missing))

    if not (1 <= args.start_step <= len(steps)):
        raise ValueError(f"--start-step must be in [1, {len(steps)}], got {args.start_step}")

    results: list[StepResult] = []
    status_path = base_dir / "status.json"

    def _write_status(*, ok: bool, failed_step: str | None, interrupted: bool = False) -> None:
        status_path.write_text(
            json.dumps(
                {
                    "started_at": results[0].started_at if results else _utc_now(),
                    "finished_at": _utc_now(),
                    "ok": ok,
                    "failed_step": failed_step,
                    "interrupted": interrupted,
                    "results": [asdict(r) for r in results],
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    try:
        for i, step in enumerate(steps, start=1):
            if i < args.start_step:
                continue
            log_path = base_dir / f"{i:02d}_{step.stem}.log"
            res = run_step(project_root, step, log_path)
            results.append(res)

            if res.returncode != 0:
                _write_status(ok=False, failed_step=res.name)
                return res.returncode
    except KeyboardInterrupt:
        _write_status(ok=False, failed_step=None, interrupted=True)
        return 130

    _write_status(ok=True, failed_step=None)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
