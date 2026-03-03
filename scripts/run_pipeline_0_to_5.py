"""Run stress-test pipeline steps 0.1 through 5.x with robust UTF-8 logging.

This avoids PowerShell redirection quirks (UTF-16) and makes long runs easy to
monitor via log files.

Usage:
  python scripts/run_pipeline_0_to_5.py

Outputs:
  analysis_outputs/runlogs/pipeline_0_to_5/latest/step*.log
  analysis_outputs/runlogs/pipeline_0_to_5/latest/status.json
"""

from __future__ import annotations

import json
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

        proc = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(project_root),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
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
    project_root = Path(__file__).resolve().parents[1]
    pipeline_dir = project_root / "SRESS TEST PIPELINE"

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
    ]

    missing = [str(p) for p in steps if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing pipeline scripts:\n" + "\n".join(missing))

    base_dir = project_root / "analysis_outputs" / "runlogs" / "pipeline_0_to_5" / "latest"
    base_dir.mkdir(parents=True, exist_ok=True)

    results: list[StepResult] = []
    for i, step in enumerate(steps, start=1):
        log_path = base_dir / f"{i:02d}_{step.stem}.log"
        res = run_step(project_root, step, log_path)
        results.append(res)
        if res.returncode != 0:
            status_path = base_dir / "status.json"
            status_path.write_text(
                json.dumps(
                    {
                        "started_at": results[0].started_at if results else _utc_now(),
                        "finished_at": _utc_now(),
                        "ok": False,
                        "failed_step": res.name,
                        "results": [asdict(r) for r in results],
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            return res.returncode

    status_path = base_dir / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "started_at": results[0].started_at if results else _utc_now(),
                "finished_at": _utc_now(),
                "ok": True,
                "results": [asdict(r) for r in results],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
