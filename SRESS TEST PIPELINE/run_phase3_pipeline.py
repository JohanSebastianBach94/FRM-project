from __future__ import annotations

import argparse
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WRAPPER_DIR = Path(__file__).resolve().parent

WRAPPER_STEPS = [
    "0.1_industry_data_collector.py",
    "0.2_industry_data_merger.py",
    "0.3_block_coverage_checker.py",
    "1.0_clean_monthly_panel.py",
    "1.1_data_health_diagnostics.py",
    "1.2_coverage_threshold_optimizer.py",
    "2.1_country_block_definer.py",
    "3.1_country_factor_preparer.py",
    "3.2_daily_factors_builder.py",
    "3.3_daily_panel_builder.py",
    "4.1_lasso_mapping_trainer.py",
    "4.2_lasso_mapping_daily_trainer.py",
    "5.1_collinearity_shortlist.py",
    "5.2_collinearity_shortlist_daily.py",
    "6.1_daily_chain_runner.py",
    "6.2_backtest_daily_runner.py",
    "6.3_iso_adcc_runner.py",
    "6.4_verification_with_logging.py",
    "7.0_coverage_threshold_watchdog.py",
    "7.1_volatility_mean_reversion_runner.py",
    "7.2_dcc_garch_trainer.py",
    "8.0_postfit_model_diagnostics.py",
    "9.0_scenario_governance.py",
    "10.0_deterministic_scenarios.py",
    "10.1_macro_narrative_and_plots.py",
    "10.2_factor_shocks_from_macro.py",
    "10.3_explainable_scenario_report.py",
    "11.1_historical_replay.py",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Phase 3 wrapper scripts in order.")
    parser.add_argument(
        "--start-step",
        type=str,
        default="0",
        help="Lower bound (prefix) of the wrappers to execute, e.g., 1 starts at 1.x wrappers.",
    )
    parser.add_argument(
        "--end-step",
        type=str,
        default="11",
        help="Upper bound (prefix) of the wrappers to execute, inclusive.",
    )
    parser.add_argument(
        "--allow-warn",
        action="store_true",
        help=(
            "Legacy flag (kept for compatibility). By default the orchestrator proceeds "
            "past Step 9 WARN-gating and records WARN blocks in the scenario manifest."
        ),
    )
    parser.add_argument(
        "--strict-warn-gate",
        action="store_true",
        help="If set, Step 9 will block on WARN readiness (requires manual resolution).",
    )
    parser.add_argument(
        "--scenario-run-id",
        type=str,
        default="latest",
        help="Run ID to use for Step 9 outputs (default: latest, required by downstream Steps 10–11).",
    )
    parser.add_argument(
        "--force-daily-adcc",
        action="store_true",
        help="Force recompute of Step 6.2 daily ADCC artifacts (passes --force to daily_adcc_prep).",
    )
    parser.add_argument(
        "--force-iso-adcc",
        action="store_true",
        help="Force recompute of Step 6.3 ISO ADCC diagnostics (passes --force to iso_adcc_diagnostics).",
    )
    parser.add_argument(
        "--force-adcc",
        action="store_true",
        help="Run full ADCC estimation in Step 7.2 (passes --force-adcc to dcc_garch trainer).",
    )
    parser.add_argument(
        "--macro-config",
        type=str,
        default="SRESS TEST PIPELINE/scenario_macro_templates.yaml",
        help="Macro template YAML/JSON to pass to Step 10.2 (default: SRESS TEST PIPELINE/scenario_macro_templates.yaml).",
    )

    # Single-switch wiring for daily factor space.
    # - shortlist: current governed 12-factor daily shortlist space
    # - literature: within-block PCA block-factor space (ISO_block_f1/f2)
    parser.add_argument(
        "--daily-factor-space",
        choices=["shortlist", "literature"],
        default=os.environ.get("DCC_DAILY_FACTOR_SPACE", "shortlist"),
        help="Daily ADCC factor space (default: shortlist). Can also be set via env DCC_DAILY_FACTOR_SPACE.",
    )
    parser.add_argument(
        "--literature-freq",
        default=os.environ.get("DCC_LITERATURE_FREQ", "M"),
        help="Literature factor frequency label used by Step 3 (default: M).",
    )
    parser.add_argument(
        "--literature-max-factors-per-block",
        type=int,
        default=int(os.environ.get("DCC_LITERATURE_MAX_FACTORS_PER_BLOCK", "2")),
        help="Max PCs per block in literature mode (default: 2).",
    )
    parser.add_argument(
        "--literature-dedupe-corr",
        type=float,
        default=float(os.environ.get("DCC_LITERATURE_DEDUPE_CORR", "0.995")),
        help="Within-block dedupe abs-corr threshold in literature mode (default: 0.995).",
    )
    return parser.parse_args()


def parse_prefix(name: str) -> Decimal:
    prefix = name.split("_", 1)[0]
    return Decimal(prefix)


def _is_prefix_in_range(prefix: Decimal, *, start: Decimal, end: Decimal) -> bool:
    """Return True if a wrapper prefix is within [start, end] with "major" end support.

    If end is an integer (e.g. 11), include all end.x wrappers by treating the
    upper bound as < end+1 (i.e. include 11.1, 11.2, ...).
    """
    if prefix < start:
        return False
    if end == end.to_integral_value():
        return prefix < (end + Decimal(1))
    return prefix <= end


def run_step(
    step_name: str,
    *,
    allow_warn: bool,
    scenario_run_id: str,
    macro_config: str,
    force_daily_adcc: bool,
    force_iso_adcc: bool,
    force_adcc: bool,
    daily_factor_space: str,
    literature_freq: str,
    literature_max_factors_per_block: int,
    literature_dedupe_corr: float,
) -> None:
    step_path = WRAPPER_DIR / step_name
    if not step_path.exists():
        raise FileNotFoundError(step_path)
    print(f"[phase3 orchestrator] Running {step_name}", flush=True)

    cmd = [sys.executable, str(step_path)]
    if allow_warn and step_name == "9.0_scenario_governance.py":
        cmd.append("--allow-warn")
    if step_name == "9.0_scenario_governance.py":
        cmd.extend(["--run-id", str(scenario_run_id)])

    if step_name.startswith("10."):
        cmd.extend(["--run-id", str(scenario_run_id)])

    if force_daily_adcc and step_name == "6.2_backtest_daily_runner.py":
        cmd.append("--force")
    if force_iso_adcc and step_name == "6.3_iso_adcc_runner.py":
        cmd.append("--force")
    if force_adcc and step_name == "7.2_dcc_garch_trainer.py":
        cmd.append("--force-adcc")

    if step_name == "10.2_factor_shocks_from_macro.py":
        cmd.extend(["--macro-config", str(macro_config)])

    if step_name == "11.1_historical_replay.py":
        cmd.extend(["--scenario-run-id", str(scenario_run_id)])

    env = os.environ.copy()

    # Ensure Steps 3 (factor prep), 6 (daily ADCC), and 9 (scenario freeze)
    # interpret the daily factor universe consistently.
    if str(daily_factor_space) == "literature":
        env["DCC_DAILY_FACTOR_SPACE"] = "literature"
        env["DCC_LITERATURE"] = "1"
        env["DCC_LITERATURE_MODE"] = "within_block"
        env["DCC_LITERATURE_FREQ"] = str(literature_freq)
        env["DCC_LITERATURE_DAILY"] = "1"
        env["DCC_LITERATURE_MAX_FACTORS_PER_BLOCK"] = str(int(literature_max_factors_per_block))
        env["DCC_LITERATURE_DEDUPE_CORR"] = str(float(literature_dedupe_corr))
    else:
        # Make the mode explicit to avoid surprises from a parent shell env.
        env["DCC_DAILY_FACTOR_SPACE"] = "shortlist"
        env["DCC_LITERATURE"] = "0"

    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True, env=env)


def main() -> None:
    args = parse_args()
    start = Decimal(args.start_step)
    end = Decimal(args.end_step)
    eligible = [
        name
        for name in WRAPPER_STEPS
        if _is_prefix_in_range(parse_prefix(name), start=start, end=end)
    ]
    allow_warn_effective = bool(args.allow_warn) or (not bool(args.strict_warn_gate))
    for wrapper in eligible:
        try:
            run_step(
                wrapper,
                allow_warn=allow_warn_effective,
                scenario_run_id=str(args.scenario_run_id),
                macro_config=str(args.macro_config),
                force_daily_adcc=bool(args.force_daily_adcc),
                force_iso_adcc=bool(args.force_iso_adcc),
                force_adcc=bool(args.force_adcc),
                daily_factor_space=str(args.daily_factor_space),
                literature_freq=str(args.literature_freq),
                literature_max_factors_per_block=int(args.literature_max_factors_per_block),
                literature_dedupe_corr=float(args.literature_dedupe_corr),
            )
        except subprocess.CalledProcessError as err:  # pragma: no cover - orchestration
            print(f"Wrapper {wrapper} failed with return code {err.returncode}", flush=True)
            raise


if __name__ == "__main__":
    main()
