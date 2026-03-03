#!/usr/bin/env python3
"""Step 10.1 — IMF/FSAP-style macro narrative + explainability plots.

Consumes Step 10 deterministic factor shocks:
  analysis_outputs/scenarios/<run_id>/deterministic/factor_shocks.csv

Produces (illustrative, not calibrated):
- deterministic/macro_narrative_paths.csv   (quarterly deltas vs baseline)
- deterministic/macro_narrative_definition.json
- deterministic/plots/*.png                (macro + factor explainability)

Design intent
- Keep canonical shock space = factor innovations (Step 10 output).
- Macro narrative here is an *overlay/explanation layer* for communication.
  It is explicitly labeled as stylized unless/until calibrated.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional

import sys

import pandas as pd

# Matplotlib is already used elsewhere in the repo; force non-interactive backend.
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Allow running from repo root by making local pipeline modules importable.
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

from macro_narrative import (
    apply_baseline_levels,
    build_macro_narrative_paths,
    build_template_config_from_dict,
    macro_definition_payload,
    severity_from_factor_shocks,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _infer_run_id(explicit: Optional[str]) -> str:
    if explicit:
        return explicit
    latest = SCENARIOS_DIR / "latest"
    if latest.exists() and latest.is_dir():
        return latest.name
    raise SystemExit("Missing --run-id (no scenarios/latest folder)")


def _default_iso_multiplier(iso: str) -> float:
    # Small, opinionated defaults to mimic FSAP-style cross-country overlays.
    # You can override via --iso-multipliers.
    iso_u = iso.strip().upper()
    if iso_u in {"ITA", "ESP"}:
        return 1.2
    if iso_u in {"DEU", "FRA"}:
        return 1.0
    if iso_u in {"USA"}:
        return 0.9
    return 1.0


def _parse_iso_multipliers(text: Optional[str]) -> Dict[str, float]:
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception as e:
        raise SystemExit(f"--iso-multipliers must be JSON like {{\"ITA\": 1.2}}: {e}")
    out: Dict[str, float] = {}
    if not isinstance(obj, dict):
        raise SystemExit("--iso-multipliers must be a JSON object")
    for k, v in obj.items():
        out[str(k).upper()] = float(v)
    return out


def _plot_macro_paths(df: pd.DataFrame, *, out_path: Path, title: str) -> None:
    # df expected: quarter, variable, iso, delta
    variables = [
        "gdp_growth_yoy",
        "cpi_infl_yoy",
        "unemployment_rate",
        "policy_rate",
        "equity_price",
        "sovereign_spread",
    ]

    fig, axes = plt.subplots(2, 3, figsize=(14, 7), constrained_layout=True)
    axes = axes.reshape(-1)

    for ax, var in zip(axes, variables):
        sub = df.loc[df["variable"] == var]
        if sub.empty:
            ax.set_axis_off()
            continue

        for iso, g in sub.groupby("iso"):
            ax.plot(g["quarter"], g["delta"], linewidth=2.0, label=str(iso))

        units = str(sub["units"].iloc[0]) if "units" in sub.columns and not sub.empty else ""
        ax.axhline(0.0, color="#666", linewidth=1.0, alpha=0.6)
        ax.set_title(f"{var} ({units})")
        ax.set_xlabel("Quarter")
        ax.grid(True, alpha=0.25)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper right", frameon=False)

    fig.suptitle(title)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def _plot_macro_levels(df: pd.DataFrame, *, out_path: Path, title: str) -> None:
    # df expected: quarter, variable, iso, stressed
    variables = [
        "gdp_growth_yoy",
        "cpi_infl_yoy",
        "unemployment_rate",
        "policy_rate",
        "equity_price",
        "sovereign_spread",
    ]

    fig, axes = plt.subplots(2, 3, figsize=(14, 7), constrained_layout=True)
    axes = axes.reshape(-1)

    for ax, var in zip(axes, variables):
        sub = df.loc[df["variable"] == var]
        if sub.empty or sub["stressed"].isna().all():
            ax.set_axis_off()
            continue

        for iso, g in sub.groupby("iso"):
            ax.plot(g["quarter"], g["stressed"], linewidth=2.0, label=str(iso))

        units = str(sub["units"].iloc[0]) if "units" in sub.columns and not sub.empty else ""
        ax.set_title(f"{var} (level; units={units})")
        ax.set_xlabel("Quarter")
        ax.grid(True, alpha=0.25)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper right", frameon=False)

    fig.suptitle(title)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def _read_yaml_or_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Macro config not found: {path}")
    if path.suffix.lower() in {".json"}:
        return json.loads(path.read_text(encoding="utf-8"))
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore
        except Exception as e:
            raise SystemExit(
                "Reading YAML requires PyYAML. Install with: pip install pyyaml\n"
                f"Import error: {e}"
            )
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    raise SystemExit(f"Unsupported macro config extension: {path.suffix}")


def _plot_factor_paths(df: pd.DataFrame, *, out_path: Path, title: str, reference_factor: str) -> None:
    sub = df.loc[df["factor"].astype(str) == str(reference_factor)].copy()
    if sub.empty:
        return

    fig, ax = plt.subplots(figsize=(11, 5), constrained_layout=True)

    for iso, g in sub.groupby("iso"):
        g = g.sort_values("h")
        ax.plot(g["h"], g["shock"], linewidth=2.0, label=str(iso))

    ax.axhline(0.0, color="#666", linewidth=1.0, alpha=0.6)
    ax.set_title(title)
    ax.set_xlabel("h (days)")
    ax.set_ylabel("shock (innovation units)")
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best", frameon=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=180)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 10.1 — Macro narrative + plots (IMF/FSAP-style)")
    parser.add_argument("--run-id", default=None, help="Scenario run id created by Step 9")
    parser.add_argument("--scenario", default=None, help="Scenario id to plot/build (default: all)")
    parser.add_argument("--quarters", type=int, default=12, help="Macro horizon in quarters (default: 12)")
    parser.add_argument(
        "--reference-factor",
        default="V2X",
        help="Factor used to compute severity (default: V2X)",
    )
    parser.add_argument(
        "--iso-multipliers",
        default=None,
        help='Optional JSON map to scale macro severity by ISO, e.g. "{\\"ITA\\": 1.3, \\"DEU\\": 0.9}"',
    )
    parser.add_argument(
        "--macro-config",
        default=None,
        help="Optional YAML/JSON macro template file for IMF-like explicit quarterly paths",
    )
    parser.add_argument(
        "--input-shocks",
        default=None,
        help=(
            "Optional path to a factor shocks CSV to use instead of deterministic/factor_shocks.csv. "
            "Example: deterministic/factor_shocks_from_macro.csv"
        ),
    )
    parser.add_argument(
        "--tag",
        default="",
        help="Optional tag appended to output filenames (e.g., 'macro' or 'quantile')",
    )
    args = parser.parse_args()

    run_id = _infer_run_id(args.run_id)
    run_dir = SCENARIOS_DIR / run_id

    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing Step 9 manifest: {manifest_path}")

    if args.input_shocks:
        shocks_path = (PROJECT_ROOT / args.input_shocks) if not Path(args.input_shocks).is_absolute() else Path(args.input_shocks)
    else:
        shocks_path = run_dir / "deterministic" / "factor_shocks.csv"

    if not shocks_path.exists():
        raise SystemExit(f"Missing shocks CSV: {shocks_path}")

    factor_shocks = pd.read_csv(shocks_path)
    if args.scenario:
        factor_shocks = factor_shocks.loc[factor_shocks["scenario_id"].astype(str) == str(args.scenario)].copy()
        if factor_shocks.empty:
            raise SystemExit(f"No factor shocks found for scenario_id={args.scenario}")

    severity_df, sev_diag = severity_from_factor_shocks(factor_shocks, reference_factor=str(args.reference_factor))

    iso_mult_overrides = _parse_iso_multipliers(args.iso_multipliers)

    template_cfg = None
    if args.macro_config:
        cfg_path = (PROJECT_ROOT / args.macro_config) if not Path(args.macro_config).is_absolute() else Path(args.macro_config)
        template_cfg = build_template_config_from_dict(_read_yaml_or_json(cfg_path))

    macro_rows = []
    for _, r in severity_df.iterrows():
        iso = str(r["iso"]).upper()
        scen = str(r["scenario_id"])
        sev = float(r["severity"])

        if template_cfg is not None:
            scen_mult = float(template_cfg.scenario_multipliers.get(scen, 1.0))
            iso_default = float(template_cfg.iso_multipliers.get(iso, _default_iso_multiplier(iso)))
            mult = float(iso_mult_overrides.get(iso, iso_default))
            quarters = int(template_cfg.horizon_quarters)
            macro_specs = template_cfg.variables
            sev_used = float(sev) * scen_mult
        else:
            mult = float(iso_mult_overrides.get(iso, _default_iso_multiplier(iso)))
            quarters = int(args.quarters)
            macro_specs = None
            sev_used = float(sev)

        macro_rows.append(
            build_macro_narrative_paths(
                iso=iso,
                scenario_id=scen,
                severity=sev_used,
                quarters=quarters,
                macro_specs=macro_specs,
                iso_multiplier=mult,
            )
        )

    macro_df = pd.concat(macro_rows, ignore_index=True) if macro_rows else pd.DataFrame()
    if macro_df.empty:
        raise SystemExit("No macro narrative rows were produced")

    out_dir = _ensure_dir(run_dir / "deterministic")
    tag = ("_" + str(args.tag).strip()) if str(args.tag).strip() else ""
    macro_path = out_dir / f"macro_narrative_paths{tag}.csv"
    macro_df.to_csv(macro_path, index=False)

    macro_levels_path = None
    macro_levels_df = None
    if template_cfg is not None and template_cfg.baseline_levels:
        macro_levels_df = apply_baseline_levels(macro_df, baseline_levels=template_cfg.baseline_levels)
        macro_levels_path = out_dir / f"macro_narrative_levels{tag}.csv"
        macro_levels_df.to_csv(macro_levels_path, index=False)

    definition = {
        "run_id": run_id,
        "inputs": {
            "manifest": str(manifest_path),
            "factor_shocks": str(shocks_path),
            "reference_factor": str(args.reference_factor),
            "macro_config": str(args.macro_config) if args.macro_config else None,
        },
        **macro_definition_payload(quarters=int((template_cfg.horizon_quarters if template_cfg else args.quarters)), severity_diagnostics=sev_diag),
        "outputs": {
            "macro_deltas": str(macro_path),
            "macro_levels": str(macro_levels_path) if macro_levels_path else None,
        },
    }
    definition_path = out_dir / f"macro_narrative_definition{tag}.json"
    definition_path.write_text(json.dumps(definition, indent=2), encoding="utf-8")

    plots_dir = _ensure_dir(out_dir / f"plots{tag}")

    # Plots per scenario (multi-ISO overlays)
    for scen_id, g in macro_df.groupby("scenario_id"):
        title = f"Macro narrative deltas (scenario={scen_id})"
        _plot_macro_paths(g, out_path=plots_dir / f"macro_narrative_{scen_id}.png", title=title)

    if macro_levels_df is not None:
        for scen_id, g in macro_levels_df.groupby("scenario_id"):
            title = f"Macro narrative levels (scenario={scen_id})"
            _plot_macro_levels(g, out_path=plots_dir / f"macro_levels_{scen_id}.png", title=title)

    for scen_id, g in factor_shocks.groupby("scenario_id"):
        title = f"{args.reference_factor} factor shock (scenario={scen_id})"
        _plot_factor_paths(
            g,
            out_path=plots_dir / f"factor_{args.reference_factor}_{scen_id}.png",
            title=title,
            reference_factor=str(args.reference_factor),
        )

    print(f"[OK] Wrote: {macro_path}")
    if macro_levels_path:
        print(f"[OK] Wrote: {macro_levels_path}")
    print(f"[OK] Wrote: {definition_path}")
    print(f"[OK] Wrote plots under: {plots_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
