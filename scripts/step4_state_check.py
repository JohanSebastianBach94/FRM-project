#!/usr/bin/env python3
"""Sanity checks to ensure Step 4 stays coherent with Step 2/3 governance.

Checks:
- Step 4 explicit targets config is parseable
- Targets exist in the cleaned monthly panel(s)
- Step 3 manifests exist and are parseable (monthly + daily)
- Targets are not flagged do_not_use in catalog.csv

This is intentionally lightweight and read-only.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pandas as pd
import yaml

BASE_DIR = Path(__file__).resolve().parents[1]
TARGETS_CONFIG_DEFAULT = BASE_DIR / "config" / "step4_targets.yaml"
CATALOG_PATH = BASE_DIR / "catalog.csv"
TARGET_PANEL_CANDIDATES = [
    BASE_DIR / "data" / "cleaned_monthly_panel_full.parquet",
    BASE_DIR / "data" / "cleaned_monthly_panel.parquet",
]
MONTHLY_FACTORS_DIR = BASE_DIR / "analysis_outputs" / "factor_preparation"
DAILY_FACTORS_DIR = BASE_DIR / "analysis_outputs" / "factor_preparation_daily"
REPORT_PATH = BASE_DIR / "analysis_outputs" / "diagnostics" / "step4_state_report.json"


def _load_yaml(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as fp:
        return yaml.safe_load(fp)


def load_targets(config_path: Path) -> Tuple[Dict[str, List[str]], List[str]]:
    payload = _load_yaml(config_path) or {}
    targets = payload.get("targets", {}) if isinstance(payload, dict) else {}
    if not isinstance(targets, dict):
        return {}, []
    global_targets = targets.get("global", [])
    per_iso = targets.get("per_iso", {})
    out_per_iso: Dict[str, List[str]] = {}
    if isinstance(per_iso, dict):
        for iso, lst in per_iso.items():
            if not isinstance(lst, list):
                continue
            cleaned = [str(x).strip() for x in lst if str(x).strip()]
            out_per_iso[str(iso).upper()] = cleaned
    out_global = [str(x).strip() for x in (global_targets or []) if str(x).strip()] if isinstance(global_targets, list) else []
    return out_per_iso, out_global


def load_target_panel() -> pd.DataFrame:
    for path in TARGET_PANEL_CANDIDATES:
        if path.exists():
            panel = pd.read_parquet(path)
            if not isinstance(panel.index, pd.DatetimeIndex):
                panel.index = pd.to_datetime(panel.index)
            return panel.sort_index()
    raise FileNotFoundError(f"Target panel missing at {TARGET_PANEL_CANDIDATES[0]} or {TARGET_PANEL_CANDIDATES[1]}")


def load_do_not_use() -> Set[str]:
    if not CATALOG_PATH.exists():
        return set()
    try:
        df = pd.read_csv(CATALOG_PATH)
    except Exception:
        return set()
    cols = {c.lower(): c for c in df.columns}
    series_col = cols.get("series")
    dnu_col = cols.get("do_not_use")
    if not series_col or not dnu_col:
        return set()
    truthy = {"1", "true", "yes", "y", "t"}
    out: Set[str] = set()
    for s, flag in zip(df[series_col], df[dnu_col]):
        if pd.isna(s):
            continue
        flag_str = "" if pd.isna(flag) else str(flag).strip().lower()
        if flag_str in truthy:
            out.add(str(s).strip())
    return out


def _load_json_list(path: Path) -> List[Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def check_manifest(path: Path) -> Tuple[bool, str]:
    if not path.exists():
        return False, "missing"
    try:
        payload = _load_json_list(path)
    except Exception as exc:
        return False, f"parse_error: {exc}"
    if not payload:
        return False, "empty"
    return True, "ok"


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Step 4 coherence state")
    parser.add_argument("--targets-config", type=str, default=str(TARGETS_CONFIG_DEFAULT))
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on warnings")
    args = parser.parse_args()

    config_path = Path(args.targets_config)
    per_iso, global_targets = load_targets(config_path)
    panel = load_target_panel()
    dnu = load_do_not_use()

    report: Dict[str, Any] = {
        "targets_config": str(config_path),
        "panel_columns": int(panel.shape[1]),
        "isos": sorted(per_iso.keys()),
        "issues": [],
        "per_iso": {},
        "manifests": {},
    }

    any_issue = False

    # manifests per ISO
    for iso in sorted(per_iso.keys()):
        month_manifest = MONTHLY_FACTORS_DIR / f"{iso}_manifest.json"
        pca_meta = MONTHLY_FACTORS_DIR / f"{iso}_pca_component_metadata.json"
        daily_manifest = DAILY_FACTORS_DIR / f"{iso}_manifest_daily.json"
        ok1, s1 = check_manifest(month_manifest)
        ok2, s2 = check_manifest(pca_meta)
        ok3, s3 = check_manifest(daily_manifest)
        report["manifests"][iso] = {
            "monthly_manifest": {"path": str(month_manifest), "status": s1},
            "monthly_pca_meta": {"path": str(pca_meta), "status": s2},
            "daily_manifest": {"path": str(daily_manifest), "status": s3},
        }
        if not ok1 or not ok2 or not ok3:
            any_issue = True

    for iso in sorted(per_iso.keys()):
        targets = list(global_targets) + list(per_iso.get(iso, []))
        missing = [t for t in targets if t not in panel.columns]
        dnu_targets = [t for t in targets if t in dnu]
        report["per_iso"][iso] = {
            "targets": targets,
            "missing_in_panel": missing,
            "do_not_use_targets": dnu_targets,
        }
        if missing or dnu_targets:
            any_issue = True

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    # print concise summary
    print(f"[OK] wrote {REPORT_PATH}")
    for iso in sorted(per_iso.keys()):
        miss = report["per_iso"][iso]["missing_in_panel"]
        if miss:
            print(f"[WARN] {iso}: missing targets in panel: {', '.join(miss[:8])}{' ...' if len(miss) > 8 else ''}")
        dnu_t = report["per_iso"][iso]["do_not_use_targets"]
        if dnu_t:
            print(f"[WARN] {iso}: targets flagged do_not_use: {', '.join(dnu_t)}")
        m = report["manifests"][iso]
        for k in ("monthly_manifest", "monthly_pca_meta", "daily_manifest"):
            if m[k]["status"] != "ok":
                print(f"[WARN] {iso}: {k} -> {m[k]['status']}")

    if args.strict and any_issue:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
