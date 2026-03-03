from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# Ensure project root is on sys.path (mirrors scripts/prepare_country_factors.py)
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Reuse the same loaders as Step 3.
from data_pipeline import load_nss_betas, load_project_config, load_stress_indicators


def _load_do_not_use_series(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        df = pd.read_csv(path)
    except Exception:
        return set()
    if "series" not in df.columns or "do_not_use" not in df.columns:
        return set()
    blocked: set[str] = set()
    for _, row in df.iterrows():
        series = str(row.get("series", "") or "").strip()
        if not series:
            continue
        flag = str(row.get("do_not_use", "") or "").strip()
        if flag and flag.upper() != "NAN":
            blocked.add(series)
    return blocked


def load_combined_panel_monthly() -> pd.DataFrame:
    expanded_candidates = [
        ROOT / "data" / "stress_indicators_expanded.csv",
        ROOT / "data_pipeline" / "data" / "stress_indicators_expanded.csv",
    ]

    combined: pd.DataFrame | None = None
    for candidate in expanded_candidates:
        if candidate.exists():
            try:
                combined = pd.read_csv(candidate, index_col="Date", parse_dates=True)
            except Exception:
                combined = pd.read_csv(candidate, index_col=0, parse_dates=True)
            break

    if combined is None:
        data = load_stress_indicators()
        combined = data.get("combined")

    if combined is None or combined.empty:
        raise RuntimeError(
            "Combined panel is unavailable. Ensure stress indicators have been built (preferably stress_indicators_expanded.csv)."
        )

    # Trim to the configured project sample window (matches catalog coverage calculations).
    try:
        cfg = load_project_config()
        date_start = getattr(cfg, "date_start", None)
        date_end = getattr(cfg, "date_end", None)
        if date_start is not None and date_end is not None:
            combined = combined.loc[(combined.index >= date_start) & (combined.index <= date_end)]
    except Exception:
        pass

    derived_dir = ROOT / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
    if derived_dir.exists():
        for csv_path in sorted(derived_dir.glob("Price_to_income_ratio_*.csv")):
            iso = csv_path.stem.replace("Price_to_income_ratio_", "").strip()
            if not iso:
                continue
            series_name = f"Price_to_income_{iso}"
            if series_name in combined.columns:
                continue
            try:
                raw = pd.read_csv(csv_path)
            except Exception:
                continue
            if raw.empty:
                continue
            date_col = "date" if "date" in raw.columns else raw.columns[0]
            value_col = "value" if "value" in raw.columns else None
            if value_col is None:
                numeric_cols = raw.select_dtypes(include="number").columns
                if numeric_cols.empty:
                    continue
                value_col = str(numeric_cols[0])
            idx = pd.to_datetime(raw[date_col], errors="coerce")
            values = pd.to_numeric(raw[value_col], errors="coerce")
            overlay = pd.Series(values.values, index=idx, name=series_name).dropna()
            if overlay.empty:
                continue
            overlay = overlay[~overlay.index.duplicated(keep="first")].sort_index()
            combined = combined.join(overlay.to_frame(), how="outer")

    nss = load_nss_betas()
    if nss is not None and not nss.empty:
        nss = nss.loc[:, ~nss.columns.duplicated()].copy()
        missing_cols = [c for c in nss.columns if c not in combined.columns]
        if missing_cols:
            combined = combined.join(nss[missing_cols], how="outer")

    # month end alignment
    try:
        combined.index = pd.to_datetime(combined.index)
    except Exception:
        pass
    combined = combined.sort_index()
    return combined.resample("M").last()


def main() -> int:
    frozen_path = ROOT / "outputs" / "country_block_definition.json"
    threshold_path = ROOT / "analysis_outputs" / "coverage_threshold_config.json"
    catalog_path = ROOT / "catalog.csv"

    out_dir = ROOT / "analysis_outputs" / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)

    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))

    # thresholds
    series_threshold = 0.62
    min_obs = 60
    if threshold_path.exists():
        try:
            payload = json.loads(threshold_path.read_text(encoding="utf-8"))
            series_threshold = float(payload.get("series_threshold", series_threshold))
        except Exception:
            pass

    do_not_use = _load_do_not_use_series(catalog_path)

    panel = load_combined_panel_monthly()
    panel_cols = set(map(str, panel.columns))

    rows: list[dict[str, object]] = []
    block_rows: list[dict[str, object]] = []

    for iso in sorted(frozen.keys()):
        blocks = (frozen.get(iso) or {}).get("blocks") or []
        for block in blocks:
            block_key = str(block.get("key") or "")
            series_list = [str(s) for s in (block.get("series_codes") or [])]

            counts = {
                "n_total": 0,
                "n_do_not_use": 0,
                "n_missing_in_panel": 0,
                "n_present": 0,
                "n_retained": 0,
                "n_drop_low_coverage": 0,
                "n_drop_low_min_obs": 0,
                "n_drop_both": 0,
            }

            for series in series_list:
                counts["n_total"] += 1
                is_dnu = series in do_not_use
                in_panel = series in panel_cols

                coverage = np.nan
                non_na = 0
                first_dt = ""
                last_dt = ""
                pass_cov = False
                pass_min = False

                if in_panel:
                    s = panel[series]
                    non_na = int(s.notna().sum())
                    coverage = float(s.notna().mean()) if len(s) else np.nan
                    first = s.first_valid_index()
                    last = s.last_valid_index()
                    first_dt = str(first.date()) if first is not None else ""
                    last_dt = str(last.date()) if last is not None else ""
                    pass_cov = bool(coverage >= series_threshold) if not np.isnan(coverage) else False
                    pass_min = bool(non_na >= min_obs)

                retained = (not is_dnu) and in_panel and pass_cov and pass_min

                if is_dnu:
                    reason = "do_not_use"
                    counts["n_do_not_use"] += 1
                elif not in_panel:
                    reason = "missing_in_panel"
                    counts["n_missing_in_panel"] += 1
                elif pass_cov and pass_min:
                    reason = "retained"
                    counts["n_retained"] += 1
                else:
                    counts["n_present"] += 1
                    cov_bad = not pass_cov
                    min_bad = not pass_min
                    if cov_bad and min_bad:
                        reason = "dropped_low_coverage_and_min_obs"
                        counts["n_drop_both"] += 1
                    elif cov_bad:
                        reason = "dropped_low_coverage"
                        counts["n_drop_low_coverage"] += 1
                    else:
                        reason = "dropped_low_min_obs"
                        counts["n_drop_low_min_obs"] += 1

                rows.append(
                    {
                        "iso": iso,
                        "block": block_key,
                        "series": series,
                        "reason": reason,
                        "in_panel": bool(in_panel),
                        "do_not_use": bool(is_dnu),
                        "coverage_frac": coverage,
                        "non_na": int(non_na),
                        "series_threshold": float(series_threshold),
                        "min_obs": int(min_obs),
                        "pass_coverage": bool(pass_cov) if in_panel else False,
                        "pass_min_obs": bool(pass_min) if in_panel else False,
                        "first_valid": first_dt,
                        "last_valid": last_dt,
                        "retained": bool(retained),
                    }
                )

            block_rows.append(
                {
                    "iso": iso,
                    "block": block_key,
                    **counts,
                    "retained_share": (counts["n_retained"] / counts["n_total"]) if counts["n_total"] else np.nan,
                    "series_threshold": float(series_threshold),
                    "min_obs": int(min_obs),
                }
            )

    df = pd.DataFrame(rows)
    df_blocks = pd.DataFrame(block_rows)

    df = df.sort_values(["iso", "block", "reason", "series"]).reset_index(drop=True)
    df_blocks = df_blocks.sort_values(["iso", "block"]).reset_index(drop=True)

    out_series = out_dir / "literature_series_usability.csv"
    out_blocks = out_dir / "literature_series_usability_by_block.csv"
    df.to_csv(out_series, index=False)
    df_blocks.to_csv(out_blocks, index=False)

    # Simple Markdown summary
    md_lines: list[str] = []
    md_lines.append("# Literature Series Usability Audit")
    md_lines.append("")
    md_lines.append(f"Thresholds: series_threshold={series_threshold:.3f}, min_obs={min_obs} monthly points")
    md_lines.append("")

    reasons = df["reason"].value_counts(dropna=False)
    md_lines.append("## Drop reasons (counts)")
    md_lines.append("")
    md_lines.append(reasons.to_frame("count").to_string())
    md_lines.append("")

    worst = df_blocks.sort_values(["retained_share", "n_retained"], ascending=[True, True]).head(20)
    md_lines.append("## Worst blocks by retained share")
    md_lines.append("")
    md_lines.append(worst[["iso", "block", "n_total", "n_retained", "n_missing_in_panel", "n_drop_low_coverage", "n_drop_low_min_obs", "n_drop_both", "retained_share"]].to_string(index=False))
    md_lines.append("")

    (out_dir / "literature_series_usability.md").write_text("\n".join(md_lines), encoding="utf-8")

    print(f"Wrote {out_series.relative_to(ROOT)}")
    print(f"Wrote {out_blocks.relative_to(ROOT)}")
    print(f"Wrote {(out_dir / 'literature_series_usability.md').relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
