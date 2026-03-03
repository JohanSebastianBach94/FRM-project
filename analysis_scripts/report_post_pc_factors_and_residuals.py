from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]

# Heuristic threshold for whether a block has "enough" usable series to be robust.
# - >=3: OK (PCA has room; less brittle)
# - 2: WARN (still workable; often just mortgage rate + HPI in real_estate)
# - 1: WARN (single-series block; no PCA)
# - 0: FAIL (empty)
MIN_OK_SERIES_PER_BLOCK = 3


def _coverage_grade(method: str, n_series_post_dedupe: int) -> tuple[str, str]:
    method = str(method or "")
    n = int(n_series_post_dedupe or 0)
    if method == "empty" or n <= 0:
        return "FAIL", "no_usable_series"
    if n == 1:
        return "WARN", "only_1_usable_series"
    if n == 2:
        return "WARN", "only_2_usable_series"
    if n < MIN_OK_SERIES_PER_BLOCK:
        return "WARN", f"only_{n}_usable_series"
    return "OK", ""


def _df_to_md_table(df: pd.DataFrame) -> str:
    """Render a DataFrame as a GitHub-flavored Markdown table without tabulate."""

    if df is None or df.empty:
        return "(none)"

    def _cell(v: object) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            if np.isnan(v):
                return ""
            return f"{v:.6g}"
        return str(v)

    cols = [str(c) for c in df.columns]
    rows = [[_cell(v) for v in row] for row in df.itertuples(index=False, name=None)]
    widths = [len(c) for c in cols]
    for row in rows:
        for i, v in enumerate(row):
            widths[i] = max(widths[i], len(v))

    def _fmt_row(values: list[str]) -> str:
        return "| " + " | ".join(v.ljust(widths[i]) for i, v in enumerate(values)) + " |"

    header = _fmt_row(cols)
    sep = "| " + " | ".join("-" * w for w in widths) + " |"
    body = "\n".join(_fmt_row(r) for r in rows)
    return "\n".join([header, sep, body])


@dataclass(frozen=True)
class IsoSummary:
    iso: str
    n_blocks_frozen: int
    n_blocks_manifest: int
    n_blocks_empty: int
    n_blocks_single: int
    n_blocks_mean: int
    n_blocks_pca: int
    n_series_post_dedupe_total: int
    n_factors_total: int
    factors_cols: list[str]


def _safe_read_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _count_methods(block_details: dict[str, dict]) -> dict[str, int]:
    out = {"empty": 0, "single_series": 0, "mean": 0, "pca": 0}
    for _, d in (block_details or {}).items():
        m = str((d or {}).get("method") or "")
        if m in out:
            out[m] += 1
    return out


def summarize_explained_variance() -> pd.DataFrame:
    """Summarize literature PCA explained-variance ratios per (iso, block)."""

    path = ROOT / "analysis_outputs" / "literature_factors" / "literature_pca_explained.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()

    # expected columns: iso, block, component, explained_variance_ratio
    for col in ["iso", "block", "component", "explained_variance_ratio"]:
        if col not in df.columns:
            return pd.DataFrame()

    df = df.copy()
    df["explained_variance_ratio"] = pd.to_numeric(df["explained_variance_ratio"], errors="coerce")

    # component suffix _fK → order K
    def _k(c: str) -> int:
        c = str(c)
        if "_f" in c:
            try:
                return int(c.rsplit("_f", 1)[1])
            except Exception:
                return 10_000
        return 10_000

    df["k"] = df["component"].map(_k)
    df = df.sort_values(["iso", "block", "k"]).dropna(subset=["explained_variance_ratio"])

    rows: list[dict[str, object]] = []
    for (iso, block), g in df.groupby(["iso", "block"], dropna=False):
        ratios = g["explained_variance_ratio"].tolist()
        pc1 = ratios[0] if len(ratios) >= 1 else np.nan
        cum2 = float(np.sum(ratios[:2])) if len(ratios) >= 2 else (ratios[0] if ratios else np.nan)
        rows.append(
            {
                "iso": str(iso),
                "block": str(block),
                "n_components": int(len(ratios)),
                "pc1_var": float(pc1) if pc1 is not None else np.nan,
                "cum2_var": float(cum2) if cum2 is not None else np.nan,
            }
        )
    out = pd.DataFrame(rows)
    return out.sort_values(["iso", "block"]).reset_index(drop=True)


def summarize_literature_manifests() -> tuple[list[IsoSummary], pd.DataFrame]:
    manifests_dir = ROOT / "analysis_outputs" / "literature_factors"
    frozen_path = ROOT / "outputs" / "country_block_definition.json"
    frozen = _safe_read_json(frozen_path)

    iso_rows: list[IsoSummary] = []
    block_rows: list[dict[str, object]] = []

    for iso in sorted(frozen.keys()):
        blocks = (frozen.get(iso) or {}).get("blocks") or []
        frozen_keys = [str(b.get("key")) for b in blocks if b.get("key")]

        mf_path = manifests_dir / f"{iso}_literature_manifest.json"
        mf = _safe_read_json(mf_path)
        block_details: dict[str, dict] = mf.get("block_details") or {}

        # Per-block
        for key in frozen_keys:
            d = block_details.get(key) or {}
            method = str(d.get("method") or "missing")
            post = d.get("series_post_dedupe") or []
            factors = d.get("factors") or []
            grade, reason = _coverage_grade(method, len(post))
            block_rows.append(
                {
                    "iso": iso,
                    "block": key,
                    "method": method,
                    "n_series_post_dedupe": int(len(post)),
                    "n_factors": int(len(factors)),
                    "coverage_grade": grade,
                    "coverage_reason": reason,
                }
            )

        # Per-ISO
        methods = _count_methods(block_details)
        n_series_post_total = 0
        n_factors_total = 0
        all_factor_cols: list[str] = []
        for key, d in block_details.items():
            n_series_post_total += int(len((d or {}).get("series_post_dedupe") or []))
            f = [str(x) for x in ((d or {}).get("factors") or [])]
            n_factors_total += int(len(f))
            all_factor_cols.extend(f)

        iso_rows.append(
            IsoSummary(
                iso=iso,
                n_blocks_frozen=int(len(frozen_keys)),
                n_blocks_manifest=int(len(block_details)),
                n_blocks_empty=int(methods["empty"]),
                n_blocks_single=int(methods["single_series"]),
                n_blocks_mean=int(methods["mean"]),
                n_blocks_pca=int(methods["pca"]),
                n_series_post_dedupe_total=int(n_series_post_total),
                n_factors_total=int(n_factors_total),
                factors_cols=sorted(set(all_factor_cols)),
            )
        )

    df_blocks = pd.DataFrame(block_rows)
    return iso_rows, df_blocks


def summarize_daily_outputs(iso: str) -> dict[str, object]:
    out_dir = ROOT / "analysis_outputs" / "diag_corr_daily"
    dt_path = out_dir / f"{iso}_Dt_daily.csv"
    sr_path = out_dir / f"{iso}_standardized_residuals_daily.csv"
    eigen_path = out_dir / f"{iso}_Sigma_daily_eigenvalues.csv"

    out: dict[str, object] = {"iso": iso}

    if dt_path.exists():
        dt = pd.read_csv(dt_path)
        cols = [c for c in dt.columns if c.lower() != "date"]
        out["dt_cols"] = int(len(cols))
        out["dt_rows"] = int(len(dt))
        out["dt_missing_frac_mean"] = float(dt[cols].isna().mean().mean()) if cols else np.nan
    else:
        out["dt_cols"] = 0
        out["dt_rows"] = 0
        out["dt_missing_frac_mean"] = np.nan

    if sr_path.exists():
        sr = pd.read_csv(sr_path)
        cols = [c for c in sr.columns if c.lower() != "date"]
        out["sr_cols"] = int(len(cols))
        out["sr_rows"] = int(len(sr))
        # Residual correlation snapshot (pairwise complete, last ~250 obs)
        if cols:
            tail = sr[cols].tail(250)
            corr = tail.corr().abs()
            np.fill_diagonal(corr.values, np.nan)
            vals = corr.values.reshape(-1)
            vals = vals[~np.isnan(vals)]
            out["sr_abs_corr_median"] = float(np.median(vals)) if len(vals) else np.nan
            out["sr_abs_corr_p95"] = float(np.quantile(vals, 0.95)) if len(vals) else np.nan
        else:
            out["sr_abs_corr_median"] = np.nan
            out["sr_abs_corr_p95"] = np.nan
    else:
        out["sr_cols"] = 0
        out["sr_rows"] = 0
        out["sr_abs_corr_median"] = np.nan
        out["sr_abs_corr_p95"] = np.nan

    if eigen_path.exists():
        eig = pd.read_csv(eigen_path)
        num = eig.select_dtypes(include=["number"])
        if not num.empty:
            last = num.iloc[-1]
            out["sigma_eig_min_last"] = float(last.min())
            out["sigma_eig_p01_over_time"] = float(np.quantile(num.min(axis=1), 0.01))
        else:
            out["sigma_eig_min_last"] = np.nan
            out["sigma_eig_p01_over_time"] = np.nan
    else:
        out["sigma_eig_min_last"] = np.nan
        out["sigma_eig_p01_over_time"] = np.nan

    return out


def write_report(path: Path) -> None:
    iso_rows, df_blocks = summarize_literature_manifests()

    # ISO-level table
    iso_table = pd.DataFrame(
        [
            {
                "iso": r.iso,
                "blocks_frozen": r.n_blocks_frozen,
                "blocks_empty": r.n_blocks_empty,
                "blocks_single": r.n_blocks_single,
                "blocks_mean": r.n_blocks_mean,
                "blocks_pca": r.n_blocks_pca,
                "series_post_dedupe_total": r.n_series_post_dedupe_total,
                "factors_total": r.n_factors_total,
            }
            for r in iso_rows
        ]
    )

    # Add ISO-level WARN/FAIL counts (derived from block-level grade).
    if not df_blocks.empty and "coverage_grade" in df_blocks.columns:
        grade_counts = (
            df_blocks.pivot_table(index="iso", columns="coverage_grade", values="block", aggfunc="count", fill_value=0)
            .reset_index()
        )
        # Normalize missing columns.
        for c in ["OK", "WARN", "FAIL"]:
            if c not in grade_counts.columns:
                grade_counts[c] = 0
        grade_counts = grade_counts.rename(columns={"OK": "blocks_ok", "WARN": "blocks_warn", "FAIL": "blocks_fail"})
        iso_table = iso_table.merge(grade_counts[["iso", "blocks_ok", "blocks_warn", "blocks_fail"]], on="iso", how="left")
    else:
        iso_table["blocks_ok"] = np.nan
        iso_table["blocks_warn"] = np.nan
        iso_table["blocks_fail"] = np.nan

    # Daily outputs table
    daily_rows = [summarize_daily_outputs(r.iso) for r in iso_rows]
    daily_table = pd.DataFrame(daily_rows)

    # Merge for a single top table
    top = iso_table.merge(daily_table, on="iso", how="left")

    lines: list[str] = []
    lines.append("# Post-PC Factor Counts & Residual Diagnostics")
    lines.append("")

    lines.append("## 2.1) PCA explained variance (blocks with method=pca)")
    lines.append("")
    ev = summarize_explained_variance()
    if ev.empty:
        lines.append("(no literature_pca_explained.csv found)")
    else:
        # Show per-ISO median PC1 share and median cum2 share.
        by_iso = (
            ev.groupby("iso")[["pc1_var", "cum2_var"]]
            .median(numeric_only=True)
            .reset_index()
            .rename(columns={"pc1_var": "median_pc1_var", "cum2_var": "median_cum2_var"})
        )
        lines.append("Per ISO (median across PCA blocks):")
        lines.append("")
        lines.append(_df_to_md_table(by_iso))
        lines.append("")
        lines.append("Per block (pc1/cum2 shares):")
        lines.append("")
        lines.append(_df_to_md_table(ev))
    lines.append("")
    lines.append("Generated by `analysis_scripts/report_post_pc_factors_and_residuals.py`.")
    lines.append("")

    lines.append("## 1) How many factors survive after within-block PCA?")
    lines.append("")
    lines.append("Per ISO (literature manifests) + downstream daily artifacts:")
    lines.append("")
    lines.append(_df_to_md_table(top))
    lines.append("")

    lines.append("## 2) Block-level breakdown")
    lines.append("")
    pivot = (
        df_blocks.pivot_table(
            index=["iso", "block"],
            values=["n_series_post_dedupe", "n_factors"],
            aggfunc="first",
        )
        .reset_index()
        .sort_values(["iso", "block"])
    )
    method_map = df_blocks.set_index(["iso", "block"])["method"].to_dict()
    pivot.insert(2, "method", [method_map.get((i, b), "?") for i, b in zip(pivot["iso"], pivot["block"])])

    if "coverage_grade" in df_blocks.columns:
        grade_map = df_blocks.set_index(["iso", "block"])["coverage_grade"].to_dict()
        reason_map = df_blocks.set_index(["iso", "block"])["coverage_reason"].to_dict()
        pivot.insert(3, "coverage_grade", [grade_map.get((i, b), "?") for i, b in zip(pivot["iso"], pivot["block"])])
        pivot.insert(4, "coverage_reason", [reason_map.get((i, b), "") for i, b in zip(pivot["iso"], pivot["block"])])
    lines.append(_df_to_md_table(pivot))
    lines.append("")

    lines.append("## 3) Notes on interpretation")
    lines.append("")
    lines.append("- Blocks are **not forced** to produce PCs. If a block has <2 usable series after coverage/min-obs filtering, PCA is not possible.")
    lines.append("- `method=empty` means the governed/frozen block exists but none of its series were usable in the Step-3 combined panel under current filters.")
    lines.append("- More raw series does **not** automatically imply higher explained variance for PC1; it depends on correlation structure.")
    lines.append(f"- Coverage grading in this report uses a simple heuristic: OK if post-dedupe usable series >= {MIN_OK_SERIES_PER_BLOCK}, WARN if 1–2, FAIL if 0/empty.")
    lines.append("")

    lines.append("## 4) Residual/correlation quality signals (quick)")
    lines.append("")
    lines.append("- `sr_abs_corr_median`/`p95` are absolute correlations across standardized residuals (last 250 obs). Lower is better for a pure univariate-GARCH prefilter, but not expected to be near zero in systemic stress regimes.")
    lines.append("- Sigma eigenvalues should stay positive; very small/negative minima often indicate numerical PSD issues or extreme collinearity.")
    lines.append("")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    out_path = ROOT / "reports" / "POST_PC_FACTORS_AND_RESIDUALS.md"
    write_report(out_path)
    print(f"Wrote {out_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
