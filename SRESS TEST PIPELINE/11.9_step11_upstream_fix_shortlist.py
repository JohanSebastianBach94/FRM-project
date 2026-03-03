"""11.9 - Prioritized upstream fix shortlist for Step 11.

Reads the full series-quality audit (Step 11.8) and produces a prioritized list
of series that are:
- flagged as stale/step-like, AND
- frequently appear as top drivers within their (episode, block)
  (top-K by max_abs_z).

The goal is to focus upstream data fixes (frequency alignment, ffill policy,
source choice) on the small set of series that distort driver narratives.

Usage
-----
python "SRESS TEST PIPELINE/11.9_step11_upstream_fix_shortlist.py" --run-dir <replay_run_dir>

Outputs
-------
- <run-dir>/upstream_fix_shortlist.csv
- <run-dir>/upstream_fix_shortlist.md

"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser(description="11.9 Upstream fix shortlist")
    ap.add_argument("--run-dir", required=True, help="Replay run dir")
    ap.add_argument("--top-k", type=int, default=5, help="Top-K per (episode, block) used to define drivers")
    ap.add_argument(
        "--include-nonstale",
        action="store_true",
        help="Also include non-stale series (not recommended for fix shortlist).",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.is_absolute():
        run_dir = ROOT / run_dir
    if not run_dir.exists():
        raise SystemExit(f"Run dir not found: {run_dir}")

    audit_path = run_dir / "series_quality_audit.csv"
    if not audit_path.exists():
        raise SystemExit(f"Missing audit CSV (run Step 11.8 first): {audit_path}")

    df = pd.read_csv(audit_path)
    if df.empty:
        raise SystemExit("Empty audit CSV")

    # Normalize types
    for col in [
        "episode_id",
        "block_id",
        "series",
    ]:
        df[col] = df[col].astype(str)

    df["max_abs_z"] = pd.to_numeric(df.get("max_abs_z"), errors="coerce")
    if "p95_abs_z" in df.columns:
        df["p95_abs_z"] = pd.to_numeric(df.get("p95_abs_z"), errors="coerce")
    if "p50_abs_z" in df.columns:
        df["p50_abs_z"] = pd.to_numeric(df.get("p50_abs_z"), errors="coerce")
    df["flag_stale"] = df.get("flag_stale").astype(bool)
    df["flag_stale_spike"] = df.get("flag_stale_spike").astype(bool)

    # Define "driver occurrences": top-K by max_abs_z within each (episode, block)
    top_k = int(args.top_k)
    df2 = df.dropna(subset=["max_abs_z"]).copy()
    df2 = df2.sort_values(["episode_id", "block_id", "max_abs_z"], ascending=[True, True, False], kind="stable")

    # rank within group
    df2["rank_in_block"] = df2.groupby(["episode_id", "block_id"]).cumcount() + 1
    drivers = df2.loc[df2["rank_in_block"] <= top_k].copy()

    if not bool(args.include_nonstale):
        drivers = drivers.loc[drivers["flag_stale"] == True].copy()

    if drivers.empty:
        out_md = run_dir / "upstream_fix_shortlist.md"
        out_md.write_text("# Upstream fix shortlist\n\n(none)\n", encoding="utf-8")
        print(f"[OK] Wrote: {out_md}")
        return 0

    # Aggregate across episodes/blocks
    agg = drivers.groupby("series").agg(
        driver_occurrences=("series", "count"),
        episodes_covered=("episode_id", lambda x: ",".join(sorted(set(map(str, x))))),
        blocks_covered=("block_id", lambda x: ",".join(sorted(set(map(str, x)))[:12]) + ("" if len(set(x)) <= 12 else ",…")),
        max_abs_z=("max_abs_z", "max"),
        p95_abs_z=("p95_abs_z", "max") if "p95_abs_z" in drivers.columns else ("max_abs_z", "median"),
        p50_abs_z=("p50_abs_z", "max") if "p50_abs_z" in drivers.columns else ("max_abs_z", "median"),
        stale_spike_hits=("flag_stale_spike", "sum"),
        mean_unique_ratio=("unique_ratio", "mean"),
        mean_consec_same=("consec_same_ratio", "mean"),
        dcc_checked_share=("dcc_checked", lambda x: float(np.mean(pd.Series(x).astype(bool))) if len(x) else 0.0),
    ).reset_index()

    # Prioritize: frequent + large + spike-y
    agg = agg.sort_values(
        ["driver_occurrences", "stale_spike_hits", "max_abs_z"],
        ascending=[False, False, False],
        kind="stable",
    )

    out_csv = run_dir / "upstream_fix_shortlist.csv"
    agg.to_csv(out_csv, index=False)

    # Markdown summary
    out_md = run_dir / "upstream_fix_shortlist.md"
    lines: list[str] = []
    lines.append("# Step 11 Upstream Fix Shortlist")
    lines.append("")
    lines.append(f"Run: `{run_dir}`")
    lines.append(f"Definition: series is shortlisted if stale and appears in top-{top_k} by max(|z|) within an episode×block")
    lines.append("")

    lines.append("## Top candidates")
    lines.append("")
    show = agg.head(30)
    for _, r in show.iterrows():
        lines.append(
            "- "
            + " | ".join(
                [
                    str(r["series"]),
                    f"occ={int(r['driver_occurrences'])}",
                    f"stale+spike_hits={int(r['stale_spike_hits'])}",
                    f"max|z|={float(r['max_abs_z']):.3f}",
                    f"p95|z|={float(r['p95_abs_z']):.3f}" if ("p95_abs_z" in agg.columns and np.isfinite(r["p95_abs_z"])) else "p95|z|=n/a",
                    f"p50|z|={float(r['p50_abs_z']):.3f}" if ("p50_abs_z" in agg.columns and np.isfinite(r["p50_abs_z"])) else "p50|z|=n/a",
                    f"uniq≈{float(r['mean_unique_ratio']):.3f}" if np.isfinite(r["mean_unique_ratio"]) else "uniq≈n/a",
                    f"consec_same≈{float(r['mean_consec_same']):.3f}" if np.isfinite(r["mean_consec_same"]) else "consec_same≈n/a",
                    f"episodes={r['episodes_covered']}",
                ]
            )
        )

    lines.append("")
    lines.append("## Suggested upstream actions")
    lines.append("")
    lines.append("For each shortlisted series, check upstream pipeline stages for:")
    lines.append("- Frequency mismatch (monthly/weekly series aligned to business days)")
    lines.append("- Forward-fill / back-fill policy (stale stretches)")
    lines.append("- Rounding/discretization")
    lines.append("- Source substitution (e.g., use traded proxy for market stress)")
    lines.append("")
    lines.append("If you must keep a low-frequency series, treat it as macro-control (not a daily driver) or downweight/omit it from driver plots.")

    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"[OK] Wrote: {out_csv}")
    print(f"[OK] Wrote: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
