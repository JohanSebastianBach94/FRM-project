"""11.7 - Literature alignment check for historical replay plots.

Goal
----
Step 11.2 produces a plot bundle (severity lines, top-driver charts, heatmaps).
This script audits whether the *drivers* implied by Step 11.1 replay z-shocks
look consistent with stylized facts from the crisis literature (GFC, Eurozone,
Covid).

Important: This does NOT read PNG pixels. It reads the underlying z-shock CSVs
and enriches series with catalog metadata.

Usage
-----
python "SRESS TEST PIPELINE/11.7_literature_alignment_check.py" --run-dir <replay_run_dir>

Outputs
-------
Writes <run-dir>/step11_literature_alignment.md

Heuristics
----------
- We score the top drivers by max(|z|) across the episode window.
- We enrich each series with (instrument, country_code, coverage_ratio, do_not_use).
- We provide a coarse keyword-based alignment score per episode.

This is intentionally lightweight: it is an *audit aid*, not a statistical test.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "catalog.csv"


def _read_csv_time_indexed(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    return df


def _safe_str(x: object) -> str:
    try:
        return "" if x is None else str(x)
    except Exception:
        return ""


def _safe_float(x: object) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _load_catalog(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    if df.empty:
        return df
    if "series" not in df.columns:
        return pd.DataFrame()
    df["series"] = df["series"].astype(str).str.strip()
    return df


def _catalog_lookup(catalog: pd.DataFrame, series_codes: list[str]) -> pd.DataFrame:
    if catalog.empty:
        return pd.DataFrame({"series": series_codes})
    sub = catalog[catalog["series"].isin(series_codes)].copy()
    missing = [s for s in series_codes if s not in set(sub["series"].tolist())]
    if missing:
        sub = pd.concat([sub, pd.DataFrame({"series": missing})], ignore_index=True)
    # Keep original order (series_codes)
    sub["_order"] = sub["series"].map({s: i for i, s in enumerate(series_codes)})
    sub = sub.sort_values("_order", kind="stable").drop(columns=["_order"], errors="ignore")
    return sub


@dataclass(frozen=True)
class EpisodeSpec:
    episode_id: str
    # regex keywords applied to catalog fields and series code
    expected_patterns: list[re.Pattern[str]]


def _episode_specs() -> dict[str, EpisodeSpec]:
    def pats(words: list[str]) -> list[re.Pattern[str]]:
        return [re.compile(w, re.IGNORECASE) for w in words]

    return {
        "gfc_2008": EpisodeSpec(
            episode_id="gfc_2008",
            expected_patterns=pats(
                [
                    r"vix|v2x|vol",
                    r"ted|commercial paper|cp|funding|euribor",
                    r"spread|oasis|oasd|cds|sovereign|btp|bonos|oat",
                    r"equity|bank",
                    r"mortgage|mbs|housing|hpi",
                ]
            ),
        ),
        "eurozone_2011": EpisodeSpec(
            episode_id="eurozone_2011",
            expected_patterns=pats(
                [
                    r"spread|btp|bonos|oat|periphery|sovereign|cds",
                    r"eurusd|eur_|fx|cross",
                    r"bank|equity",
                    r"v2x|vix|vol",
                    r"ecb|assets|policy|rate",
                ]
            ),
        ),
        "covid_2020": EpisodeSpec(
            episode_id="covid_2020",
            expected_patterns=pats(
                [
                    r"vix|v2x|vol",
                    r"spread|oasd|cds|commercial paper|ted|funding",
                    r"oil|brent|wti|commodity",
                    r"equity|bank",
                    r"policy|rate|fed|ecb|liquidity|assets",
                ]
            ),
        ),
    }


def _series_alignment_hits(row: pd.Series, patterns: list[re.Pattern[str]]) -> int:
    fields = []
    for col in [
        "series",
        "entity",
        "instrument",
        "topic_keywords",
        "extra_keywords",
        "source_group",
        "source_detail",
    ]:
        if col in row.index:
            fields.append(_safe_str(row.get(col)))
    blob = " | ".join([f for f in fields if f])
    if not blob:
        return 0
    return sum(1 for p in patterns if p.search(blob) is not None)


def _top_drivers_episode(z_dir: Path, *, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (block_severity, series_rank)

    series_rank includes where the max occurred: date and block id.
    """
    rows_series: dict[str, dict[str, Any]] = {}
    rows_block: list[dict[str, Any]] = []

    for z_path in sorted(z_dir.glob("*.csv")):
        block_id = z_path.stem
        z = _read_csv_time_indexed(z_path)
        if z.empty:
            continue

        # Block severity: max over all series and all times
        block_max = float(z.abs().to_numpy().max())
        rows_block.append({"block_id": block_id, "max_abs_z": block_max, "n_series": int(z.shape[1]), "n_obs": int(z.shape[0])})

        # Series ranking: max over time, then max across blocks
        for series_name in z.columns.astype(str):
            s = str(series_name)
            col = z[series_name]
            if col.empty:
                continue
            abs_col = col.abs()
            # idxmax returns first max; that is fine for audit
            try:
                peak_date = abs_col.idxmax()
                peak_val = float(col.loc[peak_date])
                peak_abs = float(abs_col.loc[peak_date])
            except Exception:
                continue

            prev = rows_series.get(s)
            if prev is None or peak_abs > float(prev["max_abs_z"]):
                rows_series[s] = {
                    "series": s,
                    "max_abs_z": peak_abs,
                    "peak_z": peak_val,
                    "peak_date": str(pd.Timestamp(peak_date).date()) if pd.notna(peak_date) else "",
                    "peak_block": block_id,
                }

    block_df = pd.DataFrame(rows_block).sort_values("max_abs_z", ascending=False, kind="stable")
    series_df = pd.DataFrame(list(rows_series.values()))
    if not series_df.empty:
        series_df = series_df.sort_values("max_abs_z", ascending=False, kind="stable").head(int(top_n))
    return block_df, series_df


def main() -> int:
    ap = argparse.ArgumentParser(description="11.7 Literature alignment check")
    ap.add_argument("--run-dir", required=True, help="Path to replay run directory (e.g. .../historical_replay/replay_YYYYMMDD_HHMMSS)")
    ap.add_argument("--top-series", type=int, default=30, help="How many top series to summarize per episode")
    ap.add_argument("--top-blocks", type=int, default=15, help="How many top blocks to summarize per episode")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"Run dir not found: {run_dir}")

    episodes_dir = run_dir / "episodes"
    if not episodes_dir.exists():
        raise SystemExit(f"Missing episodes dir: {episodes_dir}")

    catalog = _load_catalog(CATALOG_PATH)
    specs = _episode_specs()

    lines: list[str] = []
    lines.append("# Step 11 Literature Alignment Check")
    lines.append("")
    lines.append(f"Run: `{run_dir}`")
    lines.append(f"Catalog: `{CATALOG_PATH}` ({'OK' if CATALOG_PATH.exists() else 'MISSING'})")
    lines.append("")

    filter_report_path = run_dir / "block_series_filtering.json"
    if filter_report_path.exists():
        lines.append(f"Series filtering report: `{filter_report_path}`")
    else:
        lines.append("Series filtering report: MISSING (Step 11.1 may be unfiltered)")
    lines.append("")

    ep_dirs = [p for p in sorted(episodes_dir.iterdir()) if p.is_dir()]
    if not ep_dirs:
        raise SystemExit("No episodes found")

    for ep_dir in ep_dirs:
        ep_id = ep_dir.name
        z_dir = ep_dir / "block_z_shocks"
        if not z_dir.exists():
            continue

        block_df, top_series = _top_drivers_episode(z_dir, top_n=int(args.top_series))
        lines.append(f"## {ep_id}")
        lines.append("")

        # Episode-level quick read
        global_max = float(top_series["max_abs_z"].max()) if not top_series.empty else float("nan")
        lines.append(f"- Top-series max(|z|) among listed: {global_max:.3f}" if np.isfinite(global_max) else "- Top-series max(|z|): n/a")
        lines.append(f"- Blocks present: {int(block_df.shape[0])}")
        lines.append("")

        # Top blocks table
        lines.append(f"Top blocks by severity (max|z|), top {int(args.top_blocks)}:")
        if block_df.empty:
            lines.append("(none)")
        else:
            show = block_df.head(int(args.top_blocks))
            for _, r in show.iterrows():
                lines.append(f"- {r['block_id']}: max|z|={float(r['max_abs_z']):.3f} (series={int(r['n_series'])}, obs={int(r['n_obs'])})")
        lines.append("")

        # Top series with metadata
        lines.append(f"Top series across blocks, top {int(args.top_series)}:")
        if top_series.empty:
            lines.append("(none)")
            lines.append("")
            continue

        meta = _catalog_lookup(catalog, top_series["series"].astype(str).tolist())
        merged = pd.merge(top_series, meta, on="series", how="left", suffixes=("", "_cat"))

        # Alignment scoring
        spec = specs.get(ep_id)
        if spec is not None:
            hits = merged.apply(lambda row: _series_alignment_hits(row, spec.expected_patterns), axis=1)
            merged["alignment_hits"] = hits
            frac = float((hits > 0).mean()) if len(hits) else float("nan")
            lines.append(f"- Alignment heuristic: {100.0*frac:.1f}% of top series matched ≥1 expected theme")
        else:
            lines.append("- Alignment heuristic: (no spec for this episode id)")
        lines.append("")

        for _, r in merged.iterrows():
            series = _safe_str(r.get("series"))
            mx = _safe_float(r.get("max_abs_z"))
            peak_z = _safe_float(r.get("peak_z"))
            peak_date = _safe_str(r.get("peak_date"))
            peak_block = _safe_str(r.get("peak_block"))
            inst = _safe_str(r.get("instrument"))
            ctry = _safe_str(r.get("country_code"))
            cov = _safe_float(r.get("coverage_ratio"))
            dnu = _safe_str(r.get("do_not_use"))
            ent = _safe_str(r.get("entity"))
            hit = r.get("alignment_hits") if "alignment_hits" in r.index else None

            bits = [f"{series}: max|z|={mx:.3f}" if mx is not None else f"{series}: max|z|=?"]
            if ent:
                bits.append(ent)
            if inst:
                bits.append(f"inst={inst}")
            if ctry:
                bits.append(f"ctry={ctry}")
            if cov is not None:
                bits.append(f"cov={cov:.3f}")
            if dnu and dnu.lower() not in {"nan", "none"}:
                bits.append(f"do_not_use={dnu}")
            if peak_date:
                bits.append(f"peak={peak_z:.3f}@{peak_date}" if peak_z is not None else f"peak=?@{peak_date}")
            if peak_block:
                bits.append(f"block={peak_block}")
            if hit is not None and _safe_float(hit) is not None:
                bits.append(f"hits={int(hit)}")
            lines.append("- " + " | ".join(bits))

        lines.append("")

    out_path = run_dir / "step11_literature_alignment.md"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[OK] Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
