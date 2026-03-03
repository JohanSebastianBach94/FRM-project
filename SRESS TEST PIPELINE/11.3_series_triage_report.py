"""11.3 - Series triage report (frequency/transform review).

Implements the "triage ladder":
- Flag candidates: repeated top-drivers in episode_diagnostics.json and/or
  repeated flat-then-spike hits.
- Confirm likely frequency mismatch using catalog.csv (frequency_label,
  median_gap_days).
- Emit a ranked CSV with recommended action (fix resampling, treat as level,
  exclude, only then winsorize).

Outputs (default) under:
  SRESS TEST PIPELINE/FHS Historical Replay Plots/<run_id>/triage/

Usage:
  python "SRESS TEST PIPELINE/11.3_series_triage_report.py" --use-latest
  python "SRESS TEST PIPELINE/11.3_series_triage_report.py" --replay-run replay_YYYYMMDD_HHMMSS
  python "SRESS TEST PIPELINE/11.3_series_triage_report.py" --replay-dir analysis_outputs/scenarios/latest/historical_replay/replay_... 
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PLOTS_BASE = ROOT / "SRESS TEST PIPELINE" / "FHS Historical Replay Plots"
SCENARIO_LATEST_REPLAY_BASE = ROOT / "analysis_outputs" / "scenarios" / "latest" / "historical_replay"
CATALOG_PATH = ROOT / "catalog.csv"


@dataclass(frozen=True)
class SeriesAgg:
    series: str
    driver_hits: int
    driver_episodes: int
    driver_blocks: int
    driver_max_abs_z: float
    driver_max_episode: str
    driver_max_block: str
    flat_hits: int
    flat_episodes: int
    flat_blocks: int
    flat_max_abs: float
    flat_max_episode: str
    flat_max_block: str


def _infer_latest_plot_run_id() -> str | None:
    if not PLOTS_BASE.exists():
        return None
    runs = [p for p in PLOTS_BASE.iterdir() if p.is_dir() and p.name.startswith("replay_")]
    if not runs:
        return None
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0].name


def _resolve_replay_dir(args: argparse.Namespace) -> Path:
    if args.replay_dir:
        p = Path(args.replay_dir)
        if not p.is_absolute():
            p = ROOT / p
        return p

    if args.replay_run:
        return SCENARIO_LATEST_REPLAY_BASE / args.replay_run

    if args.use_latest:
        # Prefer plots run-id if present; else fall back to newest replay output folder.
        run_id = _infer_latest_plot_run_id()
        if run_id:
            return SCENARIO_LATEST_REPLAY_BASE / run_id
        if not SCENARIO_LATEST_REPLAY_BASE.exists():
            raise SystemExit(f"Missing replay base: {SCENARIO_LATEST_REPLAY_BASE}")
        runs = [p for p in SCENARIO_LATEST_REPLAY_BASE.iterdir() if p.is_dir()]
        if not runs:
            raise SystemExit(f"No replay runs found under: {SCENARIO_LATEST_REPLAY_BASE}")
        runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return runs[0]

    raise SystemExit("Provide --use-latest or --replay-run or --replay-dir")


def _load_catalog(catalog_path: Path) -> pd.DataFrame:
    if not catalog_path.exists():
        return pd.DataFrame(columns=["series"])
    df = pd.read_csv(catalog_path)
    if "series" not in df.columns:
        return pd.DataFrame(columns=["series"])
    df["series"] = df["series"].astype(str).str.strip()
    return df


def _is_low_frequency(row: pd.Series) -> bool:
    freq = str(row.get("frequency_label") or "").strip().lower()
    try:
        median_gap = float(row.get("median_gap_days"))
    except Exception:
        median_gap = float("nan")

    if freq and freq not in {"daily", "business_daily", "bday"}:
        return True
    if np.isfinite(median_gap) and median_gap > 3.0:
        return True
    return False


def _recommend_action(row: pd.Series) -> str:
    # Ordered, least to most deforming.
    do_not_use = str(row.get("do_not_use") or "").strip().lower() in {"true", "1", "yes"}
    low_freq = bool(row.get("is_low_frequency"))

    if do_not_use:
        return "exclude (do_not_use=true)"

    if low_freq:
        return "fix resampling; treat as level/step (no daily returns)"

    # If still extreme but not low frequency: investigate before deforming.
    max_abs = float(row.get("max_abs_shock") or 0.0)
    if max_abs >= 30:
        return "investigate transform/data; consider exclude if artifact-prone"
    if max_abs >= 12:
        return "review; winsorize/shrink only if justified"
    return "ok / monitor"


def main() -> int:
    ap = argparse.ArgumentParser(description="11.3 Series triage report")
    ap.add_argument("--use-latest", action="store_true", help="Use latest replay run")
    ap.add_argument("--replay-run", type=str, default=None, help="Replay run id under scenarios/latest/historical_replay")
    ap.add_argument("--replay-dir", type=str, default=None, help="Path to a replay run directory")
    ap.add_argument("--out-dir", type=str, default=None, help="Output folder (default under plot bundle)")
    args = ap.parse_args()

    replay_dir = _resolve_replay_dir(args)
    episodes_dir = replay_dir / "episodes"
    if not episodes_dir.exists():
        raise SystemExit(f"Missing episodes dir: {episodes_dir}")

    replay_run_id = replay_dir.name

    # Output: prefer matching plot bundle run dir if it exists.
    default_out = PLOTS_BASE / replay_run_id / "triage"
    out_dir = Path(args.out_dir) if args.out_dir else default_out
    if not out_dir.is_absolute():
        out_dir = ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    cat = _load_catalog(CATALOG_PATH)
    cat_cols = [
        "series",
        "coverage_ratio",
        "frequency_label",
        "median_gap_days",
        "source",
        "source_group",
        "topic_keywords",
        "do_not_use",
    ]
    cat_small = cat[[c for c in cat_cols if c in cat.columns]].copy()

    # Aggregation stores
    driver_hits = defaultdict(int)
    driver_episodes = defaultdict(set)
    driver_blocks = defaultdict(set)
    driver_max = defaultdict(lambda: (0.0, "", ""))  # (max_abs_z, episode, block)

    flat_hits = defaultdict(int)
    flat_episodes = defaultdict(set)
    flat_blocks = defaultdict(set)
    flat_max = defaultdict(lambda: (0.0, "", ""))  # (max_abs, episode, block)

    episode_files = sorted(episodes_dir.glob("*/episode_diagnostics.json"))
    if not episode_files:
        raise SystemExit(f"No episode_diagnostics.json found under {episodes_dir}")

    for p in episode_files:
        episode_id = p.parent.name
        try:
            obj = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue

        for block in obj.get("flagged_blocks") or []:
            block_id = str(block.get("block_id") or "")
            for s in block.get("top_series") or []:
                series = str(s.get("series") or "").strip()
                if not series:
                    continue
                try:
                    m = float(s.get("max_abs_z"))
                except Exception:
                    m = float("nan")

                driver_hits[series] += 1
                driver_episodes[series].add(episode_id)
                if block_id:
                    driver_blocks[series].add(block_id)

                cur_m, _, _ = driver_max[series]
                if np.isfinite(m) and m > cur_m:
                    driver_max[series] = (m, episode_id, block_id)

        fts = obj.get("flat_then_spike") or {}
        for hit in fts.get("hits") or []:
            series = str(hit.get("series") or "").strip()
            block_id = str(hit.get("block_id") or "")
            if not series:
                continue
            metrics = hit.get("metrics") or {}
            try:
                m = float(metrics.get("max_abs"))
            except Exception:
                m = float("nan")

            flat_hits[series] += 1
            flat_episodes[series].add(episode_id)
            if block_id:
                flat_blocks[series].add(block_id)

            cur_m, _, _ = flat_max[series]
            if np.isfinite(m) and m > cur_m:
                flat_max[series] = (m, episode_id, block_id)

    all_series = sorted(set(driver_hits.keys()) | set(flat_hits.keys()))
    rows: list[SeriesAgg] = []
    for series in all_series:
        dmax, dep, dblock = driver_max[series]
        fmax, fep, fblock = flat_max[series]
        rows.append(
            SeriesAgg(
                series=series,
                driver_hits=int(driver_hits[series]),
                driver_episodes=len(driver_episodes[series]),
                driver_blocks=len(driver_blocks[series]),
                driver_max_abs_z=float(dmax),
                driver_max_episode=str(dep),
                driver_max_block=str(dblock),
                flat_hits=int(flat_hits[series]),
                flat_episodes=len(flat_episodes[series]),
                flat_blocks=len(flat_blocks[series]),
                flat_max_abs=float(fmax),
                flat_max_episode=str(fep),
                flat_max_block=str(fblock),
            )
        )

    out = pd.DataFrame([r.__dict__ for r in rows])
    out["max_abs_shock"] = out[["driver_max_abs_z", "flat_max_abs"]].max(axis=1)
    out["episodes_total"] = out[["driver_episodes", "flat_episodes"]].max(axis=1)
    out["hits_total"] = out[["driver_hits", "flat_hits"]].sum(axis=1)

    # Score: extreme magnitude × recurrence.
    out["triage_score"] = out["max_abs_shock"].fillna(0.0) * np.log1p(out["hits_total"].clip(lower=0))

    merged = out.merge(cat_small, how="left", on="series")

    merged["frequency_label"] = merged.get("frequency_label").astype(str)
    try:
        merged["median_gap_days"] = pd.to_numeric(merged.get("median_gap_days"), errors="coerce")
    except Exception:
        merged["median_gap_days"] = np.nan

    merged["is_low_frequency"] = merged.apply(_is_low_frequency, axis=1)
    merged["recommendation"] = merged.apply(_recommend_action, axis=1)

    merged = merged.sort_values(["triage_score", "max_abs_shock"], ascending=False)

    csv_path = out_dir / "series_triage.csv"
    merged.to_csv(csv_path, index=False)

    # Small markdown summary
    md_lines = []
    md_lines.append(f"# Series triage report\n")
    md_lines.append(f"Replay run: `{replay_run_id}`\n")
    md_lines.append(f"Episodes scanned: {len(episode_files)}\n")
    md_lines.append(f"Catalog join: {'OK' if CATALOG_PATH.exists() else 'missing catalog.csv'}\n")
    def _md_table(df: pd.DataFrame) -> str:
        cols = list(df.columns)
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        lines = [header, sep]
        for _, row in df.iterrows():
            vals = []
            for c in cols:
                v = row.get(c)
                if isinstance(v, float):
                    if np.isfinite(v):
                        vals.append(f"{v:.4g}")
                    else:
                        vals.append("")
                else:
                    s = "" if v is None else str(v)
                    s = s.replace("\n", " ").replace("|", "\\|")
                    vals.append(s)
            lines.append("| " + " | ".join(vals) + " |")
        return "\n".join(lines)

    md_lines.append("\n## Top 25 suspect series\n")
    top = merged.head(25)
    top_view = top[[
        "series",
        "triage_score",
        "max_abs_shock",
        "episodes_total",
        "hits_total",
        "frequency_label",
        "median_gap_days",
        "recommendation",
    ]].copy()
    md_lines.append(_md_table(top_view))
    md_lines.append("\n\nFull CSV: `series_triage.csv`\n")

    (out_dir / "series_triage.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"[OK] Wrote: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
