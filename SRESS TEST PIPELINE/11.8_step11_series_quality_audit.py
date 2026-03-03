"""11.8 - Step 11 series quality audit (stale/step-like detection).

Why
---
Some Step 11 plots (drivers, heatmaps) rank series by max(|z|). If a series is
stale/step-like (few unique values over long windows) and then updates with a
single jump, its standardized residual can show an unrealistic spike (e.g.
|z|>20). This is typically a data/frequency alignment artifact (forward-fill,
monthly series on business-day grid), not a genuine daily shock.

This script audits *all* series in a replay run:
- Computes stale/step metrics for each series in each episode/block z_shock file.
- Cross-checks Step 11.1 diagnostics flat-then-spike hits.
- Optionally inspects the underlying DCC block standardized_residuals.csv for
  flagged series to see if the flatness is already present upstream.

It does NOT modify data. It writes reports that guide governance and fixes.

Usage
-----
python "SRESS TEST PIPELINE/11.8_step11_series_quality_audit.py" --run-dir <replay_run_dir>

Outputs
-------
- <run-dir>/series_quality_audit.csv
- <run-dir>/series_quality_audit_summary.md

"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DCC_BLOCKS_DIR = ROOT / "DCC GARCH MODEL" / "results" / "blocks"


def _read_csv_time_indexed(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    return df


def _safe_float(x: object) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _load_episode_diagnostics(ep_dir: Path) -> dict:
    p = ep_dir / "episode_diagnostics.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _flat_spike_hits(diag: dict) -> set[tuple[str, str]]:
    hits = ((diag or {}).get("flat_then_spike") or {}).get("hits")
    out: set[tuple[str, str]] = set()
    if not isinstance(hits, list):
        return out
    for h in hits:
        if not isinstance(h, dict):
            continue
        block_id = str(h.get("block_id") or "").strip()
        series = str(h.get("series") or "").strip()
        if block_id and series:
            out.add((block_id, series))
    return out


@dataclass(frozen=True)
class SeriesMetrics:
    n: int
    nunique: int
    unique_ratio: float
    consec_same_ratio: float
    max_abs: float
    p95_abs: float
    p50_abs: float
    near_zero_frac: float


def _series_metrics(s: pd.Series, *, round_decimals: int, near0_eps: float) -> SeriesMetrics | None:
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return None

    dec = int(round_decimals)
    xr = x.round(dec)
    n = int(len(xr))
    nunique = int(xr.nunique())
    unique_ratio = float(nunique) / float(n) if n else 0.0

    # consecutive sameness (after rounding)
    if n <= 1:
        consec_same = 0.0
    else:
        consec_same = float((xr.diff().fillna(0.0) == 0.0).mean())

    abs_x = x.abs()
    max_abs = float(abs_x.max())
    p95_abs = float(abs_x.quantile(0.95))
    p50_abs = float(abs_x.quantile(0.50))
    eps = float(near0_eps)
    near0 = float((abs_x <= eps).mean())

    return SeriesMetrics(
        n=n,
        nunique=nunique,
        unique_ratio=unique_ratio,
        consec_same_ratio=consec_same,
        max_abs=max_abs,
        p95_abs=p95_abs,
        p50_abs=p50_abs,
        near_zero_frac=near0,
    )


def _read_dcc_standardized_residuals_column(
    block_id: str,
    series: str,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.Series | None:
    path = DCC_BLOCKS_DIR / block_id / "standardized_residuals.csv"
    if not path.exists():
        return None

    # Fast-path: only read index + one column.
    try:
        header = pd.read_csv(path, nrows=0)
        cols = header.columns.tolist()
        if not cols:
            return None
        index_col = cols[0]
        if series not in set(cols):
            return None

        usecols = [index_col, series]
        df = pd.read_csv(path, usecols=usecols)
        df.rename(columns={index_col: "Date"}, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df = df.loc[(df.index >= start) & (df.index <= end)]
        if df.empty:
            return None
        return df[series]
    except Exception:
        try:
            df = _read_csv_time_indexed(path)
            if series not in df.columns:
                return None
            df = df.loc[(df.index >= start) & (df.index <= end)]
            if df.empty:
                return None
            return df[series]
        except Exception:
            return None


def main() -> int:
    ap = argparse.ArgumentParser(description="11.8 Step 11 series quality audit")
    ap.add_argument("--run-dir", required=True, help="Replay run dir (Step 11.1 output)")
    ap.add_argument("--episodes", default="", help="Optional comma-separated episode ids")

    ap.add_argument("--round-decimals", type=int, default=6, help="Rounding for uniqueness metrics")
    ap.add_argument("--near0-eps", type=float, default=1e-6, help="Near-zero epsilon")

    ap.add_argument("--stale-unique-ratio-threshold", type=float, default=0.15)
    ap.add_argument("--stale-consec-same-threshold", type=float, default=0.85)
    ap.add_argument("--spike-z-threshold", type=float, default=8.0)

    ap.add_argument(
        "--inspect-dcc-for-flagged",
        action="store_true",
        default=True,
        help="For flagged series, also compute the same metrics on upstream DCC standardized residuals (default: on).",
    )
    ap.add_argument("--skip-dcc-inspection", action="store_true", help="Disable DCC inspection")

    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.is_absolute():
        run_dir = ROOT / run_dir
    if not run_dir.exists():
        raise SystemExit(f"Run dir not found: {run_dir}")

    eps_base = run_dir / "episodes"
    if not eps_base.exists():
        raise SystemExit(f"Missing episodes folder: {eps_base}")

    wanted_eps = None
    if str(args.episodes).strip():
        wanted_eps = {x.strip() for x in str(args.episodes).split(",") if x.strip()}

    round_decimals = int(args.round_decimals)
    near0_eps = float(args.near0_eps)
    stale_uniq_thr = float(args.stale_unique_ratio_threshold)
    stale_consec_thr = float(args.stale_consec_same_threshold)
    spike_thr = float(args.spike_z_threshold)

    inspect_dcc = bool(args.inspect_dcc_for_flagged) and (not bool(args.skip_dcc_inspection))

    rows: list[dict[str, Any]] = []

    for ep_dir in sorted([p for p in eps_base.iterdir() if p.is_dir()], key=lambda p: p.name):
        ep_id = ep_dir.name
        if wanted_eps is not None and ep_id not in wanted_eps:
            continue

        z_dir = ep_dir / "block_z_shocks"
        if not z_dir.exists():
            continue

        diag = _load_episode_diagnostics(ep_dir)
        flat_hits = _flat_spike_hits(diag)

        # Episode window for optional upstream check.
        ep_window = (diag or {}).get("episode_window") or {}
        try:
            ep_start = pd.Timestamp(ep_window.get("start"))
            ep_end = pd.Timestamp(ep_window.get("end"))
        except Exception:
            ep_start = pd.Timestamp.min
            ep_end = pd.Timestamp.max

        for z_path in sorted(z_dir.glob("*.csv")):
            block_id = z_path.stem
            try:
                z = _read_csv_time_indexed(z_path)
            except Exception:
                continue
            if z.empty:
                continue

            for series in z.columns.astype(str):
                m = _series_metrics(z[series], round_decimals=round_decimals, near0_eps=near0_eps)
                if m is None:
                    continue

                stale_flag = bool((m.unique_ratio < stale_uniq_thr) and (m.consec_same_ratio > stale_consec_thr))
                spike_flag = bool(m.max_abs >= spike_thr)
                stale_spike_flag = bool(stale_flag and spike_flag)
                flat_spike_flag = bool((block_id, str(series)) in flat_hits)

                row: dict[str, Any] = {
                    "episode_id": ep_id,
                    "block_id": block_id,
                    "series": str(series),
                    "n": m.n,
                    "nunique": m.nunique,
                    "unique_ratio": m.unique_ratio,
                    "consec_same_ratio": m.consec_same_ratio,
                    "max_abs_z": m.max_abs,
                    "p95_abs_z": m.p95_abs,
                    "p50_abs_z": m.p50_abs,
                    "near_zero_frac": m.near_zero_frac,
                    "flag_stale": stale_flag,
                    "flag_spike": spike_flag,
                    "flag_stale_spike": stale_spike_flag,
                    "flag_flat_then_spike_step11_1": flat_spike_flag,
                    "dcc_checked": False,
                    "dcc_unique_ratio": np.nan,
                    "dcc_consec_same_ratio": np.nan,
                    "dcc_max_abs": np.nan,
                    "dcc_p95_abs": np.nan,
                }

                if inspect_dcc and (stale_flag or flat_spike_flag or stale_spike_flag):
                    src = _read_dcc_standardized_residuals_column(
                        block_id,
                        str(series),
                        start=ep_start,
                        end=ep_end,
                    )
                    if src is not None:
                        sm = _series_metrics(src, round_decimals=round_decimals, near0_eps=near0_eps)
                        if sm is not None:
                            row["dcc_checked"] = True
                            row["dcc_unique_ratio"] = sm.unique_ratio
                            row["dcc_consec_same_ratio"] = sm.consec_same_ratio
                            row["dcc_max_abs"] = sm.max_abs
                            row["dcc_p95_abs"] = sm.p95_abs

                rows.append(row)

    out_csv = run_dir / "series_quality_audit.csv"
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No data collected (empty audit).")
    df.to_csv(out_csv, index=False)

    # Summary markdown
    md_lines: list[str] = []
    md_lines.append("# Step 11 Series Quality Audit")
    md_lines.append("")
    md_lines.append(f"Run: `{run_dir}`")
    md_lines.append(f"Rows: {int(df.shape[0])}")
    md_lines.append("")

    def summarize(mask: pd.Series, title: str) -> None:
        sub = df.loc[mask].copy()
        md_lines.append(f"## {title}")
        md_lines.append("")
        md_lines.append(f"Count: {int(sub.shape[0])}")
        if sub.empty:
            md_lines.append("(none)")
            md_lines.append("")
            return
        # top by max_abs_z
        show = sub.sort_values("max_abs_z", ascending=False).head(30)
        for _, r in show.iterrows():
            md_lines.append(
                f"- {r['episode_id']} | {r['block_id']} | {r['series']} | "
                f"max|z|={float(r['max_abs_z']):.3f} | uniq={float(r['unique_ratio']):.3f} | consec_same={float(r['consec_same_ratio']):.3f} | "
                f"flat_then_spike_step11_1={bool(r['flag_flat_then_spike_step11_1'])} | dcc_checked={bool(r['dcc_checked'])}"
            )
        md_lines.append("")

    summarize(df["flag_flat_then_spike_step11_1"] == True, "Step 11.1 flat-then-spike hits")
    summarize(df["flag_stale_spike"] == True, "Stale + spike (likely forward-fill / frequency artifact)")
    summarize(df["flag_stale"] == True, "Stale/step-like (even if not spiking)")

    # Episode rollups
    md_lines.append("## Episode rollup")
    md_lines.append("")
    grp = df.groupby("episode_id").agg(
        n_rows=("series", "count"),
        n_stale=("flag_stale", "sum"),
        n_stale_spike=("flag_stale_spike", "sum"),
        max_abs_z=("max_abs_z", "max"),
    )
    for ep_id, r in grp.sort_index().iterrows():
        md_lines.append(
            f"- {ep_id}: series={int(r['n_rows'])}, stale={int(r['n_stale'])}, stale+spike={int(r['n_stale_spike'])}, max|z|={float(r['max_abs_z']):.3f}"
        )
    md_lines.append("")

    md_lines.append("## Interpretation")
    md_lines.append("")
    md_lines.append(
        "If a series is step-like (low unique_ratio, high consec_same_ratio), treating it as a daily shock driver will produce unreliable max(|z|) rankings. "
        "Fix options: (1) exclude such series from plots (driver/heatmap) but keep raw z for audit; (2) remove or frequency-adjust them upstream; "
        "(3) treat them as low-frequency macro controls rather than daily market drivers."
    )
    md_lines.append("")

    out_md = run_dir / "series_quality_audit_summary.md"
    out_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    print(f"[OK] Wrote: {out_csv}")
    print(f"[OK] Wrote: {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
