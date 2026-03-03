"""11.2 - FHS Historical Replay Plots (literature-style).

Consumes outputs from Step 11.1 (historical replay) and produces a standard plot
bundle commonly used to communicate Filtered Historical Simulation (FHS)-style
historical replays.

Plots produced (per episode, per block unless noted):
1) Episode severity over time (z-space):
   - S_max(t) = max_i |z_{t,i}|
   - S_l2(t)  = sqrt(sum_i z_{t,i}^2)
2) Top-driver time series (z-space): top-K series by max|z|.
3) Heatmap (episode-level): max|z| per block x series (top-N series overall).
4) Unit innovations vs z-shocks: side-by-side time series for top-K drivers.
5) Distribution comparison (optional): episode |z| vs baseline |z| (ECDF + box).

Outputs are written under:
  SRESS TEST PIPELINE/FHS Historical Replay Plots/<run_id>/<episode_id>/...

Usage:
  python "SRESS TEST PIPELINE/11.2_fhs_historical_replay_plots.py" --use-latest
  python "SRESS TEST PIPELINE/11.2_fhs_historical_replay_plots.py" --run-dir analysis_outputs/scenarios/latest/historical_replay/<replay_run>

Notes:
- Requires matplotlib.
- Uses only Step 11.1 outputs + block standardized residuals for baseline.
"""

from __future__ import annotations

import argparse
import json
import math
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

import matplotlib

# Headless-safe backend (works on servers/CI too)
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


ROOT = Path(__file__).resolve().parents[1]
PLOTS_BASE = ROOT / "SRESS TEST PIPELINE" / "FHS Historical Replay Plots"

SCENARIO_LATEST_REPLAY_DIR = (
    ROOT / "analysis_outputs" / "scenarios" / "latest" / "historical_replay"
)
STANDALONE_REPLAY_DIR = ROOT / "analysis_outputs" / "historical_replay"
DCC_BLOCKS_DIR = ROOT / "DCC GARCH MODEL" / "results" / "blocks"


_BLOCK_ID_RE = re.compile(r"[^a-z0-9]+")


def _bundle_readme_text(*, run_dir: Path, out_root: Path) -> str:
    return "\n".join(
        [
            "# FHS Historical Replay Plots — Guide",
            "",
            "This folder contains a ‘literature-style’ plot bundle for the Step 11.1 historical replay run.",
            "",
            f"- Source run dir: `{run_dir.as_posix()}`",
            f"- Output root: `{out_root.as_posix()}`",
            "",
            "## Folder map (per episode)",
            "Each episode subfolder contains the same plot categories:",
            "",
            "- `severity/`",
            "- `drivers/`",
            "- `z_vs_innov/`",
            "- `correlations/`",
            "- `distributions/`",
            "- `heatmaps/`",
            "- `plot_exclusions.json` (if present)",
            "",
            "## What each category means",
            "### Severity (folder: `severity/`)",
            "What it shows: how ‘big’ the episode is through time in each block, using sigmas (z; standardized residuals).",
            "",
            "Two severity measures are plotted:",
            "- $S_{max}(t)=\\max_i |z_{t,i}|$ (single-largest driver at time $t$)",
            "- $S_{L2}(t)=\\sqrt{\\sum_i z_{t,i}^2}$ (broad stress energy at time $t$)",
            "",
            "Why it’s useful: best first plot to rank ‘what blew up’ and when; it distinguishes broad stress (high $S_{L2}$) vs single-driver spikes (high $S_{max}$ only).",
            "",
            "### Drivers (folder: `drivers/`)",
            "What it shows: time series of the top-K driver series (by peak $|z|$) within each block during the episode.",
            "Note: if a driver is a release-date masked (low-frequency) series, plots may display it as **cumulated shocks (level proxy; step/LOCF for low-frequency)** to avoid misleading 1-day release spikes (plot-only; does not change replay outputs). Use `--lowfreq-display-mode raw` to force **impulses (z-shocks)**.",
            "",
            "Why it’s useful: answers ‘which risk drivers explain the severity plot,’ and whether it’s rates/credit/equity/FX/commodities doing the work.",
            "",
            "### Z vs innov (folder: `z_vs_innov/`)",
            "What it shows: side-by-side for top drivers: (a) sigmas (z) and (b) ‘unit innovations / replayed residuals’ (the replay output series).",
            "Note: release-date masked (low-frequency) series can be displayed as **cumulated shocks (level proxy; step/LOCF for low-frequency)** to avoid spike-only visuals; use `--lowfreq-display-mode raw` to force **impulses (z-shocks)**.",
            "",
            "Why it’s useful: great for diagnosing ‘fake stress’ from data artifacts. If z spikes but innovations are weird/step-like, you likely have a stale/step series issue.",
            "",
            "### Correlations (folder: `correlations/`)",
            "What it shows: correlation heatmap of sigmas (z) among the top ~20 series in that block during the episode window.",
            "",
            "Why it’s useful: tells you if stress is a coherent regime move (many series moving together) vs fragmented. Economically, coherent correlation is what makes an episode feel like ‘a macro regime’ rather than noise.",
            "",
            "### Distributions (folder: `distributions/`)",
            "What it shows: compares the episode’s $|z|$ distribution vs a baseline $|z|$ distribution from pre-episode history (ECDF + box comparison).",
            "",
            "Why it’s useful: quantifies tail-thickening. If the episode ECDF shifts right vs baseline, you’re seeing a real stress regime, not just one-off spikes.",
            "",
            "### Heatmaps (folder: `heatmaps/`)",
            "What it shows: an episode-level matrix of max$|z|$ with rows=blocks and columns=series (top-N series overall).",
            "",
            "Why it’s useful: fastest ‘system map’ of which block/series combinations are responsible for episode severity.",
            "",
            "### Plot exclusions (file: `plot_exclusions.json`)",
            "What it is: audit trail of series excluded from driver selection/heatmaps due to ‘flat-then-spike’ or ‘stale-spike’ flags.",
            "",
            "Why it’s useful: if a series disappears from drivers/heatmaps (and per-block plots), this explains it (robustness guard; not a silent drop).",
            "",
            "Note: per-block plots in `severity/`, `drivers/`, `z_vs_innov/`, `correlations/` are generated using the filtered z-shocks (i.e., with excluded series removed when possible).",
            "",
            "## Economic interpretation cheat sheet (episodes)",
            "- **gfc_2008**: banking/systemic stress + broad risk repricing. Prioritize `severity/`, `heatmaps/`, `distributions/`.",
            "- **eurozone_2011**: sovereign/bank loop + cross-country propagation. Prioritize `correlations/`, `drivers/`, `heatmaps/`.",
            "- **covid_2020**: shock speed + volatility regime shift. Prioritize `severity/` (timing), `z_vs_innov/` (artifact check), `distributions/` (regime jump).",
            "",
        ]
    )


def _episode_readme_text(*, episode_id: str, run_dir: Path, episode_out_dir: Path) -> str:
    return "\n".join(
        [
            f"# Episode: {episode_id}",
            "",
            "This episode folder contains the standard FHS historical replay plot bundle categories.",
            "",
            f"- Source run dir: `{run_dir.as_posix()}`",
            f"- Episode output dir: `{episode_out_dir.as_posix()}`",
            "",
            "## Quick reading order (recommended)",
            "1) `severity/` (what blew up, when)",
            "2) `drivers/` (which series drove it)",
            "3) `heatmaps/` (system map across blocks/series)",
            "4) `correlations/` (coherence / regime structure)",
            "5) `distributions/` (tail shift vs baseline)",
            "6) `z_vs_innov/` (sanity check: artifacts vs genuine stress)",
            "",
            "## Category meanings",
            "See the run-level `README.md` in the parent folder for definitions of each category and the severity formulas.",
            "",
        ]
    )


def _slugify_block_id(iso_code: str, block_key: str | None) -> str:
    cleaned = f"{iso_code}_{block_key or 'block'}".lower()
    cleaned = _BLOCK_ID_RE.sub("_", cleaned)
    return cleaned.strip("_")


def _load_expected_block_ids(project_root: Path) -> set[str] | None:
    """Load canonical block ids from the frozen block definition, if present."""
    path = project_root / "outputs" / "country_block_definition.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict) or not payload:
        return None

    expected: set[str] = set()
    for iso, entry in payload.items():
        if not iso:
            continue
        blocks = (entry or {}).get("blocks") if isinstance(entry, dict) else None
        if not isinstance(blocks, list):
            continue
        for b in blocks:
            if not isinstance(b, dict):
                continue
            key = str(b.get("key") or "block").strip()
            expected.add(_slugify_block_id(str(iso), key))
    return expected or None


def _shorten_label(s: str, *, max_len: int) -> str:
    s = str(s)
    if len(s) <= int(max_len):
        return s
    if max_len <= 1:
        return "…"
    return "…" + s[-(int(max_len) - 1) :]


def _is_iso_prefixed_block_id(block_id: str) -> bool:
    return bool(re.match(r"^[a-z]{3}_.+", str(block_id)))


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_csv_time_indexed(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    return df


def _ecdf(values: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    x = np.asarray(values, dtype=float)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return np.array([]), np.array([])
    x = np.sort(x)
    y = np.arange(1, x.size + 1) / x.size
    return x, y


def _infer_latest_run_dir(base: Path) -> Path | None:
    if not base.exists():
        return None
    runs = [p for p in base.iterdir() if p.is_dir() and p.name.startswith("replay_")]
    if not runs:
        return None
    return sorted(runs, key=lambda p: p.name, reverse=True)[0]


def _find_run_dir(args: argparse.Namespace) -> Path:
    if args.run_dir:
        p = Path(args.run_dir)
        if not p.is_absolute():
            p = ROOT / p
        if not p.exists():
            raise SystemExit(f"Run dir not found: {p}")
        return p

    if args.use_latest:
        latest = _infer_latest_run_dir(SCENARIO_LATEST_REPLAY_DIR)
        if latest is None:
            raise SystemExit(f"No replay runs found under {SCENARIO_LATEST_REPLAY_DIR}")
        return latest

    latest = _infer_latest_run_dir(STANDALONE_REPLAY_DIR)
    if latest is None:
        raise SystemExit(f"No replay runs found under {STANDALONE_REPLAY_DIR}")
    return latest


def _parse_csv_list(text: str) -> list[str] | None:
    if not text:
        return None
    parts = [p.strip() for p in text.split(",") if p.strip()]
    return parts or None


def _iter_episode_dirs(run_dir: Path, wanted: set[str] | None) -> list[Path]:
    base = run_dir / "episodes"
    if not base.exists():
        raise SystemExit(f"Missing episodes folder under run dir: {run_dir}")
    eps = [p for p in base.iterdir() if p.is_dir()]
    if wanted is not None:
        eps = [p for p in eps if p.name in wanted]
    return sorted(eps, key=lambda p: p.name)


def _downsample_df(df: pd.DataFrame, max_points: int = 2500) -> pd.DataFrame:
    n = int(len(df))
    if n <= max_points:
        return df
    idx = np.linspace(0, n - 1, int(max_points), dtype=int)
    return df.iloc[idx]


def _apply_concise_date_axis(ax: plt.Axes) -> None:
    locator = mdates.AutoDateLocator(maxticks=10)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))


def _plot_severity(block_id: str, z: pd.DataFrame, out_path: Path) -> None:
    s_max = z.abs().max(axis=1)
    s_l2 = np.sqrt((z**2).sum(axis=1))

    sev = pd.DataFrame({"S_max": s_max, "S_l2": s_l2})
    sev = _downsample_df(sev, max_points=2500)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(sev.index, sev["S_max"].values, label=r"$S_{max}(t)=\max_i |z_{t,i}|$", lw=1.5)
    ax.plot(sev.index, sev["S_l2"].values, label=r"$S_{L2}(t)=\sqrt{\sum_i z_{t,i}^2}$", lw=1.5)
    ax.set_title(f"{block_id} - Episode severity (sigmas; z-space)")
    ax.set_ylabel("Severity")
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=9)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _load_episode_diagnostics(ep_dir: Path) -> dict:
    path = ep_dir / "episode_diagnostics.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _flat_spike_exclusions_from_diagnostics(diag: dict) -> dict[str, set[str]]:
    """Return {block_id -> set(series)} from Step 11.1 flat_then_spike hits."""
    out: dict[str, set[str]] = {}
    hits = ((diag or {}).get("flat_then_spike") or {}).get("hits")
    if not isinstance(hits, list):
        return out
    for h in hits:
        if not isinstance(h, dict):
            continue
        block_id = str(h.get("block_id") or "").strip()
        series = str(h.get("series") or "").strip()
        if not block_id or not series:
            continue
        out.setdefault(block_id, set()).add(series)
    return out


def _stale_spike_columns(
    z: pd.DataFrame,
    *,
    spike_z_threshold: float,
    unique_ratio_threshold: float,
    round_decimals: int,
) -> set[str]:
    """Detect step/stale series that also exhibit an extreme spike.

    Motivation: series that update infrequently (few unique values) can produce
    spurious enormous standardized residual spikes on the update date.
    """
    if z.empty:
        return set()

    out: set[str] = set()
    n = int(len(z))
    if n <= 0:
        return out

    thr = float(spike_z_threshold)
    uniq_thr = float(unique_ratio_threshold)
    dec = int(round_decimals)

    for c in z.columns:
        s = pd.to_numeric(z[c], errors="coerce").dropna()
        if s.empty:
            continue
        try:
            max_abs = float(s.abs().max())
        except Exception:
            continue
        if not np.isfinite(max_abs) or max_abs < thr:
            continue

        # Uniqueness after rounding is a cheap proxy for step/stale updates.
        try:
            nunique = int(s.round(dec).nunique())
        except Exception:
            continue
        unique_ratio = float(nunique) / float(len(s)) if len(s) else 0.0
        if unique_ratio < uniq_thr:
            out.add(str(c))
    return out


def _top_drivers(z: pd.DataFrame, k: int) -> list[str]:
    mx = z.abs().max().sort_values(ascending=False)
    return [c for c in mx.head(int(k)).index.tolist() if c in z.columns]


def _plot_top_drivers(
    block_id: str,
    z: pd.DataFrame,
    drivers: list[str],
    out_path: Path,
    *,
    release_date_masked_series: set[str] | None = None,
    lowfreq_display_mode: str = "cumulative",
) -> None:
    masked = set(release_date_masked_series or set())
    zz = z[drivers].copy()
    zz = _maybe_cumulative_display(zz, columns=drivers, masked=masked, mode=lowfreq_display_mode)
    zz = _downsample_df(zz, max_points=2500)
    fig, ax = plt.subplots(figsize=(10, 4))
    for c in drivers:
        use_step = (c in masked) and (str(lowfreq_display_mode).strip().lower() == "cumulative")
        if use_step:
            ax.step(zz.index, zz[c].values, where="post", label=c, lw=1.2)
        else:
            ax.plot(zz.index, zz[c].values, label=c, lw=1.2)
    if masked and str(lowfreq_display_mode).strip().lower() == "cumulative":
        ax.set_title(
            f"{block_id} - impulses (sigmas; z) + cumulated shocks (level proxy; step/LOCF for low-frequency)"
        )
        ax.set_ylabel("sigmas (z; impulses; masked series shown as cumulated shocks level proxy)")
    else:
        ax.set_title(f"{block_id} - impulses (sigmas; z)")
        ax.set_ylabel("sigmas (z; impulses)")
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _parse_masked_series_list(text: str) -> set[str]:
    if not text or not isinstance(text, str):
        return set()
    parts = [p.strip() for p in text.split(";") if p.strip()]
    return set(parts)


def _load_release_date_masked_series_map(ep_dir: Path) -> dict[str, set[str]]:
    """Return {block_id -> set(series)} for release-date masked (low-frequency) series.

    Source: Step 11.1 episode_summary.csv
    Column: release_date_masked_series (semicolon-separated)
    """
    path = ep_dir / "episode_summary.csv"
    if not path.exists():
        return {}

    try:
        df = pd.read_csv(path)
    except Exception:
        return {}

    if df.empty or "block_id" not in df.columns:
        return {}

    out: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        block_id = str(row.get("block_id") or "").strip()
        if not block_id:
            continue
        masked = _parse_masked_series_list(str(row.get("release_date_masked_series") or "").strip())
        if masked:
            out[block_id] = set(masked)
    return out


def _maybe_cumulative_display(
    df: pd.DataFrame,
    *,
    columns: list[str],
    masked: set[str],
    mode: str,
) -> pd.DataFrame:
    """Plot-only transform to avoid 1-day 'release spikes' for masked series.

    If mode == 'cumulative', masked series are displayed as **cumulated shocks
    (level proxy)** (via cumulative sums) so release-date impulses appear as
    step/LOCF-like level moves.
    """
    if df.empty or not columns:
        return df

    mode = str(mode or "raw").strip().lower()
    if mode not in {"raw", "cumulative"}:
        mode = "raw"
    if mode == "raw" or not masked:
        return df

    out = df.copy()
    for c in columns:
        if c in out.columns and c in masked:
            s = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
            out[c] = s.cumsum()
    return out


def _plot_lowfreq_impulse_vs_level_proxy(
    block_id: str,
    z_raw: pd.DataFrame,
    *,
    masked_series: list[str],
    out_path: Path,
) -> None:
    """Two-panel view for masked (low-frequency) series only.

    Left = impulses (z-shocks)
    Right = cumulated shocks (level proxy; step/LOCF for low-frequency)

    This is plot-only and intended as a transparency aid.
    """
    masked_series = [c for c in (masked_series or []) if c in z_raw.columns]
    if not masked_series:
        return

    impulses = z_raw[masked_series].copy()
    level_proxy = impulses.copy()
    for c in masked_series:
        s = pd.to_numeric(level_proxy[c], errors="coerce").fillna(0.0)
        level_proxy[c] = s.cumsum()

    impulses = _downsample_df(impulses, max_points=2500)
    level_proxy = _downsample_df(level_proxy, max_points=2500)

    fig, axes = plt.subplots(1, 2, figsize=(14, 4), sharex=False)

    ax = axes[0]
    for c in masked_series:
        ax.plot(impulses.index, impulses[c].values, label=c, lw=1.0)
    ax.set_title("impulses (sigmas; z)")
    ax.set_ylabel("sigmas (z; impulses)")
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    for c in masked_series:
        ax.step(level_proxy.index, level_proxy[c].values, where="post", label=c, lw=1.0)
    ax.set_title("cumulated shocks (level proxy; step/LOCF for low-frequency)")
    ax.set_ylabel("cumulated sigmas (z; level proxy)")
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8)

    fig.suptitle(f"{block_id} - low-frequency transparency view", y=1.04)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_z_vs_innov(
    block_id: str,
    z: pd.DataFrame,
    innov: pd.DataFrame,
    drivers: list[str],
    out_path: Path,
    *,
    release_date_masked_series: set[str] | None = None,
    lowfreq_display_mode: str = "cumulative",
    innov_bps_series: set[str] | None = None,
) -> None:
    masked = set(release_date_masked_series or set())
    bps_series = set(innov_bps_series or set())
    bps_effective: set[str] = set()

    z_plot = z[drivers].copy()
    z_plot = _maybe_cumulative_display(z_plot, columns=drivers, masked=masked, mode=lowfreq_display_mode)
    z_plot = _downsample_df(z_plot, max_points=2500)

    innov_cols = [c for c in drivers if c in innov.columns]
    innov_plot = innov[innov_cols].copy() if (not innov.empty and innov_cols) else pd.DataFrame(index=z_plot.index)
    if not innov_plot.empty and bps_series:
        bps_cols = [c for c in innov_plot.columns if c in bps_series]
        if bps_cols:
            innov_plot[bps_cols] = innov_plot[bps_cols] * 10_000.0
            bps_effective.update(bps_cols)
    innov_plot = _maybe_cumulative_display(innov_plot, columns=innov_cols, masked=masked, mode=lowfreq_display_mode)
    if not innov_plot.empty:
        innov_plot = _downsample_df(innov_plot, max_points=2500)

    fig, axes = plt.subplots(1, 2, figsize=(14, 4), sharex=False)

    ax = axes[0]
    for c in drivers:
        use_step = (c in masked) and (str(lowfreq_display_mode).strip().lower() == "cumulative")
        if use_step:
            ax.step(z_plot.index, z_plot[c].values, where="post", label=c, lw=1.1)
        else:
            ax.plot(z_plot.index, z_plot[c].values, label=c, lw=1.1)
    if masked and str(lowfreq_display_mode).strip().lower() == "cumulative":
        ax.set_title("impulses (sigmas; z) + cumulated shocks (level proxy; step/LOCF for low-frequency)")
        ax.set_ylabel("sigmas (z; impulses; masked series shown as cumulated shocks level proxy)")
    else:
        ax.set_title("impulses (sigmas; z)")
        ax.set_ylabel("sigmas (z; impulses)")
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)

    ax = axes[1]
    for c in drivers:
        if c in innov_plot.columns:
            use_step = (c in masked) and (str(lowfreq_display_mode).strip().lower() == "cumulative")
            label = f"{c} (bps)" if (c in bps_effective) else c
            if use_step:
                ax.step(innov_plot.index, innov_plot[c].values, where="post", label=label, lw=1.1)
            else:
                ax.plot(innov_plot.index, innov_plot[c].values, label=label, lw=1.1)
    bps_note = " (selected series in bps)" if bps_effective else ""
    if masked and str(lowfreq_display_mode).strip().lower() == "cumulative":
        ax.set_title(
            "innovations (unit impulses) + cumulated innovations (level proxy; step/LOCF for low-frequency)" + bps_note
        )
        ax.set_ylabel("innovations (unit impulses; masked shown as cumulated level proxy)" + bps_note)
    else:
        ax.set_title("innovations (unit impulses)" + bps_note)
        ax.set_ylabel("innovations (unit impulses)" + bps_note)
    _apply_concise_date_axis(ax)
    ax.grid(True, alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8)

    fig.suptitle(f"{block_id} - sigmas (z) vs unit innovations", y=1.04)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _plot_corr_heatmap(block_id: str, z: pd.DataFrame, out_path: Path) -> None:
    if z.empty or z.shape[1] < 2:
        return

    cols = list(z.columns)
    # Keep label rendering fast (many plots per episode).
    if len(cols) > 20:
        mx = z.abs().max().sort_values(ascending=False)
        cols = mx.head(20).index.tolist()

    sub = z[cols].copy()
    corr = sub.corr().to_numpy(dtype=float)
    corr = np.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)

    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(corr, vmin=-1.0, vmax=1.0, cmap="RdBu_r", interpolation="nearest")
    ax.set_title(f"{block_id} - correlation of sigmas (z) (top {len(cols)} series)")
    ax.set_xticks(np.arange(len(cols)))
    ax.set_xticklabels(cols, rotation=90, fontsize=7)
    ax.set_yticks(np.arange(len(cols)))
    ax.set_yticklabels(cols, fontsize=7)
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("corr", rotation=90)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def _load_baseline_abs_z(
    block_id: str,
    columns: list[str],
    *,
    episode_start: pd.Timestamp,
    window: int,
) -> np.ndarray:
    path = DCC_BLOCKS_DIR / block_id / "standardized_residuals.csv"
    if not path.exists():
        return np.array([])

    # PERF: standardized_residuals.csv can be very wide. Avoid reading the full file
    # and then subsetting by columns; instead, read only the needed columns.
    try:
        header = pd.read_csv(path, nrows=0)
        all_cols = header.columns.tolist()
        if not all_cols:
            return np.array([])
        index_col = all_cols[0]
        available = set(all_cols)
        cols = [str(c) for c in columns if str(c) in available]
        if not cols:
            return np.array([])

        usecols = [index_col, *cols]
        df = pd.read_csv(path, usecols=usecols)
        df.rename(columns={index_col: "Date"}, inplace=True)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date")
    except Exception:
        # Fall back to the generic reader if anything goes sideways.
        df = _read_csv_time_indexed(path)
        cols = [c for c in columns if c in df.columns]
        if not cols:
            return np.array([])

    # Baseline window: last N obs strictly before episode start.
    df = df[cols].dropna(how="any")
    df = df.loc[df.index < episode_start]
    if df.empty:
        return np.array([])
    base = df.tail(int(window))
    return base.abs().to_numpy().ravel()


def _plot_distribution_compare(
    title: str,
    episode_abs_z: np.ndarray,
    baseline_abs_z: np.ndarray,
    *,
    baseline_window: int,
    out_path: Path,
) -> None:
    ep_vals = np.asarray(episode_abs_z, dtype=float)
    base_vals = np.asarray(baseline_abs_z, dtype=float)

    fig, axes = plt.subplots(1, 2, figsize=(14, 4))

    # ECDF
    ax = axes[0]
    x1, y1 = _ecdf(ep_vals)
    x0, y0 = _ecdf(base_vals)
    if x0.size:
        ax.plot(x0, y0, label=f"Baseline (|z|), last {baseline_window} obs", lw=1.5)
    if x1.size:
        ax.plot(x1, y1, label="Episode (|z|)", lw=1.5)
    ax.set_title("ECDF of |z|")
    ax.set_xlabel("|z|")
    ax.set_ylabel("ECDF")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8)

    # Box
    ax = axes[1]
    data = []
    labels = []
    if base_vals.size:
        data.append(base_vals[np.isfinite(base_vals)])
        labels.append("Baseline")
    if ep_vals.size:
        data.append(ep_vals[np.isfinite(ep_vals)])
        labels.append("Episode")
    if data:
        ax.boxplot(data, tick_labels=labels, showfliers=False)
    ax.set_title("Boxplot of |z|")
    ax.set_ylabel("|z|")
    ax.grid(True, axis="y", alpha=0.3)

    fig.suptitle(f"{title} - Episode vs baseline |z|", y=1.04)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)


def _plot_heatmap_episode(
    episode_id: str,
    max_abs_by_block_series: pd.DataFrame,
    *,
    top_n_series: int,
    out_path: Path,
) -> None:
    # Select top-N series overall by max
    overall = max_abs_by_block_series.max(axis=0).sort_values(ascending=False)
    top_cols = overall.head(int(top_n_series)).index.tolist()
    mat = max_abs_by_block_series[top_cols].copy()

    # Sort blocks by severity for scanability.
    try:
        sev = mat.max(axis=1).sort_values(ascending=False)
        mat = mat.loc[sev.index]
    except Exception:
        pass

    # Replace NaN with 0 for display; keep a mask for annotations if desired.
    data = mat.fillna(0.0).to_numpy(dtype=float)

    # Robust color scaling to avoid single spikes washing out the map.
    finite = data[np.isfinite(data)]
    finite = finite[finite > 0]
    vmax = None
    if finite.size:
        try:
            vmax = float(np.quantile(finite, 0.98))
        except Exception:
            vmax = float(np.max(finite))
    if vmax is None or (not np.isfinite(vmax)) or vmax <= 0:
        vmax = float(np.nanmax(data)) if np.isfinite(np.nanmax(data)) else 1.0
    vmax = max(float(vmax), 1.0)

    n_cols = len(top_cols)
    n_rows = int(mat.shape[0])
    width = float(np.clip(10.0 + 0.22 * n_cols, 12.0, 44.0))
    height = float(np.clip(6.0 + 0.28 * n_rows, 8.0, 28.0))

    fig, ax = plt.subplots(figsize=(width, height))
    im = ax.imshow(
        data,
        aspect="auto",
        interpolation="nearest",
        cmap="magma",
        vmin=0.0,
        vmax=vmax,
    )
    ax.set_title(f"{episode_id} - Heatmap of max|z| (top {len(top_cols)} series, vmax≈p98={vmax:.2f})")
    ax.set_xlabel("Series")
    ax.set_ylabel("Block")

    ax.set_xticks(np.arange(len(top_cols)))
    short_cols = [_shorten_label(c, max_len=20) for c in top_cols]

    # Thin tick labels when there are too many columns.
    max_ticks = 30
    step = int(math.ceil(n_cols / max_ticks)) if n_cols > max_ticks else 1
    xtick_labels = [lbl if (i % step == 0) else "" for i, lbl in enumerate(short_cols)]
    ax.set_xticklabels(xtick_labels, rotation=90, fontsize=8)

    # Sidecar mapping for auditability.
    try:
        mapping_path = out_path.with_suffix(".labels.txt")
        lines = [f"episode_id={episode_id}", f"top_n_series={len(top_cols)}", "", "full_label\tshort_label"]
        for full, short in zip(top_cols, short_cols, strict=False):
            lines.append(f"{full}\t{short}")
        mapping_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception:
        pass

    ax.set_yticks(np.arange(mat.shape[0]))
    ax.set_yticklabels([_shorten_label(x, max_len=28) for x in mat.index.tolist()], fontsize=8)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("max|z|", rotation=90)

    fig.tight_layout()
    fig.savefig(out_path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="11.2 FHS Historical Replay Plots")
    parser.add_argument("--use-latest", action="store_true", help="Use latest replay run under scenarios/latest/historical_replay")
    parser.add_argument("--run-dir", default=None, help="Explicit run dir (relative to project root or absolute)")
    parser.add_argument("--episodes", default="", help="Comma-separated episode ids (default: all in run)")
    parser.add_argument("--block-ids", default="", help="Comma-separated block ids to plot (default: all)")
    parser.add_argument("--top-k", type=int, default=5, help="Top-K driver series to plot")
    parser.add_argument("--baseline-window", type=int, default=756, help="Baseline window (obs) for distribution compare")
    parser.add_argument("--heatmap-top-series", type=int, default=50, help="Top-N series (overall) to include in episode heatmap")
    parser.add_argument(
        "--block-plot-scope",
        choices=["top", "all", "none"],
        default="top",
        help="Which blocks get per-block severity/driver/z-vs-innov plots.",
    )
    parser.add_argument(
        "--block-plot-top-n",
        type=int,
        default=25,
        help="When --block-plot-scope=top, plot only top-N blocks by severity.",
    )
    parser.add_argument(
        "--per-block-distributions",
        action="store_true",
        help="Also write per-block distribution plots (can be slow).",
    )
    parser.add_argument(
        "--dist-top-blocks",
        type=int,
        default=10,
        help="When per-block distributions are enabled, limit to top-N blocks by episode severity.",
    )
    parser.add_argument(
        "--dist-baseline-blocks",
        type=int,
        default=25,
        help="How many top-severity blocks to use when building the episode baseline |z| distribution.",
    )

    # Exclusions to keep driver selection/heatmaps robust to stale/step series.
    parser.add_argument(
        "--exclude-flat-spike-series",
        action="store_true",
        default=True,
        help="Exclude series flagged by Step 11.1 diagnostics as flat-then-spike (default: on).",
    )
    parser.add_argument(
        "--keep-flat-spike-series",
        action="store_true",
        help="Disable exclusion of Step 11.1 flat-then-spike series.",
    )
    parser.add_argument(
        "--exclude-stale-spike-series",
        action="store_true",
        default=True,
        help="Exclude step/stale series (few unique values) that also show extreme spikes (default: on).",
    )
    parser.add_argument(
        "--keep-stale-spike-series",
        action="store_true",
        help="Disable exclusion of stale-spike series.",
    )
    parser.add_argument(
        "--stale-spike-z-threshold",
        type=float,
        default=8.0,
        help="Spike threshold in z-space for stale-spike detection.",
    )
    parser.add_argument(
        "--stale-spike-unique-ratio-threshold",
        type=float,
        default=0.15,
        help="Unique-value ratio floor below which a spiky series is treated as stale/step-like.",
    )
    parser.add_argument(
        "--stale-spike-round-decimals",
        type=int,
        default=6,
        help="Rounding decimals used when counting unique values for stale-spike detection.",
    )
    parser.add_argument(
        "--lowfreq-display-mode",
        type=str,
        default="cumulative",
        choices=["raw", "cumulative"],
        help=(
            "Plot-only display for release-date masked (low-frequency) series. "
            "'cumulative' displays masked series as cumulated shocks (level proxy; step/LOCF for low-frequency); "
            "'raw' preserves impulses (z-shocks)."
        ),
    )
    parser.add_argument(
        "--innov-bps-series",
        type=str,
        default="",
        help=(
            "Comma-separated list of innovation series names to display in bps (multiplies by 10,000 in plots only). "
            "Example: --innov-bps-series=USA_DFF,USA_TSY10Y"
        ),
    )
    args = parser.parse_args()

    run_dir = _find_run_dir(args)
    wanted_eps = set(_parse_csv_list(args.episodes) or []) or None
    wanted_blocks = set(_parse_csv_list(args.block_ids) or []) or None
    innov_bps_series = set(_parse_csv_list(args.innov_bps_series) or [])

    out_root = _ensure_dir(PLOTS_BASE / run_dir.name)

    # Companion documentation (overwritten each run for freshness).
    try:
        (out_root / "README.md").write_text(
            _bundle_readme_text(run_dir=run_dir, out_root=out_root),
            encoding="utf-8",
        )
    except Exception:
        pass

    expected_blocks = _load_expected_block_ids(ROOT)

    # Save manifest copy for traceability.
    manifest = run_dir / "manifest.json"
    if manifest.exists():
        (out_root / "source_manifest.json").write_text(manifest.read_text(encoding="utf-8"), encoding="utf-8")

    index_lines: list[str] = [
        f"# FHS Historical Replay Plot Bundle",
        "",
        f"Source run: `{run_dir}`",
        f"Output root: `{out_root}`",
        "",
        "Docs:",
        "- `README.md` (what each plot category means; how to interpret economically)",
        "",
        "Episodes:",
    ]

    errors: list[str] = []

    for ep_dir in _iter_episode_dirs(run_dir, wanted_eps):
        ep_id = ep_dir.name
        print(f"[Episode] {ep_id}", flush=True)
        index_lines.append(f"- {ep_id}")

        masked_map = _load_release_date_masked_series_map(ep_dir)

        z_dir = ep_dir / "block_z_shocks"
        innov_dir = ep_dir / "block_innovations"
        if not z_dir.exists():
            print(f"[WARN] Missing z-shocks folder: {z_dir}")
            continue

        ep_out = _ensure_dir(out_root / ep_id)

        try:
            (ep_out / "README.md").write_text(
                _episode_readme_text(episode_id=ep_id, run_dir=run_dir, episode_out_dir=ep_out),
                encoding="utf-8",
            )
        except Exception:
            pass
        sev_out = _ensure_dir(ep_out / "severity")
        drv_out = _ensure_dir(ep_out / "drivers")
        zv_out = _ensure_dir(ep_out / "z_vs_innov")
        corr_out = _ensure_dir(ep_out / "correlations")
        dist_out = _ensure_dir(ep_out / "distributions")
        heat_out = _ensure_dir(ep_out / "heatmaps")

        diag = _load_episode_diagnostics(ep_dir)
        exclude_flat = bool(args.exclude_flat_spike_series) and (not bool(args.keep_flat_spike_series))
        exclude_stale = bool(args.exclude_stale_spike_series) and (not bool(args.keep_stale_spike_series))
        flat_excl_map = _flat_spike_exclusions_from_diagnostics(diag) if exclude_flat else {}
        stale_excl_map: dict[str, set[str]] = {}

        exclusions_audit: dict[str, object] = {
            "episode_id": ep_id,
            "source_episode_diagnostics": str((ep_dir / 'episode_diagnostics.json').as_posix()),
            "exclude_flat_then_spike": bool(exclude_flat),
            "exclude_stale_spike": bool(exclude_stale),
            "stale_spike": {
                "z_threshold": float(args.stale_spike_z_threshold),
                "unique_ratio_threshold": float(args.stale_spike_unique_ratio_threshold),
                "round_decimals": int(args.stale_spike_round_decimals),
            },
            "by_block": {},
        }

        # Aggregate for heatmap: rows=block, cols=series, values=max|z|
        max_rows: list[dict[str, float]] = []
        # Also collect severity so we can pick top blocks for heavier plots.
        block_severity: list[tuple[str, float]] = []
        # Episode-level distribution (aggregated across all blocks/series)
        episode_abs_all: list[np.ndarray] = []
        # Baseline distribution is IO-heavy to compute; we build it after we know top-severity blocks.
        baseline_abs_all: list[np.ndarray] = []
        baseline_block_meta: dict[str, tuple[pd.Timestamp, list[str]]] = {}

        # Pass 1: cheap aggregation only (no per-block plots)
        z_paths = sorted(z_dir.glob("*.csv"))
        if wanted_blocks is None:
            before = list(z_paths)
            z_paths = [p for p in z_paths if _is_iso_prefixed_block_id(p.stem)]
            ignored = [p.stem for p in before if p.stem not in set(x.stem for x in z_paths)]
            if ignored:
                print(
                    f"  [INFO] Ignoring {len(ignored)} non-ISO-prefixed blocks by default: {sorted(ignored)}. "
                    f"Use --block-ids to include explicitly.",
                    flush=True,
                )

            if expected_blocks is not None:
                iso_blocks = {p.stem for p in before if _is_iso_prefixed_block_id(p.stem)}
                unexpected = sorted(iso_blocks - expected_blocks)
                if unexpected:
                    print(
                        f"  [WARN] {len(unexpected)} ISO-prefixed blocks are present but not in the frozen definition; keeping them: {unexpected}",
                        flush=True,
                    )

        for z_path in z_paths:
            block_id = z_path.stem
            if wanted_blocks is not None and block_id not in wanted_blocks:
                continue

            try:
                z = _read_csv_time_indexed(z_path)
            except Exception as exc:
                errors.append(f"[{ep_id}] Failed reading z CSV for {block_id}: {exc}")
                continue
            if z.empty:
                continue

            # Compute exclusions for this block (only used for driver/heatmap selection).
            excluded: set[str] = set()
            if exclude_flat:
                excluded |= set(flat_excl_map.get(block_id, set()))
            if exclude_stale:
                stale = _stale_spike_columns(
                    z,
                    spike_z_threshold=float(args.stale_spike_z_threshold),
                    unique_ratio_threshold=float(args.stale_spike_unique_ratio_threshold),
                    round_decimals=int(args.stale_spike_round_decimals),
                )
                if stale:
                    stale_excl_map[block_id] = set(stale)
                    excluded |= set(stale)

            if excluded:
                try:
                    exclusions_audit["by_block"].setdefault(block_id, {})
                    exclusions_audit["by_block"][block_id]["excluded_series"] = sorted(excluded)
                except Exception:
                    pass

            z_sel = z.drop(columns=list(excluded), errors="ignore") if excluded else z
            # Avoid nuking a block entirely for plotting; fall back to raw.
            if z_sel.empty:
                z_sel = z

            # heatmap row
            mx = z_sel.abs().max()
            row = {"block_id": block_id, **{c: float(mx[c]) for c in mx.index}}
            max_rows.append(row)

            # severity ranking (for optional expensive plots)
            try:
                smax = float(z_sel.abs().max(axis=1).max())
                block_severity.append((block_id, smax))
            except Exception:
                pass

            # episode-level distribution accumulation (cap size to keep plotting fast)
            ep_abs = z_sel.abs().to_numpy().ravel()
            ep_abs = ep_abs[np.isfinite(ep_abs)]
            if ep_abs.size:
                if ep_abs.size > 200_000:
                    ep_abs = np.random.default_rng(0).choice(ep_abs, size=200_000, replace=False)
                episode_abs_all.append(ep_abs)

            # Capture baseline metadata for later (only for top-severity blocks)
            try:
                baseline_block_meta[block_id] = (pd.Timestamp(z.index.min()), list(z.columns))
            except Exception:
                pass

        # Pass 2: per-block plots for selected blocks only
        selected_blocks: list[str] = []
        if args.block_plot_scope == "none":
            selected_blocks = []
        elif args.block_plot_scope == "all":
            selected_blocks = [b for b, _ in sorted(block_severity, key=lambda x: x[0])]
        else:
            selected_blocks = [
                b for b, _ in sorted(block_severity, key=lambda x: x[1], reverse=True)[: int(args.block_plot_top_n)]
            ]

        if selected_blocks:
            print(f"  plotting {len(selected_blocks)} blocks ({args.block_plot_scope})", flush=True)
        for block_id in selected_blocks:
            z_path = z_dir / f"{block_id}.csv"
            if not z_path.exists():
                continue
            try:
                z = _read_csv_time_indexed(z_path)
            except Exception as exc:
                errors.append(f"[{ep_id}] Failed reading z CSV for plotting {block_id}: {exc}")
                continue
            if z.empty:
                continue

            innov_path = innov_dir / f"{block_id}.csv"
            if innov_path.exists():
                try:
                    innov = _read_csv_time_indexed(innov_path)
                except Exception as exc:
                    errors.append(f"[{ep_id}] Failed reading innovations for plotting {block_id}: {exc}")
                    innov = pd.DataFrame(index=z.index)
            else:
                innov = pd.DataFrame(index=z.index)

            excluded: set[str] = set()
            if exclude_flat:
                excluded |= set(flat_excl_map.get(block_id, set()))
            if exclude_stale:
                excluded |= set(stale_excl_map.get(block_id, set()))
            z_sel = z.drop(columns=list(excluded), errors="ignore") if excluded else z
            if z_sel.empty:
                z_sel = z
            drivers = _top_drivers(z_sel, k=args.top_k)
            if not drivers:
                continue

            try:
                # Use filtered z-shocks for plotting to avoid step/stale series creating
                # visually misleading "flat then spike" artifacts.
                _plot_severity(block_id, z_sel, sev_out / f"{block_id}.png")
            except Exception as exc:
                errors.append(f"[{ep_id}] severity plot failed for {block_id}: {exc}")
            try:
                _plot_top_drivers(
                    block_id,
                    z_sel,
                    drivers,
                    drv_out / f"{block_id}.png",
                    release_date_masked_series=masked_map.get(block_id, set()),
                    lowfreq_display_mode=str(args.lowfreq_display_mode),
                )
            except Exception as exc:
                errors.append(f"[{ep_id}] driver plot failed for {block_id}: {exc}")
            try:
                _plot_z_vs_innov(
                    block_id,
                    z_sel,
                    innov,
                    drivers,
                    zv_out / f"{block_id}.png",
                    release_date_masked_series=masked_map.get(block_id, set()),
                    lowfreq_display_mode=str(args.lowfreq_display_mode),
                    innov_bps_series=innov_bps_series,
                )
            except Exception as exc:
                errors.append(f"[{ep_id}] z_vs_innov plot failed for {block_id}: {exc}")

            # Transparency plot for masked (low-frequency) series only.
            try:
                if str(args.lowfreq_display_mode).strip().lower() == "cumulative":
                    masked_all = sorted(set(masked_map.get(block_id, set())))
                    masked_all = [c for c in masked_all if c in z_sel.columns]
                    if masked_all:
                        # Limit to a small number of masked series so the plot stays readable.
                        mx_masked = z_sel[masked_all].abs().max().sort_values(ascending=False)
                        masked_pick = [c for c in mx_masked.head(5).index.tolist() if c in masked_all]
                        _plot_lowfreq_impulse_vs_level_proxy(
                            block_id,
                            z_raw=z_sel,
                            masked_series=masked_pick,
                            out_path=zv_out / f"{block_id}__lowfreq_impulse_vs_level.png",
                        )
            except Exception as exc:
                errors.append(f"[{ep_id}] lowfreq transparency plot failed for {block_id}: {exc}")
            try:
                _plot_corr_heatmap(block_id, z_sel, corr_out / f"{block_id}.png")
            except Exception as exc:
                errors.append(f"[{ep_id}] corr heatmap failed for {block_id}: {exc}")

        if max_rows:
            df = pd.DataFrame(max_rows).set_index("block_id").sort_index()
            _plot_heatmap_episode(
                ep_id,
                df,
                top_n_series=int(args.heatmap_top_series),
                out_path=heat_out / f"{ep_id}_maxabsz_heatmap.png",
            )
            df.to_csv(heat_out / f"{ep_id}_maxabsz_matrix.csv")

        # Persist exclusions audit for this episode (helps explain why a driver disappeared).
        try:
            (ep_out / "plot_exclusions.json").write_text(json.dumps(exclusions_audit, indent=2), encoding="utf-8")
        except Exception:
            pass

        # Build baseline distribution from top-N blocks by severity (dramatically reduces IO)
        try:
            top_for_baseline = [
                b for b, _ in sorted(block_severity, key=lambda x: x[1], reverse=True)[: int(args.dist_baseline_blocks)]
            ]
            for block_id in top_for_baseline:
                meta = baseline_block_meta.get(block_id)
                if meta is None:
                    continue
                episode_start, cols = meta
                base_abs = _load_baseline_abs_z(
                    block_id,
                    cols,
                    episode_start=episode_start,
                    window=int(args.baseline_window),
                )
                base_abs = base_abs[np.isfinite(base_abs)]
                if base_abs.size:
                    if base_abs.size > 200_000:
                        base_abs = np.random.default_rng(0).choice(base_abs, size=200_000, replace=False)
                    baseline_abs_all.append(base_abs)
        except Exception as exc:
            errors.append(f"[{ep_id}] baseline distribution build failed: {exc}")

        # 5) distribution comparison (episode-level aggregated)
        try:
            ep_vals = np.concatenate(episode_abs_all) if episode_abs_all else np.array([])
            base_vals = np.concatenate(baseline_abs_all) if baseline_abs_all else np.array([])
            _plot_distribution_compare(
                title=f"{ep_id} (episode all blocks; baseline top {int(args.dist_baseline_blocks)} blocks)",
                episode_abs_z=ep_vals,
                baseline_abs_z=base_vals,
                baseline_window=int(args.baseline_window),
                out_path=dist_out / f"{ep_id}_episode_distribution.png",
            )
        except Exception as exc:
            errors.append(f"[{ep_id}] episode distribution plot failed: {exc}")

        # Optional: per-block distribution plots (top-N blocks by severity)
        if bool(args.per_block_distributions) and block_severity:
            top_blocks = [b for b, _ in sorted(block_severity, key=lambda x: x[1], reverse=True)[: int(args.dist_top_blocks)]]
            for block_id in top_blocks:
                z_path = z_dir / f"{block_id}.csv"
                if not z_path.exists():
                    continue
                try:
                    z = _read_csv_time_indexed(z_path)
                    if z.empty:
                        continue
                    ep_abs = z.abs().to_numpy().ravel()
                    ep_abs = ep_abs[np.isfinite(ep_abs)]
                    base_abs = _load_baseline_abs_z(
                        block_id,
                        list(z.columns),
                        episode_start=z.index.min(),
                        window=int(args.baseline_window),
                    )
                    _plot_distribution_compare(
                        title=f"{ep_id} / {block_id}",
                        episode_abs_z=ep_abs,
                        baseline_abs_z=base_abs,
                        baseline_window=int(args.baseline_window),
                        out_path=dist_out / f"{block_id}.png",
                    )
                except Exception as exc:
                    errors.append(f"[{ep_id}] per-block distribution failed for {block_id}: {exc}")

    (out_root / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")
    if errors:
        (out_root / "errors.log").write_text("\n".join(errors) + "\n", encoding="utf-8")
    print(f"[OK] Wrote plot bundle to: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
