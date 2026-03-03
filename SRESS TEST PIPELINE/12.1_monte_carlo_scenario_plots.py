#!/usr/bin/env python3
"""Step 12.1 — Monte Carlo scenario plots (per country subblock).

Reads Step 12 outputs under:
  analysis_outputs/scenarios/<run_id>/monte_carlo/

Writes plots under:
  SRESS TEST PIPELINE/MC scenario plots/<run_id>/

Plot policy (consistent with frequency constraints)
- Block aggregates are computed in standardized space (z-like) so we don't
    average mixed unit series (bps, %, etc.) together.
- Per-factor plots can be rendered either in standardized space (z) or in
    innovation units ("unit impulses").
- We also plot the cumulative sum as a "level proxy" for readability.
- For low-frequency macro factors (identified from macro_monthly_draws.csv),
  cumulative is rendered as a step function (LOCF-like).

This is intentionally lightweight and draw-subset oriented (uses representatives
from Step 12 by default) so it remains usable for large n_draws.
"""

from __future__ import annotations

import argparse
import json
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"
BLOCK_DEF_DEFAULT = PROJECT_ROOT / "outputs" / "country_block_definition.json"
LITERATURE_BLOCK_DEF_DEFAULT = PROJECT_ROOT / "analysis_outputs" / "literature_factors" / "country_block_definition.within_block.json"
PLOTS_ROOT_DEFAULT = Path(__file__).resolve().parent / "MC scenario plots"

# Plot output defaults (can be overridden via CLI)
_FIG_FORMATS: Set[str] = {"png"}
_FIG_DPI: int = 140


def _parse_fig_formats(text: Optional[str]) -> Set[str]:
    if not text:
        return {"png"}
    out: Set[str] = set()
    for part in str(text).split(","):
        s = str(part or "").strip().lower().lstrip(".")
        if not s:
            continue
        if s in {"png", "pdf", "svg"}:
            out.add(s)
    return out or {"png"}


def _save_figure(fig: Any, out_path_png: Path) -> None:
    """Save matplotlib figure to configured formats.

    Convention: markdown embeds use PNG, so we always write `out_path_png`.
    Additional formats (pdf/svg) are written alongside if enabled.
    """
    out_path_png.parent.mkdir(parents=True, exist_ok=True)

    dpi = int(_FIG_DPI) if int(_FIG_DPI) > 0 else 140
    try:
        fig.savefig(out_path_png, dpi=dpi, bbox_inches="tight")
    except Exception:
        fig.savefig(out_path_png, dpi=dpi)

    base = out_path_png.with_suffix("")
    if "pdf" in _FIG_FORMATS:
        try:
            fig.savefig(base.with_suffix(".pdf"), bbox_inches="tight")
        except Exception:
            fig.savefig(base.with_suffix(".pdf"))
    if "svg" in _FIG_FORMATS:
        try:
            fig.savefig(base.with_suffix(".svg"), bbox_inches="tight")
        except Exception:
            fig.savefig(base.with_suffix(".svg"))


def _plot_severity_rank_png(
    *,
    df_iso_sev: pd.DataFrame,
    fig_out: Path,
    title: str,
    today_by_iso: Optional[Dict[str, float]] = None,
    today_label: str = "Today",
    note: str = "",
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if df_iso_sev.empty or not {"iso", "severity_l2"}.issubset(set(df_iso_sev.columns)):
        return

    tail_q = 0.99
    rows: List[Tuple[str, float, float]] = []
    for iso, g in df_iso_sev.groupby("iso"):
        s = pd.to_numeric(g["severity_l2"], errors="coerce").dropna()
        if s.empty:
            continue
        rows.append((str(iso), float(s.median()), float(s.quantile(tail_q))))
    if not rows:
        return
    rows.sort(key=lambda t: t[2], reverse=True)

    labels = [r[0] for r in rows]
    med = np.array([r[1] for r in rows], dtype=float)
    p_tail = np.array([r[2] for r in rows], dtype=float)

    y = np.arange(len(labels), dtype=float)
    fig, ax = plt.subplots(figsize=(10, 4.2))
    ax.barh(y, p_tail, alpha=0.25, label=f"P{int(100*tail_q)} (tail)")
    ax.scatter(med, y, color="black", s=25, zorder=3, label="Median")

    # Optional realized "today" markers (same severity units)
    if today_by_iso:
        tx: List[float] = []
        ty: List[float] = []
        for yi, iso in zip(y, labels):
            v = today_by_iso.get(str(iso))
            if v is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if np.isfinite(fv):
                tx.append(fv)
                ty.append(float(yi))
        if tx:
            ax.scatter(
                tx,
                ty,
                color="red",
                marker="s",
                s=32,
                zorder=4,
                linewidths=0.6,
                edgecolors="black",
                label=str(today_label),
            )
    ax.set_yticks(y)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()
    ax.set_xlabel(r"Severity ($S_{L2}$)")
    ax.set_title(title)
    ax.grid(axis="x", alpha=0.25)
    ax.legend(loc="lower right", fontsize=8)

    # Numeric annotations
    for yi, m, t in zip(y, med, p_tail):
        if np.isfinite(t):
            ax.text(float(t) + 0.2, float(yi), f"{t:.2f}", va="center", fontsize=8, alpha=0.8)
    if note:
        fig.text(0.5, 0.01, str(note), ha="center", va="bottom", fontsize=9, color="0.35")
        fig.tight_layout(rect=[0, 0.05, 1, 1])
    else:
        fig.tight_layout()
    fig_out.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, fig_out)
    plt.close(fig)


def _plot_severity_box_png(
    *,
    df_iso_sev: pd.DataFrame,
    fig_out: Path,
    title: str,
    today_by_iso: Optional[Dict[str, float]] = None,
    today_label: str = "Today",
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if df_iso_sev.empty or not {"iso", "severity_l2"}.issubset(set(df_iso_sev.columns)):
        return

    labels: List[str] = []
    data: List[np.ndarray] = []
    for iso, g in df_iso_sev.groupby("iso"):
        s = pd.to_numeric(g["severity_l2"], errors="coerce").dropna()
        if s.empty:
            continue
        labels.append(str(iso))
        data.append(s.to_numpy(dtype=float))
    if not data:
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    try:
        ax.boxplot(data, tick_labels=labels, showfliers=False)
    except TypeError:
        ax.boxplot(data, labels=labels, showfliers=False)

    # Optional realized "today" markers
    if today_by_iso:
        xs: List[float] = []
        ys: List[float] = []
        for i, iso in enumerate(labels):
            v = today_by_iso.get(str(iso))
            if v is None:
                continue
            try:
                fv = float(v)
            except Exception:
                continue
            if np.isfinite(fv):
                xs.append(float(i + 1))
                ys.append(float(fv))
        if xs:
            ax.scatter(xs, ys, color="red", marker="D", s=35, zorder=4, label=str(today_label))
            ax.legend(loc="upper right", fontsize=8)
    ax.set_ylabel("Severity $S_{L2}$ (sigmas; terminal)")
    ax.set_title(title)
    fig.tight_layout()
    fig_out.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, fig_out)
    plt.close(fig)


def _plot_crisis_driver_stacks_png(
    *,
    df_term_by_iso: Dict[str, pd.DataFrame],
    crisis_mask_by_iso: Dict[str, np.ndarray],
    fig_out: Path,
    title: str,
    top_k: int = 0,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    iso_blocks: Dict[str, Dict[str, float]] = {}
    for iso, df_term in (df_term_by_iso or {}).items():
        if df_term is None or df_term.empty:
            continue
        mask = crisis_mask_by_iso.get(str(iso))
        if mask is None or not bool(np.any(mask)):
            continue
        X = df_term.to_numpy(dtype=float)
        X2 = X ** 2
        denom = X2.sum(axis=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            W = np.where(denom[:, None] > 0, X2 / denom[:, None], np.nan)
        df_share = pd.DataFrame(W, index=df_term.index, columns=df_term.columns)
        med = df_share.loc[mask].median(axis=0).fillna(0.0)
        s = float(med.sum())
        if np.isfinite(s) and s > 0:
            med = med / s
        med = med.sort_values(ascending=False)
        iso_blocks[str(iso)] = {str(k): float(v) for k, v in med.items() if np.isfinite(float(v))}

    if not iso_blocks:
        return

    all_blocks: Dict[str, float] = {}
    for m in iso_blocks.values():
        for b, v in m.items():
            all_blocks[b] = all_blocks.get(b, 0.0) + float(v)
    blocks_sorted = [b for b, _ in sorted(all_blocks.items(), key=lambda kv: kv[1], reverse=True)]
    use_blocks = blocks_sorted if (top_k is None or int(top_k) <= 0) else blocks_sorted[: int(top_k)]

    isos = sorted(iso_blocks.keys())
    fig_out.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(11, 4.5))
    bottom = np.zeros(len(isos), dtype=float)
    for b in use_blocks:
        vals = np.array([iso_blocks[iso].get(b, 0.0) for iso in isos], dtype=float)
        ax.bar(isos, vals, bottom=bottom, label=b)
        bottom += vals

    # Write a transparent breakdown so there's no hidden "Other" bucket.
    try:
        rows: List[Dict[str, Any]] = []
        for iso in isos:
            for b in use_blocks:
                rows.append({"iso": str(iso), "block": str(b), "median_share": float(iso_blocks[iso].get(b, 0.0))})
        pd.DataFrame(rows).to_csv(fig_out.parent / "CRISIS_DRIVERS__BLOCK_SHARES__BREAKDOWN.csv", index=False)
    except Exception:
        try:
            (fig_out.parent / "CRISIS_DRIVERS__BLOCK_SHARES__BREAKDOWN.error.txt").write_text(
                traceback.format_exc(), encoding="utf-8"
            )
        except Exception:
            pass

    ax.set_ylim(0, 1.0)
    ax.set_ylabel("Median share of $S_{L2}^2$")
    ax.set_title(title)
    ncol = 4
    try:
        ncol = min(5, max(3, int(np.ceil(len(use_blocks) / 2))))
    except Exception:
        ncol = 4
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=ncol, fontsize=8)
    fig.tight_layout()
    _save_figure(fig, fig_out)
    plt.close(fig)


def _corr_over_time(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    if a.size != b.size or a.size < 2:
        return float("nan")
    a = a - float(np.mean(a))
    b = b - float(np.mean(b))
    sa = float(np.sqrt(np.mean(a * a)))
    sb = float(np.sqrt(np.mean(b * b)))
    if not np.isfinite(sa) or not np.isfinite(sb) or sa <= 0 or sb <= 0:
        return float("nan")
    return float(np.mean(a * b) / (sa * sb))


def _plot_connectedness_delta_png(
    *,
    stress_by_iso: Dict[str, np.ndarray],
    baseline_mask: np.ndarray,
    crisis_mask: np.ndarray,
    baseline_corr_matrix: Optional[np.ndarray] = None,
    baseline_label: str = "baseline",
    stress_label: str = "crisis",
    baseline_vs_stress_fig_out: Optional[Path] = None,
    baseline_vs_stress_title: Optional[str] = None,
    fig_out: Path,
    title: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    isos = sorted(stress_by_iso.keys())
    if len(isos) < 2:
        return

    def _to_corr_series(x: np.ndarray) -> np.ndarray:
        """Transform a stress-index time series into the series used for correlation.

        Use first differences to avoid spurious near-1 correlations driven by
        shared drift in cumulative/level-like indices.
        """
        x = np.asarray(x, dtype=float)
        if x.size < 3:
            return x
        dx = np.diff(x)
        return dx

    def _corr_matrix(mask: np.ndarray) -> np.ndarray:
        idx = np.where(mask)[0]
        if idx.size == 0:
            return np.full((len(isos), len(isos)), np.nan)
        pairs: Dict[Tuple[int, int], List[float]] = {}
        for d in idx.tolist():
            for i in range(len(isos)):
                for j in range(i + 1, len(isos)):
                    a = _to_corr_series(stress_by_iso[isos[i]][d, :])
                    b = _to_corr_series(stress_by_iso[isos[j]][d, :])
                    c = _corr_over_time(a, b)
                    if np.isfinite(c):
                        pairs.setdefault((i, j), []).append(float(c))
        out = np.eye(len(isos), dtype=float)
        for (i, j), vals in pairs.items():
            out[i, j] = float(np.median(vals)) if vals else float("nan")
            out[j, i] = out[i, j]
        return out

    if baseline_corr_matrix is not None:
        Cb = np.asarray(baseline_corr_matrix, dtype=float)
        if Cb.shape != (len(isos), len(isos)):
            Cb = np.full((len(isos), len(isos)), np.nan)
    else:
        Cb = _corr_matrix(baseline_mask)
    Cc = _corr_matrix(crisis_mask)
    D = Cc - Cb

    # Context stats: baseline/crisis levels (median off-diagonal corr)
    def _median_offdiag(M: np.ndarray) -> float:
        if M is None or getattr(M, "size", 0) == 0:
            return float("nan")
        iu = np.triu_indices(M.shape[0], k=1)
        vals = np.asarray(M[iu], dtype=float)
        vals = vals[np.isfinite(vals)]
        return float(np.median(vals)) if vals.size else float("nan")

    base_level = _median_offdiag(Cb)
    crisis_level = _median_offdiag(Cc)
    delta_level = crisis_level - base_level if (np.isfinite(base_level) and np.isfinite(crisis_level)) else float("nan")
    iuD = np.triu_indices(D.shape[0], k=1)
    dvals = np.asarray(D[iuD], dtype=float)
    dvals = dvals[np.isfinite(dvals)]
    max_delta = float(np.max(dvals)) if dvals.size else float("nan")

    def _annotate_matrix(ax: Any, M: np.ndarray, *, fmt: str, skip_diag: bool = True) -> None:
        for i in range(len(isos)):
            for j in range(len(isos)):
                if skip_diag and i == j:
                    continue
                v = float(M[i, j]) if np.isfinite(M[i, j]) else float("nan")
                if not np.isfinite(v):
                    continue
                ax.text(j, i, format(v, fmt), ha="center", va="center", fontsize=8, color="black")

    # Optional: write baseline vs stress correlation matrices side-by-side
    if baseline_vs_stress_fig_out is not None:
        fig2, axes = plt.subplots(nrows=1, ncols=2, figsize=(12.8, 5.8))
        ax0, ax1 = axes[0], axes[1]
        im0 = ax0.imshow(Cb, vmin=-1.0, vmax=1.0, cmap="RdBu_r")
        im1 = ax1.imshow(Cc, vmin=-1.0, vmax=1.0, cmap="RdBu_r")
        for axx, ttl in [(ax0, str(baseline_label or "baseline")), (ax1, str(stress_label or "stress"))]:
            axx.set_xticks(range(len(isos)))
            axx.set_yticks(range(len(isos)))
            axx.set_xticklabels(isos)
            axx.set_yticklabels(isos)
            axx.set_title(ttl)
        _annotate_matrix(ax0, Cb, fmt="+.2f")
        _annotate_matrix(ax1, Cc, fmt="+.2f")
        # Put the colorbar fully outside the two panels (never between them)
        fig2.subplots_adjust(right=0.90, wspace=0.22)
        cax = fig2.add_axes([0.92, 0.16, 0.02, 0.68])
        fig2.colorbar(im0, cax=cax, label="corr of Δ stress index over horizon")
        if baseline_vs_stress_title:
            fig2.suptitle(str(baseline_vs_stress_title), y=1.02)
        # Note: avoid tight_layout() here because we manually place cax.
        baseline_vs_stress_fig_out.parent.mkdir(parents=True, exist_ok=True)
        _save_figure(fig2, baseline_vs_stress_fig_out)
        plt.close(fig2)

    fig, ax = plt.subplots(figsize=(7.2, 6.0))
    im = ax.imshow(D, vmin=-1.0, vmax=1.0, cmap="RdBu_r")
    ax.set_xticks(range(len(isos)))
    ax.set_yticks(range(len(isos)))
    ax.set_xticklabels(isos)
    ax.set_yticklabels(isos)
    ax.set_title(title)

    # Small annotation for baseline/crisis correlation levels (helps interpret Δ magnitudes)
    if np.isfinite(base_level) and np.isfinite(crisis_level):
        bname = str(baseline_label or "baseline")
        sname = str(stress_label or "stress")
        txt = (
            f"median corr {bname}: {base_level:+.2f}\n"
            f"median corr {sname}: {crisis_level:+.2f}\n"
            f"Δ ({sname}−{bname}): {delta_level:+.2f}"
        )
        if np.isfinite(max_delta):
            txt += f"\nmax pairwise Δ: {max_delta:+.2f}"
        ax.text(
            0.02,
            0.98,
            txt,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.75, edgecolor="none"),
        )

    _annotate_matrix(ax, D, fmt="+.2f")

    bname = str(baseline_label or "baseline")
    sname = str(stress_label or "stress")
    fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label=f"Δ corr of Δ stress index  ({sname} − {bname})")
    fig.tight_layout()
    fig_out.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, fig_out)
    plt.close(fig)


def _plot_sync_index_png(
    *,
    stress_by_iso: Dict[str, np.ndarray],
    baseline_mask: np.ndarray,
    crisis_mask: np.ndarray,
    today_value: Optional[float] = None,
    today_label: str = "Today",
    fig_out: Path,
    title: str,
) -> Tuple[float, float]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    isos = sorted(stress_by_iso.keys())
    if len(isos) < 2:
        return (float("nan"), float("nan"))
    n_draws = int(next(iter(stress_by_iso.values())).shape[0])

    def _to_corr_series(x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        if x.size < 3:
            return x
        return np.diff(x)

    def _avg_offdiag(d: int) -> float:
        vals: List[float] = []
        for i in range(len(isos)):
            for j in range(i + 1, len(isos)):
                a = _to_corr_series(stress_by_iso[isos[i]][d, :])
                b = _to_corr_series(stress_by_iso[isos[j]][d, :])
                c = _corr_over_time(a, b)
                if np.isfinite(c):
                    vals.append(float(c))
        return float(np.mean(vals)) if vals else float("nan")

    sync = np.array([_avg_offdiag(d) for d in range(n_draws)], dtype=float)
    base = sync[np.where(baseline_mask)[0]]
    cris = sync[np.where(crisis_mask)[0]]
    base_med = float(np.nanmedian(base)) if base.size else float("nan")
    cris_med = float(np.nanmedian(cris)) if cris.size else float("nan")

    fig, ax = plt.subplots(figsize=(6.8, 4.2))
    data = []
    labels = []
    if base.size:
        data.append(base[np.isfinite(base)])
        labels.append("baseline")
    if cris.size:
        data.append(cris[np.isfinite(cris)])
        labels.append("crisis")
    if data:
        try:
            ax.boxplot(data, tick_labels=labels, showfliers=False)
        except TypeError:
            ax.boxplot(data, labels=labels, showfliers=False)
    ax.set_ylabel("Avg cross-country corr of $\\Delta$ stress index")
    ax.set_title(title)

    # Numeric callouts (medians + delta)
    if np.isfinite(base_med) and np.isfinite(cris_med):
        delta = cris_med - base_med
        txt = f"median baseline: {base_med:+.2f}\nmedian crisis: {cris_med:+.2f}\nΔ (crisis−base): {delta:+.2f}"
        ax.text(
            0.02,
            0.98,
            txt,
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
            bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "none"},
        )

    # Optional realized "today" horizontal marker
    if today_value is not None:
        try:
            tv = float(today_value)
        except Exception:
            tv = float("nan")
        if np.isfinite(tv):
            ax.axhline(tv, color="red", lw=1.4, alpha=0.85)
            ax.text(
                0.98,
                tv,
                f"{str(today_label)}: {tv:+.2f}",
                ha="right",
                va="bottom",
                fontsize=9,
                color="red",
            )

            # Percentile of today's synchronization vs all MC draws
            try:
                sync_f = sync[np.isfinite(sync)]
                if sync_f.size:
                    pct = 100.0 * float(np.mean(sync_f <= float(tv)))
                    ax.text(
                        0.02,
                        0.70,
                        f"{str(today_label)} percentile: {pct:5.1f}%",
                        transform=ax.transAxes,
                        ha="left",
                        va="top",
                        fontsize=9,
                        color="red",
                        bbox={"facecolor": "white", "alpha": 0.65, "edgecolor": "none"},
                    )
            except Exception:
                pass

    # Per-box median labels (if present)
    try:
        if base.size and np.isfinite(base_med):
            ax.text(1, base_med, f"{base_med:+.2f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        if cris.size and np.isfinite(cris_med):
            xpos = 2 if base.size else 1
            ax.text(xpos, cris_med, f"{cris_med:+.2f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    except Exception:
        pass
    fig.tight_layout()
    fig_out.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, fig_out)
    plt.close(fig)

    return (base_med, cris_med)


def _write_index_markdown(*, plots_out: Path, bundle_name: str, input_run_id: str, daily_source: str) -> None:
    """Write a lightweight TOC for a plot bundle."""
    lines: List[str] = []
    lines.append(f"# Monte Carlo Plot Bundle — {bundle_name}")
    lines.append("")
    lines.append("## Provenance")
    lines.append(f"- Bundle: `{bundle_name}`")
    lines.append(f"- Input run: `{input_run_id}`")
    lines.append(f"- Source daily draws: `{daily_source}`")
    lines.append("")

    comove = plots_out / "BLOCK_COMOVEMENTS_AND_REGIMES.md"
    if comove.exists():
        lines.append("## Bundle notes")
        lines.append(f"- [BLOCK_COMOVEMENTS_AND_REGIMES.md]({comove.name})")
        lines.append("")

    lines.append("## Diagnostics")
    for name in ["coverage_report.csv", "unmapped_factors.csv", "duplicates_in_block_def.csv"]:
        p = plots_out / name
        if p.exists():
            lines.append(f"- [{name}]({name})")
    lines.append("")

    lines.append("## Countries / Blocks")
    agg_paths = sorted(plots_out.glob("*/**/__block_aggregate__impulse_vs_level.md"))
    if not agg_paths:
        lines.append("- (No block aggregate markdowns found)")
    else:
        for p in agg_paths:
            rel = p.relative_to(plots_out).as_posix()
            try:
                iso = p.parts[-3]
                block = p.parts[-2]
                lines.append(f"- [{iso} | {block}]({rel})")
            except Exception:
                lines.append(f"- [{rel}]({rel})")
    lines.append("")

    (plots_out / "INDEX.md").write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _domain_interpretation_notes(block_key: str) -> List[str]:
    k = str(block_key or "").strip().lower()
    if not k:
        return []
    if "macro" in k:
        return [
            "Macro: broad activity/inflation/growth conditions.",
            "Large persistent cumulative moves suggest a regime shift rather than noise.",
        ]
    if "financial" in k or "markets" in k:
        return [
            "Financial markets: risk premia / discounting / volatility channels.",
            "Comovement here often amplifies other blocks (risk-off / risk-on dynamics).",
        ]
    if "bank" in k:
        return [
            "Banking system: credit conditions / funding stress / banking-sector feedback loops.",
            "Sustained stress here can propagate to the real economy via tighter credit.",
        ]
    if "public" in k or "finance" in k:
        return [
            "Public finance: sovereign risk / fiscal conditions.",
            "Adverse moves here can interact with banking via sovereign-bank linkages.",
        ]
    if "real_estate" in k or "housing" in k:
        return [
            "Real estate: housing/CRE valuation and leverage sensitivity.",
            "Persistent adverse moves here can weigh on banks and household balance sheets.",
        ]
    if "external" in k or "fx" in k:
        return [
            "External/FX: exchange-rate and external balance channels.",
            "Large moves can reprice imported inflation and foreign-currency liabilities.",
        ]
    if "commod" in k:
        return [
            "Commodities: energy/commodity price channels.",
            "Large moves can hit inflation and terms-of-trade, affecting multiple blocks.",
        ]
    if "systemic" in k or "stress" in k:
        return [
            "Systemic stress: cross-market stress proxy factors.",
            "High comovement here often coincides with broad, crisis-like regimes.",
        ]
    return []


def _write_plot_markdown(
    *,
    md_path: Path,
    png_path: Path,
    run_name: str,
    iso: str,
    block_key: str,
    plot_kind: str,
    factor: Optional[str] = None,
    factor_space: Optional[str] = None,
    lowfreq: Optional[bool] = None,
    notes: Optional[List[str]] = None,
) -> None:
    lines: List[str] = []
    title_bits = [str(run_name), str(iso), str(block_key)]
    if factor:
        title_bits.append(str(factor))
    lines.append(f"# {str(plot_kind).strip().title()} plot: " + " | ".join(title_bits))
    lines.append("")
    lines.append(f"![plot]({png_path.name})")
    lines.append("")
    lines.append("## What you are looking at")
    pk = str(plot_kind or "").strip().lower()
    fs = str(factor_space or "").strip().lower()
    if pk == "factor":
        if fs == "z":
            lines.append("- **Top panel**: daily shocks in **sigmas** ($z$; standardized by $\\sigma_{t0}$).")
            lines.append("- **Bottom panel**: cumulative sum (a **level proxy**), in sigmas.")
        else:
            lines.append("- **Top panel**: daily shocks in **innovation units** ($\\Delta x_t = \\sigma_t z_t$).")
            lines.append("- **Bottom panel**: cumulative sum (a **level proxy**), in those units.")
    else:
        lines.append("- **Top panel**: block aggregate impulse (mean across factors in **sigmas**; comparable across mixed units).")
        lines.append("- **Bottom panel**: cumulative aggregate (a **level proxy**) in sigmas.")

    if lowfreq:
        lines.append("- **Low-frequency note**: the cumulative panel may be rendered as a step/LOCF-style path (release-day updates).")
    lines.append("")

    lines.append("## How to interpret")
    lines.append("- **Impulse dispersion** (colored lines spread) = scenario uncertainty about day-to-day shocks.")
    lines.append("- **Cumulative drift** (paths trend away from 0) = persistent regime direction (scenario *state*).")
    lines.append("- **Mean-reverting look** (wiggles around 0 with little drift) = stationary noise; impacts tend to wash out.")
    lines.append("")

    dom_notes = _domain_interpretation_notes(block_key)
    if dom_notes:
        lines.append("## Block context (economic transmission)")
        for n in dom_notes:
            lines.append(f"- {n}")
        lines.append("")

    lines.append("## Practical consequence checklist")
    lines.append("These are conditional interpretations (sign conventions vary by factor definition):")
    lines.append("- **Rates/yields**: higher level proxy often implies tighter financial conditions and pressure on risk assets.")
    lines.append("- **Credit spreads/systemic stress**: widening often implies risk-off, funding stress, and tighter credit supply.")
    lines.append("- **FX**: depreciation/appreciation affects imported inflation, competitiveness, and FX liabilities.")
    lines.append("- **Commodities**: higher energy/commodity prices feed inflation; lower prices can signal demand weakness.")
    lines.append("")

    if notes:
        lines.append("## Notes")
        for n in notes:
            lines.append(f"- {n}")
        lines.append("")

    md_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _draw_color_map(draw_ids: List[int]) -> Dict[int, Any]:
    import matplotlib

    cmap = matplotlib.colormaps.get_cmap("tab10")
    out: Dict[int, Any] = {}
    n = max(1, int(len(draw_ids)))
    for i, did in enumerate(draw_ids):
        out[int(did)] = cmap(i % 10) if n <= 10 else cmap(i / max(1, n - 1))
    return out


def _write_bundle_comovement_summary(
    *,
    out_path: Path,
    run_name: str,
    iso_to_block_mats: Dict[str, Dict[str, np.ndarray]],
    iso_to_block_factors: Optional[Dict[str, Dict[str, List[str]]]] = None,
    run_dir: Optional[Path] = None,
    today_window_days: int = 0,
    monte_carlo_dir: Optional[Path] = None,
    daily_source: Optional[DailySource] = None,
    lowfreq_by_iso: Optional[Dict[str, Set[str]]] = None,
    vol_t0_by_iso: Optional[Dict[str, Dict[str, float]]] = None,
    iso_to_sim_factors: Optional[Dict[str, List[str]]] = None,
) -> None:
    if not iso_to_block_mats:
        return

    # Total MC draws (for cross-country plot titles). Do not rely on CLI draw-subsetting.
    n_draws_mc: Optional[int] = None
    if monte_carlo_dir is not None:
        try:
            mc_manifest = Path(monte_carlo_dir) / "manifest.json"
            if mc_manifest.exists():
                mm = _read_json(mc_manifest)
                n_draws_mc = mm.get("n_draws")
                if n_draws_mc is None:
                    n_draws_mc = ((mm.get("signature_payload") or {}).get("args") or {}).get("n_draws")
                n_draws_mc = int(n_draws_mc)
        except Exception:
            n_draws_mc = None

    def _parse_coefficients_map(text: Any) -> Dict[str, float]:
        """Parse Step 4 `coefficients` field into {feature: coef}."""
        if text is None:
            return {}
        s = str(text).strip()
        if not s:
            return {}
        out: Dict[str, float] = {}
        # Format: feat:coef;feat:coef
        for part in s.split(";"):
            part = part.strip()
            if not part or ":" not in part:
                continue
            k, v = part.split(":", 1)
            k = str(k).strip()
            v = str(v).strip()
            if not k:
                continue
            try:
                coef = float(v)
            except Exception:
                continue
            if np.isfinite(coef):
                out[k] = coef
        return out

    def _strip_feature_lag(name: str) -> str:
        s = str(name)
        # Common Step 4 suffixes like _lag1, _lag2, ...
        for d in range(1, 25):
            suf = f"_lag{d}"
            if s.endswith(suf):
                return s[: -len(suf)]
        return s

    def _resolve_sim_factor(feature: str, sim_set: Set[str]) -> Optional[str]:
        """Map Step4 feature name to simulated factor name, best-effort."""
        f = str(feature)
        if f in sim_set:
            return f
        base = _strip_feature_lag(f)
        if base in sim_set:
            return base
        if f + "_lag0" in sim_set:
            return f + "_lag0"
        if base + "_lag0" in sim_set:
            return base + "_lag0"
        return None

    def _compute_terminal_factor_z_sums_daily(
        *,
        source: DailySource,
        iso: str,
        factors: Set[str],
        n_draws: int,
        factor_to_vol: Dict[str, float],
    ) -> Dict[str, np.ndarray]:
        if not factors or n_draws <= 0:
            return {}

        sums: Dict[str, np.ndarray] = {str(f): np.zeros(int(n_draws), dtype=float) for f in factors}
        cols = ["draw_id", "iso", "factor", "shock"]
        for part in _iter_iso_parts(source, iso=iso):
            if part.suffix == ".parquet":
                df = pd.read_parquet(part, columns=cols)
                dfs = [df]
            else:
                dfs = pd.read_csv(part, usecols=cols, chunksize=2_000_000)

            for df in dfs:
                df = df[df["iso"].astype(str) == str(iso)]
                if df.empty:
                    continue
                df = df[df["factor"].astype(str).isin(factors)]
                if df.empty:
                    continue

                df["draw_id"] = pd.to_numeric(df["draw_id"], errors="coerce")
                df = df.dropna(subset=["draw_id"])
                if df.empty:
                    continue
                df["draw_id"] = df["draw_id"].astype(int)
                df = df[(df["draw_id"] >= 0) & (df["draw_id"] < int(n_draws))]
                if df.empty:
                    continue

                df["factor"] = df["factor"].astype(str)
                v = df["factor"].map(factor_to_vol)
                v = pd.to_numeric(v, errors="coerce").fillna(1.0).replace(0.0, 1.0)
                df["shock_z"] = pd.to_numeric(df["shock"], errors="coerce") / v

                g = df.groupby(["draw_id", "factor"], as_index=False)["shock_z"].sum()
                for r in g.itertuples(index=False):
                    try:
                        did = int(getattr(r, "draw_id"))
                        fac = str(getattr(r, "factor"))
                        if fac in sums:
                            sums[fac][did] += float(getattr(r, "shock_z"))
                    except Exception:
                        continue

        return sums

    def _compute_terminal_factor_z_sums_monthly(
        *,
        macro_csv: Path,
        iso: str,
        factors: Set[str],
        n_draws: int,
        factor_to_vol: Dict[str, float],
    ) -> Dict[str, np.ndarray]:
        if not macro_csv.exists() or not factors or n_draws <= 0:
            return {}

        # Determine available columns without loading the full file.
        try:
            header_cols = list(pd.read_csv(macro_csv, nrows=0).columns)
        except Exception:
            header_cols = []
        need = [c for c in ["draw_id", "iso", "factor", "shock"] if c in header_cols] if header_cols else ["draw_id", "iso", "factor", "shock"]

        sums: Dict[str, np.ndarray] = {str(f): np.zeros(int(n_draws), dtype=float) for f in factors}
        try:
            chunks = pd.read_csv(macro_csv, usecols=need, chunksize=2_000_000)
        except Exception:
            # Fallback: try without usecols
            chunks = pd.read_csv(macro_csv, chunksize=2_000_000)

        for chunk in chunks:
            if not {"draw_id", "iso", "factor", "shock"}.issubset(set(chunk.columns)):
                continue
            chunk = chunk[chunk["iso"].astype(str) == str(iso)]
            if chunk.empty:
                continue
            chunk = chunk[chunk["factor"].astype(str).isin(factors)]
            if chunk.empty:
                continue
            chunk["draw_id"] = pd.to_numeric(chunk["draw_id"], errors="coerce")
            chunk = chunk.dropna(subset=["draw_id"])
            if chunk.empty:
                continue
            chunk["draw_id"] = chunk["draw_id"].astype(int)
            chunk = chunk[(chunk["draw_id"] >= 0) & (chunk["draw_id"] < int(n_draws))]
            if chunk.empty:
                continue
            chunk["factor"] = chunk["factor"].astype(str)
            v = chunk["factor"].map(factor_to_vol)
            v = pd.to_numeric(v, errors="coerce").fillna(1.0).replace(0.0, 1.0)
            chunk["shock_z"] = pd.to_numeric(chunk["shock"], errors="coerce") / v
            g = chunk.groupby(["draw_id", "factor"], as_index=False)["shock_z"].sum()
            for r in g.itertuples(index=False):
                try:
                    did = int(getattr(r, "draw_id"))
                    fac = str(getattr(r, "factor"))
                    if fac in sums:
                        sums[fac][did] += float(getattr(r, "shock_z"))
                except Exception:
                    continue
        return sums

    def _safe_slug(text: str) -> str:
        s = "".join(ch if (ch.isalnum() or ch in "-_") else "_" for ch in str(text))
        s = "_".join(p for p in s.split("_") if p)
        return s[:120] if len(s) > 120 else s

    def _write_target_influence_cake(
        *,
        fig_out: Path,
        df_share: pd.DataFrame,
        df_sev: pd.DataFrame,
        title: str,
    ) -> None:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(14, 4))
        legend_labels: List[str] = []

        # Write per-regime breakdowns so "Other" is explicit.
        try:
            def _safe_med_q(a: np.ndarray) -> Tuple[float, float, float]:
                a = np.asarray(a, dtype=float)
                a = a[np.isfinite(a)]
                if a.size == 0:
                    return (float("nan"), float("nan"), float("nan"))
                return (float(np.median(a)), float(np.quantile(a, q_lo)), float(np.quantile(a, q_hi)))

            prefix = fig_out.stem.replace("CAKE", "BREAKDOWN")
            if prefix == fig_out.stem:
                prefix = f"{fig_out.stem}__BREAKDOWN"
            out_dir = fig_out.parent
            for reg in ["adverse", "severe", "crisis"]:
                mask = (df_sev["regime"].to_numpy() == reg)
                if not bool(np.any(mask)):
                    continue
                Wreg = df_share.loc[mask]
                med = Wreg.median(axis=0).sort_values(ascending=False)
                top6 = med.head(6).index.tolist()
                other_series = 1.0 - Wreg[top6].sum(axis=1, skipna=True)
                other_series = other_series.clip(lower=0.0, upper=1.0)

                rows: List[Dict[str, Any]] = []
                for c in med.index.tolist():
                    s = pd.to_numeric(Wreg[c], errors="coerce")
                    med0, lo0, hi0 = _safe_med_q(s.to_numpy(dtype=float))
                    rows.append(
                        {
                            "block": str(c),
                            "median_share": med0,
                            f"q{int(q_lo*100)}": lo0,
                            f"q{int(q_hi*100)}": hi0,
                            "in_top6": bool(c in top6),
                        }
                    )
                # Add an explicit Other row that matches the pie definition.
                try:
                    o = pd.to_numeric(other_series, errors="coerce")
                    med0, lo0, hi0 = _safe_med_q(o.to_numpy(dtype=float))
                    rows.append(
                        {
                            "block": "Other (sum of remaining)",
                            "median_share": med0,
                            f"q{int(q_lo*100)}": lo0,
                            f"q{int(q_hi*100)}": hi0,
                            "in_top6": False,
                        }
                    )
                except Exception:
                    pass

                df_out = pd.DataFrame(rows)
                df_out = df_out.sort_values(by=["median_share"], ascending=False)
                (out_dir / f"{prefix}__{reg}.csv").write_text(df_out.to_csv(index=False), encoding="utf-8")
        except Exception:
            try:
                (fig_out.parent / f"{fig_out.stem}__BREAKDOWN.error.txt").write_text(traceback.format_exc(), encoding="utf-8")
            except Exception:
                pass

        for ax, reg in zip(axes, ["adverse", "severe", "crisis"]):
            mask = (df_sev["regime"].to_numpy() == reg)
            if not mask.any():
                ax.axis("off")
                continue
            w_med = df_share.loc[mask].median(axis=0).sort_values(ascending=False)
            top = w_med.head(6)
            other = max(0.0, float(1.0 - float(top.sum())))
            labels = [str(x) for x in top.index.tolist()] + (["Other"] if other > 1e-9 else [])
            sizes = top.to_numpy(dtype=float).tolist() + ([other] if other > 1e-9 else [])
            ax.pie(sizes, labels=None, autopct=lambda p: f"{p:.0f}%" if p >= 8 else "")
            ax.set_title(reg)

            if reg == "adverse":
                legend_labels = labels

        fig.suptitle(title)
        if legend_labels:
            fig.legend(legend_labels, loc="lower center", ncol=4, fontsize=8)
        fig.tight_layout(rect=(0, 0.08, 1, 0.95))
        fig_out.parent.mkdir(parents=True, exist_ok=True)
        _save_figure(fig, fig_out)
        plt.close(fig)

    lines: List[str] = []
    lines.append(f"# MC Block Comovements & Regimes — {run_name}")
    lines.append("")
    lines.append("This note summarizes how block aggregates co-move across Monte Carlo draws, and groups draws into coarse regime labels.")
    lines.append("")
    lines.append("## Regime construction (simple, explainable)")
    lines.append("- For each draw we compute each block’s **terminal cumulative block aggregate** (sigmas; z-space).")
    lines.append("- We compute an economy-level severity score per draw: $S_{L2}=\\sqrt{\\sum_b (\\text{cum}_b(T))^2}$. ")
    lines.append("- Regimes are defined by severity quantiles: **baseline** (0–50%), **adverse** (50–80%), **severe** (80–95%), **crisis** (95–100%).")
    lines.append("")

    q_lo, q_hi = 0.10, 0.90

    def _fmt_med_q(series: pd.Series) -> str:
        try:
            med = float(series.median())
            lo = float(series.quantile(q_lo))
            hi = float(series.quantile(q_hi))
            return f"{med:+.2f} [{lo:+.2f}, {hi:+.2f}]"
        except Exception:
            return "(n/a)"

    def _share_df(df_term: pd.DataFrame) -> pd.DataFrame:
        # Share of L2-severity contribution per column using squared terminal cumulatives.
        X2 = df_term.to_numpy(dtype=float) ** 2
        denom = X2.sum(axis=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            W = np.where(denom[:, None] > 0, X2 / denom[:, None], np.nan)
        out = pd.DataFrame(W, index=df_term.index, columns=df_term.columns)
        return out

    def _write_share_section(
        *,
        header: str,
        df_term: pd.DataFrame,
        df_sev: pd.DataFrame,
        top_cols: List[str],
        fig_out: Optional[Path] = None,
    ) -> None:
        lines.append(header)
        lines.append("Shares are fractions of $S_{L2}^2$ attributable to each block (using squared terminal cumulatives).")
        lines.append("Interpretation: a large share means the block contributes more to the **simulation severity metric** used here.")
        lines.append("This can happen because the block has larger tail dispersion / higher volatility / stronger cumulative drift over the horizon.")
        lines.append("It does **not** by itself prove the block has larger causal impact on GDP/PD/LGD; for that, compute shares in propagated target space.")
        lines.append(f"Reported as median share with quantile band q{int(q_lo*100)}/q{int(q_hi*100)} within each regime.")
        lines.append("")

        # Compute shares across *all* columns so the "Other" slice is interpretable.
        W = _share_df(df_term)
        for reg in ["adverse", "severe", "crisis"]:
            mask = (df_sev["regime"].to_numpy() == reg)
            if not mask.any():
                continue
            w_med = W.loc[mask].median(axis=0).sort_values(ascending=False)
            lines.append(f"**{reg}**")
            for c in w_med.head(10).index.tolist():
                s = W.loc[mask, c]
                try:
                    med = float(s.median())
                    lo = float(s.quantile(q_lo))
                    hi = float(s.quantile(q_hi))
                    lines.append(f"- {c}: {100*med:5.1f}% [{100*lo:5.1f}%, {100*hi:5.1f}%]")
                except Exception:
                    lines.append(f"- {c}: (n/a)")
            lines.append("")

        # Per-regime CSV breakdown so "Other" is enumerated.
        if fig_out is not None:
            try:
                def _safe_med_q(a: np.ndarray) -> Tuple[float, float, float]:
                    a = np.asarray(a, dtype=float)
                    a = a[np.isfinite(a)]
                    if a.size == 0:
                        return (float("nan"), float("nan"), float("nan"))
                    return (float(np.median(a)), float(np.quantile(a, q_lo)), float(np.quantile(a, q_hi)))

                prefix = fig_out.stem.replace("CAKE", "BREAKDOWN")
                if prefix == fig_out.stem:
                    prefix = f"{fig_out.stem}__BREAKDOWN"
                out_dir = fig_out.parent
                for reg in ["adverse", "severe", "crisis"]:
                    mask = (df_sev["regime"].to_numpy() == reg)
                    if not bool(np.any(mask)):
                        continue
                    Wreg = W.loc[mask]
                    med = Wreg.median(axis=0).sort_values(ascending=False)
                    top6 = med.head(6).index.tolist()
                    other_series = 1.0 - Wreg[top6].sum(axis=1, skipna=True)
                    other_series = other_series.clip(lower=0.0, upper=1.0)

                    rows: List[Dict[str, Any]] = []
                    for c in med.index.tolist():
                        s = pd.to_numeric(Wreg[c], errors="coerce")
                        med0, lo0, hi0 = _safe_med_q(s.to_numpy(dtype=float))
                        rows.append(
                            {
                                "block": str(c),
                                "median_share": med0,
                                f"q{int(q_lo*100)}": lo0,
                                f"q{int(q_hi*100)}": hi0,
                                "in_top6": bool(c in top6),
                            }
                        )
                    try:
                        o = pd.to_numeric(other_series, errors="coerce")
                        med0, lo0, hi0 = _safe_med_q(o.to_numpy(dtype=float))
                        rows.append(
                            {
                                "block": "Other (sum of remaining)",
                                "median_share": med0,
                                f"q{int(q_lo*100)}": lo0,
                                f"q{int(q_hi*100)}": hi0,
                                "in_top6": False,
                            }
                        )
                    except Exception:
                        pass

                    df_out = pd.DataFrame(rows)
                    df_out = df_out.sort_values(by=["median_share"], ascending=False)
                    (out_dir / f"{prefix}__{reg}.csv").write_text(df_out.to_csv(index=False), encoding="utf-8")

                lines.append("Breakdowns (full list; includes \"Other\"):")
                for reg in ["adverse", "severe", "crisis"]:
                    p = out_dir / f"{prefix}__{reg}.csv"
                    if p.exists():
                        try:
                            rel = p.relative_to(out_path.parent).as_posix()
                        except Exception:
                            rel = str(p.name)
                        rel_md = str(rel).replace(" ", "%20")
                        lines.append(f"- [{p.name}]({rel_md})")
                lines.append("")
            except Exception:
                try:
                    (fig_out.parent / f"{fig_out.stem}__BREAKDOWN.error.txt").write_text(traceback.format_exc(), encoding="utf-8")
                except Exception:
                    pass

        # Optional 'cake' image: 3 pies (adverse/severe/crisis)
        if fig_out is not None:
            try:
                import matplotlib

                matplotlib.use("Agg")
                import matplotlib.pyplot as plt

                fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(14, 4))
                for ax, reg in zip(axes, ["adverse", "severe", "crisis"]):
                    mask = (df_sev["regime"].to_numpy() == reg)
                    if not mask.any():
                        ax.axis("off")
                        continue
                    w_med = W.loc[mask].median(axis=0).sort_values(ascending=False)
                    # Top slices + Other
                    top = w_med.head(6)
                    other = max(0.0, float(1.0 - float(top.sum())))
                    labels = [str(x) for x in top.index.tolist()] + (["Other"] if other > 1e-9 else [])
                    sizes = top.to_numpy(dtype=float).tolist() + ([other] if other > 1e-9 else [])
                    ax.pie(sizes, labels=None, autopct=lambda p: f"{p:.0f}%" if p >= 8 else "")
                    ax.set_title(reg)
                fig.suptitle("Severity share decomposition (median within regime)")
                # Put a legend once (from first axis labels)
                handles = []
                legend_labels = []
                # Build legend entries from union of top labels in adverse
                mask_a = (df_sev["regime"].to_numpy() == "adverse")
                if mask_a.any():
                    w_med_a = W.loc[mask_a].median(axis=0).sort_values(ascending=False).head(6)
                    legend_labels = [str(x) for x in w_med_a.index.tolist()] + ["Other"]
                if legend_labels:
                    fig.legend(legend_labels, loc="lower center", ncol=4, fontsize=8)
                fig.tight_layout(rect=(0, 0.08, 1, 0.95))
                fig_out.parent.mkdir(parents=True, exist_ok=True)
                _save_figure(fig, fig_out)
                plt.close(fig)
                try:
                    rel_img = fig_out.relative_to(out_path.parent).as_posix()
                except Exception:
                    rel_img = str(fig_out.name)
                rel_img_md = str(rel_img).replace(" ", "%20")
                lines.append(f"![severity share cake]({rel_img_md})")
                lines.append("")
            except Exception:
                pass

    # Whole-economy view: stack all ISO/block terminal cumulatives into one vector per draw.
    try:
        term_all: Dict[str, np.ndarray] = {}
        min_n: Optional[int] = None
        for iso, block_mats in iso_to_block_mats.items():
            for block_key, M in (block_mats or {}).items():
                if M is None or M.size == 0:
                    continue
                n = int(M.shape[0])
                min_n = n if (min_n is None) else min(min_n, n)
        if min_n is not None and min_n > 0:
            for iso, block_mats in iso_to_block_mats.items():
                for block_key, M in (block_mats or {}).items():
                    if M is None or M.size == 0:
                        continue
                    M0 = np.where(np.isfinite(M[:min_n, :]), M[:min_n, :], 0.0)
                    term_all[f"{iso}/{block_key}"] = np.cumsum(M0, axis=1)[:, -1]

        if term_all:
            df_all = pd.DataFrame(term_all)
            sev_l2_all = np.sqrt((df_all.to_numpy(dtype=float) ** 2).sum(axis=1))
            q50, q80, q95 = np.quantile(sev_l2_all, [0.5, 0.8, 0.95])

            def label_all(s: float) -> str:
                if s <= q50:
                    return "baseline"
                if s <= q80:
                    return "adverse"
                if s <= q95:
                    return "severe"
                return "crisis"

            regimes_all = np.array([label_all(float(s)) for s in sev_l2_all], dtype=object)
            df_sev_all = pd.DataFrame({"severity_l2": sev_l2_all, "regime": regimes_all})

            lines.append("## All ISOs combined")
            lines.append("")
            counts = df_sev_all["regime"].value_counts().to_dict()
            lines.append("Regime counts (draws):")
            for k in ["baseline", "adverse", "severe", "crisis"]:
                lines.append(f"- {k}: {int(counts.get(k, 0))}")
            lines.append("")

            top_cols = df_all.var(axis=0).sort_values(ascending=False).head(12).index.tolist()
            lines.append("Top ISO/block columns by cross-draw variability:")
            lines.append("- " + ", ".join(str(c) for c in top_cols))
            lines.append("")

            lines.append(f"### Typical terminal cumulative (median with q{int(q_lo*100)}/q{int(q_hi*100)}; sigmas)")
            lines.append("")
            for reg in ["baseline", "adverse", "severe", "crisis"]:
                mask = (df_sev_all["regime"].to_numpy() == reg)
                if not mask.any():
                    continue
                med = df_all.loc[mask, top_cols].median(axis=0).sort_values(key=lambda s: s.abs(), ascending=False)
                lines.append(f"**{reg}**")
                for c in med.head(10).index.tolist():
                    lines.append(f"- {c}: {_fmt_med_q(df_all.loc[mask, c])}")
            lines.append("")

            _write_share_section(
                header="### Severity share decomposition (\"cake\" slices; All ISOs combined)",
                df_term=df_all,
                df_sev=df_sev_all,
                top_cols=top_cols,
                fig_out=(out_path.parent / "SEVERITY SHARE CAKES" / "SEVERITY_SHARE_CAKE__ALL_ISOS.png"),
            )

            # Publishable cross-country summary figures (computed from existing block matrices).
            try:
                # Use a harsher crisis tail for publishable cross-country plots.
                # Baseline: bottom 50% by severity. Crisis: top 1% by severity.
                sev_all = pd.to_numeric(df_sev_all["severity_l2"], errors="coerce").to_numpy(dtype=float)
                sev_f = sev_all[np.isfinite(sev_all)]
                if sev_f.size:
                    q_base = float(np.quantile(sev_f, 0.50))
                    q_p95 = float(np.quantile(sev_f, 0.95))
                    q_p99 = float(np.quantile(sev_f, 0.99))
                    baseline_mask_all = (sev_all <= q_base)
                    stress_mask_all_p95 = (sev_all >= q_p95)
                    crisis_mask_all = (sev_all >= q_p99)
                else:
                    baseline_mask_all = (df_sev_all["regime"].to_numpy() == "baseline")
                    crisis_mask_all = (df_sev_all["regime"].to_numpy() == "crisis")
                    stress_mask_all_p95 = np.asarray(crisis_mask_all, dtype=bool)

                # Ensure the new connectedness subfolder exists; also remove legacy root-level connectedness heatmaps.
                try:
                    conn_dir = out_path.parent / "CONNECTEDNESS DELTAS"
                    conn_dir.mkdir(parents=True, exist_ok=True)
                    for legacy in [
                        "CONNECTEDNESS_DELTA__CRISIS_MINUS_TODAY.png",
                        "CONNECTEDNESS_DELTA__CRISIS_MINUS_BASELINE.png",
                    ]:
                        try:
                            (out_path.parent / legacy).unlink(missing_ok=True)
                        except TypeError:
                            p_legacy = out_path.parent / legacy
                            if p_legacy.exists():
                                p_legacy.unlink()
                        except Exception:
                            pass
                except Exception:
                    pass

                df_iso_sev_rows: List[Dict[str, Any]] = []
                df_term_by_iso: Dict[str, pd.DataFrame] = {}
                crisis_mask_by_iso: Dict[str, np.ndarray] = {}
                stress_by_iso: Dict[str, np.ndarray] = {}

                # Optional realized "today" marker (computed from frozen inputs)
                today_days_note: Optional[int] = None
                today_end: Optional[pd.Timestamp] = None
                today_label = "Today"
                today_sev_by_iso: Dict[str, float] = {}
                today_stress_by_iso: Dict[str, np.ndarray] = {}
                today_end_by_iso: Dict[str, pd.Timestamp] = {}
                today_days_by_iso: Dict[str, int] = {}

                for iso2, block_mats2 in sorted(iso_to_block_mats.items()):
                    if not block_mats2:
                        continue
                    term2: Dict[str, np.ndarray] = {}
                    ss: Optional[np.ndarray] = None

                    for bk, M in (block_mats2 or {}).items():
                        if M is None or getattr(M, "size", 0) == 0:
                            continue
                        M0 = np.where(np.isfinite(M), M, 0.0)
                        cum = np.cumsum(M0, axis=1)
                        term2[str(bk)] = cum[:, -1]
                        if ss is None:
                            ss = np.zeros_like(cum, dtype=float)
                        ss += cum ** 2

                    if not term2 or ss is None:
                        continue

                    df_term2 = pd.DataFrame(term2)
                    df_term_by_iso[str(iso2)] = df_term2

                    sev2 = np.sqrt((df_term2.to_numpy(dtype=float) ** 2).sum(axis=1))
                    for v in sev2.tolist():
                        df_iso_sev_rows.append({"iso": str(iso2), "severity_l2": float(v)})

                    # Within-ISO crisis definition (P99+) for driver shares.
                    q50_2, q80_2, q99_2 = np.quantile(sev2, [0.5, 0.8, 0.99])
                    _ = q50_2, q80_2
                    crisis_mask_by_iso[str(iso2)] = (sev2 >= float(q99_2))

                    stress_by_iso[str(iso2)] = np.sqrt(ss)

                # Compute realized marker AFTER we know which ISOs exist.
                try:
                    H = int(today_window_days) if today_window_days is not None else 0
                except Exception:
                    H = 0
                if run_dir is not None and H > 1:
                    today_end_dates: List[pd.Timestamp] = []
                    today_days_used: List[int] = []
                    for iso2 in sorted(iso_to_block_mats.keys()):
                        factors_map = (iso_to_block_factors or {}).get(str(iso2)) or {}
                        if not factors_map:
                            continue
                        block_to_factors_real: Dict[str, Set[str]] = {
                            str(bk): set(str(x) for x in (fs or []))
                            for bk, fs in factors_map.items()
                            if fs
                        }
                        if not block_to_factors_real:
                            continue
                        end_date, n_days, sev_today, stress_today = _compute_realized_today_metrics(
                            run_dir=Path(run_dir),
                            iso=str(iso2),
                            block_to_factors=block_to_factors_real,
                            factor_to_vol=(vol_t0_by_iso.get(str(iso2)) or {}) if vol_t0_by_iso else {},
                            window_days=int(H),
                        )
                        if sev_today is not None:
                            try:
                                v = float(sev_today)
                            except Exception:
                                v = float("nan")
                            if np.isfinite(v):
                                today_sev_by_iso[str(iso2)] = v
                        if stress_today is not None and getattr(stress_today, "size", 0) >= 2:
                            today_stress_by_iso[str(iso2)] = np.asarray(stress_today, dtype=float)
                        if end_date is not None:
                            try:
                                end_dt = pd.to_datetime(end_date)
                                today_end_dates.append(end_dt)
                                today_end_by_iso[str(iso2)] = end_dt
                            except Exception:
                                pass
                        if n_days is not None:
                            try:
                                nd = int(n_days)
                                today_days_used.append(nd)
                                today_days_by_iso[str(iso2)] = nd
                            except Exception:
                                pass

                    today_end = max(today_end_dates) if today_end_dates else None
                    today_days_note = int(min(today_days_used)) if today_days_used else int(H)
                    today_label = "Today"

                df_iso_sev = pd.DataFrame(df_iso_sev_rows)
                if not df_iso_sev.empty:
                    _plot_severity_rank_png(
                        df_iso_sev=df_iso_sev,
                        fig_out=out_path.parent / "SEVERITY_RANKING__ISOS.png",
                        title=(
                            f"Cross-country severity ranking ({int(n_draws_mc)} MC draws)"
                            if (n_draws_mc is not None and int(n_draws_mc) > 0)
                            else "Cross-country severity ranking (MC draws)"
                        ),
                        today_by_iso=today_sev_by_iso or None,
                        today_label=today_label,
                        note=r"$S_{L2}$ = L2 norm of terminal cumulative block shocks (in $\sigma$ units)",
                    )
                    _plot_severity_box_png(
                        df_iso_sev=df_iso_sev,
                        fig_out=out_path.parent / "SEVERITY_DISTRIBUTION__ISOS.png",
                        title="Cross-country severity distributions (terminal; MC draws)",
                        today_by_iso=today_sev_by_iso or None,
                        today_label=today_label,
                    )

                if df_term_by_iso and crisis_mask_by_iso:
                    _plot_crisis_driver_stacks_png(
                        df_term_by_iso=df_term_by_iso,
                        crisis_mask_by_iso=crisis_mask_by_iso,
                        fig_out=out_path.parent / "CRISIS_DRIVERS__BLOCK_SHARES.png",
                        title="Crisis regime drivers",
                    )

                base_med = float("nan")
                cris_med = float("nan")
                if stress_by_iso and baseline_mask_all.any() and crisis_mask_all.any():
                    # Keep array lengths consistent with masks.
                    try:
                        n_ref = int(len(baseline_mask_all))
                        for k, v in list(stress_by_iso.items()):
                            a = np.asarray(v, dtype=float)
                            if a.ndim != 2 or a.shape[0] <= 1:
                                stress_by_iso.pop(k, None)
                                continue
                            if a.shape[0] < n_ref:
                                n_ref = int(a.shape[0])
                        if n_ref > 0 and n_ref < int(len(baseline_mask_all)):
                            baseline_mask_all = np.asarray(baseline_mask_all[:n_ref], dtype=bool)
                            stress_mask_all_p95 = np.asarray(stress_mask_all_p95[:n_ref], dtype=bool)
                            crisis_mask_all = np.asarray(crisis_mask_all[:n_ref], dtype=bool)
                            for k, v in list(stress_by_iso.items()):
                                stress_by_iso[k] = np.asarray(v, dtype=float)[:n_ref, :]
                    except Exception:
                        pass

                    conn_dir = out_path.parent / "CONNECTEDNESS DELTAS"
                    conn_dir.mkdir(parents=True, exist_ok=True)

                    # Cleanup legacy filenames so all connectedness heatmaps live under CONNECTEDNESS DELTAS/
                    for legacy in [
                        "CONNECTEDNESS_DELTA__CRISIS_MINUS_TODAY.png",
                        "CONNECTEDNESS_DELTA__CRISIS_MINUS_BASELINE.png",
                    ]:
                        try:
                            (out_path.parent / legacy).unlink(missing_ok=True)  # py3.8+: ignore if absent
                        except TypeError:
                            try:
                                p_legacy = out_path.parent / legacy
                                if p_legacy.exists():
                                    p_legacy.unlink()
                            except Exception:
                                pass
                        except Exception:
                            pass

                    # If realized "today" stress series exist for >=2 ISOs, use them as the baseline.
                    common_isos = sorted(set(stress_by_iso.keys()).intersection(set(today_stress_by_iso.keys())))
                    if len(common_isos) >= 2:
                        stress_mc_common = {k: stress_by_iso[k] for k in common_isos}
                        stress_today_common = {k: today_stress_by_iso[k] for k in common_isos}
                        _, C_today = _realized_corr_matrix(stress_today_by_iso=stress_today_common)
                        today_sync = _realized_sync_value(stress_today_by_iso=stress_today_common)

                        base_med, cris_med = _plot_sync_index_png(
                            stress_by_iso=stress_mc_common,
                            baseline_mask=baseline_mask_all,
                            crisis_mask=crisis_mask_all,
                            today_value=today_sync if np.isfinite(today_sync) else None,
                            today_label=str(today_label),
                            fig_out=out_path.parent / "SYNC_INDEX__TODAY_VS_CRISIS.png",
                            title="Stress synchronization index (avg cross-country corr; baseline ≤P50 vs crisis P99+; with realized today marker)",
                        )

                        # Connectedness deltas (heatmaps) — all under CONNECTEDNESS DELTAS/
                        if stress_mask_all_p95 is not None and np.asarray(stress_mask_all_p95, dtype=bool).any():
                            _plot_connectedness_delta_png(
                                stress_by_iso=stress_mc_common,
                                baseline_mask=baseline_mask_all,
                                crisis_mask=np.asarray(stress_mask_all_p95, dtype=bool),
                                baseline_label="baseline (≤P50)",
                                stress_label="stress (≥P95)",
                                baseline_vs_stress_fig_out=conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P95PLUS.png",
                                baseline_vs_stress_title="Connectedness matrices: baseline (≤P50) vs stress (≥P95)",
                                fig_out=conn_dir / "CONNECTEDNESS_DELTA__P95PLUS_MINUS_P50.png",
                                title="Connectedness delta: stress (≥P95) − baseline (≤P50) (corr of Δ stress index over time)",
                            )
                        _plot_connectedness_delta_png(
                            stress_by_iso=stress_mc_common,
                            baseline_mask=baseline_mask_all,
                            crisis_mask=crisis_mask_all,
                            baseline_label="baseline (≤P50)",
                            stress_label="stress (≥P99)",
                            baseline_vs_stress_fig_out=conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P99PLUS.png",
                            baseline_vs_stress_title="Connectedness matrices: baseline (≤P50) vs stress (≥P99)",
                            fig_out=conn_dir / "CONNECTEDNESS_DELTA__P99PLUS_MINUS_P50.png",
                            title="Connectedness delta: stress (≥P99) − baseline (≤P50) (corr of Δ stress index over time)",
                        )
                        _plot_connectedness_delta_png(
                            stress_by_iso=stress_mc_common,
                            baseline_mask=baseline_mask_all,
                            crisis_mask=crisis_mask_all,
                            baseline_corr_matrix=C_today,
                            baseline_label=str(today_label),
                            stress_label="stress (≥P99)",
                            baseline_vs_stress_fig_out=conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__TODAY_VS_P99PLUS.png",
                            baseline_vs_stress_title="Co-movement matrices: today vs Crisis",
                            fig_out=conn_dir / "CONNECTEDNESS_DELTA__P99PLUS_MINUS_TODAY.png",
                            title="Connectedness delta: stress (≥P99) − today (realized recent window) (corr of Δ stress index over time)",
                        )
                    else:
                        # Fallback: simulated baseline.
                        if stress_mask_all_p95 is not None and np.asarray(stress_mask_all_p95, dtype=bool).any():
                            _plot_connectedness_delta_png(
                                stress_by_iso=stress_by_iso,
                                baseline_mask=baseline_mask_all,
                                crisis_mask=np.asarray(stress_mask_all_p95, dtype=bool),
                                baseline_label="baseline (≤P50)",
                                stress_label="stress (≥P95)",
                                baseline_vs_stress_fig_out=conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P95PLUS.png",
                                baseline_vs_stress_title="Connectedness matrices: baseline (≤P50) vs stress (≥P95)",
                                fig_out=conn_dir / "CONNECTEDNESS_DELTA__P95PLUS_MINUS_P50.png",
                                title="Connectedness delta: stress (≥P95) − baseline (≤P50) (corr of Δ stress index over time)",
                            )
                        _plot_connectedness_delta_png(
                            stress_by_iso=stress_by_iso,
                            baseline_mask=baseline_mask_all,
                            crisis_mask=crisis_mask_all,
                            baseline_label="baseline (≤P50)",
                            stress_label="stress (≥P99)",
                            baseline_vs_stress_fig_out=conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P99PLUS.png",
                            baseline_vs_stress_title="Connectedness matrices: baseline (≤P50) vs stress (≥P99)",
                            fig_out=conn_dir / "CONNECTEDNESS_DELTA__P99PLUS_MINUS_P50.png",
                            title="Connectedness delta: stress (≥P99) − baseline (≤P50) (corr of Δ stress index over time)",
                        )
                        base_med, cris_med = _plot_sync_index_png(
                            stress_by_iso=stress_by_iso,
                            baseline_mask=baseline_mask_all,
                            crisis_mask=crisis_mask_all,
                            fig_out=out_path.parent / "SYNC_INDEX__BASELINE_VS_CRISIS.png",
                            title="Stress synchronization index (avg cross-country corr; baseline ≤P50 vs crisis P99+)",
                        )

                # Write executive summary markdown.
                exec_lines: List[str] = []
                exec_lines.append(f"# Monte Carlo Cross-Country Executive Summary — {run_name}")
                exec_lines.append("")
                exec_lines.append("This is generated from existing Step 12 block-aggregate matrices (z-space).")
                exec_lines.append("It is tail-focused and designed for cross-country comparability.")
                exec_lines.append("")
                rank_rows: List[Tuple[str, float, float]] = []
                if not df_iso_sev.empty:
                    rows: List[Tuple[str, float, float]] = []
                    for iso3, g3 in df_iso_sev.groupby("iso"):
                        s3 = pd.to_numeric(g3["severity_l2"], errors="coerce").dropna()
                        if s3.empty:
                            continue
                        rows.append((str(iso3), float(s3.median()), float(s3.quantile(0.99))))
                    rows.sort(key=lambda t: t[2], reverse=True)
                    rank_rows = rows[:]
                    exec_lines.append("## Tail severity ranking")
                    exec_lines.append("(Median and P99 of $S_{L2}$ across draws; higher = more severe simulated stress.)")
                    exec_lines.append("")
                    exec_lines.append("| ISO | Median | P99 |")
                    exec_lines.append("|---|---:|---:|")
                    for iso3, m3, p3 in rows:
                        exec_lines.append(f"| {iso3} | {m3:.2f} | {p3:.2f} |")
                    exec_lines.append("")

                # Realized today: show where we sit relative to the MC distribution (percentile)
                if today_sev_by_iso and not df_iso_sev.empty:
                    try:
                        exec_lines.append("## Today vs simulated distribution")
                        exec_lines.append("(Realized $S_{L2}$ computed from frozen inputs; percentile computed within each ISO’s MC draw distribution.)")
                        exec_lines.append("")
                        exec_lines.append("| ISO | Today $S_{L2}$ | Today percentile | MC median | MC P99 |")
                        exec_lines.append("|---|---:|---:|---:|---:|")

                        today_csv_rows: List[Dict[str, Any]] = []

                        # Prefer the same ordering as the tail ranking table.
                        iso_order = [r[0] for r in rank_rows] if rank_rows else sorted(df_iso_sev["iso"].astype(str).unique())
                        for iso3 in iso_order:
                            g3 = df_iso_sev[df_iso_sev["iso"].astype(str) == str(iso3)]
                            s3 = pd.to_numeric(g3["severity_l2"], errors="coerce").dropna()
                            if s3.empty:
                                continue
                            mc_med = float(s3.median())
                            mc_p99 = float(s3.quantile(0.99))
                            tv = today_sev_by_iso.get(str(iso3))
                            if tv is None:
                                continue
                            try:
                                tvf = float(tv)
                            except Exception:
                                continue
                            if not np.isfinite(tvf):
                                continue
                            pct = float(100.0 * float(np.mean(s3.to_numpy(dtype=float) <= tvf)))

                            exec_lines.append(f"| {iso3} | {tvf:.2f} | {pct:5.1f}% | {mc_med:.2f} | {mc_p99:.2f} |")
                            today_csv_rows.append(
                                {
                                    "iso": str(iso3),
                                    "today_severity_l2": tvf,
                                    "today_percentile_within_iso_mc": pct,
                                    "mc_median": mc_med,
                                    "mc_p99": mc_p99,
                                    "today_window_days": int(today_days_by_iso.get(str(iso3), today_days_note)) if today_days_note is not None else None,
                                    "today_end_date": (
                                        str(pd.to_datetime(today_end_by_iso.get(str(iso3), today_end)).date())
                                        if (today_end is not None or str(iso3) in today_end_by_iso)
                                        else None
                                    ),
                                }
                            )
                        exec_lines.append("")

                        if today_csv_rows:
                            pd.DataFrame(today_csv_rows).to_csv(out_path.parent / "TODAY_MARKER__SUMMARY.csv", index=False)
                    except Exception:
                        pass

                # Crisis drivers: add a compact concentration summary (HHI)
                try:
                    p_drv = out_path.parent / "CRISIS_DRIVERS__BLOCK_SHARES__BREAKDOWN.csv"
                    if p_drv.exists():
                        df_drv = pd.read_csv(p_drv)
                        if {"iso", "block", "median_share"}.issubset(set(df_drv.columns)):
                            exec_lines.append("## Crisis driver concentration")
                            exec_lines.append("(Computed from median crisis-regime shares of $S_{L2}^2$ by block; HHI near 1 = concentrated.)")
                            exec_lines.append("")
                            exec_lines.append("| ISO | Top block | Top share | HHI |")
                            exec_lines.append("|---|---|---:|---:|")

                            for iso3, g3 in df_drv.groupby("iso"):
                                s = pd.to_numeric(g3["median_share"], errors="coerce").fillna(0.0).to_numpy(dtype=float)
                                s = np.where(np.isfinite(s), s, 0.0)
                                tot = float(np.sum(s))
                                if tot > 0:
                                    s = s / tot
                                hhi = float(np.sum(s ** 2)) if s.size else float("nan")
                                try:
                                    i_top = int(np.argmax(s))
                                    top_share = float(s[i_top])
                                    top_block = str(g3.iloc[i_top]["block"])
                                except Exception:
                                    top_share = float("nan")
                                    top_block = "(n/a)"

                                if np.isfinite(top_share) and np.isfinite(hhi):
                                    exec_lines.append(f"| {str(iso3)} | {top_block} | {100*top_share:5.1f}% | {hhi:.2f} |")
                            exec_lines.append("")
                except Exception:
                    pass

                exec_lines.append("## Figures")
                conn_dir = out_path.parent / "CONNECTEDNESS DELTAS"
                fig_paths: List[Path] = [
                    out_path.parent / "SEVERITY_RANKING__ISOS.png",
                    out_path.parent / "SEVERITY_DISTRIBUTION__ISOS.png",
                    out_path.parent / "CRISIS_DRIVERS__BLOCK_SHARES.png",
                    out_path.parent / "SYNC_INDEX__TODAY_VS_CRISIS.png",
                    out_path.parent / "SYNC_INDEX__BASELINE_VS_CRISIS.png",
                    conn_dir / "CONNECTEDNESS_DELTA__P95PLUS_MINUS_P50.png",
                    conn_dir / "CONNECTEDNESS_DELTA__P99PLUS_MINUS_P50.png",
                    conn_dir / "CONNECTEDNESS_DELTA__P99PLUS_MINUS_TODAY.png",
                    conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P95PLUS.png",
                    conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__P50_VS_P99PLUS.png",
                    conn_dir / "CONNECTEDNESS_BASELINE_VS_STRESS__TODAY_VS_P99PLUS.png",
                ]
                for p in fig_paths:
                    if p.exists():
                        try:
                            rel = p.relative_to(out_path.parent).as_posix()
                        except Exception:
                            rel = str(p.name)
                        rel_md = str(rel).replace(" ", "%20")
                        exec_lines.append(f"- ![{p.name}]({rel_md})")
                exec_lines.append("")
                if today_sev_by_iso and today_days_note is not None:
                    exec_lines.append("## Realized today marker")
                    txt = f"Computed over the last {int(today_days_note)} days"
                    # End dates can differ by ISO if realized inputs have different last-available dates.
                    # Report this explicitly to avoid implying a single common window end.
                    try:
                        end_dates = [pd.to_datetime(v) for v in (today_end_by_iso or {}).values() if v is not None]
                    except Exception:
                        end_dates = []
                    if end_dates:
                        uniq = sorted({d.normalize() for d in end_dates})
                        if len(uniq) == 1:
                            txt += f" ending {uniq[0].date()}"
                        else:
                            # Small N in this report; list per ISO for transparency.
                            parts: List[str] = []
                            for iso_k in sorted((today_end_by_iso or {}).keys()):
                                try:
                                    d = pd.to_datetime((today_end_by_iso or {}).get(iso_k))
                                    parts.append(f"{iso_k}={d.date()}")
                                except Exception:
                                    continue
                            if parts:
                                txt += " (end date varies by ISO: " + ", ".join(parts) + ")"
                            else:
                                txt += f" (end date varies by ISO: {uniq[0].date()}..{uniq[-1].date()})"
                    txt += ": standardized residuals × Dt, scaled by vol_t0, low-frequency gated, then **demeaned per factor** before cumulation (to match mean-zero MC innovations), aggregated to blocks in z-space."
                    exec_lines.append(txt)
                    exec_lines.append("")
                if np.isfinite(base_med) and np.isfinite(cris_med):
                    exec_lines.append("## Synchronization takeaway")
                    exec_lines.append("(Computed as average cross-country correlation of first differences $\\Delta$stress; this avoids spurious inflation from shared drift.)")
                    exec_lines.append(
                        f"Average cross-country synchronization (median) rises from **{base_med:+.2f}** (baseline ≤P50) to **{cris_med:+.2f}** (crisis P99+)."
                    )
                    exec_lines.append("")

                (out_path.parent / "EXEC_SUMMARY.md").write_text("\n".join(exec_lines).rstrip() + "\n", encoding="utf-8")

                # Link from this comovement note for discoverability.
                lines.append("## Executive summary")
                lines.append("")
                if (out_path.parent / "EXEC_SUMMARY.md").exists():
                    lines.append("- [EXEC_SUMMARY.md](EXEC_SUMMARY.md)")
                for name in [
                    "SEVERITY_RANKING__ISOS.png",
                    "CRISIS_DRIVERS__BLOCK_SHARES.png",
                    "CONNECTEDNESS DELTAS/CONNECTEDNESS_DELTA__P99PLUS_MINUS_P50.png",
                ]:
                    p = out_path.parent / name
                    if p.exists():
                        rel_md = str(name).replace(" ", "%20")
                        lines.append(f"- ![{Path(name).name}]({rel_md})")
                lines.append("")
            except Exception:
                try:
                    (out_path.parent / "EXEC_SUMMARY.error.txt").write_text(traceback.format_exc(), encoding="utf-8")
                except Exception:
                    pass
    except Exception:
        pass

    for iso, block_mats in sorted(iso_to_block_mats.items()):
        if not block_mats:
            continue

        blocks = sorted(block_mats.keys())
        n_draws = None
        for b in blocks:
            M = block_mats[b]
            if M is not None and getattr(M, "size", 0):
                n_draws = int(M.shape[0])
                break
        if not n_draws:
            continue

        term: Dict[str, np.ndarray] = {}
        for b in blocks:
            M = block_mats[b]
            if M is None or M.size == 0:
                continue
            M0 = np.where(np.isfinite(M), M, 0.0)
            term[b] = np.cumsum(M0, axis=1)[:, -1]
        if not term:
            continue

        df = pd.DataFrame(term)
        sev_l2 = np.sqrt((df.to_numpy(dtype=float) ** 2).sum(axis=1))

        q50, q80, q95 = np.quantile(sev_l2, [0.5, 0.8, 0.95])

        def label(s: float) -> str:
            if s <= q50:
                return "baseline"
            if s <= q80:
                return "adverse"
            if s <= q95:
                return "severe"
            return "crisis"

        regimes = np.array([label(float(s)) for s in sev_l2], dtype=object)
        df_sev = pd.DataFrame({"severity_l2": sev_l2, "regime": regimes})

        lines.append(f"## {iso}")
        lines.append("")
        counts = df_sev["regime"].value_counts().to_dict()
        lines.append("Regime counts (draws):")
        for k in ["baseline", "adverse", "severe", "crisis"]:
            lines.append(f"- {k}: {int(counts.get(k, 0))}")
        lines.append("")

        top_blocks = df.var(axis=0).sort_values(ascending=False).head(12).index.tolist()
        lines.append("Top blocks by cross-draw variability (used for comovement summaries):")
        lines.append("- " + ", ".join(str(b) for b in top_blocks))
        lines.append("")

        lines.append(f"### Typical block terminal cumulative (median with q{int(q_lo*100)}/q{int(q_hi*100)}; sigmas)")
        lines.append("(Signs depend on factor definitions; focus on magnitude + co-movement patterns.)")
        lines.append("")
        for reg in ["baseline", "adverse", "severe", "crisis"]:
            mask = (df_sev["regime"].to_numpy() == reg)
            if not mask.any():
                continue
            med = df.loc[mask, top_blocks].median(axis=0).sort_values(key=lambda s: s.abs(), ascending=False)
            lines.append(f"**{reg}**")
            for b in med.head(8).index.tolist():
                lines.append(f"- {b}: {_fmt_med_q(df.loc[mask, b])}")
        lines.append("")

        # Outcome-space influence proxy using Step 4 mappings
        try:
            fc_csv = PROJECT_ROOT / "analysis_outputs" / f"feature_contributions_{iso}.csv"
            if fc_csv.exists() and (daily_source is not None) and (monte_carlo_dir is not None):
                fc = pd.read_csv(fc_csv)
                if {"target", "coefficients"}.issubset(set(fc.columns)):
                    # Keep all target-influence artifacts in one place.
                    ti_dir = out_path.parent / "TARGET INFLUENCE"
                    ti_dir.mkdir(parents=True, exist_ok=True)
                    # Cleanup legacy root-level target influence outputs.
                    try:
                        for legacy in out_path.parent.glob("TARGET_INFLUENCE_*"):
                            try:
                                legacy.unlink(missing_ok=True)
                            except TypeError:
                                if legacy.exists():
                                    legacy.unlink()
                            except Exception:
                                pass
                    except Exception:
                        pass

                    sim_list = (iso_to_sim_factors or {}).get(str(iso)) or []
                    sim_set = set(str(x) for x in sim_list)
                    if sim_set:
                        low_set = (lowfreq_by_iso or {}).get(str(iso)) or set()
                        factor_to_vol = (vol_t0_by_iso or {}).get(str(iso)) or {}

                        # Parse per-target coefficient maps and resolve to simulated factors
                        target_rows: List[Dict[str, Any]] = []
                        need_daily: Set[str] = set()
                        need_monthly: Set[str] = set()

                        for r in fc.itertuples(index=False):
                            try:
                                target = str(getattr(r, "target"))
                            except Exception:
                                continue
                            coef_text = getattr(r, "coefficients", "")
                            coef_map = _parse_coefficients_map(coef_text)
                            if not coef_map:
                                continue

                            resolved: Dict[str, float] = {}
                            ignored: List[str] = []
                            abs_total = float(np.sum([abs(float(v)) for v in coef_map.values() if np.isfinite(float(v))]))
                            abs_matched = 0.0
                            for feat, coef in coef_map.items():
                                fac = _resolve_sim_factor(feat, sim_set)
                                if fac is None:
                                    ignored.append(str(feat))
                                    continue
                                resolved[fac] = resolved.get(fac, 0.0) + float(coef)
                                abs_matched += abs(float(coef))

                                if str(fac) in low_set:
                                    need_monthly.add(str(fac))
                                else:
                                    need_daily.add(str(fac))

                            target_rows.append(
                                {
                                    "target": target,
                                    "coef_map": resolved,
                                    "abs_total": abs_total,
                                    "abs_matched": abs_matched,
                                    "ignored": ignored,
                                    "test_r2": getattr(r, "test_r2", None),
                                    "permutation_pvalue": getattr(r, "permutation_pvalue", None),
                                    "target_transform": getattr(r, "target_transform", None),
                                    "feature_source_used": getattr(r, "feature_source_used", None),
                                }
                            )

                        if target_rows and (need_daily or need_monthly):
                            # Compute terminal cumulative shocks for needed factors
                            term_daily = _compute_terminal_factor_z_sums_daily(
                                source=daily_source,
                                iso=str(iso),
                                factors=set(need_daily),
                                n_draws=int(n_draws),
                                factor_to_vol=factor_to_vol,
                            )
                            term_monthly: Dict[str, np.ndarray] = {}
                            if need_monthly:
                                macro_csv = Path(monte_carlo_dir) / "macro_monthly_draws.csv"
                                term_monthly = _compute_terminal_factor_z_sums_monthly(
                                    macro_csv=macro_csv,
                                    iso=str(iso),
                                    factors=set(need_monthly),
                                    n_draws=int(n_draws),
                                    factor_to_vol=factor_to_vol,
                                )

                            term_factor: Dict[str, np.ndarray] = {}
                            term_factor.update(term_daily)
                            term_factor.update(term_monthly)

                            # Build factor -> blocks (split equally if factor belongs to multiple blocks)
                            factors_map = (iso_to_block_factors or {}).get(str(iso)) or {}
                            f_to_blocks: Dict[str, List[str]] = {}
                            for bk, flist in (factors_map or {}).items():
                                for f in flist or []:
                                    f_to_blocks.setdefault(str(f), []).append(str(bk))

                            lines.append("### Outcome-space influence proxy (Step 4 targets)")
                            lines.append("This section projects simulated factor shocks into Step 4 targets using the stored linear coefficients.")
                            lines.append("Important: this is a **proxy** because Step 4 feature scaling may differ from the Step 12 simulation shock units.")
                            lines.append("We use **terminal cumulative standardized shocks** (sum of daily/monthly $z$ shocks) and ignore AR target lags when they are not simulated.")
                            lines.append("")

                            for tr in target_rows:
                                tgt = str(tr["target"])
                                coef_map = tr["coef_map"]
                                if not coef_map:
                                    continue

                                # Compose predicted target impact per draw + per-block contributions
                                block_contrib: Dict[str, np.ndarray] = {}
                                unattributed = np.zeros(int(n_draws), dtype=float)

                                for fac, coef in coef_map.items():
                                    v = term_factor.get(str(fac))
                                    if v is None:
                                        continue
                                    contrib = float(coef) * v
                                    blocks_for_f = f_to_blocks.get(str(fac)) or []
                                    if not blocks_for_f:
                                        unattributed += contrib
                                        continue
                                    w = 1.0 / float(len(blocks_for_f))
                                    for bk in blocks_for_f:
                                        block_contrib.setdefault(str(bk), np.zeros(int(n_draws), dtype=float))
                                        block_contrib[str(bk)] += w * contrib

                                if unattributed.any():
                                    block_contrib.setdefault("UNMAPPED", np.zeros(int(n_draws), dtype=float))
                                    block_contrib["UNMAPPED"] += unattributed

                                if not block_contrib:
                                    continue

                                df_c = pd.DataFrame(block_contrib)
                                y = df_c.sum(axis=1)

                                # Share decomposition uses squared contributions (sign-agnostic attribution of magnitude)
                                X2 = df_c.to_numpy(dtype=float) ** 2
                                denom = X2.sum(axis=1)
                                with np.errstate(divide="ignore", invalid="ignore"):
                                    W = np.where(denom[:, None] > 0, X2 / denom[:, None], np.nan)
                                df_share = pd.DataFrame(W, index=df_c.index, columns=df_c.columns)

                                # Model quality context
                                try:
                                    r2 = float(tr.get("test_r2")) if tr.get("test_r2") is not None else float("nan")
                                except Exception:
                                    r2 = float("nan")
                                try:
                                    pv = float(tr.get("permutation_pvalue")) if tr.get("permutation_pvalue") is not None else float("nan")
                                except Exception:
                                    pv = float("nan")
                                abs_total = float(tr.get("abs_total") or 0.0)
                                abs_matched = float(tr.get("abs_matched") or 0.0)
                                cov = (abs_matched / abs_total) if abs_total > 0 else float("nan")

                                lines.append(f"#### {tgt}")
                                tt = tr.get("target_transform")
                                src = tr.get("feature_source_used")
                                meta_bits = []
                                if tt:
                                    meta_bits.append(f"transform={tt}")
                                if src:
                                    meta_bits.append(f"features={src}")
                                if np.isfinite(r2):
                                    meta_bits.append(f"test_r2={r2:.3f}")
                                if np.isfinite(pv):
                                    meta_bits.append(f"perm_p={pv:.3g}")
                                if np.isfinite(cov):
                                    meta_bits.append(f"coef_coverage≈{100*cov:.1f}%")
                                if meta_bits:
                                    lines.append("- " + "; ".join(meta_bits))
                                if np.isfinite(r2) and (r2 < 0.10):
                                    lines.append("- WARNING: Low test $R^2$: treat regime/attribution patterns as low confidence.")
                                if np.isfinite(pv) and (pv > 0.10):
                                    lines.append("- WARNING: High permutation p-value: weak signal; attribution may be unstable.")

                                ignored = tr.get("ignored") or []
                                if ignored:
                                    shown = ", ".join(str(x) for x in ignored[:8])
                                    more = "" if len(ignored) <= 8 else f" (+{len(ignored)-8} more)"
                                    lines.append(f"- Ignored non-simulated features (often AR lags): {shown}{more}")
                                lines.append("")

                                # Regime-conditioned target impact summary (signed)
                                q_lo_t, q_hi_t = q_lo, q_hi
                                lines.append(f"Typical projected target impact (median with q{int(q_lo_t*100)}/q{int(q_hi_t*100)}; arbitrary units)")
                                for reg in ["baseline", "adverse", "severe", "crisis"]:
                                    mask = (df_sev["regime"].to_numpy() == reg)
                                    if not mask.any():
                                        continue
                                    ser = y.loc[mask]
                                    try:
                                        med = float(ser.median())
                                        lo = float(ser.quantile(q_lo_t))
                                        hi = float(ser.quantile(q_hi_t))
                                        lines.append(f"- {reg}: {med:+.3f} [{lo:+.3f}, {hi:+.3f}]")
                                    except Exception:
                                        lines.append(f"- {reg}: (n/a)")
                                lines.append("")

                                # Influence shares by block (top slices)
                                lines.append(f"Influence share decomposition by block (median share with q{int(q_lo*100)}/q{int(q_hi*100)} within regime)")
                                for reg in ["adverse", "severe", "crisis"]:
                                    mask = (df_sev["regime"].to_numpy() == reg)
                                    if not mask.any():
                                        continue
                                    w_med = df_share.loc[mask].median(axis=0).sort_values(ascending=False)
                                    lines.append(f"**{reg}**")
                                    try:
                                        nontriv = int((pd.to_numeric(w_med, errors="coerce").fillna(0.0) > 1e-6).sum())
                                        if nontriv <= 1:
                                            lines.append("Note: attribution is degenerate here (only one block has non-trivial mapped contribution); interpret shares as coverage/mapping-limited.")
                                    except Exception:
                                        pass
                                    for c in w_med.head(8).index.tolist():
                                        s = df_share.loc[mask, c]
                                        try:
                                            med = float(s.median())
                                            lo = float(s.quantile(q_lo))
                                            hi = float(s.quantile(q_hi))
                                            lines.append(f"- {c}: {100*med:5.1f}% [{100*lo:5.1f}%, {100*hi:5.1f}%]")
                                        except Exception:
                                            lines.append(f"- {c}: (n/a)")
                                    lines.append("")

                                # Cake plot
                                try:
                                    slug = _safe_slug(tgt)
                                    fig_out = ti_dir / f"TARGET_INFLUENCE_CAKE__{iso}__{slug}.png"
                                    _write_target_influence_cake(
                                        fig_out=fig_out,
                                        df_share=df_share,
                                        df_sev=df_sev,
                                        title=f"Target influence shares (median within regime) — {iso} — {tgt}",
                                    )
                                    rel_img = fig_out.relative_to(out_path.parent).as_posix().replace(" ", "%20")
                                    lines.append(f"![target influence cake]({rel_img})")
                                    # Link the explicit breakdowns so the "Other" slice is transparent.
                                    try:
                                        prefix = fig_out.stem.replace("CAKE", "BREAKDOWN")
                                        if prefix == fig_out.stem:
                                            prefix = f"{fig_out.stem}__BREAKDOWN"
                                        out_dir = fig_out.parent
                                        avail: List[str] = []
                                        for reg in ["adverse", "severe", "crisis"]:
                                            p = out_dir / f"{prefix}__{reg}.csv"
                                            if p.exists():
                                                avail.append(p.relative_to(out_path.parent).as_posix())
                                        if avail:
                                            lines.append("Breakdowns (full list; includes \"Other\"):")
                                            for name in avail:
                                                rel = str(name).replace(" ", "%20")
                                                disp = Path(str(name)).name
                                                lines.append(f"- [{disp}]({rel})")
                                    except Exception:
                                        pass
                                    lines.append("")
                                except Exception:
                                    pass

        except Exception:
            pass

        # "Cake" slices: which blocks contribute to severity in adverse regimes.
        try:
            sev_share_dir = out_path.parent / "SEVERITY SHARE CAKES"
            sev_share_dir.mkdir(parents=True, exist_ok=True)
            # Cleanup legacy root-level severity-share outputs.
            try:
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.png"):
                    legacy.unlink(missing_ok=True)
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.pdf"):
                    legacy.unlink(missing_ok=True)
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.svg"):
                    legacy.unlink(missing_ok=True)
                for legacy in out_path.parent.glob("SEVERITY_SHARE_BREAKDOWN__*.csv"):
                    legacy.unlink(missing_ok=True)
            except TypeError:
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.png"):
                    if legacy.exists():
                        legacy.unlink()
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.pdf"):
                    if legacy.exists():
                        legacy.unlink()
                for legacy in out_path.parent.glob("SEVERITY_SHARE_CAKE__*.svg"):
                    if legacy.exists():
                        legacy.unlink()
                for legacy in out_path.parent.glob("SEVERITY_SHARE_BREAKDOWN__*.csv"):
                    if legacy.exists():
                        legacy.unlink()
            except Exception:
                pass
            _write_share_section(
                header="### Severity share decomposition (\"cake\" slices)",
                df_term=df,
                df_sev=df_sev,
                top_cols=top_blocks,
                fig_out=sev_share_dir / f"SEVERITY_SHARE_CAKE__{iso}.png",
            )
        except Exception:
            pass

        # Comovement: correlation across draws of terminal cumulative
        try:
            corr_raw = df[top_blocks].corr()
            corr = corr_raw.fillna(0.0)
            lines.append("### Comovement snapshot (corr of terminal cumulative across draws)")
            lines.append("Positive = blocks tend to move together across scenarios; negative = trade-offs.")
            lines.append("")
            pairs: List[Tuple[str, str, float]] = []
            cols = list(corr.columns)
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    pairs.append((cols[i], cols[j], float(corr.iat[i, j])))
            pairs.sort(key=lambda t: abs(t[2]), reverse=True)
            for a, b, v in pairs[:12]:
                lines.append(f"- {a} ↔ {b}: corr={v:+.2f}")
            lines.append("")

            # Diagnostics for suspicious near-perfect correlations.
            suspicious: List[Tuple[str, str, float]] = []
            for a, b, _ in pairs[: min(200, len(pairs))]:
                try:
                    v = float(corr_raw.loc[a, b])
                except Exception:
                    continue
                if not np.isfinite(v):
                    continue
                if abs(v) >= 0.999:
                    suspicious.append((a, b, v))

            if suspicious:
                lines.append("### Perfect-corr diagnostics (red flag check)")
                lines.append("Near-perfect correlations can mean: shared factor membership, duplicated mappings, or one block being (almost) a scalar multiple of another.")
                lines.append("")

                factors_map = (iso_to_block_factors or {}).get(str(iso)) or {}
                for a, b, v in suspicious[:6]:
                    a_vec = df[a].to_numpy(dtype=float)
                    b_vec = df[b].to_numpy(dtype=float)
                    std_a = float(np.nanstd(a_vec))
                    std_b = float(np.nanstd(b_vec))
                    z_a = float(np.mean(np.isclose(a_vec, 0.0, atol=1e-12)))
                    z_b = float(np.mean(np.isclose(b_vec, 0.0, atol=1e-12)))
                    identical = bool(np.allclose(a_vec, b_vec, atol=1e-12, rtol=1e-9))
                    neg_identical = bool(np.allclose(a_vec, -b_vec, atol=1e-12, rtol=1e-9))

                    # Scalar-multiple check: k minimizing ||b - k a||
                    denom = float(np.dot(a_vec, a_vec))
                    k = float(np.dot(a_vec, b_vec) / denom) if denom > 0 else float("nan")
                    resid = b_vec - k * a_vec if np.isfinite(k) else (b_vec * np.nan)
                    rel_resid = float(np.nanstd(resid) / (std_b + 1e-12)) if np.isfinite(std_b) else float("nan")

                    fa = set(str(x) for x in (factors_map.get(str(a)) or []))
                    fb = set(str(x) for x in (factors_map.get(str(b)) or []))
                    overlap = len(fa & fb) if (fa or fb) else 0
                    union = len(fa | fb) if (fa or fb) else 0
                    jacc = (overlap / union) if union else 0.0

                    lines.append(f"- **{a} ↔ {b}**: corr={v:+.4f}; std=({std_a:.3g}, {std_b:.3g}); zero%≈({100*z_a:.1f}%, {100*z_b:.1f}%)")
                    if identical:
                        lines.append("  - Looks **identical** across draws (possible duplicate mapping / same factors).")
                    elif neg_identical:
                        lines.append("  - Looks **sign-flipped identical** (possible sign convention mismatch).")
                    elif np.isfinite(rel_resid) and rel_resid < 1e-3:
                        lines.append(f"  - Looks like a **scalar multiple**: b≈{k:+.3f}·a (tiny relative residual).")
                    if union:
                        lines.append(f"  - Factor membership overlap: {overlap}/{union} (Jaccard={jacc:.2f}).")

                lines.append("")
                lines.append("Suggested fixes if this is unintended:")
                lines.append("- Ensure banking_system and real_estate blocks do **not** share the same mapped factors.")
                lines.append("- Check duplicates in block definitions (see duplicates_in_block_def.csv) and unmapped factors.")
                lines.append("- If one block is structurally a proxy of the other, keep both but treat corr≈1 as expected and document it.")
                lines.append("")
        except Exception:
            pass

    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_csv_list(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [p.strip() for p in str(text).split(",") if p.strip()]


def _find_run_dir(*, run_id: Optional[str], use_latest: bool) -> Path:
    if use_latest or (run_id is None):
        return SCENARIOS_DIR / "latest"
    return SCENARIOS_DIR / str(run_id)


def _load_block_definitions(path: Path) -> Dict[str, Dict[str, List[str]]]:
    """Return mapping: iso -> block_key -> series_codes"""
    raw = json.loads(path.read_text(encoding="utf-8"))
    out: Dict[str, Dict[str, List[str]]] = {}
    for iso, payload in raw.items():
        blocks = payload.get("blocks") or []
        iso_map: Dict[str, List[str]] = {}
        for b in blocks:
            key = str(b.get("key") or "unknown")
            series = [str(s) for s in (b.get("series_codes") or [])]
            iso_map[key] = series
        out[str(iso)] = iso_map
    return out


def _load_mc_factor_list(run_dir: Path, *, iso: str) -> List[str]:
    """Load factor names actually used in MC simulation.

    We source these from the frozen Step 9 inputs (Dt_daily header), which is
    stable and fast to read.
    """
    dt_csv = run_dir / "inputs" / str(iso) / "covariance" / f"{iso}_Dt_daily.csv"
    if not dt_csv.exists():
        return []
    cols = list(pd.read_csv(dt_csv, nrows=0).columns)
    out: List[str] = []
    for c in cols:
        c = str(c)
        if c in {"date", "Rt_daily"}:
            continue
        out.append(c)
    return out


def _load_realized_inputs_wide(*, run_dir: Path, iso: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load realized standardized residuals and conditional vol (Dt) for an ISO.

    Expected under: <run_dir>/inputs/<ISO>/covariance/
      - <ISO>_standardized_residuals_daily.csv
      - <ISO>_Dt_daily.csv

    Returns (df_z, df_dt) with a parsed datetime 'date' column.
    """
    base = run_dir / "inputs" / str(iso) / "covariance"
    z_csv = base / f"{iso}_standardized_residuals_daily.csv"
    dt_csv = base / f"{iso}_Dt_daily.csv"
    if not z_csv.exists() or not dt_csv.exists():
        return (pd.DataFrame(), pd.DataFrame())
    try:
        df_z = pd.read_csv(z_csv)
        df_dt = pd.read_csv(dt_csv)
    except Exception:
        return (pd.DataFrame(), pd.DataFrame())
    if "date" in df_z.columns:
        df_z["date"] = pd.to_datetime(df_z["date"], errors="coerce")
    if "date" in df_dt.columns:
        df_dt["date"] = pd.to_datetime(df_dt["date"], errors="coerce")
    df_z = df_z.dropna(subset=["date"]) if "date" in df_z.columns else pd.DataFrame()
    df_dt = df_dt.dropna(subset=["date"]) if "date" in df_dt.columns else pd.DataFrame()
    return (df_z, df_dt)


def _compute_realized_today_metrics(
    *,
    run_dir: Path,
    iso: str,
    block_to_factors: Dict[str, Set[str]],
    factor_to_vol: Dict[str, float],
    window_days: int,
) -> Tuple[Optional[pd.Timestamp], Optional[int], Optional[float], Optional[np.ndarray]]:
    """Compute realized 'today' severity and stress index over last H days.

    Severity uses the same definition as MC draws but over a realized window:
        - reconstruct innovations: shock = z_t * Dt_t
        - low-frequency gating (release-date impulses only) to match MC behavior
        - standardize to z-like 'sigmas' via vol_t0: shock_z = shock / vol_t0
        - IMPORTANT: de-mean (per factor) the gated shock_z series over a lookback
          window before cumulation so realized shocks are consistent with the mean-zero
          innovation assumption used by MC draws.
        - per block: mean shock_z across mapped factors
        - cumulative per block; terminal cumulative per block
        - severity_today = L2 norm across blocks of terminal cumulative
        - stress_today(t) = L2 norm across blocks of cumulative(t)

    Returns: (end_date, n_days_used, severity_today, stress_series)
    """
    H = int(window_days) if window_days is not None else 0
    if H <= 1:
        return (None, None, None, None)

    df_z, df_dt = _load_realized_inputs_wide(run_dir=run_dir, iso=iso)
    if df_z.empty or df_dt.empty:
        return (None, None, None, None)
    if "date" not in df_z.columns or "date" not in df_dt.columns:
        return (None, None, None, None)

    # Align on dates
    try:
        df = df_z.merge(df_dt, on="date", how="inner", suffixes=("__z", "__dt"))
    except Exception:
        return (None, None, None, None)
    if df.empty:
        return (None, None, None, None)

    df = df.sort_values("date")

    # Determine factors we can use (must exist in both files)
    union_factors: Set[str] = set()
    for fs in (block_to_factors or {}).values():
        union_factors |= set(str(x) for x in (fs or set()))
    if not union_factors:
        return (None, None, None, None)

    # In the merged df, columns became <factor>__z and <factor>__dt
    usable_factors: List[str] = []
    for f in sorted(union_factors):
        if f"{f}__z" in df.columns and f"{f}__dt" in df.columns:
            usable_factors.append(str(f))
    if not usable_factors:
        return (None, None, None, None)

    # Use full aligned history for gating + de-meaning inference, then slice last H days.
    df_win = df.tail(H)
    if df_win.empty:
        return (None, None, None, None)

    # Build per-factor arrays (vectorized)
    Z_all = df[[f"{f}__z" for f in usable_factors]].apply(pd.to_numeric, errors="coerce")
    Dt_all = df[[f"{f}__dt" for f in usable_factors]].apply(pd.to_numeric, errors="coerce")
    vols = pd.Series({f: float(factor_to_vol.get(f, 1.0) or 1.0) for f in usable_factors})
    vols = pd.to_numeric(vols, errors="coerce").fillna(1.0).replace(0.0, 1.0)

    # Low-frequency gating: MC daily_draws apply shocks only on factor-specific
    # update dates for low-frequency series (else shock = 0). If we treat realized
    # z_t as a daily shock for low-frequency factors, we will massively overstate
    # realized severity versus MC (because Dt is typically constant between releases).
    #
    # We infer low-frequency factors from Dt change frequency and gate them using
    # Dt step-change dates, mirroring Step 12.0's behavior.
    dt_change_tol = 1e-9
    lowfreq_frac_threshold = 0.10
    lookback = int(min(756, len(df)))

    dt_diff = Dt_all.diff().abs()
    dt_change = (dt_diff > float(dt_change_tol))
    if not dt_change.empty:
        try:
            dt_change.iloc[0, :] = True
        except Exception:
            pass

    if len(dt_change) > lookback:
        dt_change_lb = dt_change.tail(lookback)
    else:
        dt_change_lb = dt_change

    lowfreq_factors: Set[str] = set()
    try:
        frac_change = dt_change_lb.mean(axis=0)
        for i, f in enumerate(usable_factors):
            try:
                v = float(frac_change.iloc[i])
            except Exception:
                v = float("nan")
            if np.isfinite(v) and v < float(lowfreq_frac_threshold):
                lowfreq_factors.add(str(f))
    except Exception:
        lowfreq_factors = set()

    # Build a full-history gating matrix so we can de-mean in a way consistent with MC.
    gate_all = np.ones((int(len(df)), int(len(usable_factors))), dtype=float)
    if lowfreq_factors and not dt_change.empty:
        for j, f in enumerate(usable_factors):
            if str(f) not in lowfreq_factors:
                continue
            try:
                gate_all[:, j] = dt_change.iloc[:, j].to_numpy(dtype=bool).astype(float)
            except Exception:
                pass

    denom = vols.to_numpy(dtype=float)[None, :]
    # shock_z_all = (z * Dt) / vol_t0 (with lowfreq gating)
    try:
        shock_all = Z_all.to_numpy(dtype=float) * Dt_all.to_numpy(dtype=float)
    except Exception:
        shock_all = Z_all.to_numpy() * Dt_all.to_numpy()
        shock_all = shock_all.astype(float, copy=False)
    with np.errstate(divide="ignore", invalid="ignore"):
        shock_z_all = (shock_all / denom) * gate_all

    # De-mean per factor over a lookback window so cumulation doesn't inherit
    # nonzero means from input artifacts (MC innovations are mean-zero).
    demean_lookback = int(min(756, shock_z_all.shape[0]))
    if demean_lookback >= 10:
        lb = shock_z_all[-demean_lookback:, :]
    else:
        lb = shock_z_all
    with np.errstate(invalid="ignore"):
        mu = np.nanmean(lb, axis=0, keepdims=True)

    shock_z_win = shock_z_all[-int(len(df_win)) :, :] - mu

    # Per block: mean across factors
    T = int(shock_z_win.shape[0])
    block_keys: List[str] = []
    block_mat: List[np.ndarray] = []
    f_index = {f: i for i, f in enumerate(usable_factors)}
    for bk, fs in sorted((block_to_factors or {}).items()):
        idxs = [f_index.get(str(f)) for f in (fs or set())]
        idxs = [i for i in idxs if i is not None]
        if not idxs:
            continue
        A = shock_z_win[:, idxs]
        with np.errstate(invalid="ignore"):
            m = np.nanmean(A, axis=1)
        if m.size != T:
            continue
        block_keys.append(str(bk))
        block_mat.append(np.asarray(m, dtype=float))

    if not block_mat:
        return (None, None, None, None)

    B = np.vstack(block_mat)  # (n_blocks, T)
    B0 = np.where(np.isfinite(B), B, 0.0)
    cum = np.cumsum(B0, axis=1)
    term = cum[:, -1]
    sev = float(np.sqrt(np.sum(term ** 2)))
    stress = np.sqrt(np.sum(cum ** 2, axis=0))

    end_date = None
    try:
        end_date = pd.to_datetime(df_win["date"].iloc[-1])
    except Exception:
        end_date = None
    return (end_date, int(T), sev if np.isfinite(sev) else None, stress.astype(float))


def _realized_corr_matrix(*, stress_today_by_iso: Dict[str, np.ndarray]) -> Tuple[List[str], np.ndarray]:
    isos = sorted(stress_today_by_iso.keys())
    n = len(isos)
    if n < 2:
        return (isos, np.full((n, n), np.nan))
    out = np.eye(n, dtype=float)
    for i in range(n):
        for j in range(i + 1, n):
            a = np.asarray(stress_today_by_iso[isos[i]], dtype=float)
            b = np.asarray(stress_today_by_iso[isos[j]], dtype=float)
            ad = np.diff(a) if a.size >= 3 else a
            bd = np.diff(b) if b.size >= 3 else b
            c = _corr_over_time(ad, bd)
            out[i, j] = float(c)
            out[j, i] = float(c)
    return (isos, out)


def _realized_sync_value(*, stress_today_by_iso: Dict[str, np.ndarray]) -> float:
    isos = sorted(stress_today_by_iso.keys())
    if len(isos) < 2:
        return float("nan")
    vals: List[float] = []
    for i in range(len(isos)):
        for j in range(i + 1, len(isos)):
            a = np.asarray(stress_today_by_iso[isos[i]], dtype=float)
            b = np.asarray(stress_today_by_iso[isos[j]], dtype=float)
            ad = np.diff(a) if a.size >= 3 else a
            bd = np.diff(b) if b.size >= 3 else b
            c = _corr_over_time(ad, bd)
            if np.isfinite(c):
                vals.append(float(c))
    return float(np.mean(vals)) if vals else float("nan")


def _strip_lag_suffix(name: str) -> str:
    # Handles ..._lag0, ..._lag1, etc.
    if "_lag" not in name:
        return name
    base, maybe = name.rsplit("_lag", 1)
    if maybe.isdigit():
        return base
    return name


def _reverse_block_map(block_defs: Dict[str, Dict[str, List[str]]]) -> Tuple[
    Dict[str, Dict[str, Set[str]]],
    List[Dict[str, Any]],
]:
    """Return (iso -> series_code -> set(block_key), duplicates_rows)."""
    out: Dict[str, Dict[str, Set[str]]] = {}
    dup_rows: List[Dict[str, Any]] = []
    for iso, blocks in block_defs.items():
        m: Dict[str, Set[str]] = {}
        for block_key, series in blocks.items():
            for s in series:
                k = str(s)
                m.setdefault(k, set()).add(str(block_key))

        # Track duplicates
        for series_code, keys in m.items():
            if len(keys) > 1:
                dup_rows.append(
                    {
                        "iso": str(iso),
                        "series_code": str(series_code),
                        "blocks": ";".join(sorted(keys)),
                        "n_blocks": int(len(keys)),
                    }
                )

        out[str(iso)] = m
    return out, dup_rows


def _auto_select_block_def(run_dir: Path, *, iso: str) -> Path:
    """Pick a sensible block-definition file for plotting.

    If the MC factors look like ISO_block_f1 (literature-style), use the
    literature block definition. Otherwise, use the raw governed block
    definition.
    """
    factors = _load_mc_factor_list(run_dir, iso=iso)
    if factors and any(f.startswith(f"{iso}_") and f.endswith("_f1") for f in factors):
        if LITERATURE_BLOCK_DEF_DEFAULT.exists():
            return LITERATURE_BLOCK_DEF_DEFAULT
    return BLOCK_DEF_DEFAULT


def _load_representative_draw_ids(reps_json: Path, *, max_draws: int) -> List[int]:
    if not reps_json.exists():
        return list(range(max_draws))
    try:
        payload = json.loads(reps_json.read_text(encoding="utf-8"))
    except Exception:
        return list(range(max_draws))

    draw_ids: List[int] = []
    for row in payload:
        v = row.get("draw_id")
        if v is None:
            continue
        try:
            draw_ids.append(int(v))
            continue
        except Exception:
            pass

        # Handle strings like "draw_0"
        s = str(v)
        digits = "".join(ch for ch in s if ch.isdigit())
        if digits:
            try:
                draw_ids.append(int(digits))
            except Exception:
                pass
    if not draw_ids:
        draw_ids = list(range(max_draws))
    # Keep only first max_draws
    return draw_ids[: int(max_draws)]


def _load_lowfreq_factors(monte_carlo_dir: Path) -> Dict[str, Set[str]]:
    """Return mapping iso -> set(low-frequency factors).

    Prefer the small diagnostics file written by Step 12
    (diagnostics/lowfreq_classification.csv) to avoid reading a potentially
    huge macro_monthly_draws.csv.
    """
    diag_csv = monte_carlo_dir / "diagnostics" / "lowfreq_classification.csv"
    if diag_csv.exists():
        try:
            df = pd.read_csv(diag_csv)
            if {"iso", "factor", "is_low_frequency"}.issubset(set(df.columns)):
                df = df[df["is_low_frequency"].astype(bool)]
                out: Dict[str, Set[str]] = {}
                for iso, g in df.groupby("iso"):
                    out[str(iso)] = set(str(x) for x in g["factor"].dropna().unique())
                return out
        except Exception:
            pass

    macro_csv = monte_carlo_dir / "macro_monthly_draws.csv"
    if not macro_csv.exists():
        return {}

    out: Dict[str, Set[str]] = {}
    try:
        # Stream-friendly fallback
        for chunk in pd.read_csv(macro_csv, usecols=["iso", "factor"], chunksize=2_000_000):
            chunk = chunk.dropna(subset=["iso", "factor"])
            for iso, g in chunk.groupby("iso"):
                s = out.setdefault(str(iso), set())
                s |= set(str(x) for x in g["factor"].dropna().unique())
        return out
    except Exception:
        return {}


def _load_vol_t0_map(monte_carlo_dir: Path) -> Dict[str, Dict[str, float]]:
    """Load per-(iso,factor) vol scaling from Step 12 diagnostics.

    Step 12 writes diagnostics/dims.csv with columns: iso,factor,vol_t0.
    If missing, we fall back to empty (vol defaults to 1.0).
    """
    dims_csv = monte_carlo_dir / "diagnostics" / "dims.csv"
    if not dims_csv.exists():
        return {}
    try:
        df = pd.read_csv(dims_csv)
    except Exception:
        return {}
    if not {"iso", "factor", "vol_t0"}.issubset(set(df.columns)):
        return {}

    out: Dict[str, Dict[str, float]] = {}
    for iso, g in df.groupby("iso"):
        m: Dict[str, float] = {}
        for r in g.itertuples(index=False):
            try:
                f = str(getattr(r, "factor"))
                v = float(getattr(r, "vol_t0"))
                if not np.isfinite(v) or v <= 0:
                    v = 1.0
                m[f] = v
            except Exception:
                continue
        out[str(iso)] = m
    return out


def _attach_standardized_shock(df: pd.DataFrame, *, factor_to_vol: Dict[str, float]) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    v = out["factor"].astype(str).map(factor_to_vol)
    v = pd.to_numeric(v, errors="coerce").fillna(1.0).replace(0.0, 1.0)
    out["shock_z"] = pd.to_numeric(out["shock"], errors="coerce") / v
    return out


@dataclass(frozen=True)
class DailySource:
    path: Path
    mode: str  # single|sharded
    fmt: str  # csv|parquet


def _resolve_daily_source(monte_carlo_dir: Path) -> DailySource:
    manifest = monte_carlo_dir / "manifest.json"
    if not manifest.exists():
        # Fallback to conventional locations
        if (monte_carlo_dir / "daily_draws").is_dir():
            return DailySource(path=monte_carlo_dir / "daily_draws", mode="sharded", fmt="csv")
        return DailySource(path=monte_carlo_dir / "daily_draws.csv", mode="single", fmt="csv")

    m = _read_json(manifest)
    outputs = m.get("outputs") or {}
    p = Path(outputs.get("daily_draws") or (monte_carlo_dir / "daily_draws.csv"))
    mode = str(outputs.get("daily_draws_mode") or ("sharded" if p.is_dir() else "single"))
    fmt = str(outputs.get("daily_draws_format") or "csv")

    if not p.is_absolute():
        # Stored as relative sometimes
        p = (PROJECT_ROOT / p).resolve()

    return DailySource(path=p, mode=mode, fmt=fmt)


def _iter_iso_parts(source: DailySource, *, iso: str) -> Iterable[Path]:
    if source.path.is_file():
        yield source.path
        return

    # Directory cases
    if (source.path / iso).is_dir():
        iso_dir = source.path / iso
        yield from sorted(iso_dir.glob("*.parquet"))
        yield from sorted(iso_dir.glob("*.csv"))
        yield from sorted(iso_dir.glob("*.csv.gz"))
        return

    # Single-parquet parts under daily_draws/part_*__ISO.parquet
    for p in sorted(source.path.glob(f"*__{iso}.parquet")):
        yield p


def _read_filtered_daily(
    source: DailySource,
    *,
    iso: str,
    draw_ids: Set[int],
    factors: Set[str],
) -> pd.DataFrame:
    """Return filtered daily rows for one ISO (draw subset + factor subset)."""
    cols = ["draw_id", "iso", "date", "factor", "shock"]
    parts: List[pd.DataFrame] = []

    for part in _iter_iso_parts(source, iso=iso):
        if part.suffix == ".parquet":
            df = pd.read_parquet(part, columns=cols)
            df = df[df["iso"].astype(str) == str(iso)]
            df = df[df["draw_id"].astype(int).isin(draw_ids)]
            df = df[df["factor"].astype(str).isin(factors)]
            if not df.empty:
                parts.append(df)
        else:
            # CSV / CSV.GZ: stream
            reader = pd.read_csv(part, usecols=cols, chunksize=2_000_000)
            for chunk in reader:
                chunk = chunk[chunk["iso"].astype(str) == str(iso)]
                if chunk.empty:
                    continue
                chunk["draw_id"] = pd.to_numeric(chunk["draw_id"], errors="coerce").astype("Int64")
                chunk = chunk[chunk["draw_id"].isin(draw_ids)]
                if chunk.empty:
                    continue
                chunk = chunk[chunk["factor"].astype(str).isin(factors)]
                if not chunk.empty:
                    parts.append(chunk)

    if not parts:
        return pd.DataFrame(columns=cols)

    df_all = pd.concat(parts, ignore_index=True)
    df_all["draw_id"] = pd.to_numeric(df_all["draw_id"], errors="coerce").astype(int)
    df_all["date"] = pd.to_datetime(df_all["date"], errors="coerce")
    df_all["factor"] = df_all["factor"].astype(str)
    df_all = df_all.dropna(subset=["date"])
    return df_all


def _plot_factor(
    *,
    out_path: Path,
    run_name: str,
    iso: str,
    block_key: str,
    factor: str,
    df: pd.DataFrame,
    lowfreq: bool,
    value_col: str,
    ylabel0: str,
    ylabel1: str,
    scale: float = 1.0,
    draw_colors: Optional[Dict[int, Any]] = None,
    factor_space: str = "",
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if df.empty:
        return

    value_col = str(value_col)
    if value_col not in df.columns:
        return

    piv = df.pivot_table(index="date", columns="draw_id", values=value_col, aggfunc="mean").sort_index()
    try:
        piv = piv.astype(float)
    except Exception:
        piv = piv.apply(pd.to_numeric, errors="coerce")
    if piv.empty:
        return

    sc = float(scale) if scale is not None else 1.0
    if not np.isfinite(sc) or sc == 0.0:
        sc = 1.0
    if sc != 1.0:
        piv = piv * sc

    cum = piv.cumsum(axis=0)

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 6), sharex=True)
    ax0, ax1 = axes

    draw_colors = draw_colors or {}
    for col in piv.columns:
        did = int(col)
        color = draw_colors.get(did)
        y0 = piv[col].values
        y1 = cum[col].values
        if lowfreq:
            ax0.plot(piv.index, y0, lw=1.2, alpha=0.9, color=color, label=f"draw {did}")
            ax1.step(cum.index, y1, where="post", lw=1.2, alpha=0.9, color=color, label=f"draw {did}")
        else:
            ax0.plot(piv.index, y0, lw=1.2, alpha=0.9, color=color, label=f"draw {did}")
            ax1.plot(cum.index, y1, lw=1.2, alpha=0.9, color=color, label=f"draw {did}")

    ax0.set_title(f"{run_name} | {iso} | {block_key} | {factor}")
    ax0.set_ylabel(str(ylabel0))
    ax1.set_ylabel(str(ylabel1))
    ax1.set_xlabel("date")

    ax0.grid(True, alpha=0.2)
    ax1.grid(True, alpha=0.2)

    handles, labels = ax0.get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=min(6, len(handles)), fontsize=8)

    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, out_path)
    plt.close(fig)

    try:
        _write_plot_markdown(
            md_path=out_path.with_suffix(".md"),
            png_path=out_path,
            run_name=str(run_name),
            iso=str(iso),
            block_key=str(block_key),
            plot_kind="factor",
            factor=str(factor),
            factor_space=str(factor_space),
            lowfreq=bool(lowfreq),
        )
    except Exception:
        pass


def _plot_block_aggregate(
    *,
    out_path: Path,
    run_name: str,
    iso: str,
    block_key: str,
    df: pd.DataFrame,
    lowfreq_factors: Set[str],
    factor_to_vol: Dict[str, float],
    draw_colors: Optional[Dict[int, Any]] = None,
) -> None:
    """One plot per block: mean standardized shock (z-like) across factors + cumulative."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if df.empty:
        return

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return

    # Aggregate: per draw/date, mean across factors (standardized)
    df = _attach_standardized_shock(df, factor_to_vol=factor_to_vol)
    if "shock_z" not in df.columns:
        return
    agg = (
        df.groupby(["date", "draw_id"], as_index=False)["shock_z"].mean()
        .rename(columns={"shock_z": "shock_mean"})
        .sort_values(["draw_id", "date"])
    )
    piv = agg.pivot_table(index="date", columns="draw_id", values="shock_mean", aggfunc="mean").sort_index()
    if piv.empty:
        return
    cum = piv.cumsum(axis=0)

    is_lowfreq_block = False
    try:
        is_lowfreq_block = bool(lowfreq_factors) and (len(lowfreq_factors) >= max(1, int(0.5 * df["factor"].nunique())))
    except Exception:
        is_lowfreq_block = False

    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 6), sharex=True)
    ax0, ax1 = axes

    draw_colors = draw_colors or {}
    for col in piv.columns:
        did = int(col)
        color = draw_colors.get(did)
        y0 = piv[col].values
        y1 = cum[col].values
        ax0.plot(piv.index, y0, lw=1.2, alpha=0.9, color=color, label=f"draw {did}")
        if is_lowfreq_block:
            ax1.step(cum.index, y1, where="post", lw=1.2, alpha=0.9, color=color, label=f"draw {did}")
        else:
            ax1.plot(cum.index, y1, lw=1.2, alpha=0.9, color=color, label=f"draw {did}")

    ax0.set_title(f"{run_name} | {iso} | {block_key} | block aggregate (mean sigmas across factors)")
    ax0.set_ylabel("sigmas (z; mean impulses)")
    ax1.set_ylabel("cumulated sigmas (z; level proxy)")
    ax1.set_xlabel("date")
    ax0.grid(True, alpha=0.2)
    ax1.grid(True, alpha=0.2)

    handles, labels = ax0.get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=min(6, len(handles)), fontsize=8)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, out_path)
    plt.close(fig)

    try:
        _write_plot_markdown(
            md_path=out_path.with_suffix(".md"),
            png_path=out_path,
            run_name=str(run_name),
            iso=str(iso),
            block_key=str(block_key),
            plot_kind="block",
            factor_space="z",
            lowfreq=bool(is_lowfreq_block),
            notes=[
                "Block aggregate is the mean across factors in standardized space (sigmas).",
                "It is intended for comparability across mixed-unit series (bps, %, etc.).",
            ],
        )
    except Exception:
        pass


def _infer_date_index_from_source(source: DailySource, *, iso: str) -> List[pd.Timestamp]:
    """Infer the date index used in daily_draws for an ISO."""
    for part in _iter_iso_parts(source, iso=iso):
        if part.suffix == ".parquet":
            df = pd.read_parquet(part, columns=["date"]).head(5000)
        else:
            df = pd.read_csv(part, usecols=["date"], nrows=20000)
        dates = pd.to_datetime(df["date"], errors="coerce").dropna().unique().tolist()
        if dates:
            return sorted(pd.to_datetime(dates))
    return []


def _compute_block_aggregate_matrix(
    source: DailySource,
    *,
    iso: str,
    factors: Set[str],
    n_draws: int,
    date_index: List[pd.Timestamp],
    factor_to_vol: Dict[str, float],
) -> np.ndarray:
    """Compute block aggregate mean shock per draw per date.

    Returns matrix shape (n_draws, T).
    """
    if not date_index:
        return np.empty((int(n_draws), 0), dtype=float)

    date_pos = {pd.to_datetime(d).strftime("%Y-%m-%d"): i for i, d in enumerate(date_index)}
    T = len(date_index)
    out = np.full((int(n_draws), int(T)), np.nan, dtype=float)

    cols = ["draw_id", "iso", "date", "factor", "shock"]
    for part in _iter_iso_parts(source, iso=iso):
        if part.suffix == ".parquet":
            df = pd.read_parquet(part, columns=cols)
            df = df[df["iso"].astype(str) == str(iso)]
            df = df[df["factor"].astype(str).isin(factors)]
            if df.empty:
                continue
            df["draw_id"] = pd.to_numeric(df["draw_id"], errors="coerce")
            df = df.dropna(subset=["draw_id"])
            df["draw_id"] = df["draw_id"].astype(int)
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["date"])
            # Standardize before aggregating across mixed units
            df["factor"] = df["factor"].astype(str)
            v = df["factor"].map(factor_to_vol).astype(float)
            v = v.replace([0.0, np.nan, np.inf, -np.inf], 1.0)
            df["shock_z"] = pd.to_numeric(df["shock"], errors="coerce") / v
            g = df.groupby(["draw_id", "date"], as_index=False)["shock_z"].mean()
        else:
            chunks = pd.read_csv(part, usecols=cols, chunksize=2_000_000)
            gs: List[pd.DataFrame] = []
            for chunk in chunks:
                chunk = chunk[chunk["iso"].astype(str) == str(iso)]
                if chunk.empty:
                    continue
                chunk = chunk[chunk["factor"].astype(str).isin(factors)]
                if chunk.empty:
                    continue
                chunk["draw_id"] = pd.to_numeric(chunk["draw_id"], errors="coerce")
                chunk = chunk.dropna(subset=["draw_id"])
                if chunk.empty:
                    continue
                chunk["draw_id"] = chunk["draw_id"].astype(int)
                chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce").dt.strftime("%Y-%m-%d")
                chunk = chunk.dropna(subset=["date"])
                if chunk.empty:
                    continue
                chunk["factor"] = chunk["factor"].astype(str)
                v = chunk["factor"].map(factor_to_vol).astype(float)
                v = v.replace([0.0, np.nan, np.inf, -np.inf], 1.0)
                chunk["shock_z"] = pd.to_numeric(chunk["shock"], errors="coerce") / v
                gs.append(chunk.groupby(["draw_id", "date"], as_index=False)["shock_z"].mean())
            if not gs:
                continue
            g = pd.concat(gs, ignore_index=True)
            g = g.groupby(["draw_id", "date"], as_index=False)["shock_z"].mean()

        for r in g.itertuples(index=False):
            try:
                did = int(r.draw_id)
                j = date_pos.get(str(r.date))
                if j is None:
                    continue
                if 0 <= did < int(n_draws):
                    out[did, j] = float(r.shock_z)
            except Exception:
                continue

    return out


def _compute_block_aggregate_matrices_for_iso(
    source: DailySource,
    *,
    iso: str,
    block_to_factors: Dict[str, Set[str]],
    n_draws: int,
    date_index: List[pd.Timestamp],
    factor_to_vol: Dict[str, float],
) -> Dict[str, np.ndarray]:
    """Compute mean-shock matrices for all blocks in one pass over shards.

    Returns: block_key -> matrix (n_draws, T)
    """
    if not date_index:
        return {k: np.empty((int(n_draws), 0), dtype=float) for k in block_to_factors.keys()}

    date_pos = {pd.to_datetime(d).strftime("%Y-%m-%d"): i for i, d in enumerate(date_index)}
    T = len(date_index)

    # Sum/count per block
    sums: Dict[str, np.ndarray] = {k: np.zeros((int(n_draws), int(T)), dtype=float) for k in block_to_factors.keys()}
    cnts: Dict[str, np.ndarray] = {k: np.zeros((int(n_draws), int(T)), dtype=np.uint16) for k in block_to_factors.keys()}

    # Union of factors we care about
    union_factors: Set[str] = set()
    for fs in block_to_factors.values():
        union_factors |= set(str(x) for x in fs)

    cols = ["draw_id", "iso", "date", "factor", "shock"]
    for part in _iter_iso_parts(source, iso=iso):
        if part.suffix == ".parquet":
            df = pd.read_parquet(part, columns=cols)
        else:
            # For CSV shards, load whole shard (each shard is already draw-chunk sized)
            df = pd.read_csv(part, usecols=cols)

        df = df[df["iso"].astype(str) == str(iso)]
        if df.empty:
            continue
        df = df[df["factor"].astype(str).isin(union_factors)]
        if df.empty:
            continue
        df["draw_id"] = pd.to_numeric(df["draw_id"], errors="coerce")
        df = df.dropna(subset=["draw_id"])
        if df.empty:
            continue
        df["draw_id"] = df["draw_id"].astype(int)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df = df.dropna(subset=["date"])
        if df.empty:
            continue

        # Standardize shocks (z-like) using vol_t0 so blocks don't average mixed units.
        df["factor"] = df["factor"].astype(str)
        v = df["factor"].map(factor_to_vol)
        v = pd.to_numeric(v, errors="coerce").fillna(1.0).replace(0.0, 1.0)
        df["shock_z"] = pd.to_numeric(df["shock"], errors="coerce") / v

        # Per block: sum and count (mean across factors)
        for block_key, fs in block_to_factors.items():
            if not fs:
                continue
            sub = df[df["factor"].astype(str).isin(fs)]
            if sub.empty:
                continue
            g = sub.groupby(["draw_id", "date"], as_index=False).agg(shock_sum=("shock_z", "sum"), shock_cnt=("shock_z", "count"))
            S = sums[block_key]
            C = cnts[block_key]
            for r in g.itertuples(index=False):
                try:
                    did = int(r.draw_id)
                    j = date_pos.get(str(r.date))
                    if j is None:
                        continue
                    if 0 <= did < int(n_draws):
                        S[did, j] += float(r.shock_sum)
                        C[did, j] += int(r.shock_cnt)
                except Exception:
                    continue

    out: Dict[str, np.ndarray] = {}
    for block_key in block_to_factors.keys():
        S = sums[block_key]
        C = cnts[block_key].astype(float)
        with np.errstate(divide="ignore", invalid="ignore"):
            M = np.where(C > 0, S / C, np.nan)
        out[block_key] = M

    return out


def _plot_block_aggregate_quantile_bands(
    *,
    out_path: Path,
    run_name: str,
    iso: str,
    block_key: str,
    date_index: List[pd.Timestamp],
    mat: np.ndarray,
    step_cumulative: bool,
    q_low: float = 5.0,
    q_mid: float = 50.0,
    q_high: float = 95.0,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    if mat.size == 0 or not date_index:
        return

    # Replace missing with 0 for cumulative (should be complete in practice)
    mat0 = np.where(np.isfinite(mat), mat, 0.0)
    cum = np.cumsum(mat0, axis=1)

    def qs(A: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        lo = np.nanpercentile(A, q_low, axis=0)
        mid = np.nanpercentile(A, q_mid, axis=0)
        hi = np.nanpercentile(A, q_high, axis=0)
        return lo, mid, hi

    lo0, mid0, hi0 = qs(mat0)
    lo1, mid1, hi1 = qs(cum)

    x = pd.to_datetime(date_index)
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 6), sharex=True)
    ax0, ax1 = axes

    ax0.fill_between(x, lo0, hi0, alpha=0.25, label=f"q{q_low:.0f}-q{q_high:.0f} band")
    ax0.plot(x, mid0, lw=1.4, label=f"median (q{q_mid:.0f})")
    ax0.set_ylabel(f"sigmas (z; q{q_mid:.0f} with q{q_low:.0f}-q{q_high:.0f} band)")
    ax0.grid(True, alpha=0.2)

    ax1.fill_between(x, lo1, hi1, alpha=0.25, label=f"q{q_low:.0f}-q{q_high:.0f} band")
    if step_cumulative:
        ax1.step(x, mid1, where="post", lw=1.4, label=f"median (q{q_mid:.0f})")
    else:
        ax1.plot(x, mid1, lw=1.4, label=f"median (q{q_mid:.0f})")
    ax1.set_ylabel("cumulated sigmas (z; level proxy)")
    ax1.set_xlabel("date")
    ax1.grid(True, alpha=0.2)

    ax0.set_title(f"{run_name} | {iso} | {block_key} | block aggregate quantile bands")
    ax0.legend(loc="upper left", fontsize=8)
    ax1.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _save_figure(fig, out_path)
    plt.close(fig)

    try:
        _write_plot_markdown(
            md_path=out_path.with_suffix(".md"),
            png_path=out_path,
            run_name=str(run_name),
            iso=str(iso),
            block_key=str(block_key),
            plot_kind="block",
            factor_space="z",
            lowfreq=bool(step_cumulative),
            notes=[
                f"Quantile bands are computed across all draws (q{q_low:.0f}/q{q_mid:.0f}/q{q_high:.0f}).",
                "This plot summarizes distributional uncertainty rather than individual draw paths.",
            ],
        )
    except Exception:
        pass


def main() -> int:
    global _FIG_FORMATS, _FIG_DPI

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=str, default=None)
    parser.add_argument("--use-latest", action="store_true")
    parser.add_argument(
        "--bundle-name",
        type=str,
        default=None,
        help=(
            "Output bundle folder name under plots-root (default: input run id). "
            "Use this to write a sibling bundle like 'alternative' while reading inputs from 'latest'."
        ),
    )
    parser.add_argument(
        "--block-def",
        type=str,
        default="auto",
        help="Path to block definition json (or 'auto' to infer).",
    )
    parser.add_argument("--plots-root", type=str, default=str(PLOTS_ROOT_DEFAULT))

    parser.add_argument(
        "--fig-formats",
        type=str,
        default="png",
        help="Comma-separated figure formats to write alongside PNG (supported: png,pdf,svg). Default: png.",
    )
    parser.add_argument(
        "--fig-dpi",
        type=int,
        default=140,
        help="PNG DPI (PDF/SVG ignore DPI). Default: 140.",
    )

    parser.add_argument("--max-draws", type=int, default=10)
    parser.add_argument(
        "--draw-ids",
        type=str,
        default=None,
        help="Comma-separated draw ids to plot (overrides representatives).",
    )

    parser.add_argument("--only-iso", type=str, default=None)
    parser.add_argument("--only-block", type=str, default=None)

    parser.add_argument(
        "--block-aggregate-style",
        type=str,
        default="representatives",
        choices=["representatives", "quantile_bands"],
        help=(
            "How to visualize the per-block aggregate. Representatives plots individual draw paths; "
            "quantile_bands plots median + 5/95 across all draws."
        ),
    )

    parser.add_argument(
        "--factor-space",
        type=str,
        default="z",
        choices=["innov", "z"],
        help=(
            "Per-factor plot vertical units: 'z' expresses shocks in sigmas (standardized by vol_t0); "
            "'innov' uses innovation units (unit impulses). Block aggregates are always computed in standardized space."
        ),
    )
    parser.add_argument(
        "--today-window-days",
        type=int,
        default=60,
        help=(
            "Realized 'today' marker window length in days (default: 60). "
            "Uses frozen inputs under <run_dir>/inputs/<ISO>/covariance/. "
            "Set to 0 to disable realized overlays."
        ),
    )
    parser.add_argument(
        "--bps-factors",
        type=str,
        default="",
        help=(
            "Comma-separated factor names to display in bps (innovation-unit plots only). "
            "Applies a ×10,000 scale in plots for those factors."
        ),
    )

    args = parser.parse_args()

    _FIG_FORMATS = _parse_fig_formats(getattr(args, "fig_formats", None))
    try:
        _FIG_DPI = int(getattr(args, "fig_dpi", 140) or 140)
    except Exception:
        _FIG_DPI = 140

    run_dir = _find_run_dir(run_id=getattr(args, "run_id", None), use_latest=bool(getattr(args, "use_latest", False)))
    if not run_dir.exists():
        raise SystemExit(f"Run dir not found: {run_dir}")

    monte = run_dir / "monte_carlo"
    if not monte.exists():
        raise SystemExit(f"Missing monte_carlo dir: {monte}")

    input_run_id = str(run_dir.name)
    bundle_name = str(args.bundle_name).strip() if getattr(args, "bundle_name", None) else input_run_id
    plots_root = Path(args.plots_root)
    plots_out = _ensure_dir(plots_root / bundle_name)

    only_iso = str(args.only_iso) if args.only_iso else None
    only_block = str(args.only_block) if args.only_block else None
    today_window_days = int(getattr(args, "today_window_days", 0) or 0)
    bps_factors = set(_parse_csv_list(getattr(args, "bps_factors", "")))

    if str(args.block_def).strip().lower() == "auto":
        probe_iso = only_iso
        if not probe_iso:
            inputs_dir = run_dir / "inputs"
            if inputs_dir.exists():
                for p in sorted(inputs_dir.iterdir()):
                    if p.is_dir():
                        probe_iso = p.name
                        break
        probe_iso = probe_iso or "USA"
        block_def_path = _auto_select_block_def(run_dir, iso=str(probe_iso))
    else:
        block_def_path = Path(args.block_def)

    try:
        block_def_path = block_def_path.resolve()
    except Exception:
        pass

    block_defs = _load_block_definitions(block_def_path)
    rev_map, dup_rows = _reverse_block_map(block_defs)

    lowfreq_by_iso = _load_lowfreq_factors(monte)
    vol_t0_by_iso = _load_vol_t0_map(monte)

    reps_json = monte / "representatives" / "top_draws.json"
    if args.draw_ids:
        draw_ids = [int(x) for x in _parse_csv_list(args.draw_ids)]
    else:
        draw_ids = _load_representative_draw_ids(reps_json, max_draws=int(args.max_draws))

    draw_set = set(int(x) for x in draw_ids)
    if not draw_set:
        raise SystemExit("No draw ids selected")

    draw_colors = _draw_color_map(draw_ids)
    daily_source = _resolve_daily_source(monte)

    mc_manifest = monte / "manifest.json"
    n_draws_mc = None
    if mc_manifest.exists():
        try:
            mm = _read_json(mc_manifest)
            # Support both legacy and current Step 12.0 manifests.
            # Newer manifests store args under signature_payload.args.
            n_draws_mc = mm.get("n_draws")
            if n_draws_mc is None:
                n_draws_mc = ((mm.get("signature_payload") or {}).get("args") or {}).get("n_draws")
            n_draws_mc = int(n_draws_mc)
        except Exception:
            n_draws_mc = None

    coverage_rows: List[Dict[str, Any]] = []
    unmapped_rows: List[Dict[str, Any]] = []

    iso_to_block_mats: Dict[str, Dict[str, np.ndarray]] = {}
    iso_to_block_factors: Dict[str, Dict[str, List[str]]] = {}
    iso_to_sim_factors: Dict[str, List[str]] = {}

    for iso in sorted(block_defs.keys()):
        if only_iso and iso != only_iso:
            continue

        mc_factors = _load_mc_factor_list(run_dir, iso=iso)
        if not mc_factors:
            continue

        iso_to_sim_factors[str(iso)] = [str(x) for x in mc_factors]

        iso_rev = rev_map.get(iso) or {}
        factor_to_blocks: Dict[str, Set[str]] = {}
        for f in mc_factors:
            base = _strip_lag_suffix(f)
            keys = iso_rev.get(f) or iso_rev.get(base)
            if keys:
                factor_to_blocks[f] = set(str(x) for x in keys)
            else:
                factor_to_blocks[f] = {"unmapped"}
                unmapped_rows.append({"iso": iso, "factor": f, "base": base})

        block_to_factors: Dict[str, List[str]] = {}
        for f, blocks in factor_to_blocks.items():
            for b in blocks:
                block_to_factors.setdefault(b, []).append(f)

        iso_to_block_factors[str(iso)] = {
            str(k): sorted(set(str(x) for x in v))
            for k, v in (block_to_factors or {}).items()
            if str(k) != "unmapped"
        }

        for block_key, expected in sorted((block_defs.get(iso) or {}).items()):
            expected_set = set(str(x) for x in expected)
            present_bases = set(_strip_lag_suffix(x) for x in mc_factors)
            present_expected = sorted(expected_set.intersection(present_bases))
            missing_expected = sorted(expected_set.difference(present_bases))
            coverage_rows.append(
                {
                    "iso": iso,
                    "block": str(block_key),
                    "expected_series": int(len(expected_set)),
                    "present_expected": int(len(present_expected)),
                    "missing_expected": int(len(missing_expected)),
                    "missing_expected_list": ";".join(missing_expected[:200]),
                }
            )

        rep_factors_union: Set[str] = set()
        for fs in block_to_factors.values():
            rep_factors_union |= set(str(x) for x in fs)
        rep_df_iso = _read_filtered_daily(daily_source, iso=iso, draw_ids=draw_set, factors=rep_factors_union)
        rep_df_iso = _attach_standardized_shock(rep_df_iso, factor_to_vol=(vol_t0_by_iso.get(iso) or {}))

        block_mats: Optional[Dict[str, np.ndarray]] = None
        date_index: Optional[List[pd.Timestamp]] = None
        if str(args.block_aggregate_style) == "quantile_bands" and n_draws_mc is not None:
            date_index = _infer_date_index_from_source(daily_source, iso=iso)
            block_mats = _compute_block_aggregate_matrices_for_iso(
                daily_source,
                iso=iso,
                block_to_factors={k: set(v) for k, v in block_to_factors.items() if k != "unmapped"},
                n_draws=int(n_draws_mc),
                date_index=date_index,
                factor_to_vol=(vol_t0_by_iso.get(iso) or {}),
            )

        if block_mats is not None:
            iso_to_block_mats[str(iso)] = {str(k): v for k, v in block_mats.items()}

        low_set = lowfreq_by_iso.get(iso) or set()
        for block_key, factors_in_block in sorted(block_to_factors.items()):
            if only_block and block_key != only_block:
                continue
            if not factors_in_block:
                continue

            factors_set = set(str(s) for s in factors_in_block)
            df_iso = rep_df_iso[rep_df_iso["factor"].astype(str).isin(factors_set)] if not rep_df_iso.empty else rep_df_iso

            out_dir = plots_out / iso / str(block_key)
            _ensure_dir(out_dir)

            lowfreq_in_block = set(f for f in factors_set if f in low_set)
            step_cum = bool(lowfreq_in_block) and (len(lowfreq_in_block) >= max(1, int(0.5 * len(factors_set))))

            if not df_iso.empty:
                _plot_block_aggregate(
                    out_path=out_dir / "__block_aggregate__impulse_vs_level.png",
                    run_name=bundle_name,
                    iso=iso,
                    block_key=str(block_key),
                    df=df_iso,
                    lowfreq_factors=lowfreq_in_block,
                    factor_to_vol=(vol_t0_by_iso.get(iso) or {}),
                    draw_colors=draw_colors,
                )

            if str(args.block_aggregate_style) == "quantile_bands" and block_mats is not None and date_index is not None:
                mat = block_mats.get(str(block_key))
                if mat is not None:
                    _plot_block_aggregate_quantile_bands(
                        out_path=out_dir / "__block_aggregate__quantile_bands.png",
                        run_name=bundle_name,
                        iso=iso,
                        block_key=str(block_key),
                        date_index=date_index,
                        mat=mat,
                        step_cumulative=step_cum,
                    )

            if not df_iso.empty:
                for f in sorted(factors_set):
                    dff = df_iso[df_iso["factor"] == f]
                    if dff.empty:
                        continue
                    out_path = out_dir / f"{f}__impulse_vs_level.png"

                    if str(args.factor_space) == "z":
                        value_col = "shock_z"
                        ylabel0 = "sigmas (z; impulses)"
                        ylabel1 = "cumulated sigmas (z; level proxy)"
                        scale = 1.0
                    else:
                        value_col = "shock"
                        if f in bps_factors:
                            ylabel0 = "innovations (bps impulses)"
                            ylabel1 = "cumulated innovations (bps level proxy)"
                            scale = 10000.0
                        else:
                            ylabel0 = "innovations (unit impulses)"
                            ylabel1 = "cumulated innovations (level proxy)"
                            scale = 1.0

                    _plot_factor(
                        out_path=out_path,
                        run_name=bundle_name,
                        iso=iso,
                        block_key=str(block_key),
                        factor=f,
                        df=dff,
                        lowfreq=(f in low_set),
                        value_col=value_col,
                        ylabel0=ylabel0,
                        ylabel1=ylabel1,
                        scale=scale,
                        draw_colors=draw_colors,
                        factor_space=str(args.factor_space),
                    )

    (plots_out / "_DONE.txt").write_text(
        f"bundle={bundle_name}\ninput_run={input_run_id}\nsource={daily_source.path}\ndraw_ids={sorted(draw_set)}\nblock_def={block_def_path}\nblock_aggregate_style={args.block_aggregate_style}\nfactor_space={args.factor_space}\n",
        encoding="utf-8",
    )

    if coverage_rows:
        pd.DataFrame(coverage_rows).to_csv(plots_out / "coverage_report.csv", index=False)
    unmapped_path = plots_out / "unmapped_factors.csv"
    if unmapped_rows:
        pd.DataFrame(unmapped_rows).to_csv(unmapped_path, index=False)
    else:
        if unmapped_path.exists():
            unmapped_path.unlink()
    dup_path = plots_out / "duplicates_in_block_def.csv"
    if dup_rows:
        pd.DataFrame(dup_rows).to_csv(dup_path, index=False)
    else:
        if dup_path.exists():
            dup_path.unlink()

    if iso_to_block_mats:
        try:
            _write_bundle_comovement_summary(
                out_path=plots_out / "BLOCK_COMOVEMENTS_AND_REGIMES.md",
                run_name=bundle_name,
                iso_to_block_mats=iso_to_block_mats,
                iso_to_block_factors=iso_to_block_factors,
                run_dir=run_dir,
                today_window_days=today_window_days,
                monte_carlo_dir=monte,
                daily_source=daily_source,
                lowfreq_by_iso=lowfreq_by_iso,
                vol_t0_by_iso=vol_t0_by_iso,
                iso_to_sim_factors=iso_to_sim_factors,
            )
        except Exception:
            (plots_out / "BLOCK_COMOVEMENTS_AND_REGIMES.error.txt").write_text(
                traceback.format_exc(),
                encoding="utf-8",
            )
    else:
        (plots_out / "BLOCK_COMOVEMENTS_AND_REGIMES.md").write_text(
            "\n".join(
                [
                    f"# MC Block Comovements & Regimes — {bundle_name}",
                    "",
                    "This summary was not generated in this run.",
                    "",
                    "Reason: `--block-aggregate-style` was not `quantile_bands`, so all-draw block-aggregate matrices were not computed.",
                    "",
                    "Re-run Step 12.1 with:",
                    "- `--block-aggregate-style quantile_bands`",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    try:
        _write_index_markdown(
            plots_out=plots_out,
            bundle_name=str(bundle_name),
            input_run_id=str(input_run_id),
            daily_source=str(daily_source.path),
        )
    except Exception:
        (plots_out / "INDEX.error.txt").write_text(traceback.format_exc(), encoding="utf-8")

    print(f"[OK] Wrote plots to: {plots_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
