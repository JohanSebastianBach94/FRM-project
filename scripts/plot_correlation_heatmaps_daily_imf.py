"""Daily IMF-style correlation heatmaps for FOR-ST outputs.

This script mirrors the monthly correlation diagnostic but operates
on daily Rt panels produced by the FOR-ST pipeline and focuses on
explicit crisis windows (GFC, euro crisis, COVID, etc.).
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from scripts.plot_correlation_heatmaps import (
    aggregate_to_matrix,
    extract_factors_from_pairs,
    load_series_metadata,
)


@dataclass
class CrisisWindow:
    name: str
    start: pd.Timestamp
    end: pd.Timestamp


def parse_crisis_arg(arg: str) -> CrisisWindow:
    """Parse a crisis spec like 'GFC:2007-07-01:2009-03-31'."""

    try:
        name, start, end = arg.split(":", 2)
    except ValueError as exc:  # pragma: no cover - CLI guard
        raise ValueError(
            "Crisis must be NAME:YYYY-MM-DD:YYYY-MM-DD"
        ) from exc
    return CrisisWindow(name=name, start=pd.to_datetime(start), end=pd.to_datetime(end))


def load_rt_daily(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date").sort_index()
    else:
        df.index = pd.to_datetime(df.index)
        df = df.sort_index()
    return df


def plot_heatmap(
    corr_mat: np.ndarray,
    labels: List[str],
    title: str,
    out_path: Path,
) -> None:
    sns.set(style="white")

    cmap = sns.diverging_palette(240, 10, as_cmap=True)
    mask = np.isnan(corr_mat)

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        corr_mat,
        mask=mask,
        cmap=cmap,
        vmin=-1,
        vmax=1,
        center=0,
        square=True,
        cbar_kws={"shrink": 0.8},
        xticklabels=labels,
        yticklabels=labels,
    )
    plt.title(title)
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path, dpi=300)
    plt.close()


def run_for_iso_model(
    iso: str,
    model: str,
    base_dir: Path,
    crises: List[CrisisWindow],
    tranquil_window: Tuple[pd.Timestamp, pd.Timestamp] | None,
    top_factors: int,
) -> None:
    analysis_dir = base_dir / "analysis_outputs"
    rt_path = analysis_dir / "diag_corr_daily" / f"{iso}_Rt_{model}.csv"

    if not rt_path.exists():
        return

    rt_df = load_rt_daily(rt_path)
    metadata = load_series_metadata()

    # Derive factor list from Rt column names using existing helper
    factor_codes = extract_factors_from_pairs(rt_df.columns, max_factors=top_factors)

    out_dir = analysis_dir / "heatmaps_daily_imf" / iso / model

    # Optional tranquil period
    if tranquil_window is not None:
        t_start, t_end = tranquil_window
        tranquil = rt_df.loc[t_start:t_end]
        if len(tranquil) > 0:
            corr_tranquil = aggregate_to_matrix(tranquil, factor_codes)
            labels = [metadata.get(f, f) for f in factor_codes]
            plot_heatmap(
                corr_tranquil,
                labels,
                f"{iso} {model} tranquil {t_start.date()}–{t_end.date()}",
                out_dir / f"{iso}_{model}_tranquil.png",
            )

    # Crisis windows
    for cw in crises:
        sub = rt_df.loc[cw.start : cw.end]
        if len(sub) == 0:
            continue
        corr_mat = aggregate_to_matrix(sub, factor_codes)
        labels = [metadata.get(f, f) for f in factor_codes]
        title = f"{iso} {model} {cw.name} {cw.start.date()}–{cw.end.date()}"
        out_path = out_dir / f"{iso}_{model}_{cw.name}.png"
        plot_heatmap(corr_mat, labels, title, out_path)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily IMF-style correlation heatmaps")
    parser.add_argument("--isos", nargs="*", default=["ITA"], help="ISO codes")
    parser.add_argument("--models", nargs="*", default=["garch"], help="Model names")
    parser.add_argument("--base-dir", type=str, default=".", help="Project root")
    parser.add_argument(
        "--crisis",
        nargs="*",
        default=[],
        help="Crisis specs NAME:YYYY-MM-DD:YYYY-MM-DD",
    )
    parser.add_argument(
        "--tranquil",
        type=str,
        default="",
        help="Optional tranquil window YYYY-MM-DD:YYYY-MM-DD",
    )
    parser.add_argument(
        "--top-factors",
        type=int,
        default=10,
        help="Maximum number of factors for heatmaps",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    base_dir = Path(args.base_dir).resolve()

    crises = [parse_crisis_arg(c) for c in args.crisis]

    tranquil_window = None
    if args.tranquil:
        start_str, end_str = args.tranquil.split(":", 1)
        tranquil_window = (pd.to_datetime(start_str), pd.to_datetime(end_str))

    for iso in args.isos:
        for model in args.models:
            run_for_iso_model(
                iso=iso,
                model=model,
                base_dir=base_dir,
                crises=crises,
                tranquil_window=tranquil_window,
                top_factors=args.top_factors,
            )


if __name__ == "__main__":  # pragma: no cover
    main()
