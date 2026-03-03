"""Generate a LinkedIn-friendly pipeline schematic as a PNG/SVG using Matplotlib.

Companion slide for the Monte Carlo severity + connectedness plots.

Usage:
  python tools/make_pipeline_schematic.py
  python tools/make_pipeline_schematic.py --out "SRESS TEST PIPELINE/MC scenario plots/latest/PIPELINE_SCHEMATIC.png" --svg
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Tuple


Color = Tuple[float, float, float]


def _add_box(
    ax,
    xy: Tuple[float, float],
    w: float,
    h: float,
    title: str,
    subtitle: str,
    *,
    facecolor: Color,
    edgecolor: str,
    title_size: float = 12.5,
    subtitle_size: float = 10.5,
) -> None:
    import matplotlib.patches as patches

    x, y = xy
    box = patches.FancyBboxPatch(
        (x, y),
        w,
        h,
        boxstyle="round,pad=0.02,rounding_size=0.025",
        linewidth=1.6,
        edgecolor=edgecolor,
        facecolor=facecolor,
    )
    ax.add_patch(box)

    ax.text(
        x + 0.04 * w,
        y + h - 0.28 * h,
        title,
        ha="left",
        va="center",
        fontsize=title_size,
        fontweight="bold",
        color="0.10",
    )

    ax.text(
        x + 0.04 * w,
        y + 0.36 * h,
        subtitle,
        ha="left",
        va="center",
        fontsize=subtitle_size,
        color="0.15",
        wrap=True,
    )


def _add_arrow(ax, start: Tuple[float, float], end: Tuple[float, float], *, color: str = "0.25") -> None:
    import matplotlib.patches as patches

    arrow = patches.FancyArrowPatch(
        start,
        end,
        arrowstyle="-|>",
        mutation_scale=16,
        linewidth=1.6,
        color=color,
        shrinkA=6,
        shrinkB=6,
        connectionstyle="arc3,rad=0.0",
    )
    ax.add_patch(arrow)


def build_schematic(out_path: Path, *, title: str, footer: str, also_svg: bool) -> None:
    import matplotlib.pyplot as plt

    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(14.5, 7.5))
    ax = fig.add_axes([0.03, 0.05, 0.94, 0.88])
    ax.set_axis_off()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_facecolor("0.985")

    # Title inside the axes so it reliably appears with bbox_inches="tight"
    ax.text(
        0.5,
        1.02,
        title,
        transform=ax.transAxes,
        ha="center",
        va="bottom",
        fontsize=17,
        fontweight="bold",
        color="0.10",
        clip_on=False,
    )

    # Layout coordinates (normalized 0..1)
    w = 0.27
    h = 0.145

    left_x = 0.05
    mid_x = 0.365
    right_x = 0.68

    y_top = 0.745
    y_mid = 0.47
    y_bot = 0.20

    # Palette (muted, professional, prints well)
    C_INPUT: Color = (0.92, 0.96, 1.00)  # light blue
    C_MODEL: Color = (0.93, 0.98, 0.94)  # light green
    C_SIM: Color = (1.00, 0.96, 0.90)  # light amber
    C_OUT: Color = (0.98, 0.94, 0.98)  # light purple
    EDGE = "0.25"

    _add_box(
        ax,
        (left_x, y_top),
        w,
        h,
        "Inputs & prep",
        "Macro/market series → cleaning\nstandardization + health checks",
        facecolor=C_INPUT,
        edgecolor=EDGE,
    )
    _add_box(
        ax,
        (mid_x, y_top),
        w,
        h,
        "Country blocks (Step 2)",
        "Group factors into themes\n(banking, FX, public finance, commodities …)",
        facecolor=C_MODEL,
        edgecolor=EDGE,
    )
    _add_box(
        ax,
        (right_x, y_top),
        w,
        h,
        "Factor dynamics",
        "Volatility + correlations\n(ADCC / DCC‑GARCH family)\n+ mean reversion",
        facecolor=C_MODEL,
        edgecolor=EDGE,
        subtitle_size=10.2,
    )

    _add_box(
        ax,
        (mid_x, y_mid),
        w,
        h,
        "Monte Carlo scenarios (Step 12)",
        "Simulate many joint futures\nfor block shock paths",
        facecolor=C_SIM,
        edgecolor=EDGE,
    )
    _add_box(
        ax,
        (right_x, y_mid),
        w,
        h,
        "Regimes",
        "Bucket draws by severity quantiles\n(baseline → adverse → crisis)",
        facecolor=C_SIM,
        edgecolor=EDGE,
    )

    _add_box(
        ax,
        (left_x, y_bot),
        w,
        h,
        "Realized overlay",
        "Compute \"Today\" from the latest window\n(in comparable sigma units)",
        facecolor=C_INPUT,
        edgecolor=EDGE,
        subtitle_size=10.2,
    )
    _add_box(
        ax,
        (mid_x, y_bot),
        w,
        h,
        "Reporting (Step 12.1)",
        "Severity distributions + ranking\nCrisis drivers (block shares)",
        facecolor=C_OUT,
        edgecolor=EDGE,
    )
    _add_box(
        ax,
        (right_x, y_bot),
        w,
        h,
        "Network lens",
        "Connectedness deltas\n(stress regime vs median)",
        facecolor=C_OUT,
        edgecolor=EDGE,
    )

    # Arrows
    _add_arrow(ax, (left_x + w, y_top + h / 2), (mid_x, y_top + h / 2))
    _add_arrow(ax, (mid_x + w, y_top + h / 2), (right_x, y_top + h / 2))

    _add_arrow(ax, (right_x + w / 2, y_top), (mid_x + w / 2, y_mid + h))

    _add_arrow(ax, (mid_x + w, y_mid + h / 2), (right_x, y_mid + h / 2))

    _add_arrow(ax, (mid_x + w / 2, y_mid), (mid_x + w / 2, y_bot + h))
    _add_arrow(ax, (right_x + w / 2, y_mid), (right_x + w / 2, y_bot + h))

    # Realized overlay feeds reporting
    _add_arrow(ax, (left_x + w, y_bot + h / 2), (mid_x, y_bot + h / 2))

    if footer:
        ax.text(
            0.5,
            0.03,
            footer,
            ha="center",
            va="center",
            fontsize=10.5,
            color="0.35",
        )

    fig.savefig(out_path, dpi=260, bbox_inches="tight")
    if also_svg:
        fig.savefig(out_path.with_suffix(".svg"), bbox_inches="tight")

    plt.close(fig)


def _default_out() -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "SRESS TEST PIPELINE" / "MC scenario plots" / "latest" / "PIPELINE_SCHEMATIC.png"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=_default_out(), help="Output PNG path")
    parser.add_argument(
        "--title",
        default="Stress-testing via Monte Carlo",
        help="Figure title",
    )
    parser.add_argument(
        "--footer",
        default="",
        help="Optional footer text (leave empty for no watermark)",
    )
    parser.add_argument("--svg", action="store_true", help="Also write an SVG next to the PNG")
    args = parser.parse_args()

    build_schematic(args.out, title=args.title, footer=args.footer, also_svg=args.svg)
    print(f"Wrote: {args.out}")
    if args.svg:
        print(f"Wrote: {args.out.with_suffix('.svg')}")


if __name__ == "__main__":
    main()
