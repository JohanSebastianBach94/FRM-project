"""Generate a more designed pipeline schematic using Graphviz.

Outputs PNG + SVG into the Monte Carlo plot bundle (default: latest).

Requirements:
- Graphviz installed (dot on PATH)
- Python package: graphviz

Usage:
  python tools/make_pipeline_schematic_graphviz.py
  python tools/make_pipeline_schematic_graphviz.py --bundle latest
  python tools/make_pipeline_schematic_graphviz.py --out-dir "SRESS TEST PIPELINE/MC scenario plots/latest"
  python tools/make_pipeline_schematic_graphviz.py --title "Stress-testing via Monte Carlo scenarios"
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def _require_dot() -> str:
    dot_path = shutil.which("dot")
    if not dot_path:
        raise SystemExit(
            "Graphviz 'dot' not found on PATH. Install Graphviz (dot) into the active environment."
        )
    return dot_path


def build_graph(title: str):
    from graphviz import Digraph

    # Muted palette similar to the Matplotlib slide
    C_INPUT = "#EBF5FF"   # light blue
    C_MODEL = "#ECF8EF"   # light green
    C_SIM = "#FFF3E6"     # light amber
    C_OUT = "#F7EEF7"     # light purple

    g = Digraph("pipeline", format="png")
    g.attr(
        rankdir="LR",
        bgcolor="white",
        fontname="Helvetica",
        labelloc="t",
        label=title,
        fontsize="22",
        pad="0.25",
        nodesep="0.35",
        ranksep="0.55",
    )

    g.attr("node", shape="box", style="rounded,filled", color="#444444", fontname="Helvetica", fontsize="12")
    g.attr("edge", color="#444444", penwidth="1.4", arrowsize="0.85")

    # Nodes
    g.node("inputs", "Inputs & prep\n\nMacro/market series -> cleaning\nstandardization + health checks", fillcolor=C_INPUT)
    g.node(
        "blocks",
        "Country blocks (Step 2)\n\nGroup factors into themes\n(banking, FX, public finance, commodities ...)",
        fillcolor=C_MODEL,
    )
    g.node(
        "dynamics",
        "Factor dynamics\n\nVolatility + correlations\n(ADCC / DCC-GARCH family)\n+ mean reversion",
        fillcolor=C_MODEL,
    )
    g.node(
        "mc",
        "Monte Carlo scenarios (Step 12)\n\nSimulate many joint futures\nfor block shock paths",
        fillcolor=C_SIM,
    )
    g.node(
        "regimes",
        "Regimes\n\nBucket draws by severity quantiles\n(baseline -> adverse -> crisis)",
        fillcolor=C_SIM,
    )

    # Outputs (bottom row)
    g.node(
        "reporting",
        "Reporting (Step 12.1)\n\nSeverity distributions + ranking\nCrisis drivers (block shares)",
        fillcolor=C_OUT,
    )
    g.node(
        "network",
        "Network lens\n\nConnectedness deltas\n(stress regime vs median)",
        fillcolor=C_OUT,
    )
    g.node(
        "realized",
        "Realized overlay\n\nCompute \"Today\" from the latest window\n(in comparable sigma units)",
        fillcolor=C_INPUT,
    )

    # Row/rank hints for nicer layout
    with g.subgraph(name="cluster_top") as s:
        s.attr(rank="same")
        s.node("inputs")
        s.node("blocks")
        s.node("dynamics")
        s.node("mc")
        s.node("regimes")

    with g.subgraph(name="cluster_bottom") as s:
        s.attr(rank="same")
        s.node("realized")
        s.node("reporting")
        s.node("network")

    # Main flow
    g.edge("inputs", "blocks")
    g.edge("blocks", "dynamics")
    g.edge("dynamics", "mc")
    g.edge("mc", "regimes")

    # Reporting and network outputs
    g.edge("mc", "reporting")
    g.edge("regimes", "reporting")
    g.edge("regimes", "network")
    g.edge("realized", "reporting")

    return g


def render(out_dir: Path, title: str, basename: str) -> None:
    _require_dot()

    out_dir.mkdir(parents=True, exist_ok=True)
    graph = build_graph(title)

    # Render PNG
    graph.format = "png"
    png_path = Path(graph.render(filename=str(out_dir / basename), cleanup=True))

    # Render SVG
    graph.format = "svg"
    svg_path = Path(graph.render(filename=str(out_dir / basename), cleanup=True))

    print(f"Wrote: {png_path}")
    print(f"Wrote: {svg_path}")


def _default_out_dir(bundle: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "SRESS TEST PIPELINE" / "MC scenario plots" / bundle


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", default="latest", help="Bundle folder under MC scenario plots")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory (overrides --bundle)")
    parser.add_argument(
        "--title",
        default="Stress-testing via Monte Carlo scenarios",
        help="Graph title",
    )
    parser.add_argument(
        "--basename",
        default="PIPELINE_SCHEMATIC_GRAPHVIZ",
        help="Output file base name without extension",
    )
    args = parser.parse_args()

    out_dir = args.out_dir if args.out_dir is not None else _default_out_dir(args.bundle)
    render(out_dir=out_dir, title=args.title, basename=args.basename)


if __name__ == "__main__":
    main()
