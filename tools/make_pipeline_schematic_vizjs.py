"""Render a Graphviz DOT schematic via viz.js (Graphviz compiled to WASM) using Playwright.

This avoids native Graphviz/dot issues on Windows and still produces a true DOT->SVG render.

Outputs:
- PNG screenshot (LinkedIn-friendly)
- SVG (vector)

Usage:
  python tools/make_pipeline_schematic_vizjs.py
  python tools/make_pipeline_schematic_vizjs.py --bundle latest --title "Stress-testing via Monte Carlo scenarios"
"""

from __future__ import annotations

import argparse
from pathlib import Path


def build_dot(title: str, variant: str) -> str:
  if variant == "classic":
    return _build_dot_classic(title)
  elif variant == "three-blocks":
    return _build_dot_three_blocks(title)
  elif variant == "three-blocks-vertical":
    return _build_dot_three_blocks_vertical(title)
  elif variant == "three-cards":
    return _build_dot_three_cards(title)
  else:
    raise ValueError(f"Unknown variant: {variant}")


def _build_dot_classic(title: str) -> str:
    # Keep DOT strictly ASCII for compatibility.
    # Layout: top row main pipeline; bottom row realized + outputs.
    return f"""digraph pipeline {{
  rankdir=LR;
  bgcolor=\"white\";
  labelloc=\"t\";
  label=\"{title}\";
  fontsize=24;
  fontname=\"Helvetica\";
  pad=0.25;
  nodesep=0.35;
  ranksep=0.55;

  node [shape=box, style=\"rounded,filled\", color=\"#444444\", fontname=\"Helvetica\", fontsize=12];
  edge [color=\"#444444\", penwidth=1.4, arrowsize=0.85];

  inputs   [fillcolor=\"#EBF5FF\", label=\"Inputs & prep\\n\\nMacro/market series -> cleaning\\nstandardization + health checks\"];
  blocks   [fillcolor=\"#ECF8EF\", label=\"Country blocks (Step 2)\\n\\nGroup factors into themes\\n(banking, FX, public finance, commodities ...)\"];
  dynamics [fillcolor=\"#ECF8EF\", label=\"Factor dynamics\\n\\nVolatility + correlations\\n(ADCC / DCC-GARCH family)\\n+ mean reversion\"];
  mc       [fillcolor=\"#FFF3E6\", label=\"Monte Carlo scenarios (Step 12)\\n\\nSimulate many joint futures\\nfor block shock paths\"];
  regimes  [fillcolor=\"#FFF3E6\", label=\"Regimes\\n\\nBucket draws by severity quantiles\\n(baseline -> adverse -> crisis)\"];

  realized  [fillcolor=\"#EBF5FF\", label=\"Realized overlay\\n\\nCompute \\\"Today\\\" from the latest window\\n(in comparable sigma units)\"];
  reporting [fillcolor=\"#F7EEF7\", label=\"Reporting (Step 12.1)\\n\\nSeverity distributions + ranking\\nCrisis drivers (block shares)\"];
  network   [fillcolor=\"#F7EEF7\", label=\"Network lens\\n\\nConnectedness deltas\\n(stress regime vs median)\"];

  subgraph cluster_top {{
    rank=same;
    inputs; blocks; dynamics; mc; regimes;
  }}

  subgraph cluster_bottom {{
    rank=same;
    realized; reporting; network;
  }}

  inputs -> blocks -> dynamics -> mc -> regimes;

  mc -> reporting;
  regimes -> reporting;
  regimes -> network;
  realized -> reporting;
}}
"""


def _build_dot_three_blocks(title: str) -> str:
    # Three large parallel boxes (clusters) with arrows between them.
    return f"""digraph pipeline {{
  rankdir=LR;
  compound=true;
  bgcolor=\"white\";
  labelloc=\"t\";
  label=\"{title}\";
  fontsize=24;
  fontname=\"Helvetica\";
  pad=0.25;
  nodesep=0.45;
  ranksep=0.75;

  node [shape=box, style=\"rounded,filled\", color=\"#444444\", fontname=\"Helvetica\", fontsize=12];
  edge [color=\"#444444\", penwidth=1.6, arrowsize=0.9];

  subgraph cluster_model {{
    label=\"Model and framework\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    inputs   [fillcolor=\"#EBF5FF\", label=\"Inputs & prep (Step 1)\\n\\nMacro/market series -> cleaning\\nstandardization + health checks\"];
    blocks   [fillcolor=\"#ECF8EF\", label=\"Country blocks (Step 2)\\n\\nGroup factors into themes\\n(banking, FX, public finance, commodities ...)\"];
    dynamics [fillcolor=\"#ECF8EF\", label=\"Factor dynamics (Step 3)\\n\\nVolatility + correlations\\n(ADCC / DCC-GARCH family)\\n+ mean reversion\"];

    inputs -> blocks -> dynamics;
  }}

  subgraph cluster_mc {{
    label=\"MC scenario building\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    mc      [fillcolor=\"#FFF3E6\", label=\"Monte Carlo scenarios (Step 4)\\n\\nSimulate many joint futures\\nfor block shock paths\"];
    regimes [fillcolor=\"#FFF3E6\", label=\"Regimes (Step 5)\\n\\nBucket draws by severity quantiles\\n(baseline -> adverse -> crisis)\"];

    mc -> regimes;
  }}

  subgraph cluster_reporting {{
    label=\"Reporting and Ranking\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    realized  [fillcolor=\"#EBF5FF\", label=\"Realized overlay\\n\\nCompute \\\"Today\\\" from the latest window\\n(in comparable sigma units)\"];
    reporting [fillcolor=\"#F7EEF7\", label=\"Reporting\\n\\nSeverity distributions + ranking\\nCrisis drivers (block shares)\"];
    network   [fillcolor=\"#F7EEF7\", label=\"Network lens\\n\\nConnectedness deltas\\n(stress regime vs median)\"];

    realized -> reporting;
  }}

  # Big-box arrows: Model -> MC -> Reporting
  dynamics -> mc [ltail=cluster_model, lhead=cluster_mc];
  regimes -> reporting [ltail=cluster_mc, lhead=cluster_reporting];

  # Extra semantic links
  regimes -> network;
}}
"""


def _build_dot_three_blocks_vertical(title: str) -> str:
    # Stack the three big clusters vertically to get a more square aspect.
    return f"""digraph pipeline {{
  rankdir=TB;
  compound=true;
  bgcolor=\"white\";
  labelloc=\"t\";
  label=\"{title}\";
  fontsize=24;
  fontname=\"Helvetica\";
  pad=0.25;
  nodesep=0.45;
  ranksep=0.75;
  newrank=true;

  node [shape=box, style=\"rounded,filled\", color=\"#444444\", fontname=\"Helvetica\", fontsize=12];
  edge [color=\"#444444\", penwidth=1.6, arrowsize=0.9];

  subgraph cluster_model {{
    label=\"Model and framework\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    inputs   [fillcolor=\"#EBF5FF\", label=\"Inputs & prep (Step 1)\\n\\nMacro/market series -> cleaning\\nstandardization + health checks\"];
    blocks   [fillcolor=\"#ECF8EF\", label=\"Country blocks (Step 2)\\n\\nGroup factors into themes\\n(banking, FX, public finance, commodities ...)\"];
    dynamics [fillcolor=\"#ECF8EF\", label=\"Factor dynamics (Step 3)\\n\\nVolatility + correlations\\n(ADCC / DCC-GARCH family)\\n+ mean reversion\"];

    inputs -> blocks -> dynamics;
  }}

  subgraph cluster_mc {{
    label=\"MC scenario building\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    mc      [fillcolor=\"#FFF3E6\", label=\"Monte Carlo scenarios (Step 4)\\n\\nSimulate many joint futures\\nfor block shock paths\"];
    regimes [fillcolor=\"#FFF3E6\", label=\"Regimes (Step 5)\\n\\nBucket draws by severity quantiles\\n(baseline -> adverse -> crisis)\"];

    mc -> regimes;
  }}

  subgraph cluster_reporting {{
    label=\"Reporting and Ranking\";
    labelloc=\"t\";
    fontsize=14;
    fontname=\"Helvetica\";
    style=\"rounded\";
    color=\"#444444\";
    penwidth=1.6;

    realized  [fillcolor=\"#EBF5FF\", label=\"Realized overlay\\n\\nCompute \\\"Today\\\" from the latest window\\n(in comparable sigma units)\"];
    reporting [fillcolor=\"#F7EEF7\", label=\"Reporting\\n\\nSeverity distributions + ranking\\nCrisis drivers (block shares)\"];
    network   [fillcolor=\"#F7EEF7\", label=\"Network lens\\n\\nConnectedness deltas\\n(stress regime vs median)\"];

    realized -> reporting;
  }}

  # Big-box arrows: Model -> MC -> Reporting (top to bottom)
  dynamics -> mc [ltail=cluster_model, lhead=cluster_mc];
  regimes -> reporting [ltail=cluster_mc, lhead=cluster_reporting];

  # Extra semantic link
  regimes -> network;
}}
"""


def _build_dot_three_cards(title: str) -> str:
    # Three parallel cards (left-to-right). Each card contains vertically stacked
    # sub-boxes rendered via an HTML-like label, which is much more reliable than
    # trying to get per-cluster rankdir overrides.
    def _html_escape(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )

    def card_label(card_title: str, rows: list[tuple[str, str, str]]) -> str:
        # rows: (bgcolor, heading, body)
        # HTML labels require escaping (notably '&' in headings like 'Inputs & prep').
        row_html = "".join(
            f"""
      <TR>
        <TD BGCOLOR=\"{bg}\" ALIGN=\"LEFT\">
          <FONT POINT-SIZE=\"12\"><B>{_html_escape(heading)}</B></FONT><BR/>
          <FONT POINT-SIZE=\"10\">{_html_escape(body)}</FONT>
        </TD>
      </TR>"""
            for (bg, heading, body) in rows
        )

        return f"""<
    <TABLE BORDER=\"1\" CELLBORDER=\"0\" CELLSPACING=\"0\" CELLPADDING=\"10\" COLOR=\"#444444\">
      <TR>
        <TD ALIGN=\"CENTER\" BGCOLOR=\"#FFFFFF\">
          <FONT POINT-SIZE=\"14\"><B>{_html_escape(card_title)}</B></FONT>
        </TD>
      </TR>
      <TR>
        <TD BGCOLOR=\"#FFFFFF\" HEIGHT=\"6\"></TD>
      </TR>
{row_html}
    </TABLE>
  >"""

    model_rows = [
        (
            "#EBF5FF",
            "Inputs & prep (Step 1)",
        "Macro/market series -> cleaning, standardization, health checks",
        ),
        (
            "#ECF8EF",
            "Country blocks (Step 2)",
            "Group factors into themes (banking, FX, public finance, commodities, ...)",
        ),
        (
            "#ECF8EF",
            "Factor dynamics (Step 3)",
            "Volatility + correlations (ADCC / DCC-GARCH family) + mean reversion",
        ),
    ]

    mc_rows = [
        (
            "#FFF3E6",
            "Monte Carlo scenarios (Step 4)",
            "Simulate many joint futures for block shock paths",
        ),
        (
            "#FFF3E6",
            "Regimes (Step 5)",
        "Bucket draws by severity quantiles (baseline -> adverse -> crisis)",
        ),
    ]

    report_rows = [
        (
            "#EBF5FF",
            "Realized overlay",
        "Compute \"Today\" from the latest window (comparable sigma units)",
        ),
        (
            "#F7EEF7",
            "Reporting",
            "Severity distributions + ranking; crisis drivers (block shares)",
        ),
        (
            "#F7EEF7",
            "Network lens",
            "Connectedness deltas (stress regime vs median)",
        ),
    ]

    model_label = card_label("Model and framework", model_rows)
    mc_label = card_label("MC scenario building", mc_rows)
    report_label = card_label("Reporting and Ranking", report_rows)

    return f"""digraph pipeline {{
  rankdir=LR;
  bgcolor=\"white\";
  labelloc=\"t\";
  label=\"{title}\";
  fontsize=24;
  fontname=\"Helvetica\";
  pad=0.25;
  nodesep=0.55;
  ranksep=0.8;

  edge [color=\"#444444\", penwidth=2.0, arrowsize=0.95];
  node [shape=box, style=\"rounded\", color=\"#444444\", fontname=\"Helvetica\"]; 

  model  [label={model_label}];
  mc     [label={mc_label}];
  report [label={report_label}];

  model -> mc -> report;
}}
"""


def _default_out_dir(bundle: str) -> Path:
    repo_root = Path(__file__).resolve().parents[1]
    return repo_root / "SRESS TEST PIPELINE" / "MC scenario plots" / bundle


HTML_TEMPLATE = """<!doctype html>
<html>
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <style>
    html, body { margin: 0; padding: 0; background: white; }
    #wrap { padding: 24px 28px; }
  </style>
</head>
<body>
  <div id=\"wrap\"></div>

  <script src=\"https://unpkg.com/viz.js@2.1.2/viz.js\"></script>
  <script src=\"https://unpkg.com/viz.js@2.1.2/full.render.js\"></script>
  <script>
    (async () => {
      const dot = DOT_SOURCE;
      const wrap = document.getElementById('wrap');
      try {
        const viz = new Viz();
        const svg = await viz.renderSVGElement(dot);
        svg.setAttribute('data-rendered', '1');
        wrap.appendChild(svg);
      } catch (e) {
        wrap.innerText = 'viz.js render failed: ' + (e && e.message ? e.message : String(e));
      }
    })();
  </script>
</body>
</html>
"""


def render(out_dir: Path, title: str, basename: str, variant: str) -> None:
    from playwright.sync_api import sync_playwright

    out_dir.mkdir(parents=True, exist_ok=True)

    dot = build_dot(title=title, variant=variant)
    # Embed DOT as a JS string literal (JSON-style escaping)
    dot_js = (
        '"' + dot.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n') + '"'
    )

    html = HTML_TEMPLATE.replace("DOT_SOURCE", dot_js)
    html_path = out_dir / f"{basename}.html"
    svg_path = out_dir / f"{basename}.svg"
    png_path = out_dir / f"{basename}.png"

    html_path.write_text(html, encoding="utf-8")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 900})
        page.goto(html_path.as_uri())

        # Wait for either a successful SVG render or a viz.js error message.
        page.wait_for_function(
            """() => {
  const wrap = document.getElementById('wrap');
  if (!wrap) return false;
  const svg = wrap.querySelector("svg[data-rendered='1']");
  if (svg) return true;
  const t = (wrap.innerText || '').trim();
  return t.startsWith('viz.js render failed:');
}""",
            timeout=60000,
        )

        svg_el = page.query_selector("svg[data-rendered='1']")
        if not svg_el:
            err_text = page.eval_on_selector(
                "#wrap", "(el) => (el && el.innerText) ? el.innerText : ''"
            )
            browser.close()
            raise RuntimeError(
                (err_text or "").strip() or "viz.js render failed (unknown error)"
            )

        svg_outer = page.eval_on_selector("svg[data-rendered='1']", "(el) => el.outerHTML")
        svg_path.write_text(svg_outer, encoding="utf-8")

        # Screenshot the SVG element (clean for LinkedIn)
        page.locator("svg[data-rendered='1']").screenshot(path=str(png_path))

        browser.close()

    print(f"Wrote: {png_path}")
    print(f"Wrote: {svg_path}")
    print(f"Wrote: {html_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", default="latest", help="Bundle folder under MC scenario plots")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory (overrides --bundle)")
    parser.add_argument(
        "--title",
        default="Stress-testing via Monte Carlo scenarios",
        help="Diagram title",
    )
    parser.add_argument(
        "--basename",
        default="PIPELINE_SCHEMATIC_VIZJS",
        help="Output file base name without extension",
    )
    parser.add_argument(
      "--variant",
      default="classic",
      choices=["classic", "three-blocks", "three-blocks-vertical", "three-cards"],
      help="Diagram layout variant",
    )
    args = parser.parse_args()

    out_dir = args.out_dir if args.out_dir is not None else _default_out_dir(args.bundle)
    render(out_dir=out_dir, title=args.title, basename=args.basename, variant=args.variant)


if __name__ == "__main__":
    main()
