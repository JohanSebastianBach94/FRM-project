#!/usr/bin/env python3
"""Generate all presentation figures for the Beamer slides.

Produces (in PAPER/figures/):
  fig_gpr_calibration.pdf     - GPRT 1985-2026 + 3 forward AR(1) scenario paths
  fig_energy_fancharts.pdf    - 2x3 multi-panel fan charts (energy commodities)
  fig_fert_metals_fancharts.pdf - 2x4 multi-panel fan charts (fertilizers + metals)
  fig_country_exposure.pdf    - stacked bar chart: 5 countries at h=12 Escalation

Run from the PAPER directory:
    python make_figures.py --run-id v4_koyck_apr2026
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
from matplotlib.lines import Line2D

# ─── Paths ────────────────────────────────────────────────────────────────────
PAPER_DIR     = Path(__file__).resolve().parent
PROJECT_ROOT  = PAPER_DIR.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"
GPRT_CSV      = PROJECT_ROOT / "data_repository" / "raw" / "geopolitical" / "GPRT.csv"
GPR_DAILY_XLS = PROJECT_ROOT / "data_repository" / "data_gpr_daily_recent.xls"
PINK_SHEET    = PROJECT_ROOT / "data_repository" / "CMO-Historical-Data-Monthly.xlsx"
FIGURES_DIR   = PAPER_DIR / "figures"
FIGURES_DIR.mkdir(exist_ok=True)

# ─── Scenario colours ─────────────────────────────────────────────────────────
SCEN_COLOR = {
    "De-escalation": "#1f77b4",   # blue
    "Baseline":      "#2ca02c",   # green
    "Escalation":    "#d62728",   # red
}
SCEN_ORDER = ["De-escalation", "Baseline", "Escalation"]

# ─── Human-readable commodity labels ─────────────────────────────────────────
COMMODITY_LABEL = {
    "CrudeOil":  "Crude Oil (WTI)",
    "Brent":     "Brent Crude",
    "NatGas_US": "Nat. Gas (US)",
    "NatGas_EU": "Nat. Gas (EU TTF)",
    "LNG_Japan": "LNG Japan",
    "Coal_AU":   "Coal (Australia)",
    "Urea":      "Urea",
    "DAP":       "DAP",
    "TSP":       "TSP",
    "Phosphate": "Phosphate",
    "Wheat":     "Wheat",
    "Maize":     "Maize",
    "Copper":    "Copper",
    "Aluminum":  "Aluminum",
}

# ─── Unit labels (USD per unit) ───────────────────────────────────────────────
UNIT_LABEL = {
    "CrudeOil":  "USD/bbl",
    "Brent":     "USD/bbl",
    "NatGas_US": "USD/MMBtu",
    "NatGas_EU": "USD/MMBtu",
    "LNG_Japan": "USD/MMBtu",
    "Coal_AU":   "USD/mt",
    "Urea":      "USD/mt",
    "DAP":       "USD/mt",
    "TSP":       "USD/mt",
    "Phosphate": "USD/mt",
    "Wheat":     "USD/mt",
    "Maize":     "USD/mt",
    "Copper":    "USD/mt",
    "Aluminum":  "USD/mt",
}

# ─── Commodity groups for colour-coding the stacked bar ──────────────────────
COMMODITY_GROUP = {
    "CrudeOil":  "Energy",
    "Brent":     "Energy",
    "NatGas_US": "Energy",
    "NatGas_EU": "Energy",
    "LNG_Japan": "Energy",
    "Coal_AU":   "Energy",
    "Urea":      "Fertilizer",
    "DAP":       "Fertilizer",
    "TSP":       "Fertilizer",
    "Phosphate": "Fertilizer",
    "Wheat":     "Agriculture",
    "Maize":     "Agriculture",
    "Copper":    "Metals",
    "Aluminum":  "Metals",
}
GROUP_COLOR = {
    "Energy":      "#d62728",
    "Fertilizer":  "#ff7f0e",
    "Agriculture": "#2ca02c",
    "Metals":      "#9467bd",
}


# ══════════════════════════════════════════════════════════════════════════════
# Figure 1 – GPR historical + forward scenario paths
# ══════════════════════════════════════════════════════════════════════════════

def _ar1_raw(gprt0: float, mu_inf_raw: float, rho: float, H: int) -> list[float]:
    """AR(1) path in RAW (non-z) scale, h=1..H."""
    return [mu_inf_raw + (rho ** h) * (gprt0 - mu_inf_raw) for h in range(1, H + 1)]


def fig_gpr_calibration(run_id: str) -> None:
    """GPRT historical series + 3 forward AR(1) scenario paths."""
    gpr = pd.read_csv(GPRT_CSV, parse_dates=["date"], index_col="date")["value"]

    # Current point (March 2026) and scenario parameters (from scenarios.py)
    rho_T     = 0.710
    gprt_0    = 293.0          # raw March 2026
    scenarios = {
        "De-escalation": {"mu_inf": 103.0, "color": SCEN_COLOR["De-escalation"]},
        "Baseline":      {"mu_inf": 202.0, "color": SCEN_COLOR["Baseline"]},
        "Escalation":    {"mu_inf": 355.0, "color": SCEN_COLOR["Escalation"]},
    }
    H = 12
    # Forward dates: April 2026 … March 2027
    last_date = gpr.index[-1]   # 2026-03-01
    fwd_dates = pd.date_range(start=last_date, periods=H + 1, freq="MS")  # h=0..12

    fig, ax = plt.subplots(figsize=(11, 4))

    # Historical
    ax.plot(gpr.index, gpr.values, color="black", lw=1.2, zorder=2, label="GPRT (historical)")

    # Vertical divider at current date
    ax.axvline(last_date, color="grey", lw=0.8, ls="--", alpha=0.7)

    # Forward paths
    for scen_name, spec in scenarios.items():
        path = [gprt_0] + _ar1_raw(gprt_0, spec["mu_inf"], rho_T, H)
        ax.plot(fwd_dates, path, color=spec["color"], lw=2.0, ls="--",
                label=scen_name, zorder=3)

    # Key event labels
    events = [
        ("1990-08-01", "Gulf War"),
        ("2001-09-01", "9/11"),
        ("2003-03-01", "Iraq War"),
        ("2014-03-01", "Crimea"),
        ("2022-02-01", "Ukraine War"),
        ("2023-10-01", "Gaza"),
        ("2026-03-01", "Iran War\n(Mar 2026)"),
    ]
    ymax = max(gpr.max(), 355) * 1.05
    for date_str, label in events:
        dt = pd.Timestamp(date_str)
        if dt in gpr.index or dt <= gpr.index[-1]:
            val = gpr.get(dt, gpr.asof(dt)) if dt <= gpr.index[-1] else gprt_0
        else:
            val = gprt_0
        ax.annotate(
            label,
            xy=(dt, val),
            xytext=(0, 18),
            textcoords="offset points",
            fontsize=7,
            ha="center",
            arrowprops=dict(arrowstyle="-", color="grey", lw=0.6),
            zorder=4,
        )

    ax.set_xlim(pd.Timestamp("1985-01-01"), fwd_dates[-1])
    ax.set_ylim(0, ymax)
    ax.set_xlabel("")
    ax.set_ylabel("GPRT (index)")
    ax.set_title("GPR Threats Index 1985–2026 and Scenario Paths", fontsize=11)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.85)
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    out = FIGURES_DIR / "fig_gpr_calibration.pdf"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  [OK] {out.name}")


# ══════════════════════════════════════════════════════════════════════════════
# Figure 2 & 3 – Fan-chart multi-panels
# ══════════════════════════════════════════════════════════════════════════════

def _load_mc_stats(run_id: str) -> dict:
    path = SCENARIOS_DIR / run_id / "step_13" / "channel_a" / "channel_a_mc_stats.json"
    with open(path) as f:
        return json.load(f)


def _load_point_paths(run_id: str) -> dict:
    path = SCENARIOS_DIR / run_id / "step_13" / "channel_a" / "channel_a_point_paths.json"
    with open(path) as f:
        return json.load(f)


def _panel_fancharts(
    commodities: list[str],
    run_id: str,
    out_path: Path,
    title: str,
    ncols: int = 3,
) -> None:
    mc    = _load_mc_stats(run_id)           # {scenario: {commodity: {p05,p50,p95,...}}}
    pp    = _load_point_paths(run_id)        # {scenario: {commodity: [P0..P12]}}

    n     = len(commodities)
    nrows = int(np.ceil(n / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(4.5 * ncols, 3.2 * nrows), sharex=True)
    axes_flat = np.array(axes).ravel()

    H = 12
    months = list(range(H + 1))  # 0..12

    for i, key in enumerate(commodities):
        ax = axes_flat[i]
        for scen in SCEN_ORDER:
            P0   = pp[scen][key][0]
            p50  = [P0] + mc[scen][key]["p50"]
            p05  = [P0] + mc[scen][key]["p05"]
            p95  = [P0] + mc[scen][key]["p95"]
            c    = SCEN_COLOR[scen]
            ax.fill_between(months, p05, p95, color=c, alpha=0.10, linewidth=0)
            ax.plot(months, p50, color=c, lw=1.6, label=scen if i == 0 else None)

        # h=12 Escalation change annotation
        P0_esc  = pp["Escalation"][key][0]
        P12_esc = mc["Escalation"][key]["p50"][-1]
        pct     = (P12_esc / P0_esc - 1.0) * 100.0
        sign    = "+" if pct >= 0 else ""
        ax.set_title(
            f"{COMMODITY_LABEL.get(key, key)}\n"
            f"Esc h=12: {sign}{pct:.1f}%",
            fontsize=8.5,
        )
        ax.set_ylabel(UNIT_LABEL.get(key, "USD"), fontsize=7)
        ax.tick_params(labelsize=7)
        ax.grid(True, alpha=0.2)
        ax.set_xticks([0, 3, 6, 9, 12])

    # Turn off unused subplots
    for j in range(n, len(axes_flat)):
        axes_flat[j].axis("off")

    # x-label only on bottom row
    for ax in axes_flat[(nrows - 1) * ncols : nrows * ncols]:
        ax.set_xlabel("Months ahead", fontsize=8)

    # Shared legend at figure top
    leg_handles = [
        Line2D([0], [0], color=SCEN_COLOR[s], lw=2, label=s) for s in SCEN_ORDER
    ]
    fig.legend(handles=leg_handles, loc="upper center", ncol=3, fontsize=9,
               framealpha=0.9, bbox_to_anchor=(0.5, 1.00))

    fig.suptitle(title, fontsize=11, y=1.03)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] {out_path.name}")


def fig_energy_fancharts(run_id: str) -> None:
    _panel_fancharts(
        commodities=["CrudeOil", "Brent", "NatGas_US", "NatGas_EU", "LNG_Japan", "Coal_AU"],
        run_id=run_id,
        out_path=FIGURES_DIR / "fig_energy_fancharts.pdf",
        title="Energy Commodities — Scenario Fan Charts (Channel A)",
        ncols=3,
    )


def fig_fert_metals_fancharts(run_id: str) -> None:
    """Legacy combined figure kept for reference."""
    _panel_fancharts(
        commodities=["Urea", "DAP", "TSP", "Phosphate", "Wheat", "Maize", "Copper", "Aluminum"],
        run_id=run_id,
        out_path=FIGURES_DIR / "fig_fert_metals_fancharts.pdf",
        title="Fertilizers, Agriculture & Metals — Scenario Fan Charts (Channel A)",
        ncols=4,
    )


def fig_fert_ag_fancharts(run_id: str) -> None:
    _panel_fancharts(
        commodities=["Urea", "DAP", "TSP", "Phosphate", "Wheat", "Maize"],
        run_id=run_id,
        out_path=FIGURES_DIR / "fig_fert_ag_fancharts.pdf",
        title="Fertilizers & Agriculture — Scenario Fan Charts (Channel A)",
        ncols=3,
    )


def fig_metals_fancharts(run_id: str) -> None:
    _panel_fancharts(
        commodities=["Copper", "Aluminum"],
        run_id=run_id,
        out_path=FIGURES_DIR / "fig_metals_fancharts.pdf",
        title="Metals — Scenario Fan Charts (Channel A)",
        ncols=2,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Figure 4 – Country exposure stacked bar at h=12, Escalation
# ══════════════════════════════════════════════════════════════════════════════

def fig_country_exposure(run_id: str) -> None:
    """Stacked bar: per-commodity trade-weighted price impact at h=12 Escalation."""
    # Load comtrade weights
    wt_path = SCENARIOS_DIR / run_id / "step_13" / "country_impact" / "comtrade_weights.json"
    with open(wt_path) as f:
        weights = json.load(f)   # {ISO: {commodity: net_trade_share}}

    # Load channel A summary for pct_point at h=12 Escalation
    summ = pd.read_csv(
        SCENARIOS_DIR / run_id / "step_13" / "channel_a" / "channel_a_summary.csv"
    )
    esc12 = summ[(summ["scenario"] == "Escalation") & (summ["h"] == 12)].set_index("commodity")

    ISOS = ["DEU", "ESP", "FRA", "ITA", "USA"]
    COMMODITIES = list(COMMODITY_LABEL.keys())

    # Build exposure matrix: ISO × commodity
    data: dict[str, dict[str, float]] = {}
    for iso in ISOS:
        w = weights.get(iso, {})
        data[iso] = {}
        for c in COMMODITIES:
            pct = float(esc12.loc[c, "pct_point"]) if c in esc12.index else 0.0
            wt  = float(w.get(c, 0.0))
            data[iso][c] = wt * pct   # exposure contribution (pp)

    # Build DataFrame
    df = pd.DataFrame(data, index=COMMODITIES).T   # ISO × commodity

    # Separate positive and negative contributions for stacked bar
    pos = df.clip(lower=0)
    neg = df.clip(upper=0)

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(len(ISOS))
    bar_w = 0.55

    bottom_pos = np.zeros(len(ISOS))
    bottom_neg = np.zeros(len(ISOS))
    legend_handles: list[mpatches.Patch] = []
    added_groups: set[str] = set()

    for c in COMMODITIES:
        grp   = COMMODITY_GROUP[c]
        color = GROUP_COLOR[grp]
        pos_vals = pos[c].values
        neg_vals = neg[c].values

        ax.bar(x, pos_vals, bar_w, bottom=bottom_pos, color=color, alpha=0.85, linewidth=0)
        ax.bar(x, neg_vals, bar_w, bottom=bottom_neg, color=color, alpha=0.85, linewidth=0)
        bottom_pos += pos_vals
        bottom_neg += neg_vals

        if grp not in added_groups:
            legend_handles.append(mpatches.Patch(facecolor=color, label=grp))
            added_groups.add(grp)

    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(ISOS, fontsize=10)
    ax.set_ylabel("Trade-weighted price impact (pp)", fontsize=9)
    ax.set_title(
        "Country Exposure to Escalation Scenario at h = 12\n"
        "(comtrade net-trade-share × median price change, Channel A)",
        fontsize=10,
    )
    ax.legend(handles=legend_handles, loc="upper right", fontsize=8, framealpha=0.9)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    out = FIGURES_DIR / "fig_country_exposure.pdf"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"  [OK] {out.name}")


# ══════════════════════════════════════════════════════════════════════════════
# Figure 0 – War overview: GPR daily + commodity price changes Feb→Mar 2026
# ══════════════════════════════════════════════════════════════════════════════

def fig_war_overview() -> None:
    """Two-panel: (L) GPR daily Oct 2024–Apr 2026; (R) % price change Feb→Mar 2026."""
    # ── Load GPR daily ────────────────────────────────────────────────────────
    gpr = pd.read_excel(GPR_DAILY_XLS, sheet_name=0)
    gpr["date"] = pd.to_datetime(gpr["DAY"].astype(str), format="%Y%m%d", errors="coerce")
    gpr = gpr.dropna(subset=["date", "GPRD"])
    gpr = (
        gpr[gpr["date"] >= "2024-10-01"]
        .set_index("date")[["GPRD", "GPRD_ACT"]]
    )

    # ── Load Pink Sheet monthly prices Feb & Mar 2026 ─────────────────────────
    ps = pd.read_excel(PINK_SHEET, sheet_name="Monthly Prices", header=4, skiprows=[5])
    ps = ps.rename(columns={"Unnamed: 0": "date"})
    ps = ps[ps["date"].notna() & ps["date"].astype(str).str.match(r"\d{4}M\d{2}")]
    ps["date"] = pd.to_datetime(ps["date"].astype(str).str.replace("M", ""), format="%Y%m")
    ps = ps.set_index("date")

    price_items = [
        ("Crude Oil",   "Crude oil, average",           "Energy"),
        ("Brent",       "Crude oil, Brent",              "Energy"),
        ("NatGas EU",   "Natural gas, Europe",           "Energy"),
        ("LNG Japan",   "Liquefied natural gas, Japan",  "Energy"),
        ("Coal AU",     "Coal, Australian",              "Energy"),
        ("DAP",         "DAP",                           "Fertilizer"),
        ("Wheat",       "Wheat, US SRW",                 "Agriculture"),
        ("Maize",       "Maize",                         "Agriculture"),
        ("Copper",      "Copper",                        "Metals"),
        ("Aluminum",    "Aluminum",                      "Metals"),
    ]
    labels, pct_changes, bar_colors = [], [], []
    for label, col, group in price_items:
        if col not in ps.columns:
            continue
        try:
            p_feb = float(ps.loc[pd.Timestamp("2026-02-01"), col])
            p_mar = float(ps.loc[pd.Timestamp("2026-03-01"), col])
            pct = (p_mar - p_feb) / p_feb * 100
        except (KeyError, ZeroDivisionError):
            continue
        labels.append(label)
        pct_changes.append(pct)
        bar_colors.append(GROUP_COLOR[group])

    # ── Plot ──────────────────────────────────────────────────────────────────
    fig, (ax_gpr, ax_bar) = plt.subplots(
        1, 2, figsize=(11, 4.5), gridspec_kw={"width_ratios": [1.5, 1]}
    )

    # Left: GPR daily line chart
    war_start = pd.Timestamp("2026-02-28")
    pre  = gpr[gpr.index <  war_start]
    post = gpr[gpr.index >= war_start]
    ax_gpr.fill_between(pre.index,  0, pre["GPRD"],  color="#aec7e8", alpha=0.35)
    ax_gpr.fill_between(post.index, 0, post["GPRD"], color="#d62728", alpha=0.12)
    ax_gpr.plot(gpr.index, gpr["GPRD"],     color="#1f77b4", lw=1.3, label="GPR Total")
    ax_gpr.plot(gpr.index, gpr["GPRD_ACT"], color="#d62728", lw=1.0,
                linestyle="--", label="GPR Acts")
    peak_date = gpr["GPRD"].idxmax()
    peak_val  = gpr["GPRD"].max()
    ax_gpr.annotate(
        f"Peak {peak_val:.0f}\n({peak_date.strftime('%b %d')})",
        xy=(peak_date, peak_val),
        xytext=(18, -45), textcoords="offset points",
        fontsize=8, color="#d62728",
        arrowprops=dict(arrowstyle="->", color="#d62728", lw=0.9),
    )
    ax_gpr.axhline(100, color="gray", lw=0.7, linestyle=":")
    ax_gpr.axvline(war_start, color="#d62728", lw=0.9, linestyle=":", alpha=0.7)
    ax_gpr.set_ylabel("GPR Index  (1985–2019 = 100)", fontsize=9)
    ax_gpr.set_title("Daily Geopolitical Risk Index", fontsize=10, pad=4)
    ax_gpr.legend(fontsize=8, loc="upper left")
    ax_gpr.tick_params(labelsize=8)
    ax_gpr.xaxis.set_major_formatter(mdates.DateFormatter("%b\n%Y"))
    ax_gpr.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    # Right: horizontal bar chart
    y_pos = np.arange(len(labels))
    bars = ax_bar.barh(y_pos, pct_changes, color=bar_colors,
                       alpha=0.85, edgecolor="white", height=0.65)
    ax_bar.axvline(0, color="black", lw=0.8)
    ax_bar.set_yticks(y_pos)
    ax_bar.set_yticklabels(labels, fontsize=9)
    ax_bar.set_xlabel("% Change  Feb \u2192 Mar 2026", fontsize=9)
    ax_bar.set_title("Commodity Price Impact", fontsize=10, pad=4)
    ax_bar.tick_params(labelsize=8)
    x_max = max(pct_changes) * 1.25 + 8
    ax_bar.set_xlim(left=min(min(pct_changes) - 8, -5), right=x_max)
    for bar, val in zip(bars, pct_changes):
        offset = 1.2 if val >= 0 else -1.2
        ha     = "left"  if val >= 0 else "right"
        ax_bar.text(
            val + offset, bar.get_y() + bar.get_height() / 2,
            f"{val:+.1f}%", va="center", ha=ha, fontsize=8,
        )
    grp_patches = [
        mpatches.Patch(color=GROUP_COLOR[g], label=g, alpha=0.85)
        for g in ["Energy", "Fertilizer", "Agriculture", "Metals"]
    ]
    ax_bar.legend(handles=grp_patches, fontsize=8, loc="lower right")

    fig.suptitle(
        "Iran War Shock (2026): Geopolitical Risk and Commodity Price Changes",
        fontsize=11, y=1.02,
    )
    fig.tight_layout()
    out = FIGURES_DIR / "fig_war_overview.pdf"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  [OK] {out.name}")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate Beamer presentation figures.")
    p.add_argument("--run-id", default="v4_koyck_apr2026",
                   help="Simulation run-id (subfolder of analysis_outputs/scenarios/)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    print(f"Generating figures for run-id: {args.run_id}")
    print(f"Output directory: {FIGURES_DIR}")

    # fig_war_overview()  # slide replaced with text+table; kept for reference
    fig_gpr_calibration(args.run_id)
    fig_energy_fancharts(args.run_id)
    fig_fert_ag_fancharts(args.run_id)
    fig_metals_fancharts(args.run_id)
    fig_country_exposure(args.run_id)

    print("Done.")


if __name__ == "__main__":
    main()
