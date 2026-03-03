"""Generate a Markdown report comparing current block series vs common literature.

Outputs:
- analysis_outputs/block_series_vs_literature.md

This is intentionally lightweight: literature lists are generic factor families used
in macro-finance / stress-testing practice, and "differences" are heuristic.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]
BLOCKS_JSON = ROOT / "outputs" / "country_block_definition.json"
OUT_MD = ROOT / "analysis_outputs" / "block_series_vs_literature.md"


LITERATURE_EXPECTATIONS: dict[str, list[str]] = {
    "macro": [
        "Real activity: GDP / industrial production / output gap",
        "Prices: CPI / inflation expectations",
        "Labor market: unemployment",
        "Policy/financial conditions: policy rate, term spread",
        "Credit cycle: credit growth / private-sector leverage",
    ],
    "public_finance": [
        "Fiscal stance: debt-to-GDP, deficit balance",
        "Sovereign risk: CDS or sovereign yield spreads",
        "Yield curve: level/slope/curvature (e.g., Nelson–Siegel/DNSS factors)",
        "Ratings/probability-of-default proxies (where available)",
    ],
    "banking_system": [
        "Bank equity returns / bank equity index",
        "Bank funding stress: interbank spreads (TED, Euribor-OIS, FRA-OIS)",
        "Bank credit risk: bank CDS spreads / CoCo spreads",
        "Balance-sheet health: capital ratios, NPLs, provisions (often quarterly)",
    ],
    "financial_markets": [
        "Equity market returns",
        "Implied volatility: VIX/V2X (or realized vol)",
        "Corporate credit spreads (IG/HY) and/or swap spreads",
        "Liquidity/risk appetite proxies (TEDRATE/EURIBOR/MOVE)",
    ],
    "systemic_stress": [
        "Cross-asset stress: swaption/forward vol proxies plus a funding-stress proxy (implied equity vol is handled by the financial_markets block)",
        "Funding stress: short-term funding/liquidity spread proxies (e.g., commercial paper spread)",
        "(Optional) Central bank balance sheet / liquidity measures (where relevant and not duplicated)",
        "(Optional) Composite stress index style basket",
    ],
    "real_estate": [
        "House price index (HPI)",
        "Mortgage rates / lending standards",
        "Household leverage / mortgage credit",
        "Affordability: price-to-income, rent-to-income",
    ],
    "external_fx": [
        "Exchange rate level (broad index and key crosses)",
        "FX volatility (sometimes optional)",
        "External balance/capital flow proxies (often quarterly)",
    ],
    "commodities": [
        "Energy (oil/gas)",
        "Industrial metals",
        "Agriculture / food commodities",
        "Terms-of-trade shocks (for commodity importers/exporters)",
    ],
}


def _tag_current_features(series_codes: list[str]) -> list[str]:
    codes = set(series_codes)
    tags: list[str] = []

    def has(prefix: str) -> bool:
        return any(c.startswith(prefix) for c in codes)

    if "VIXCLS" in codes:
        tags.append("Implied equity volatility: VIX")
    if "V2X" in codes:
        tags.append("Implied equity volatility: V2X (EuroStoxx 50)")
    if any("TED" in c or "EURIBOR" in c or "MOVE" in c or "LIBOR" in c or "SOFR" in c for c in codes):
        tags.append("Liquidity/risk-appetite proxy (TEDRATE/EURIBOR/MOVE)")
    if any(c.startswith("COMM_PAPER_SPREAD") for c in codes):
        tags.append("Funding stress proxy (commercial paper spread)")
    if has("SWAPTION_VOL"):
        tags.append("Swaption-implied volatility proxy (SWAPTION_VOL_*)")
    if has("credit_spread_"):
        tags.append("Corporate credit spread proxy (country-labelled)")
    if has("CDS_") or has("cds_"):
        tags.append("Sovereign CDS proxy")
    if has("spread_") or has("BTP_") or has("Bonos_") or has("OAT_"):
        tags.append("Sovereign yield spread proxy")
    if has("Bank_equity_index_"):
        tags.append("Bank equity index proxy")
    if "ECBASSETS" in codes or "WALCL" in codes:
        tags.append("Central bank balance sheet proxy (ECBASSETS/WALCL)")
    elif any(c.startswith("ECB") for c in codes):
        tags.append("Central bank / ECB policy-liquidity proxy")
    if any(c.startswith("USD_") or c.endswith("_USD") for c in codes):
        tags.append("FX cross rates / USD factor")

    return tags


def main() -> None:
    if not BLOCKS_JSON.exists():
        raise FileNotFoundError(f"Missing {BLOCKS_JSON}")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(BLOCKS_JSON.read_text(encoding="utf-8"))

    lines: list[str] = []
    lines.append(f"# Country blocks: current series vs literature")
    lines.append("")
    lines.append(f"Generated: {date.today().isoformat()}")
    lines.append("")
    lines.append("This file compares:")
    lines.append("- **Current situation**: series lists from `outputs/country_block_definition.json`.")
    lines.append("- **Literature (generic)**: common factor families used in stress testing / macro-finance practice.")
    lines.append("")
    lines.append("Notes:")
    lines.append("- The literature lists are *generic* (not jurisdiction-specific).")
    lines.append("- ‘Differences’ are heuristic and highlight obvious gaps/proxies.")
    lines.append("")

    for iso in sorted(data.keys()):
        country = data[iso]
        lines.append(f"## {iso}")
        lines.append("")
        blocks = country.get("blocks", [])
        for block in blocks:
            key = str(block.get("key", ""))
            series_codes = [str(c) for c in (block.get("series_codes") or []) if str(c).strip()]
            lines.append(f"### {key}")
            lines.append("")
            lines.append("**Current series**")
            if series_codes:
                lines.append("- " + ", ".join(series_codes))
            else:
                lines.append("- (none)")

            tags = _tag_current_features(series_codes)
            if tags:
                lines.append("")
                lines.append("**Current signals present**")
                for t in tags:
                    lines.append(f"- {t}")

            expected = LITERATURE_EXPECTATIONS.get(key)
            if expected:
                lines.append("")
                lines.append("**Literature: typical drivers in this block**")
                for e in expected:
                    lines.append(f"- {e}")
                if key == "systemic_stress":
                    lines.append("")
                    lines.append("- Implied-volatility is handled in the financial_markets block (VIXCLS/V2X); systemic_stress centers on swaption/credit proxies plus a funding-stress proxy (commercial paper spread).")

            # Lightweight differences callout
            diffs: list[str] = []
            if key == "financial_markets":
                if "VIXCLS" not in series_codes and "V2X" not in series_codes:
                    diffs.append("Missing implied-volatility proxy (VIX/V2X) in this block.")
                if not any("TED" in s or "EURIBOR" in s or "MOVE" in s or "LIBOR" in s or "SOFR" in s for s in series_codes):
                    diffs.append("Missing liquidity/risk-appetite proxy (TEDRATE/EURIBOR/MOVE) in this block.")
                if not any(s.startswith("credit_spread_") for s in series_codes):
                    diffs.append("Missing corporate credit-spread proxy in this block.")
            elif key == "systemic_stress":
                if not any(s.startswith("SWAPTION_VOL") for s in series_codes):
                    diffs.append("Missing swaption-vol proxy (SWAPTION_VOL_*) in this block if implied vol is desired.")
                if not any(s.startswith("COMM_PAPER_SPREAD") for s in series_codes):
                    diffs.append("Missing funding-stress proxy (commercial paper spread) in this block.")
            if key == "banking_system":
                if not any(s.startswith("Bank_equity_index_") for s in series_codes):
                    diffs.append("No bank equity index proxy (common in top-down bank stress).")
                if not any("TED" in s or "EURIBOR" in s or "SOFR" in s for s in series_codes):
                    diffs.append("No explicit interbank/funding spread proxy (TED / Euribor-OIS / etc.).")
            if key == "public_finance":
                if not any("GC.DOD" in s or "DEBT" in s for s in series_codes):
                    diffs.append("No explicit debt-to-GDP style fiscal stock proxy.")
                if not any(s.startswith("CDS_") or s.lower().startswith("cds_") for s in series_codes):
                    diffs.append("No sovereign CDS proxy (often used where liquid).")

            if diffs:
                lines.append("")
                lines.append("**Differences / gaps vs literature (heuristic)**")
                for d in diffs:
                    lines.append(f"- {d}")

            lines.append("")

        lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
