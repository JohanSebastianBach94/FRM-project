"""Alias mapping between block-config names and ingested panel columns.

This module is deliberately *thin*: it only re-labels existing
stress-indicator / NSS-beta columns or known composites. It does not
introduce any new data or transformations.
"""

from __future__ import annotations

from typing import Dict


# One-way mapping: block name -> actual column name in the
# integrated stress panel (load_stress_indicators + load_nss_betas).
#
# IMPORTANT: Only include aliases that are actually present in the
# current pipelines; do not invent new series.
BLOCK_SERIES_ALIASES: Dict[str, str] = {
    # Equity indices (Investing / Yahoo vs internal labels)
    "^GSPC": "SP500",
    "^GDAXI": "DAX",
    "^IBEX": "IBEX_35",

    # Monetary aggregates (EUR M3 alias)
    "EUR_M3_MABMM301EZM189S": "MABMM301EZM189S",

    # Sovereign bond aliases -> FRED long-term yields
    "BOND_Italy_10Y": "IRLTLT01ITM156N",
    "BOND_France_10Y": "IRLTLT01FRM156N",
    "BOND_Germany_10Y": "IRLTLT01DEM156N",
    "BOND_Spain_10Y": "IRLTLT01ESM156N",
    "BOND_United_10Y": "IRLTLT01USM156N",

    # Note: 2Y aliases are *not* present in the current stress
    # panel and remain optional; they will be dropped cleanly.
}


def resolve_series_name(name: str) -> str:
    """Return the actual column name to use for a block series.

    If ``name`` is present in ``BLOCK_SERIES_ALIASES`` we return the
    mapped value, otherwise we leave it unchanged.
    """

    return BLOCK_SERIES_ALIASES.get(name, name)


def apply_aliases(series_names):
    """Apply alias resolution to an iterable of series names.

    Parameters
    ----------
    series_names : Iterable[str]
        Names coming from ``country_blocks_extended.yaml``.

    Returns
    -------
    list[str]
        Resolved names, suitable for indexing the integrated panel.
    """

    return [resolve_series_name(s) for s in series_names]
