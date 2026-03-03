"""Run the daily FOR-ST volatility + correlation pipeline.

This script is a thin orchestrator that wires together:

1. A pre-built daily factor panel per ISO
2. Volatility estimation ("FOR ST" GARCH/mean-reversion proxies)
3. Dynamic correlation estimation ("FOR ST" ADCC proxies)

It is intentionally minimal and can be extended as the
underlying model implementations mature.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from models.garch.garch_for_st import VolatilityConfig, build_vol_panel_for_st
from models.adcc.adcc_for_st import CorrelationConfig, build_rt_panel_for_st


DEFAULT_ISOS = ["ITA", "ESP", "FRA", "DEU", "USA"]
DEFAULT_FACTORS = [
    "NAEXKP01ITQ661S",  # GDP (example ITA code)
    "ITACPIALLMINMEI",  # CPI
    "LRHUTTTTITM156S",  # Unemployment
    "MORTGAGE30US",     # Mortgage rate
    "ITA_beta0",
    "ITA_beta1",
    "ITA_beta2",
    "VIXCLS",
    "BAMLH0A0HYM2",
    "ITA_FCI",
]


def run_for_iso(iso: str, base_dir: Path, model_name: str = "garch") -> None:
    """Run the full FOR-ST chain for a single ISO.

    Parameters
    ----------
    iso: Country ISO code.
    base_dir: Project root directory.
    model_name: Volatility model name (currently informational only).
    """

    analysis_dir = base_dir / "analysis_outputs"

    daily_factors_path = analysis_dir / "daily_factors" / f"{iso}_daily_factors.csv"
    vol_out_path = analysis_dir / "vol_daily" / f"{iso}_vol_{model_name}.csv"
    rt_out_path = analysis_dir / "diag_corr_daily" / f"{iso}_Rt_{model_name}.csv"

    # Factor names are currently hard-wired for ITA-style template.
    # In practice, these should be mapped per-ISO (e.g. using
    # stress_indicators_config).
    factors = DEFAULT_FACTORS

    vol_cfg = VolatilityConfig(iso=iso, factors=factors, model_name=model_name)
    vol_df = build_vol_panel_for_st(daily_factors_path, vol_cfg, vol_out_path)

    # For now we treat raw demeaned series as residuals; later this
    # should be replaced by properly standardised residuals from the
    # volatility models.
    resid_path = daily_factors_path

    corr_cfg = CorrelationConfig(iso=iso, factors=factors, model_name="adcc")
    _ = build_rt_panel_for_st(resid_path, corr_cfg, rt_out_path)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily FOR-ST pipeline")
    parser.add_argument(
        "--isos",
        nargs="*",
        default=DEFAULT_ISOS,
        help="List of ISO country codes to process",
    )
    parser.add_argument(
        "--base-dir",
        type=str,
        default=".",
        help="Project root directory",
    )
    parser.add_argument(
        "--model-name",
        type=str,
        default="garch",
        help="Name of volatility model (label only for now)",
    )
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    args = parse_args(argv)
    base_dir = Path(args.base_dir).resolve()

    for iso in args.isos:
        run_for_iso(iso, base_dir, model_name=args.model_name)


if __name__ == "__main__":  # pragma: no cover
    main()
