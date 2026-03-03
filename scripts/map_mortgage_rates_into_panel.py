#!/usr/bin/env python3
"""Map ECB MIR mortgage-rate series into the merged stress-indicators panel.

Problem this fixes
- The merged panel (data_pipeline/data/stress_indicators_expanded.csv) historically contained
  long-term yields (IRLTLT01*) but not the intended per-country mortgage-rate proxies.
- Project blocks expect canonical series names like `Mortgage_rate_DEU`, which should be
  sourced from ECB MIR (MFI interest-rate statistics) series.

What this script does
- Reads ECB MIR CSVs under data_repository/raw/ecb/
- Builds daily (or panel-frequency) series by forward-filling the monthly observations
  across the panel date index
- Writes/overwrites the canonical columns:
    Mortgage_rate_DEU, Mortgage_rate_FRA, Mortgage_rate_ITA, Mortgage_rate_ESP

Usage
  python scripts/map_mortgage_rates_into_panel.py
  python scripts/map_mortgage_rates_into_panel.py --panel data_pipeline/data/stress_indicators_expanded.csv

Notes
- This script intentionally does NOT remove any existing columns from the panel.
- It only overwrites the mortgage-rate columns if present, otherwise it adds them.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

ECB_SERIES_BY_ISO = {
    "DEU": "MIR.M.DE.B.A2C.A.R.A.2250.EUR.N",
    "FRA": "MIR.M.FR.B.A2C.A.R.A.2250.EUR.N",
    "ITA": "MIR.M.IT.B.A2C.A.R.A.2250.EUR.N",
    "ESP": "MIR.M.ES.B.A2C.A.R.A.2250.EUR.N",
}

DEFAULT_PANELS = [
    ROOT / "data_pipeline" / "data" / "stress_indicators_expanded.csv",
    ROOT / "data" / "stress_indicators_expanded.csv",
]


def _read_ecb_monthly_series(path: Path) -> pd.Series:
    df = pd.read_csv(path, dtype={"date": str})
    if "date" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Unexpected ECB CSV schema in {path}")

    # ECB MIR monthly observations come as YYYY-MM.
    dates = pd.to_datetime(df["date"].astype(str).str.strip() + "-01", errors="coerce")
    values = pd.to_numeric(df["value"], errors="coerce")

    out = pd.Series(values.values, index=dates)
    out = out[~out.index.isna()]
    out = out.sort_index()
    out = out[~out.index.duplicated(keep="last")]
    out.name = path.stem
    return out


def _map_into_panel(panel_path: Path) -> None:
    if not panel_path.exists():
        return

    panel = pd.read_csv(panel_path, index_col=0, parse_dates=True)
    if not isinstance(panel.index, pd.DatetimeIndex):
        panel.index = pd.to_datetime(panel.index, errors="coerce")
    panel = panel.sort_index()

    for iso, ecb_id in ECB_SERIES_BY_ISO.items():
        src_path = ROOT / "data_repository" / "raw" / "ecb" / f"{ecb_id}.csv"
        if not src_path.exists():
            raise FileNotFoundError(f"Missing ECB mortgage series CSV: {src_path}")

        monthly = _read_ecb_monthly_series(src_path)
        # Map monthly observations onto panel dates: forward-fill across the panel index.
        mapped = monthly.reindex(panel.index, method="ffill")
        # Do not back-fill before first observation.
        mapped.loc[mapped.index < monthly.index.min()] = pd.NA

        col = f"Mortgage_rate_{iso}"
        panel[col] = mapped.astype("float64")

    panel.to_csv(panel_path)
    print(f"Updated mortgage-rate columns in {panel_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--panel",
        action="append",
        default=None,
        help="Panel CSV path to update. Can be passed multiple times.",
    )
    args = parser.parse_args()

    if args.panel:
        panels = [Path(p) if Path(p).is_absolute() else (ROOT / p) for p in args.panel]
    else:
        panels = DEFAULT_PANELS

    updated_any = False
    for panel_path in panels:
        if panel_path.exists():
            _map_into_panel(panel_path)
            updated_any = True

    if not updated_any:
        raise FileNotFoundError(
            "No panel file found to update. Looked for: " + ", ".join(str(p) for p in panels)
        )


if __name__ == "__main__":
    main()
