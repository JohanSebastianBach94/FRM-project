"""Derive missing sovereign and CDS proxies plus the commercial-paper spread."""
import io
import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
MARKET_DATA_DIR = ROOT / "data_repository" / "raw" / "market_data"
DERIVED_DIR = ROOT / "data_repository" / "raw" / "providers" / "derived_risk_drivers"
FRED_DIR = ROOT / "data_repository" / "raw" / "fred"
MARKET_DATA_DIR.mkdir(parents=True, exist_ok=True)
DERIVED_DIR.mkdir(parents=True, exist_ok=True)

FRED_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"
DEFAULT_TIMEOUT = 30

LOGGER = logging.getLogger("derive_public_finance_proxies")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)

BOND_LABELS = {
    "USA": "United",
    "DEU": "Germany",
    "ESP": "Spain",
    "FRA": "France",
    "ITA": "Italy",
}

FRED_YIELDS: Dict[str, Dict[str, str]] = {
    "USA": {"5Y": "DGS5", "10Y": "DGS10", "3M": "DGS3"},
    "DEU": {"5Y": "IRLTLT01DEM156N", "10Y": "IRLTLT01DEM156N"},
    "ESP": {"5Y": "IRLTLT01ESM156N", "10Y": "IRLTLT01ESM156N"},
    "FRA": {"5Y": "IRLTLT01FRM156N", "10Y": "IRLTLT01FRM156N"},
    "ITA": {"5Y": "IRLTLT01ITM156N", "10Y": "IRLTLT01ITM156N"},
}

SPREAD_REQUESTS = [
    {"target": "ESP", "base": "DEU", "tenor": "10Y"},
    {"target": "ESP", "base": "USA", "tenor": "10Y"},
    {"target": "FRA", "base": "DEU", "tenor": "10Y"},
    {"target": "ITA", "base": "DEU", "tenor": "10Y"},
    {"target": "FRA", "base": "USA", "tenor": "10Y"},
    {"target": "ITA", "base": "USA", "tenor": "10Y"},
]

CREDIT_SPREAD_INDEXES = {
    # Keep these as DAILY, high-coverage proxies. These are ICE BofA / BAML option-adjusted
    # spread or yield indices from FRED.
    "USA": "BAMLC0A4CBBB",  # US Corporate BBB OAS (a standard broad credit risk proxy)
    "DEU": "BAMLC0A1CAAAEY",
    "FRA": "BAMLC0A0CM",
    "ITA": "BAMLH0A1HYBB",
    "ESP": "BAMLH0A1HYBB",
}


def fetch_fred_series(series_id: str) -> pd.Series:
    """Load a FRED series, preferring local cached CSVs.

    The pipeline maintains a local cache under data_repository/raw/fred.
    If the file is present, we use it (avoids network / preserves native frequency).

    Falls back to fetching via the FRED graph endpoint if missing.
    """
    local_path = FRED_DIR / f"{series_id}.csv"
    if local_path.exists():
        df = pd.read_csv(local_path)
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        value_col = next((c for c in df.columns if c != date_col), None)
        if date_col is None or value_col is None:
            LOGGER.warning("Local FRED %s schema changed", local_path.name)
            return pd.Series(dtype=float)
        df = df[[date_col, value_col]].dropna()
        df = df.rename(columns={date_col: "date", value_col: series_id})
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.set_index("date")
        s = pd.to_numeric(df[series_id], errors="coerce").dropna()
        return s

    try:
        response = requests.get(
            FRED_BASE,
            params={"id": series_id, "cosd": "1990-01-01"},
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.warning("FRED fetch failed for %s: %s", series_id, exc)
        return pd.Series(dtype=float)
    df = pd.read_csv(io.StringIO(response.text))
    date_col = next((c for c in df.columns if "date" in c.lower()), None)
    value_col = next((c for c in df.columns if c != date_col), None)
    if date_col is None or value_col is None:
        LOGGER.warning("FRED %s schema changed", series_id)
        return pd.Series(dtype=float)
    df = df[[date_col, value_col]].dropna()
    df = df.rename(columns={date_col: "date", value_col: series_id})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df = df.set_index("date")
    return pd.to_numeric(df[series_id], errors="coerce").dropna()


def _export_series(series: pd.Series, label: str, column_name: str = "value") -> None:
    if series.empty:
        LOGGER.warning("No data to export for %s", label)
        return
    path = DERIVED_DIR / f"{label}.csv"
    df = series.to_frame(column_name)
    df.index.name = "date"
    df.to_csv(path)
    LOGGER.info("Exported %s (%d rows)", path.name, len(df))


def _read_csv(path: Path) -> pd.Series:
    if not path.exists():
        return pd.Series(dtype=float)
    df = pd.read_csv(path, parse_dates=["Date"])
    df = df.rename(columns={"Date": "date", "Value": "yield"})
    df = df.dropna(subset=["date", "yield"]).set_index("date")
    return df["yield"].dropna()


def _write_bond(series: pd.Series, label: str, tenor: str) -> Path:
    df = series.reset_index()
    df.columns = ["Date", "Value"]
    path = MARKET_DATA_DIR / f"BOND_{label}_{tenor}.csv"
    df.to_csv(path, index=False)
    LOGGER.info("Saved bond data %s", path.name)
    return path


def ensure_bond_series(iso: str, tenor: str) -> Optional[pd.Series]:
    label = BOND_LABELS.get(iso)
    if not label:
        LOGGER.warning("No label for %s", iso)
        return None
    path = MARKET_DATA_DIR / f"BOND_{label}_{tenor}.csv"
    if path.exists():
        return _read_csv(path)
    series_id = FRED_YIELDS.get(iso, {}).get(tenor)
    if not series_id:
        LOGGER.warning("No FRED series configured for %s %s", iso, tenor)
        return None
    series = fetch_fred_series(series_id)
    if series.empty:
        return None
    _write_bond(series, label, tenor)
    return series


def _resample_monthly(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return series.resample("ME").last().dropna()


def derive_cds_proxies() -> None:
    base = ensure_bond_series("USA", "5Y")
    if base is None:
        LOGGER.warning("Cannot compute CDS proxies without USA base")
        return
    for iso in ["DEU", "ESP"]:
        target = ensure_bond_series(iso, "5Y")
        if target is None:
            LOGGER.warning("Missing %s 5Y data", iso)
            continue
        merged = target.to_frame("target").join(base.to_frame("base"), how="inner")
        if merged.empty:
            LOGGER.warning("No overlapping dates for CDS proxy %s", iso)
            continue
        cds = (merged["target"] - merged["base"]).rename("cds_proxy")
        _export_series(_resample_monthly(cds), f"cds_proxy_{iso}_vs_USA_5Y", "cds_proxy")


def derive_spreads() -> None:
    for request in SPREAD_REQUESTS:
        target = ensure_bond_series(request["target"], request["tenor"])
        base = ensure_bond_series(request["base"], request["tenor"])
        if target is None or base is None:
            LOGGER.warning("Missing data for spread %s vs %s", request["target"], request["base"])
            continue
        merged = target.to_frame("target").join(base.to_frame("base"), how="inner")
        if merged.empty:
            LOGGER.warning("No overlap for spread %s vs %s", request["target"], request["base"])
            continue
        spread = (merged["target"] - merged["base"]).rename("spread")
        label = f"spread_{request['target']}_vs_{request['base']}_{request['tenor']}"
        _export_series(_resample_monthly(spread), label, "spread")


def derive_eu_credit_spreads() -> None:
    for iso, series_id in CREDIT_SPREAD_INDEXES.items():
        credit = fetch_fred_series(series_id)
        if credit.empty:
            LOGGER.warning("Missing credit series for %s credit spread (%s)", iso, series_id)
            continue

        # FRED credit spread series can include weekend rows (often forward-filled).
        # Downstream the pipeline treats most market risk drivers as trading-day series,
        # so we drop weekends here to prevent weekend artifacts and keep calendars consistent.
        credit = credit[credit.index.weekday < 5]

        # IMPORTANT:
        # The BAML* series used here are already credit-yield or credit-spread indices
        # (depending on the specific FRED code). For stress-testing drivers we want a
        # high-coverage DAILY proxy; subtracting a (often monthly) sovereign yield can
        # (a) destroy frequency, and (b) create sign/scale errors.
        # We therefore export the raw daily series as the credit spread proxy.
        _export_series(credit, f"credit_spread_{iso}", "credit_spread")


def derive_comm_paper_spread() -> None:
    corp = fetch_fred_series("BAMLH0A0HYM2")
    treas = fetch_fred_series("DGS3")
    if corp.empty or treas.empty:
        LOGGER.warning("Cannot derive commercial-paper spread because one series is empty")
        return
    merged = corp.to_frame("corporate").join(treas.to_frame("treasury"), how="inner")
    if merged.empty:
        LOGGER.warning("No overlapping dates for commercial-paper spread")
        return
    spread = (merged["corporate"] - merged["treasury"]).rename("comm_paper_spread")
    _export_series(_resample_monthly(spread), "comm_paper_spread_USA", "comm_paper_spread")


def main() -> None:
    LOGGER.info("Deriving public-finance proxies")
    derive_cds_proxies()
    derive_spreads()
    derive_eu_credit_spreads()
    derive_comm_paper_spread()
    LOGGER.info("Public-finance proxies derivation complete")


if __name__ == "__main__":
    main()