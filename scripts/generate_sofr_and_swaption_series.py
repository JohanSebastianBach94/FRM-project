"""Rebuild the derived SOFR and swaption proxy CSVs from their source inputs."""
from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data_repository" / "raw"
DERIVED_DRIVERS = RAW / "providers" / "derived_risk_drivers"
STRUCTURAL = RAW / "structural"

TERM_NOTE_PATH = STRUCTURAL / "FED_Note_Term_SOFR.csv"
SOFR_PATH = DERIVED_DRIVERS / "SOFR_3m.csv"
MOVE_DAILY_PATH = DERIVED_DRIVERS / "move_index_daily.csv"
MOVE_MONTHLY_PATH = DERIVED_DRIVERS / "move_index_monthly.csv"
V2X_INDEX_PATH = DERIVED_DRIVERS / "v2x_index.csv"
VXST_INDEX_PATH = DERIVED_DRIVERS / "vxst_index.csv"
# Prefer the higher-frequency realized-vol file when it exists, otherwise keep using the monthly feed.
MOVE_PATH = MOVE_DAILY_PATH if MOVE_DAILY_PATH.exists() else MOVE_MONTHLY_PATH
USA_SWAPTION_PATH = DERIVED_DRIVERS / "swaption_vol_proxy_USA.csv"
DEU_SWAPTION_PATH = DERIVED_DRIVERS / "swaption_vol_proxy_DEU.csv"
EU_HIGH_YIELD = RAW / "fred" / "BAMLH0A2HYBEY.csv"
EU_AAA = RAW / "fred" / "BAMLC0A2CAAEY.csv"
SOFR_TARGET_END = pd.Timestamp("2025-12-12")
BANK_INDEX_END = pd.Timestamp("2025-12-01")

SOFR_PROXY_SPECS = [
    {
        "name": "ted_rate",
        "path": DERIVED_DRIVERS / "ted_rate_monthly.csv",
        "date_col": "date",
        "value_col": "TEDRATE",
    },
    {
        "name": "comm_paper_spread",
        "path": DERIVED_DRIVERS / "comm_paper_spread_USA.csv",
        "date_col": "date",
        "value_col": "comm_paper_spread",
    },
    {
        "name": "move_index",
        "path": MOVE_PATH,
        "date_col": "date",
        "value_col": "value",
        "skiprows": 3,
        "names": ["date", "value"],
    },
    {
        "name": "v2x_index",
        "path": V2X_INDEX_PATH,
        "date_col": "date",
        "value_col": "value",
    },
    {
        "name": "vxst_index",
        "path": VXST_INDEX_PATH,
        "date_col": "date",
        "value_col": "value",
    },
    {
        "name": "vix",
        "path": RAW / "fred" / "VIXCLS.csv",
        "date_col": "DATE",
        "value_col": "VIXCLS",
    },
    {
        "name": "swap_curve_10y",
        "path": RAW / "fred" / "DSWP10.csv",
        "date_col": "DATE",
        "value_col": "DSWP10",
    },
    {
        "name": "oil",
        "path": RAW / "fred" / "DCOILWTICO.csv",
        "date_col": "DATE",
        "value_col": "DCOILWTICO",
    },
    {
        "name": "ted",
        "path": RAW / "fred" / "TEDRATE.csv",
        "date_col": "DATE",
        "value_col": "TEDRATE",
    },
]

SWAPTION_PROXY_SPECS_USA = [
    {
        "name": "move_index",
        "path": MOVE_PATH,
        "date_col": "date",
        "value_col": "value",
        "skiprows": 3,
        "names": ["date", "value"],
    },
    {
        "name": "v2x_index",
        "path": V2X_INDEX_PATH,
        "date_col": "date",
        "value_col": "value",
    },
    {
        "name": "vix",
        "path": RAW / "fred" / "VIXCLS.csv",
        "date_col": "DATE",
        "value_col": "VIXCLS",
    },
    {
        "name": "swap_curve_10y",
        "path": RAW / "fred" / "DSWP10.csv",
        "date_col": "DATE",
        "value_col": "DSWP10",
    },
    {
        "name": "oil",
        "path": RAW / "fred" / "DCOILWTICO.csv",
        "date_col": "DATE",
        "value_col": "DCOILWTICO",
    },
    {
        "name": "ted",
        "path": RAW / "fred" / "TEDRATE.csv",
        "date_col": "DATE",
        "value_col": "TEDRATE",
    },
    {
        "name": "effective_fed_funds",
        "path": RAW / "fred" / "DFF.csv",
        "date_col": "DATE",
        "value_col": "DFF",
    },
    {
        "name": "sofr_90day_avg",
        "path": RAW / "fred" / "SOFR90DAYAVG.csv",
        "date_col": "date",
        "value_col": "value",
    },
]

SWAPTION_PROXY_SPECS_DEU = [
    {
        "name": "vix",
        "path": RAW / "fred" / "VIXCLS.csv",
        "date_col": "DATE",
        "value_col": "VIXCLS",
    },
    {
        "name": "v2x_index",
        "path": V2X_INDEX_PATH,
        "date_col": "date",
        "value_col": "value",
    },
    {
        "name": "swap_curve_10y",
        "path": RAW / "fred" / "DSWP10.csv",
        "date_col": "DATE",
        "value_col": "DSWP10",
    },
    {
        "name": "oil",
        "path": RAW / "fred" / "DCOILWTICO.csv",
        "date_col": "DATE",
        "value_col": "DCOILWTICO",
    },
    {
        "name": "baml_hy",
        "path": EU_HIGH_YIELD,
        "date_col": "DATE",
        "value_col": "BAMLH0A2HYBEY",
    },
    {
        "name": "baml_aaa",
        "path": EU_AAA,
        "date_col": "DATE",
        "value_col": "BAMLC0A2CAAEY",
    },
    {
        "name": "eur_short_term",
        "path": RAW / "fred" / "IR3TIB01EZM156N.csv",
        "date_col": "date",
        "value_col": "value",
    },
]

BANK_INDEX_FILES = {
    "Bank_equity_index_USA": DERIVED_DRIVERS.parent / "bank_indices" / "bank_equity_index_USA.csv",
    "Bank_equity_index_DEU": DERIVED_DRIVERS.parent / "bank_indices" / "bank_equity_index_DEU.csv",
    "Bank_equity_index_FRA": DERIVED_DRIVERS.parent / "bank_indices" / "bank_equity_index_FRA.csv",
    "Bank_equity_index_ITA": DERIVED_DRIVERS.parent / "bank_indices" / "bank_equity_index_ITA.csv",
    "Bank_equity_index_ESP": DERIVED_DRIVERS.parent / "bank_indices" / "bank_equity_index_ESP.csv",
}

SWAPTION_TARGET_END = SOFR_TARGET_END

BANK_INDEX_PROXY_SPECS = [dict(spec) for spec in SWAPTION_PROXY_SPECS_USA]


def _load_proxy_series(spec: dict, target_index: pd.DatetimeIndex) -> pd.Series:
    date_col = spec.get("date_col", "date")
    value_col = spec["value_col"]
    read_kwargs = {
        "parse_dates": [date_col],
        "dayfirst": False,
    }
    path = spec["path"]
    if not Path(path).exists():
        print(f"Missing proxy file {path}; filling NaNs for {spec['name']}")
        return pd.Series(index=target_index, dtype=float)
    if spec.get("skiprows"):
        read_kwargs["skiprows"] = spec["skiprows"]
    if spec.get("names"):
        read_kwargs["names"] = spec["names"]
        read_kwargs["header"] = None
        date_col = spec["names"][0]
        value_col = spec["names"][1]
    df = pd.read_csv(spec["path"], **read_kwargs)
    df = df.rename(columns={date_col: "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "value"])
    series = df.set_index("date")["value"].sort_index()
    series = series[~series.index.duplicated(keep="last")]
    series = series.reindex(target_index)
    return series.ffill().bfill()


def _build_proxy_dataframe(specs: list[dict], target_index: pd.DatetimeIndex) -> pd.DataFrame:
    df = pd.DataFrame({spec["name"]: _load_proxy_series(spec, target_index) for spec in specs}, index=target_index)
    df = df.dropna(axis=1, how='all')
    if df.empty:
        raise RuntimeError("No proxy data available to build the design matrix.")
    return df


def _fit_proxy_coeffs(proxy_df: pd.DataFrame, target_series: pd.Series) -> np.ndarray:
    mask = target_series.notna()
    available = target_series.index[mask]
    training = proxy_df.loc[available].dropna()
    if training.empty:
        raise RuntimeError("No data available to train the proxy model.")
    target = target_series.loc[training.index]
    design = np.column_stack([np.ones(len(training)), training.to_numpy()])
    coeffs, *_ = np.linalg.lstsq(design, target.to_numpy(), rcond=None)
    weights_text = "; ".join(
        f"{name}={value:.5f}" for name, value in zip(training.columns, coeffs[1:])
    )
    start = training.index.min().date()
    end = training.index.max().date()
    print(
        f"Trained proxy model on {len(training)} rows ({start}–{end}); "
        f"intercept={coeffs[0]:.5f}; {weights_text}"
    )
    return coeffs


def _predict_with_proxies(proxy_df: pd.DataFrame, coeffs: np.ndarray) -> pd.Series:
    design = np.column_stack([np.ones(len(proxy_df)), proxy_df.to_numpy()])
    return pd.Series(design @ coeffs, index=proxy_df.index)


def _apply_proxy_model(
    manual_series: pd.Series, specs: list[dict], target_index: pd.DatetimeIndex
) -> pd.Series:
    proxy_df = _build_proxy_dataframe(specs, target_index)
    target_series = manual_series.reindex(target_index)
    coeffs = _fit_proxy_coeffs(proxy_df, target_series)
    return target_series.combine_first(_predict_with_proxies(proxy_df, coeffs))


def _build_realized_sofr() -> pd.DataFrame:
    term = pd.read_csv(TERM_NOTE_PATH, skiprows=10)
    term["DATE"] = pd.to_datetime(term["DATE"], dayfirst=True, errors="coerce")
    term["REALIZED_3M"] = pd.to_numeric(term["REALIZED_3M"], errors="coerce")
    realized = term[["DATE", "REALIZED_3M"]].dropna()
    realized = realized.rename(columns={"REALIZED_3M": "sofr_3m"})

    realized_series = realized.set_index("DATE")["sofr_3m"]

    manual = pd.read_csv(SOFR_PATH)
    manual["DATE"] = pd.to_datetime(manual["date"], dayfirst=False, errors="coerce")
    manual["sofr_3m"] = pd.to_numeric(manual["sofr_3m"], errors="coerce")
    manual_series = manual.dropna(subset=["DATE", "sofr_3m"]).set_index("DATE")["sofr_3m"].sort_index()

    target_start = min(realized_series.index.min(), manual_series.index.min())
    target_index = pd.bdate_range(start=target_start, end=SOFR_TARGET_END)

    df = pd.DataFrame(index=target_index)
    df["sofr_realized"] = realized_series.reindex(target_index)
    df["sofr_manual"] = manual_series.reindex(target_index)

    proxy_series = _apply_proxy_model(realized_series, SOFR_PROXY_SPECS, target_index)

    combined = (
        df["sofr_realized"].combine_first(df["sofr_manual"]).combine_first(proxy_series)
    )
    combined = combined.round(6)
    start = target_index[0].strftime("%Y-%m-%d")
    end = target_index[-1].strftime("%Y-%m-%d")
    print(f"Extended SOFR history to {start}–{end} ({len(target_index)} business days)")

    result = combined.reset_index()
    result.columns = ["date", "sofr_3m"]
    return result


def _build_swaption_series(path: Path, specs: list[dict], target_end: pd.Timestamp) -> pd.DataFrame:
    df = (
        pd.read_csv(path, parse_dates=["date"])
        .dropna(subset=["date", "value"])
        .sort_values("date")
    )
    df = df.drop_duplicates(subset="date", keep="last")
    manual_series = df.set_index("date")["value"].astype(float)

    # These proxy files may already have been forward-filled to business-day frequency.
    # If we treat every row as a hard observation, the proxy model never gets used and
    # the resulting series inherits stepwise artifacts. To avoid that, treat only value
    # change-points as "observed" and let the proxy model fill the in-between days.
    change_points = manual_series.ne(manual_series.shift(1))
    change_points.iloc[0] = True
    observed_series = manual_series.loc[change_points]
    target_index = pd.bdate_range(start=manual_series.index.min(), end=target_end)

    combined = _apply_proxy_model(observed_series, specs, target_index).round(6)
    result = combined.reset_index()
    result.columns = ["date", "value"]
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    return result


def _build_bank_index_series(name: str, path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["date"]).sort_values("date")
    value_col = next((col for col in ("close", "value") if col in df.columns), None)
    if value_col is None:
        raise KeyError(f"bank index file {path} missing value column")
    df = df.dropna(subset=["date", value_col])
    df = df.drop_duplicates(subset="date", keep="last")
    manual_series = df.set_index("date")[value_col].astype(float)
    target_index = pd.date_range(start=manual_series.index.min(), end=BANK_INDEX_END, freq="MS")

    combined = _apply_proxy_model(manual_series, BANK_INDEX_PROXY_SPECS, target_index).round(6)
    result = combined.reset_index()
    result.columns = ["date", "value"]
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    return result


def _write_dataframe(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    parser = ArgumentParser(description="Crawl underlying feeds to build SOFR and derivative proxies.")
    parser.add_argument("--skip-sofr", action="store_true", help="Do not overwrite the SOFR file.")
    parser.add_argument("--skip-swaption", action="store_true", help="Do not rebuild swaption proxies.")
    parser.add_argument(
        "--skip-bank-index",
        action="store_true",
        help="Do not regenerate the bank equity indexes.",
    )
    args = parser.parse_args()

    if not args.skip_sofr:
        sofr_df = _build_realized_sofr()
        _write_dataframe(sofr_df, SOFR_PATH)
        print(f"Written {SOFR_PATH} ({len(sofr_df)} rows)")

    if not args.skip_swaption:
        used = []
        usa_df = _build_swaption_series(USA_SWAPTION_PATH, SWAPTION_PROXY_SPECS_USA, SWAPTION_TARGET_END)
        _write_dataframe(usa_df, USA_SWAPTION_PATH)
        used.append(f"USA {len(usa_df)} rows")

        deu_df = _build_swaption_series(DEU_SWAPTION_PATH, SWAPTION_PROXY_SPECS_DEU, SWAPTION_TARGET_END)
        _write_dataframe(deu_df, DEU_SWAPTION_PATH)
        used.append(f"DEU {len(deu_df)} rows")

        print(f"Written swaption proxies ({'; '.join(used)})")

    if not args.skip_bank_index:
        bank_summaries = []
        for name, path in sorted(BANK_INDEX_FILES.items()):
            bank_df = _build_bank_index_series(name, path)
            _write_dataframe(bank_df, path)
            bank_summaries.append(f"{name} {len(bank_df)} rows")
        print(f"Written bank equity indexes ({'; '.join(bank_summaries)})")


if __name__ == "__main__":
    main()
