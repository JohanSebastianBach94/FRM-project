"""Generate crisis window heatmaps for DCC and ADCC correlation outputs."""
from __future__ import annotations

import argparse
import itertools
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import matplotlib

matplotlib.use("Agg")  # use headless backend to avoid GUI interruptions

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
try:
    import seaborn as sns
except Exception:
    # lightweight fallback when seaborn is not installed (allows headless runs)
    import types
    import matplotlib.pyplot as _plt

    def _set_style(_):
        return

    def _fallback_heatmap(data, ax=None, **kwargs):
        """Minimal heatmap fallback using matplotlib.imshow when seaborn is missing."""
        if ax is None:
            ax = _plt.gca()
        # extract array from pandas DataFrame if necessary
        arr = data.values if hasattr(data, "values") else data
        cmap = kwargs.get("cmap", "viridis")
        vmin = kwargs.get("vmin", None)
        vmax = kwargs.get("vmax", None)
        norm = kwargs.get("norm", None)
        im = ax.imshow(arr, cmap=cmap, vmin=vmin, vmax=vmax, norm=norm, aspect="equal", interpolation="nearest")
        if kwargs.get("cbar"):
            _plt.colorbar(im, ax=ax)
        # tick labels handling
        xt = kwargs.get("xticklabels", True)
        yt = kwargs.get("yticklabels", True)
        if xt is not False:
            labels = xt if xt is not True else (data.columns if hasattr(data, "columns") else None)
            if labels is not None:
                ax.set_xticks(np.arange(len(labels)) + 0.5)
                ax.set_xticklabels(labels, rotation=90, fontsize=7)
        if yt is not False:
            labels = yt if yt is not True else (data.index if hasattr(data, "index") else None)
            if labels is not None:
                ax.set_yticks(np.arange(len(labels)) + 0.5)
                ax.set_yticklabels(labels, fontsize=7)
        return im

    sns = types.SimpleNamespace(heatmap=_fallback_heatmap, set_style=_set_style)
from matplotlib.colors import TwoSlopeNorm

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = BASE_DIR / "heatmaps_final"
DEFAULT_OUTPUT_DIR.mkdir(exist_ok=True)

LABEL_MAP: Dict[str, str] = {
    "VIXCLS": "VIX",
    "TEDRATE": "TED Spread",
    "BAMLH0A0HYM2": "US HY OAS",
    "BAMLC0A4CBBB": "US BBB OAS",
    "DFF": "Fed Funds",
    "DCOILBRENTEU": "Brent Crude",
    "IRLTLT01USM156N": "US 10Y Yield",
    "ITA_beta0": "Italy Level",
    "ITA_beta1": "Italy Slope",
    "ITA_beta2": "Italy Curvature",
    "BTP_Bund_Spread": "Italy-Germany Spread",
    "ITA_Equity": "FTSE MIB",
    "ESP_beta0": "Spain Level",
    "ESP_beta1": "Spain Slope",
    "ESP_beta2": "Spain Curvature",
    "Bonos_Bund_Spread": "Spain-Germany Spread",
    "IBEX_35": "IBEX 35",
    "FRA_beta0": "France Level",
    "FRA_beta1": "France Slope",
    "FRA_beta2": "France Curvature",
    "OAT_Bund_Spread": "France-Germany Spread",
    "FRA_Equity": "CAC 40",
    "DEU_beta0": "Germany Level",
    "DEU_beta1": "Germany Slope",
    "DEU_beta2": "Germany Curvature",
    "DAX": "DAX",
    "USA_beta0": "US Level",
    "USA_beta1": "US Slope",
    "USA_beta2": "US Curvature",
    "SP500": "S&P 500",
}


RISK_PAIR_LABELS: Dict[str, str] = {
    "US 10Y Yield – France-Germany Spread": "US duration vs OAT-Bund spread",
    "US 10Y Yield – Italy-Germany Spread": "US duration vs BTP-Bund spread",
    "US 10Y Yield – Spain-Germany Spread": "US duration vs Bonos-Bund spread",
    "US 10Y Yield – Fed Funds": "US duration vs policy rate",
    "Fed Funds – France-Germany Spread": "US policy vs OAT-Bund spread",
    "Fed Funds – Italy-Germany Spread": "US policy vs BTP-Bund spread",
    "Fed Funds – Spain-Germany Spread": "US policy vs Bonos-Bund spread",
    "Fed Funds – US 10Y Yield": "US policy vs duration",
    "Brent Crude – S&P 500": "Oil vs US equities",
    "Brent Crude – CAC 40": "Oil vs France equities",
    "Brent Crude – DAX": "Oil vs Germany equities",
    "Brent Crude – FTSE MIB": "Oil vs Italy equities",
    "Brent Crude – IBEX 35": "Oil vs Spain equities",
    "Brent Crude – Fed Funds": "Oil vs US policy",
    "Brent Crude – US 10Y Yield": "Oil vs US duration",
    "US HY OAS – Brent Crude": "US HY credit vs oil",
    "US BBB OAS – Brent Crude": "US IG credit vs oil",
    "US HY OAS – Fed Funds": "US HY credit vs policy",
    "US BBB OAS – Fed Funds": "US IG credit vs policy",
    "US Slope – US Curvature": "US curve slope vs curvature",
    "US Level – US Slope": "US curve level vs slope",
    "US Level – US Curvature": "US curve level vs curvature",
    "TED Spread – Fed Funds": "Funding stress vs policy rate",
    "TED Spread – US HY OAS": "Funding stress vs US HY credit",
    "TED Spread – US BBB OAS": "Funding stress vs US IG credit",
    "VIX – TED Spread": "Equity vol vs funding stress",
    "VIX – Brent Crude": "Equity vol vs oil",
    "VIX – US 10Y Yield": "Equity vol vs US duration",
}


INDICATOR_CATEGORY: Dict[str, str] = {
    "VIX": "Equity volatility",
    "TED Spread": "Funding stress",
    "US HY OAS": "US HY credit spread",
    "US BBB OAS": "US IG credit spread",
    "Fed Funds": "US policy rate",
    "Brent Crude": "Oil price",
    "US 10Y Yield": "US 10Y yield",
    "Italy Level": "Italy curve level",
    "Italy Slope": "Italy curve slope",
    "Italy Curvature": "Italy curve curvature",
    "Italy-Germany Spread": "BTP-Bund spread",
    "FTSE MIB": "Italy equities",
    "Spain Level": "Spain curve level",
    "Spain Slope": "Spain curve slope",
    "Spain Curvature": "Spain curve curvature",
    "Spain-Germany Spread": "Bonos-Bund spread",
    "IBEX 35": "Spain equities",
    "France Level": "France curve level",
    "France Slope": "France curve slope",
    "France Curvature": "France curve curvature",
    "France-Germany Spread": "OAT-Bund spread",
    "CAC 40": "France equities",
    "Germany Level": "Germany curve level",
    "Germany Slope": "Germany curve slope",
    "Germany Curvature": "Germany curve curvature",
    "DAX": "Germany equities",
    "US Level": "US curve level",
    "US Slope": "US curve slope",
    "US Curvature": "US curve curvature",
    "S&P 500": "US equities",
}


ROOT_DIR = Path(__file__).resolve().parent.parent
RT_PARAMS_DIR = ROOT_DIR / "Output" / "nss_parameters"
RT_OVERLAY_FILE = RT_PARAMS_DIR / "Rt_strategy_overlay.pkl"
RT_DRIVER_FILE = RT_PARAMS_DIR / "Rt_driver_contrib.pkl"


DIFF_ANNOT_THRESHOLD = 0.20


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate crisis heatmaps from DCC outputs")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional directory for saving heatmaps (defaults to heatmaps_final)",
    )
    parser.add_argument(
        "--suffix",
        default=None,
        help="Optional suffix appended to output filenames for scenario tagging.",
    )
    parser.add_argument(
        "--dcc-dir",
        default=None,
        help="Override directory containing DCC outputs (dynamic_correlations_34series.csv)",
    )
    parser.add_argument(
        "--adcc-dir",
        default=None,
        help="Override directory containing ADCC outputs.",
    )
    parser.add_argument(
        "--skip-dcc",
        action="store_true",
        help="Skip processing the DCC model outputs.",
    )
    parser.add_argument(
        "--skip-adcc",
        action="store_true",
        help="Skip processing the ADCC model outputs.",
    )
    parser.add_argument(
        "--variant",
        choices=("orig", "A", "B"),
        default="orig",
        help="Output variant: 'orig' (default), 'A' (global percentile scale + colorbar), 'B' (TwoSlopeNorm + clipping + annotations)",
    )
    return parser.parse_args()


def build_annotation_column(
    series: pd.Series, *, threshold: float | None = None, signed: bool = False
) -> np.ndarray:
    """Create a column vector of annotation strings with optional thresholding."""
    formatted: List[str] = []
    for value in series:
        if pd.isna(value):
            formatted.append("")
            continue
        if threshold is not None and abs(value) < threshold:
            formatted.append("")
            continue
        fmt = "{:+.2f}" if signed else "{:.2f}"
        formatted.append(fmt.format(value))
    return np.array(formatted, dtype=object)[:, np.newaxis]


def annotate_symmetric_deltas(ax: plt.Axes, matrix: pd.DataFrame, threshold: float) -> None:
    """Annotate upper and lower triangles with +/- values over the threshold."""
    for i, row_label in enumerate(matrix.index):
        for j, col_label in enumerate(matrix.columns):
            if j <= i:
                continue
            value = matrix.loc[row_label, col_label]
            if pd.isna(value) or abs(value) < threshold:
                continue
            label = f"{value:+.2f}"
            ax.text(j + 0.5, i + 0.5, label, color="black", ha="center", va="center", fontsize=7)
            ax.text(i + 0.5, j + 0.5, label, color="black", ha="center", va="center", fontsize=7)


EVENTS: List[Dict[str, str]] = [
    {"name": "DotCom Bubble Peak", "date": "2000-03-10"},
    {"name": "Lehman Bankruptcy", "date": "2008-09-15"},
    {"name": "COVID Pandemic", "date": "2020-03-11"},
]

# Historical market bottom used in earlier analyses (Mar 9 2009)
EVENTS.insert(0, {"name": "Market Bottom 2009", "date": "2009-03-09"})

EVENT_DATE_MAP: Dict[str, pd.Timestamp] = {event["name"]: pd.Timestamp(event["date"]) for event in EVENTS}

PRE_EVENT_DAYS = 20
POST_EVENT_DAYS = 20
BASELINE_DAYS = 60

COUNTRY_MODELS: Dict[str, Dict[str, Iterable[str]]] = {
    "ita": {
        "name": "Italy",
        "global": [
            "VIXCLS",
            "TEDRATE",
            "BAMLH0A0HYM2",
            "BAMLC0A4CBBB",
            "DFF",
            "DCOILBRENTEU",
            "IRLTLT01USM156N",
        ],
        "country": ["ITA_beta0", "ITA_beta1", "ITA_beta2", "BTP_Bund_Spread", "ITA_Equity"],
    },
    "esp": {
        "name": "Spain",
        "global": [
            "VIXCLS",
            "TEDRATE",
            "BAMLH0A0HYM2",
            "BAMLC0A4CBBB",
            "DFF",
            "DCOILBRENTEU",
            "IRLTLT01USM156N",
        ],
        "country": ["ESP_beta0", "ESP_beta1", "ESP_beta2", "Bonos_Bund_Spread", "IBEX_35"],
    },
    "fra": {
        "name": "France",
        "global": [
            "VIXCLS",
            "TEDRATE",
            "BAMLH0A0HYM2",
            "BAMLC0A4CBBB",
            "DFF",
            "DCOILBRENTEU",
            "IRLTLT01USM156N",
        ],
        "country": ["FRA_beta0", "FRA_beta1", "FRA_beta2", "OAT_Bund_Spread", "FRA_Equity"],
    },
    "deu": {
        "name": "Germany",
        "global": [
            "VIXCLS",
            "TEDRATE",
            "BAMLH0A0HYM2",
            "BAMLC0A4CBBB",
            "DFF",
            "DCOILBRENTEU",
            "IRLTLT01USM156N",
        ],
        "country": ["DEU_beta0", "DEU_beta1", "DEU_beta2", "DAX"],
    },
    "usa": {
        "name": "United States",
        "global": [
            "VIXCLS",
            "TEDRATE",
            "BAMLH0A0HYM2",
            "BAMLC0A4CBBB",
            "DCOILBRENTEU",
        ],
        "country": ["USA_beta0", "USA_beta1", "USA_beta2", "IRLTLT01USM156N", "DFF", "SP500"],
    },
}


def slugify(text: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in text).strip("_")


def load_dynamic_correlations(path: Path) -> pd.DataFrame:
    csv_path = path / "dynamic_correlations_34series.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing dynamic correlations at {csv_path}")
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    except Exception as exc:  # fallback for malformed/corrupted CSVs
        print(f"[WARN] standard read_csv failed for {csv_path}: {exc}. Trying engine='python' with on_bad_lines='skip'.")
        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True, engine="python", on_bad_lines="skip")
        except Exception as exc2:
            # surface a helpful error if both readers fail
            raise IOError(f"Failed reading dynamic correlations CSV at {csv_path}: {exc2}")
    df.sort_index(inplace=True)
    return df


def build_matrix(series: List[str], pair_means: pd.Series) -> pd.DataFrame:
    n = len(series)
    mat = pd.DataFrame(np.eye(n), index=series, columns=series)
    for i in range(n):
        for j in range(i + 1, n):
            s1, s2 = series[i], series[j]
            key = f"{s1}_{s2}"
            if key not in pair_means and f"{s2}_{s1}" in pair_means:
                key = f"{s2}_{s1}"
            value = pair_means.get(key, np.nan)
            mat.iloc[i, j] = value
            mat.iloc[j, i] = value
    return mat


def lookup_risk_label(indicator_a: str, indicator_b: str) -> str:
    key = f"{indicator_a} – {indicator_b}"
    if key in RISK_PAIR_LABELS:
        return RISK_PAIR_LABELS[key]
    key = f"{indicator_b} – {indicator_a}"
    if key in RISK_PAIR_LABELS:
        return RISK_PAIR_LABELS[key]
    cat_a = INDICATOR_CATEGORY.get(indicator_a)
    cat_b = INDICATOR_CATEGORY.get(indicator_b)
    if cat_a and cat_b:
        return f"{cat_a} vs {cat_b}"
    return ""


def write_crisis_markdown(df: pd.DataFrame, output_path: Path) -> None:
    lines: List[str] = []
    for (model_name, event_name), group in df.groupby(["model", "event"], sort=False):
        lines.append(f"### {model_name} – {event_name}")
        lines.append(
            "| Rank | Country | Indicator A | Indicator B | Risk Label | Baseline | Peak | Crisis Mean | Δ Mean | Δ Peak | Abs(Peak−Baseline) |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
        for _, row in group.iterrows():
            risk_label = row.get("risk_label", "") or ""
            lines.append(
                "| {rank} | {country} | {a} | {b} | {label} | {baseline:.3f} | {peak:.3f} | {crisis:.3f} | {delta_mean:.3f} | {delta_peak:.3f} | {abs_peak:.3f} |".format(
                    rank=int(row["table_rank"]),
                    country=row["country"],
                    a=row["indicator_a"],
                    b=row["indicator_b"],
                    label=risk_label,
                    baseline=row["baseline_corr"],
                    peak=row["peak_corr"],
                    crisis=row["event_corr"],
                    delta_mean=row["mean_shift"],
                    delta_peak=row["max_shift"],
                    abs_peak=row["abs_peak_vs_baseline"],
                )
            )
        lines.append("")

    output_path.write_text("\n".join(lines), encoding="utf-8")


def load_rt_dataframe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_pickle(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    return df.sort_index()


def locate_rt_row(df: pd.DataFrame, reference_date: pd.Timestamp | None) -> pd.Series | None:
    if df.empty or reference_date is None or pd.isna(reference_date):
        return None
    date_ts = pd.to_datetime(reference_date)
    idx = df.index.get_indexer([date_ts], method="ffill")[0]
    if idx < 0:
        return None
    return df.iloc[idx]


def sample_rt_context(reference_date: pd.Timestamp | None, overlay_df: pd.DataFrame, driver_df: pd.DataFrame) -> Dict[str, float]:
    context = {
        "rt_scaled_rt": np.nan,
        "rt_overlay": np.nan,
        "rt_driver_level": np.nan,
        "rt_driver_slope": np.nan,
        "rt_driver_curvature": np.nan,
        "rt_driver_lambda_dynamic": np.nan,
        "rt_driver_curve_volatility": np.nan,
    }
    overlay_row = locate_rt_row(overlay_df, reference_date)
    if overlay_row is not None:
        context["rt_scaled_rt"] = overlay_row.get("scaled_rt", np.nan)
        context["rt_overlay"] = overlay_row.get("overlay", np.nan)
    driver_row = locate_rt_row(driver_df, reference_date)
    if driver_row is not None:
        context["rt_driver_level"] = driver_row.get("level", np.nan)
        context["rt_driver_slope"] = driver_row.get("slope", np.nan)
        context["rt_driver_curvature"] = driver_row.get("curvature", np.nan)
        context["rt_driver_lambda_dynamic"] = driver_row.get("lambda_dynamic", np.nan)
        context["rt_driver_curve_volatility"] = driver_row.get("curve_volatility", np.nan)
    return context


def attach_rt_context(row: pd.Series, overlay_df: pd.DataFrame, driver_df: pd.DataFrame) -> pd.Series:
    peak_date = row.get("peak_date")
    event_date = EVENT_DATE_MAP.get(row.get("event"))
    reference_date = peak_date if pd.notna(peak_date) else event_date
    context = sample_rt_context(reference_date, overlay_df, driver_df)
    for key, value in context.items():
        row[key] = value
    return row


def triangle_values(matrix: pd.DataFrame) -> pd.Series:
    mask = np.triu(np.ones_like(matrix, dtype=bool), k=1)
    return matrix.where(mask).stack()


def compute_window(df: pd.DataFrame, center: pd.Timestamp, pre_days: int, post_days: int) -> pd.DataFrame:
    start = center - pd.Timedelta(days=pre_days)
    end = center + pd.Timedelta(days=post_days)
    mask = (df.index >= start) & (df.index <= end)
    return df.loc[mask]


def compute_baseline(df: pd.DataFrame, center: pd.Timestamp, window_days: int) -> pd.DataFrame:
    start = center - pd.Timedelta(days=window_days)
    end = center - pd.Timedelta(days=1)
    mask = (df.index >= start) & (df.index <= end)
    return df.loc[mask]


def lookup_pair(pair_series: pd.Series, s1: str, s2: str) -> float:
    key = f"{s1}_{s2}"
    if key in pair_series:
        return pair_series[key]
    key = f"{s2}_{s1}"
    if key in pair_series:
        return pair_series[key]
    return np.nan


def lookup_pair_from_row(row: pd.Series, s1: str, s2: str) -> float:
    key = f"{s1}_{s2}"
    if key in row.index:
        return row[key]
    key = f"{s2}_{s1}"
    if key in row.index:
        return row[key]
    return np.nan




def build_block_matrix(groups: List[Tuple[str, List[str]]], pair_values: pd.Series) -> pd.DataFrame:
    labels = [name for name, _ in groups]
    n = len(groups)
    mat = pd.DataFrame(np.nan, index=labels, columns=labels)
    for i, (name_i, series_i) in enumerate(groups):
        for j, (name_j, series_j) in enumerate(groups[i:], start=i):
            if i == j:
                combos = itertools.combinations(series_i, 2)
            else:
                combos = itertools.product(series_i, series_j)
            values = [lookup_pair(pair_values, a, b) for a, b in combos]
            values = [v for v in values if pd.notna(v)]
            if not values and i == j:
                values = [1.0]
            agg_value = float(np.nanmean(values)) if values else np.nan
            mat.iloc[i, j] = mat.iloc[j, i] = agg_value
    return mat


def safe_scale(value_matrix: pd.DataFrame, floor: float) -> float:
    with np.errstate(all="ignore"):
        candidate = np.nanmax(np.abs(value_matrix.values))
    if np.isnan(candidate):
        return floor
    return max(candidate, floor)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    # Non-destructive variant subfolders: keep original outputs in root, variants in subfolders
    variant_dir = output_dir / args.variant if getattr(args, "variant", "orig") and args.variant != "orig" else output_dir
    variant_dir.mkdir(parents=True, exist_ok=True)
    suffix_token = f"_{slugify(args.suffix)}" if args.suffix else ""

    model_configs: List[Dict[str, object]] = []
    if not args.skip_dcc:
        dcc_path = Path(args.dcc_dir).resolve() if args.dcc_dir else BASE_DIR / "outputs_final"
        model_configs.append({"key": "dcc", "name": "DCC", "path": dcc_path})
    if not args.skip_adcc:
        adcc_path = Path(args.adcc_dir).resolve() if args.adcc_dir else BASE_DIR / "outputs_final_adcc"
        model_configs.append({"key": "adcc", "name": "ADCC", "path": adcc_path})

    if not model_configs:
        print("No models selected. Use --skip-dcc/--skip-adcc carefully.")
        return

    sns.set_style("white")
    rt_overlay_df = load_rt_dataframe(RT_OVERLAY_FILE)
    rt_driver_df = load_rt_dataframe(RT_DRIVER_FILE)
    if rt_overlay_df.empty:
        print(f"⚠️ Rt overlay pickle missing at {RT_OVERLAY_FILE}")
    if rt_driver_df.empty:
        print(f"⚠️ Rt driver contributions missing at {RT_DRIVER_FILE}")

    summary_records: List[Dict[str, object]] = []
    top_pair_records: List[Dict[str, object]] = []

    for model_cfg in model_configs:
        model_path = Path(model_cfg["path"])
        if not model_path.exists():
            print(f"[SKIP] {model_cfg['name']} outputs missing at {model_path}")
            continue

        try:
            dyn_corr = load_dynamic_correlations(model_path)
        except FileNotFoundError as exc:
            print(f"[SKIP] {exc}")
            continue

        print(f"\n=== {model_cfg['name']} ===")
        for event in EVENTS:
            event_name = event["name"]
            event_date = pd.Timestamp(event["date"])
            event_slug = slugify(event_name)

            event_window = compute_window(dyn_corr, event_date, PRE_EVENT_DAYS, POST_EVENT_DAYS)
            baseline_window = compute_baseline(dyn_corr, event_date, BASELINE_DAYS)

            if event_window.empty or baseline_window.empty:
                print(
                    f"  [WARN] {event_name}: insufficient data (event {len(event_window)}, baseline {len(baseline_window)})"
                )
                continue

            rows = len(COUNTRY_MODELS)
            fig, axes = plt.subplots(rows, 4, figsize=(24, 4 * rows))
            axes = np.atleast_2d(axes)
            block_fig, block_axes = plt.subplots(rows, 3, figsize=(14, 3.5 * rows))
            block_axes = np.atleast_2d(block_axes)

            baseline_means = baseline_window.mean()
            event_means = event_window.mean()

            # Precompute per-country matrices so we can optionally compute global percentile scales
            event_pair_records: List[Dict[str, object]] = []
            per_country: List[Dict[str, object]] = []

            for row_idx, (country_code, spec) in enumerate(COUNTRY_MODELS.items()):
                series = list(spec["global"]) + list(spec["country"])

                peak_corr_signed: Dict[str, float] = {}
                peak_date_signed: Dict[str, pd.Timestamp] = {}
                for i in range(len(series)):
                    for j in range(i + 1, len(series)):
                        s1, s2 = series[i], series[j]
                        key = f"{s1}_{s2}"
                        if key not in event_window.columns and f"{s2}_{s1}" not in event_window.columns:
                            continue
                        series_key = key if key in event_window.columns else f"{s2}_{s1}"
                        series_values = event_window[series_key].dropna()
                        if series_values.empty:
                            continue
                        baseline_value = lookup_pair(baseline_means, s1, s2)
                        if pd.isna(baseline_value):
                            continue
                        diff_series = (series_values - baseline_value).dropna()
                        if diff_series.empty:
                            continue
                        idx_max = diff_series.abs().idxmax()
                        peak_value = series_values.loc[idx_max]
                        peak_corr_signed[series_key] = peak_value
                        peak_date_signed[(s1, s2)] = idx_max

                peak_corr_series = pd.Series(peak_corr_signed, dtype=float)

                baseline_mat = build_matrix(series, baseline_means)
                event_mat = build_matrix(series, event_means)
                diff_mat = event_mat - baseline_mat
                np.fill_diagonal(diff_mat.values, 0.0)

                peak_mat = build_matrix(series, peak_corr_series)
                peak_mat = peak_mat.where(~np.isnan(peak_mat), baseline_mat)
                np.fill_diagonal(peak_mat.values, 1.0)
                max_diff_mat = peak_mat - baseline_mat
                np.fill_diagonal(max_diff_mat.values, 0.0)
                display_labels = [LABEL_MAP.get(code, code) for code in series]

                baseline_vals = triangle_values(baseline_mat)
                event_vals = triangle_values(event_mat)
                diff_vals = triangle_values(diff_mat).abs()
                max_diff_vals = triangle_values(max_diff_mat).abs()

                per_country.append(
                    {
                        "code": country_code,
                        "name": spec["name"],
                        "series": series,
                        "display_labels": display_labels,
                        "baseline_mat": baseline_mat,
                        "event_mat": event_mat,
                        "diff_mat": diff_mat,
                        "peak_mat": peak_mat,
                        "max_diff_mat": max_diff_mat,
                        "peak_dates": peak_date_signed,
                        "diff_vals": diff_vals,
                        "max_diff_vals": max_diff_vals,
                        "baseline_vals": baseline_vals,
                        "event_vals": event_vals,
                    }
                )

                summary_records.append(
                    {
                        "model": model_cfg["name"],
                        "event": event_name,
                        "event_date": event_date.date(),
                        "country": spec["name"],
                        "baseline_days": len(baseline_window),
                        "event_days": len(event_window),
                        "baseline_mean_corr": baseline_vals.mean(),
                        "event_mean_corr": event_vals.mean(),
                        "diff_mean_abs": diff_vals.mean(),
                        "diff_max_abs": max_diff_vals.max(),
                        **sample_rt_context(event_date, rt_overlay_df, rt_driver_df),
                    }
                )

            # Determine variant-specific scales
            variant = args.variant if hasattr(args, "variant") else "orig"
            # default floor and percentile
            floor = 0.05
            pct = 98
            all_diff_abs = np.concatenate([pc["diff_vals"].to_numpy() for pc in per_country if not pc["diff_vals"].empty]) if per_country else np.array([floor])
            all_maxdiff_abs = np.concatenate([pc["max_diff_vals"].to_numpy() for pc in per_country if not pc["max_diff_vals"].empty]) if per_country else np.array([floor])
            global_diff_scale = max(np.nanpercentile(np.abs(all_diff_abs), pct) if all_diff_abs.size else floor, floor)
            global_maxdiff_scale = max(np.nanpercentile(np.abs(all_maxdiff_abs), pct) if all_maxdiff_abs.size else floor, floor)

            # prepare figure axes
            rows = len(per_country)
            fig, axes = plt.subplots(rows, 4, figsize=(24, 4 * rows))
            axes = np.atleast_2d(axes)
            block_fig, block_axes = plt.subplots(rows, 3, figsize=(14, 3.5 * rows))
            block_axes = np.atleast_2d(block_axes)

            if event_pair_records:
                event_pairs_df = pd.DataFrame(event_pair_records)
                event_pairs_df.sort_values("max_abs_shift", ascending=False, inplace=True)
                spotlight_df = event_pairs_df.head(8).copy()
                spotlight_labels = []
                for _, row in spotlight_df.iterrows():
                    risk_token = (row["risk_label"] or "").strip()
                    pair_token = row["pair"].strip()
                    if risk_token and risk_token.lower() != pair_token.lower():
                        composite = f"{risk_token} ({pair_token})"
                    else:
                        composite = pair_token
                    peak_dt = row.get("peak_date")
                    if pd.notna(peak_dt):
                        peak_str = pd.to_datetime(peak_dt).date().isoformat()
                        spotlight_labels.append(f"{row['country']} | {composite} [Peak {peak_str}]")
                    else:
                        spotlight_labels.append(f"{row['country']} | {composite}")

                def compute_scale(series: pd.Series, floor: float = 0.05) -> float:
                    max_abs = series.abs().max()
                    if pd.isna(max_abs):
                        return floor
                    return max(max_abs, floor)

                spotlight_metrics = [
                    ("Baseline Mean", spotlight_df["baseline_corr"], -1.0, 1.0, False, False),
                    ("Peak Crisis", spotlight_df["peak_corr"], -1.0, 1.0, False, False),
                    ("Crisis Mean", spotlight_df["event_corr"], -1.0, 1.0, False, False),
                    (
                        "Δ Mean (Crisis vs Baseline)",
                        spotlight_df["mean_shift"],
                        -compute_scale(spotlight_df["mean_shift"]),
                        compute_scale(spotlight_df["mean_shift"]),
                        True,
                        True,
                    ),
                    (
                        "Δ Peak (vs Baseline)",
                        spotlight_df["max_shift"],
                        -compute_scale(spotlight_df["max_shift"]),
                        compute_scale(spotlight_df["max_shift"]),
                        True,
                        True,
                    ),
                ]

                spotlight_fig, spotlight_axes = plt.subplots(
                    1,
                    len(spotlight_metrics),
                    figsize=(4 * len(spotlight_metrics), 0.5 * len(spotlight_df) + 3),
                    sharey=True,
                )
                spotlight_axes = np.atleast_1d(spotlight_axes)

                yticks = np.arange(len(spotlight_labels)) + 0.5

                for ax_idx, (title, series_data, vmin, vmax, signed, thresholded) in enumerate(spotlight_metrics):
                    data = series_data.to_numpy()[:, np.newaxis]
                    if thresholded:
                        annot_data = build_annotation_column(
                            series_data,
                            threshold=DIFF_ANNOT_THRESHOLD,
                            signed=True,
                        )
                    else:
                        annot_data = build_annotation_column(series_data, signed=signed)
                    sns.heatmap(
                        data,
                        ax=spotlight_axes[ax_idx],
                        cmap="RdBu_r",
                        center=0.0,
                        vmin=vmin,
                        vmax=vmax,
                        annot=annot_data,
                        fmt="",
                        cbar=False,
                        yticklabels=False,
                    )
                    spotlight_axes[ax_idx].set_title(title, fontsize=11, weight="bold")
                    spotlight_axes[ax_idx].set_yticks(yticks)
                    spotlight_axes[ax_idx].tick_params(axis="y", length=0)

                spotlight_axes[0].set_yticklabels(spotlight_labels, rotation=0, fontsize=8)
                spotlight_axes[0].tick_params(axis="y", labelsize=8, pad=8)
                for ax in spotlight_axes[1:]:
                    ax.set_yticklabels([])
                    ax.tick_params(axis="y", labelleft=False)

                # avoid expensive tight_layout on large figures; use a reasonable adjustment
                spotlight_fig.subplots_adjust(left=0.7)
                spotlight_axes[0].set_yticklabels(spotlight_labels, rotation=0, fontsize=8)
                spotlight_axes[0].tick_params(axis="y", labelsize=8, pad=8)
                spotlight_path = variant_dir / f"{model_cfg['key']}_{event_slug}{suffix_token}_spotlight.png"
                spotlight_fig.savefig(spotlight_path, dpi=300, bbox_inches="tight")
                plt.close(spotlight_fig)
                print(f"  [OK] {event_name} spotlight: {spotlight_path}")

            # --- Draw per-country panel heatmaps (Baseline | Peak Crisis | Δ Mean | Δ Peak) ---
            # Variant logic: A uses global percentile-based scaling; B uses TwoSlopeNorm centered at 0
            for row_idx, pc in enumerate(per_country):
                labels = pc["display_labels"]
                baseline_mat = pc["baseline_mat"]
                event_mat = pc["event_mat"]
                diff_mat = pc["diff_mat"]
                max_diff_mat = pc["max_diff_mat"]

                # column 0: baseline correlations (signed, -1..1)
                ax0 = axes[row_idx, 0]
                sns.heatmap(
                    baseline_mat,
                    ax=ax0,
                    cmap="viridis",
                    vmin=-1.0,
                    vmax=1.0,
                    cbar=(row_idx == 0),
                    xticklabels=labels,
                    yticklabels=labels,
                    square=True,
                    linewidths=0.6,
                    linecolor="lightgray",
                )
                ax0.tick_params(axis="x", rotation=90, labelsize=7)
                ax0.tick_params(axis="y", rotation=0, labelsize=7)

                # column 1: peak crisis correlations (signed)
                ax1 = axes[row_idx, 1]
                sns.heatmap(
                    event_mat,
                    ax=ax1,
                    cmap="viridis",
                    vmin=-1.0,
                    vmax=1.0,
                    cbar=(row_idx == 0),
                    xticklabels=False,
                    yticklabels=False,
                    square=True,
                    linewidths=0.6,
                    linecolor="lightgray",
                )

                # column 2: Δ Mean (crisis vs baseline) - signed, center at 0
                ax2 = axes[row_idx, 2]
                if variant == "A":
                    dv = global_diff_scale
                    sns.heatmap(
                        diff_mat,
                        ax=ax2,
                        cmap="RdBu_r",
                        center=0.0,
                        vmin=-dv,
                        vmax=dv,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                elif variant == "B":
                    # ensure vmin != vmax for TwoSlopeNorm
                    dv = max(global_diff_scale, 1e-6)
                    norm = TwoSlopeNorm(vmin=-dv, vcenter=0.0, vmax=dv)
                    sns.heatmap(
                        diff_mat,
                        ax=ax2,
                        cmap="RdBu_r",
                        norm=norm,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                else:
                    # original behavior: scale per-country
                    dv = safe_scale(diff_mat, floor)
                    sns.heatmap(
                        diff_mat,
                        ax=ax2,
                        cmap="RdBu_r",
                        center=0.0,
                        vmin=-dv,
                        vmax=dv,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                annotate_symmetric_deltas(ax2, diff_mat, DIFF_ANNOT_THRESHOLD)

                # column 3: Δ Peak (abs shifts) - use absolute values and same scaling logic
                ax3 = axes[row_idx, 3]
                if variant == "A":
                    mdv = global_maxdiff_scale
                    sns.heatmap(
                        max_diff_mat,
                        ax=ax3,
                        cmap="Reds",
                        vmin=0.0,
                        vmax=mdv,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                elif variant == "B":
                    mdv = max(global_maxdiff_scale, 1e-6)
                    sns.heatmap(
                        max_diff_mat,
                        ax=ax3,
                        cmap="Reds",
                        vmin=0.0,
                        vmax=mdv,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                else:
                    mdv = safe_scale(max_diff_mat, floor)
                    sns.heatmap(
                        max_diff_mat,
                        ax=ax3,
                        cmap="Reds",
                        vmin=0.0,
                        vmax=mdv,
                        cbar=(row_idx == 0),
                        xticklabels=False,
                        yticklabels=False,
                        square=True,
                        linewidths=0.6,
                        linecolor="lightgray",
                    )
                # tidy ticks for row labels only on leftmost column
                # make tick labels a bit larger for readability
                if row_idx != 0:
                    axes[row_idx, 0].set_ylabel(pc["name"], rotation=0, labelpad=50, fontsize=9)
                axes[row_idx, 0].tick_params(axis="x", rotation=90, labelsize=8)
                axes[row_idx, 0].tick_params(axis="y", rotation=0, labelsize=8)

                # Set per-panel titles that match the original visual layout (country + column)
                try:
                    axes[row_idx, 0].set_title(f"{pc['name']} Baseline ({model_cfg['name']})", fontsize=11, weight='bold')
                    axes[row_idx, 1].set_title(f"{pc['name']} Peak Crisis ({model_cfg['name']})", fontsize=11, weight='bold')
                    axes[row_idx, 2].set_title(f"{pc['name']} Δ Mean vs Baseline ({model_cfg['name']})", fontsize=11, weight='bold')
                    axes[row_idx, 3].set_title(f"{pc['name']} Δ Peak vs Baseline ({model_cfg['name']})", fontsize=11, weight='bold')
                except Exception:
                    # defensive: continue if any axis is missing
                    pass

            for col_idx, title in enumerate(
                [
                    "Baseline",
                    "Peak Crisis",
                    "Δ Mean Crisis vs Baseline",
                    "Δ Peak vs Baseline",
                ]
            ):
                axes[0, col_idx].set_title(title, fontsize=12, weight="bold")

            # avoid matplotlib tight_layout (can hang on complex figures); use modest spacing
            fig.subplots_adjust(hspace=0.35, wspace=0.25)
            output_path = variant_dir / f"{model_cfg['key']}_{event_slug}{suffix_token}_panel.png"
            fig.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close(fig)
            print(f"  [OK] {event_name}: {output_path}")

            # avoid tight_layout for block figures as well
            block_fig.subplots_adjust(hspace=0.3)
            block_output_path = variant_dir / f"{model_cfg['key']}_{event_slug}{suffix_token}_blocks.png"
            block_fig.savefig(block_output_path, dpi=300, bbox_inches="tight")
            plt.close(block_fig)
            print(f"  [OK] {event_name} blocks: {block_output_path}")

    if summary_records:
        summary_df = pd.DataFrame(summary_records)
        summary_name = "event_correlation_summary.csv"
        if suffix_token:
            summary_name = f"event_correlation_summary{suffix_token}.csv"
        summary_path = output_dir / summary_name
        summary_df.to_csv(summary_path, index=False)
        print(f"\nSummary saved to {summary_path}")
    else:
        print("\nNo summary generated (no successful models/events).")

    if top_pair_records:
        top_df = pd.DataFrame(top_pair_records)
        top_df.sort_values(by=["model", "event", "country", "rank"], inplace=True)
        top_name = "top_pair_moves.csv"
        if suffix_token:
            top_name = f"top_pair_moves{suffix_token}.csv"
        top_path = output_dir / top_name
        top_df.to_csv(top_path, index=False)
        print(f"Top pair moves saved to {top_path}")

        crisis_tables: List[pd.DataFrame] = []
        for (model_name, event_name), group in top_df.groupby(["model", "event"], sort=False):
            subset = group.sort_values("max_abs_shift", ascending=False).head(5).copy()
            if subset.empty:
                continue
            subset["table_rank"] = range(1, len(subset) + 1)
            subset["abs_peak_vs_baseline"] = (subset["peak_corr"] - subset["baseline_corr"]).abs()
            crisis_tables.append(subset)

        if crisis_tables:
            crisis_df = pd.concat(crisis_tables, ignore_index=True)
            crisis_df = crisis_df.apply(lambda row: attach_rt_context(row, rt_overlay_df, rt_driver_df), axis=1)
            crisis_name = "crisis_tables.csv"
            if suffix_token:
                crisis_name = f"crisis_tables{suffix_token}.csv"
            crisis_path = output_dir / crisis_name
            crisis_columns = [
                "model",
                "event",
                "table_rank",
                "country",
                "indicator_a",
                "indicator_b",
                "pair",
                "risk_label",
                "baseline_corr",
                "peak_corr",
                "peak_date",
                "event_corr",
                "mean_shift",
                "max_shift",
                "max_abs_shift",
                "abs_peak_vs_baseline",
                "rt_scaled_rt",
                "rt_overlay",
                "rt_driver_level",
                "rt_driver_slope",
                "rt_driver_curvature",
                "rt_driver_lambda_dynamic",
                "rt_driver_curve_volatility",
            ]
            crisis_df.to_csv(crisis_path, columns=crisis_columns, index=False)
            print(f"Crisis tables saved to {crisis_path}")

            crisis_md_name = crisis_path.with_suffix(".md")
            write_crisis_markdown(crisis_df, crisis_md_name)
            print(f"Crisis tables markdown saved to {crisis_md_name}")


if __name__ == "__main__":
    main()