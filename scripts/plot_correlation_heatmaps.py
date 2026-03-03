import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DIAG_CORR_DIR = PROJECT_ROOT / "analysis_outputs" / "diag_corr"
CONFIG_DIR = PROJECT_ROOT / "config"
OUTPUT_DIR = PROJECT_ROOT / "analysis_outputs" / "heatmaps"


def load_series_metadata() -> dict:
    """Return human-readable labels for base series codes.

    Uses names from ``config.stress_indicators_config`` when available and
    falls back to tidy versions of the raw code.
    """
    import importlib.util

    label_map: dict[str, str] = {}

    cfg_path = CONFIG_DIR / "stress_indicators_config.py"
    if cfg_path.exists():
        spec = importlib.util.spec_from_file_location("_sic", cfg_path)
        if spec and spec.loader:
            sic = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(sic)  # type: ignore[arg-type]

            cfg_dicts = [
                getattr(sic, name)
                for name in (
                    "CREDIT_SPREADS",
                    "SOVEREIGN_YIELDS_10Y",
                    "INFLATION_CPI",
                    "GDP_GROWTH",
                    "UNEMPLOYMENT",
                    "POLICY_RATES",
                    "GLOBAL_RISK_FACTORS",
                )
                if hasattr(sic, name)
            ]
            for d in cfg_dicts:
                for code, meta in d.items():
                    nice = meta.get("name") or code
                    label_map[code] = nice.replace("_", " ")

    # Manual aliases for internal constructs
    label_map.setdefault("ITA_financial_markets_pc1", "ITA Financial Markets PC1")
    label_map.setdefault("ITA_financial_markets_pc2", "ITA Financial Markets PC2")
    label_map.setdefault("ITA_FCI", "Italy Financial Conditions Index")
    return label_map


def choose_stress_series(columns) -> str | None:
    cols = list(columns)
    # Prefer FCI
    fci = [c for c in cols if "FCI" in c]
    if fci:
        return fci[0]
    # Then VIX
    vix = [c for c in cols if "VIXCLS" in c]
    if vix:
        return vix[0]
    # Then HY spread
    hy = [c for c in cols if "BAMLH0A0HYM2" in c]
    if hy:
        return hy[0]
    return None


def extract_factors_from_pairs(columns, max_factors: int) -> list[str]:
    """Choose a core set of factor codes for the matrix.

    For the ITA Rt structure we prioritise:
    - level/slope/curvature betas: ITA_beta0/1/2
    - global risk: VIXCLS, BAMLH0A0HYM2
    - domestic FCI: ITA_FCI
    - real-economy drivers: NAEXKP01ITQ661S (GDP proxy), ITACPIALLMINMEI (CPI),
      LRHUTTTTITM156S (unemployment), MORTGAGE30US (US mortgage rate).
    """

    preferred_order = [
        "NAEXKP01ITQ661S",
        "ITACPIALLMINMEI",
        "LRHUTTTTITM156S",
        "MORTGAGE30US",
        "ITA_beta0",
        "ITA_beta1",
        "ITA_beta2",
        "VIXCLS",
        "BAMLH0A0HYM2",
        "ITA_FCI",
    ]

    available = set(str(c).split("_")[0] for c in columns)
    factors: list[str] = []
    for code in preferred_order:
        if code in available and code not in factors:
            factors.append(code)
        if len(factors) >= max_factors:
            break

    # Fallback: top-level codes from remaining columns
    if len(factors) < max_factors:
        for col in columns:
            base = str(col).split("_")[0]
            if base not in factors:
                factors.append(base)
            if len(factors) >= max_factors:
                break

    return factors[:max_factors]


def aggregate_to_matrix(rt_regime: pd.DataFrame, factor_codes: list[str]) -> pd.DataFrame:
    """Build a factor-by-factor correlation matrix from Rt pair series.

    Rules:
    - Only use *cross-factor* pairs: base codes must be different.
    - Ignore any pairs where both sides reduce to the same base
      (e.g. VIXCLS_lag0_VIXCLS_lag1).
    - After filling, force the diagonal to 1.0.
    """

    factors = list(factor_codes)
    mat = pd.DataFrame(index=factors, columns=factors, dtype=float)

    # Initialise with NaNs; diagonal will be set to 1.0 at the end
    mat.loc[:, :] = np.nan

    mean_series = rt_regime.mean(axis=0)

    for col, val in mean_series.items():
        parts = str(col).split("_")
        if len(parts) < 2:
            continue

        # base codes are before any lag suffixes
        a_full, b_full = parts[0], parts[1]
        a_base = a_full.split("_lag")[0]
        b_base = b_full.split("_lag")[0]

        # skip self/self (including different lags of same series)
        if a_base == b_base:
            continue

        if a_base in mat.index and b_base in mat.columns:
            mat.loc[a_base, b_base] = val
            mat.loc[b_base, a_base] = val

    # Force diagonal to 1.0 for all factors
    for f in factors:
        mat.loc[f, f] = 1.0

    return mat


def _short_label(raw: str, max_len: int = 12) -> str:
    """Generate a readable, compact label.

    Prefer human-friendly names from metadata, fall back to the base code
    truncated to ``max_len``.
    """
    base = str(raw).split("_")[0]
    meta = load_series_metadata()
    if base in meta:
        txt = meta[base]
    else:
        txt = base
    if len(txt) > max_len:
        return txt[:max_len]
    return txt


def plot_heatmap(corr_mat: pd.DataFrame, labels: dict, title: str, out_path: Path):
    # use compact labels for readability on dense heatmaps
    display_index = [_short_label(f) for f in corr_mat.index]
    display_columns = [_short_label(f) for f in corr_mat.columns]

    plt.figure(figsize=(10, 8))
    sns.heatmap(
        corr_mat.values,
        xticklabels=display_columns,
        yticklabels=display_index,
        cmap=sns.diverging_palette(240, 10, as_cmap=True),
        vmin=-1.0,
        vmax=1.0,
        square=True,
        cbar_kws={"shrink": 0.8},
        annot=False,
    )
    plt.title(title)
    plt.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_path)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot normal vs 99th percentile correlation heatmaps for an ISO.")
    parser.add_argument("--iso", type=str, required=True, help="Country ISO code, e.g. ITA")
    parser.add_argument("--normal-quantile", type=float, default=0.5)
    parser.add_argument("--stress-quantile", type=float, default=0.99)
    parser.add_argument("--top-factors", type=int, default=12)
    args = parser.parse_args()

    iso = args.iso.upper()
    rt_path = DIAG_CORR_DIR / f"{iso}_Rt.csv"
    if not rt_path.exists():
        raise FileNotFoundError(f"Rt file not found for ISO {iso}: {rt_path}")

    rt = pd.read_csv(rt_path, index_col=0)

    stress_col = choose_stress_series(rt.columns)
    if stress_col is None:
        raise RuntimeError("Could not find a suitable stress indicator column (FCI/VIX/HY) in Rt columns.")

    s = rt[stress_col].replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty:
        raise RuntimeError("Stress indicator series empty after NaN filtering.")

    q_norm = s.quantile(args.normal_quantile)
    q_stress = s.quantile(args.stress_quantile)

    stress_full = rt[stress_col]
    normal_rt = rt.loc[stress_full <= q_norm]
    stress_rt = rt.loc[stress_full >= q_stress]

    if normal_rt.empty or stress_rt.empty:
        raise RuntimeError("Normal or stress regime has no observations; adjust quantiles.")

    factor_codes = extract_factors_from_pairs(rt.columns, args.top_factors)

    normal_mat = aggregate_to_matrix(normal_rt, factor_codes)
    stress_mat = aggregate_to_matrix(stress_rt, factor_codes)

    label_map = load_series_metadata()

    iso_dir = OUTPUT_DIR / iso
    plot_heatmap(normal_mat, label_map, f"{iso} — Correlation (Normal Regime)", iso_dir / f"{iso}_correlation_normal.png")
    plot_heatmap(stress_mat, label_map, f"{iso} — Correlation (99th Percentile Stress)", iso_dir / f"{iso}_correlation_p99.png")


if __name__ == "__main__":
    main()
