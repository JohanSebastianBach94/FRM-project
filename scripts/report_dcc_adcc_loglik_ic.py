from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BLOCKS_DIR = ROOT / "DCC GARCH MODEL" / "results" / "blocks"
OUT_DIR = ROOT / "analysis_outputs" / "diagnostics"
OUT_PATH = OUT_DIR / "dcc_adcc_loglik_ic_summary.csv"


@dataclass
class LLResult:
    ll_corr: float
    ll_univariate: float
    ll_total: float
    n_obs: int
    n_series: int
    k_params: int


def _nearest_spd_correlation(R: np.ndarray, eps: float = 1e-10) -> np.ndarray:
    """Project to SPD and renormalize to a correlation matrix (diag=1)."""
    R = (R + R.T) / 2
    w, V = np.linalg.eigh(R)
    w = np.maximum(w, eps)
    R_spd = (V * w) @ V.T
    d = np.sqrt(np.clip(np.diag(R_spd), eps, None))
    Dinv = np.diag(1.0 / d)
    R_corr = Dinv @ R_spd @ Dinv
    R_corr = (R_corr + R_corr.T) / 2
    np.fill_diagonal(R_corr, 1.0)
    return R_corr


def _build_corr_matrix(pair_row: pd.Series, series: list[str]) -> np.ndarray:
    K = len(series)
    idx = {name: i for i, name in enumerate(series)}
    R = np.eye(K, dtype=float)
    for key, val in pair_row.items():
        if pd.isna(val):
            continue
        if "__" not in key:
            continue
        a, b = key.split("__", 1)
        if a not in idx or b not in idx:
            continue
        i, j = idx[a], idx[b]
        R[i, j] = float(val)
        R[j, i] = float(val)
    return R


def _corr_loglik(resids: pd.DataFrame, pair_ts: pd.DataFrame) -> tuple[float, int, int]:
    """Gaussian QML for standardized residuals with time-varying correlation R_t."""
    # Align on common dates and drop NaNs in residuals
    res = resids.dropna(how="any")
    pair_ts = pair_ts.dropna(how="all")
    common = res.index.intersection(pair_ts.index)
    res = res.loc[common]
    pair_ts = pair_ts.loc[common]

    if res.empty:
        return float("nan"), 0, resids.shape[1]

    series = list(res.columns)
    K = len(series)
    const = K * math.log(2 * math.pi)

    ll = 0.0
    used = 0
    for t in range(len(res)):
        e = res.iloc[t].to_numpy(dtype=float).reshape(-1, 1)
        R = _build_corr_matrix(pair_ts.iloc[t], series)

        try:
            sign, logdet = np.linalg.slogdet(R)
        except np.linalg.LinAlgError:
            sign, logdet = 0, 0

        if sign <= 0 or not np.isfinite(logdet):
            R = _nearest_spd_correlation(R)
            sign, logdet = np.linalg.slogdet(R)
            if sign <= 0 or not np.isfinite(logdet):
                continue

        try:
            quad = float(e.T @ np.linalg.solve(R, e))
        except np.linalg.LinAlgError:
            R = _nearest_spd_correlation(R)
            quad = float(e.T @ np.linalg.solve(R, e))

        ll += -0.5 * (const + float(logdet) + quad)
        used += 1

    return float(ll), int(used), int(K)


def _univariate_loglik_and_k(block_dir: Path) -> tuple[float, int]:
    params_path = block_dir / "dcc_garch_parameters.csv"
    if not params_path.exists():
        return float("nan"), 0

    df = pd.read_csv(params_path, index_col=0)
    ll = pd.to_numeric(df.get("loglikelihood"), errors="coerce").sum()

    # Parameter count per series: omega, alpha, beta, plus nu if present.
    has_nu = "nu" in df.columns and pd.to_numeric(df["nu"], errors="coerce").notna().any()
    k_per = 4 if has_nu else 3
    k = int(len(df) * k_per)

    return float(ll), k


def compute_block(block_dir: Path) -> dict:
    metrics_path = block_dir / "fit_metrics.json"
    metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.exists() else {}

    resids_path = block_dir / "standardized_residuals.csv"
    if not resids_path.exists():
        raise FileNotFoundError(f"Missing {resids_path}")

    resids = pd.read_csv(resids_path, index_col=0, parse_dates=True).sort_index()

    ll_uni, k_uni = _univariate_loglik_and_k(block_dir)

    out: dict[str, object] = {
        "block": block_dir.name,
        "label": metrics.get("label"),
        "n_series": metrics.get("n_series"),
        "n_observations": metrics.get("n_observations"),
        "dcc_a": metrics.get("dcc_a"),
        "dcc_b": metrics.get("dcc_b"),
        "dcc_a_plus_b": metrics.get("a_plus_b") if "a_plus_b" in metrics else metrics.get("dcc_a_plus_b"),
        "adcc_gamma": metrics.get("adcc_gamma"),
        "garch_convergence_rate": metrics.get("garch_convergence_rate"),
        "overfitting_flag": metrics.get("overfitting_flag"),
    }

    # DCC correlation LL
    dcc_pairs = block_dir / "correlation_time_series.csv"
    if dcc_pairs.exists():
        pair_ts = pd.read_csv(dcc_pairs, index_col=0, parse_dates=True).sort_index()
        ll_corr, used, K = _corr_loglik(resids, pair_ts)
        k = k_uni + 2
        ll_total = ll_uni + ll_corr if np.isfinite(ll_uni) and np.isfinite(ll_corr) else float("nan")
        out.update(
            {
                "dcc_ll_corr": ll_corr,
                "dcc_ll_univariate": ll_uni,
                "dcc_ll_total": ll_total,
                "dcc_ll_used_obs": used,
                "dcc_aic": (2 * k - 2 * ll_total) if np.isfinite(ll_total) else float("nan"),
                "dcc_bic": (k * math.log(max(used, 1)) - 2 * ll_total) if np.isfinite(ll_total) else float("nan"),
                "dcc_k_params": k,
            }
        )

    # ADCC correlation LL
    adcc_pairs = block_dir / "adcc_correlation_time_series.csv"
    if adcc_pairs.exists():
        pair_ts = pd.read_csv(adcc_pairs, index_col=0, parse_dates=True).sort_index()
        ll_corr, used, K = _corr_loglik(resids, pair_ts)
        k = k_uni + 3
        ll_total = ll_uni + ll_corr if np.isfinite(ll_uni) and np.isfinite(ll_corr) else float("nan")
        out.update(
            {
                "adcc_ll_corr": ll_corr,
                "adcc_ll_univariate": ll_uni,
                "adcc_ll_total": ll_total,
                "adcc_ll_used_obs": used,
                "adcc_aic": (2 * k - 2 * ll_total) if np.isfinite(ll_total) else float("nan"),
                "adcc_bic": (k * math.log(max(used, 1)) - 2 * ll_total) if np.isfinite(ll_total) else float("nan"),
                "adcc_k_params": k,
            }
        )

    return out


def main() -> None:
    if not BLOCKS_DIR.exists():
        raise SystemExit(f"Missing blocks dir: {BLOCKS_DIR}")

    rows: list[dict] = []
    for block_dir in sorted(BLOCKS_DIR.iterdir()):
        if not block_dir.is_dir():
            continue
        metrics_path = block_dir / "fit_metrics.json"
        if not metrics_path.exists():
            continue
        try:
            rows.append(compute_block(block_dir))
        except Exception as exc:
            rows.append({"block": block_dir.name, "error": str(exc)})

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(OUT_PATH, index=False)

    print(df.head(12).to_string(index=False))
    print(f"\nWrote {OUT_PATH}")


if __name__ == "__main__":
    main()
