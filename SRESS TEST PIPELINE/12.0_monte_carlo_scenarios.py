#!/usr/bin/env python3
"""Step 12.0 — Stochastic Monte Carlo scenarios (mixed-frequency; distributional cross-country stress).

This step consumes the Step 9 scenario run contract under:
  analysis_outputs/scenarios/<run_id>/manifest.json
and produces Monte Carlo factor-shock draws under:
  analysis_outputs/scenarios/<run_id>/monte_carlo/

Design intent (per IMPLEMENTATION_PLAN.md)
- Scenarios live in factor innovation space (return-like shocks).
- Low-frequency macro is NOT synthetically simulated at daily frequency.
    Macro shocks are simulated on a monthly grid and then aligned to daily via
    step-hold/LOCF (implemented here as zero shocks on non-update days).

Outputs
- daily_draws.csv: long table of simulated daily factor innovation shocks
- macro_monthly_draws.csv: authoritative macro shocks on the monthly grid
- summary.json: basic distributional summaries over shocks (MVP)
- representatives/: a small set of representative draw ids
- manifest.json + diagnostics/: audit bundle (seed, family, input checks)

Notes
- Sampling families supported: multivariate Normal and multivariate Student-t.
- Correlation is static for the horizon and assembled as:
    - within-ISO blocks: Rt_daily_pairs at as-of t0
    - cross-ISO blocks: empirical correlation from frozen standardized residuals
        (Step 9 run inputs) for the same factor names
- Scaling uses Dt_daily at as-of t0 (diagonal volatilities).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"

_FACTOR_BLOCK_RE = re.compile(r"^(?P<iso>[A-Za-z]{3})_(?P<block>[A-Za-z0-9_]+)_f(?P<k>\d+)$")
_HIGH_FREQ_LABELS = {"daily", "trading"}


def _resolve_from_root(path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return (PROJECT_ROOT / p).resolve()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")


def _find_run_dir(*, run_id: Optional[str], use_latest: bool) -> Path:
    if use_latest or (run_id is None):
        return SCENARIOS_DIR / "latest"
    return SCENARIOS_DIR / str(run_id)


def _nearest_spd_corr(R: np.ndarray, *, eig_floor: float = 1e-6) -> np.ndarray:
    R = np.asarray(R, dtype=float)
    if R.ndim != 2 or R.shape[0] != R.shape[1]:
        raise ValueError(f"Correlation matrix must be square, got shape={R.shape}")

    R = np.nan_to_num(R, nan=0.0, posinf=0.0, neginf=0.0)
    R = (R + R.T) / 2.0
    np.fill_diagonal(R, 1.0)

    # Eigenvalue floor projection (keeps it close and stable for Cholesky)
    vals, vecs = np.linalg.eigh(R)
    vals = np.maximum(vals, float(eig_floor))
    R_spd = (vecs * vals) @ vecs.T

    # Renormalize back to correlation
    d = np.sqrt(np.clip(np.diag(R_spd), 1e-12, np.inf))
    Dinv = np.diag(1.0 / d)
    R_corr = Dinv @ R_spd @ Dinv
    R_corr = np.nan_to_num(R_corr, nan=0.0, posinf=0.0, neginf=0.0)
    R_corr = (R_corr + R_corr.T) / 2.0
    np.fill_diagonal(R_corr, 1.0)
    return R_corr


def _chol_corr(R: np.ndarray, *, eig_floor: float = 1e-6) -> np.ndarray:
    try:
        return np.linalg.cholesky(_nearest_spd_corr(R, eig_floor=eig_floor))
    except Exception:
        # Last resort: identity
        return np.eye(R.shape[0], dtype=float)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_csv_list(text: Optional[str]) -> List[str]:
    if not text:
        return []
    return [p.strip() for p in str(text).split(",") if p.strip()]


def _pyarrow_available() -> bool:
    try:
        import pyarrow  # noqa: F401

        return True
    except Exception:
        return False


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _file_sha256(path: Path) -> str:
    try:
        return _sha256_bytes(path.read_bytes())
    except Exception:
        return ""


def _stable_json_hash(payload: Dict[str, Any]) -> str:
    try:
        b = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    except Exception:
        b = str(payload).encode("utf-8")
    return _sha256_bytes(b)


def _read_dt_row(dt_csv: Path, *, factors: List[str], asof_t0: Optional[str]) -> Tuple[pd.Timestamp, Dict[str, float]]:
    df = pd.read_csv(dt_csv, parse_dates=["date"])
    if "date" not in df.columns:
        raise ValueError(f"Dt file missing 'date' column: {dt_csv}")

    df = df.set_index("date").sort_index()
    if df.empty:
        raise ValueError(f"Empty Dt file: {dt_csv}")

    if asof_t0:
        t0 = pd.to_datetime(asof_t0)
        df = df.loc[df.index <= t0]
        if df.empty:
            raise ValueError(f"No Dt rows <= asof_t0={asof_t0} in {dt_csv}")

    row = df.iloc[-1]
    t0_eff = pd.to_datetime(df.index[-1])

    vols: Dict[str, float] = {}
    for f in factors:
        v = row.get(f)
        try:
            vf = float(v)
        except Exception:
            vf = float("nan")
        if not np.isfinite(vf) or vf <= 0:
            vf = 1.0
        vols[str(f)] = float(vf)

    return t0_eff, vols


def _infer_lowfreq_factors_from_dt(
    dt_csv: Path,
    *,
    factors: List[str],
    update_frac_threshold: float,
    tol: float,
) -> Tuple[set[str], Dict[str, float]]:
    """Infer low-frequency (release-date/step-like) factors from Dt_daily.

    We treat a factor as low-frequency if its Dt_daily values change on fewer
    than `update_frac_threshold` of business days.
    """
    df = pd.read_csv(dt_csv, parse_dates=["date"]).set_index("date").sort_index()
    low: set[str] = set()
    update_fracs: Dict[str, float] = {}
    for f in factors:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce")
        if s.isna().all():
            continue
        changed = (s - s.shift(1)).abs()
        is_upd = changed > float(tol)
        frac = float(is_upd.fillna(False).mean()) if len(is_upd) else 0.0
        update_fracs[str(f)] = frac
        if np.isfinite(frac) and frac < float(update_frac_threshold):
            low.add(str(f))
    return low, update_fracs


def _load_series_frequency_map(*, series_metadata_path: Path) -> Dict[str, str]:
    """Load native series frequency mapping.

    Prefers config/series_metadata.yaml (native frequency, not daily-expanded).
    Returns: series -> frequency label (lowercased)
    """
    if not series_metadata_path.exists():
        return {}

    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(series_metadata_path.read_text(encoding="utf-8"))
        meta = (payload or {}).get("series_metadata") or {}
        if not isinstance(meta, dict):
            return {}
        out: Dict[str, str] = {}
        for k, v in meta.items():
            if not k:
                continue
            if not isinstance(v, dict):
                continue
            freq = str(v.get("frequency") or "").strip().lower()
            if freq:
                out[str(k).strip()] = freq
        return out
    except Exception:
        return {}


def _load_release_calendar(*, path: Path) -> Dict[str, Any]:
    """Load explicit release calendar rules.

    Expected schema:
      release_calendar:
        defaults: {monthly|quarterly|annual|yearly: {months, bday_of_month, period_months?}}
        iso_overrides: {ISO: {freq: {...}}}
        block_overrides: {block: {freq: {...}}}
        factor_overrides: {factor: {freq: {...}}}
    """
    if not path.exists():
        return {}
    try:
        import yaml  # type: ignore

        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        return payload or {}
    except Exception:
        return {}


def _build_forward_update_dates_from_release_calendar(
    *,
    iso: str,
    factors: List[str],
    forward_dates: List[pd.Timestamp],
    low_set: set[str],
    meta_diag: Dict[str, Dict[str, Any]],
    release_calendar: Dict[str, Any],
    strict: bool,
) -> Tuple[Dict[str, set[str]], Dict[str, Dict[str, Any]]]:
    """Build forward update dates from an explicit release calendar (no Dt use)."""
    if not forward_dates:
        return {}, {}

    cal_root = (release_calendar or {}).get("release_calendar") or {}
    defaults = (cal_root.get("defaults") or {}) if isinstance(cal_root, dict) else {}
    iso_overrides = (cal_root.get("iso_overrides") or {}) if isinstance(cal_root, dict) else {}
    block_overrides = (cal_root.get("block_overrides") or {}) if isinstance(cal_root, dict) else {}
    factor_overrides = (cal_root.get("factor_overrides") or {}) if isinstance(cal_root, dict) else {}

    fwd = pd.to_datetime(pd.Index(forward_dates)).sort_values()
    fwd_df = pd.DataFrame({"date": fwd})
    fwd_df["month"] = fwd_df["date"].dt.to_period("M")
    fwd_df["month_num"] = fwd_df["date"].dt.month.astype(int)
    fwd_df["bday_of_month"] = fwd_df.groupby("month").cumcount() + 1
    fwd_df["n_bdays_in_month"] = fwd_df.groupby("month")["date"].transform("size").astype(int)
    fwd_df["bday_from_end"] = fwd_df["bday_of_month"] - (fwd_df["n_bdays_in_month"] + 1)
    fwd_df["date_str"] = fwd_df["date"].dt.strftime("%Y-%m-%d")

    def _norm_freq(x: Any) -> str:
        s = str(x or "").strip().lower()
        if s in {"yearly", "annual"}:
            return "annual" if s == "annual" else "yearly"
        return s

    def _rule_for(*, factor: str, block: Optional[str], implied: str) -> Dict[str, Any]:
        # Precedence: factor > block > iso > defaults
        freq = _norm_freq(implied)
        out: Dict[str, Any] = {}

        def _merge(d: Any) -> None:
            if isinstance(d, dict):
                out.update(d)

        _merge(((defaults or {}).get(freq) or {}))
        _merge((((iso_overrides or {}).get(str(iso)) or {}).get(freq) or {}))
        if block:
            _merge((((block_overrides or {}).get(str(block)) or {}).get(freq) or {}))
        _merge((((factor_overrides or {}).get(str(factor)) or {}).get(freq) or {}))
        return out

    freq_to_period = {"monthly": 1, "quarterly": 3, "annual": 12, "yearly": 12}
    out: Dict[str, set[str]] = {}
    diag: Dict[str, Dict[str, Any]] = {}

    for f in [str(x) for x in (factors or [])]:
        if f not in (low_set or set()):
            continue

        md = (meta_diag or {}).get(f) or {}
        implied = str(md.get("implied_frequency") or "").strip().lower()
        parsed = _parse_factor_block(f)
        block = parsed[1] if parsed is not None else None

        if not implied:
            # Governance choice: if low-frequency but no implied frequency, default monthly unless strict.
            if strict:
                diag[f] = {
                    "schedule_source": "release_calendar",
                    "status": "error",
                    "reason": "missing_implied_frequency",
                    "implied_frequency": "",
                }
                continue
            implied = "monthly"

        rule = _rule_for(factor=f, block=block, implied=implied)
        months = rule.get("months")
        bday = rule.get("bday_of_month")

        allowed_dates: set[str] = set()
        used_months: List[int] = []

        if months is None:
            months = "all"

        if isinstance(months, str) and months.strip().lower() == "all":
            used_months = list(range(1, 13))
        elif isinstance(months, (list, tuple, set)):
            try:
                used_months = [int(m) for m in months]
                used_months = [m for m in used_months if 1 <= m <= 12]
            except Exception:
                used_months = []
        else:
            used_months = []

        try:
            bday_i = int(bday)
        except Exception:
            bday_i = 1

        if used_months:
            sub = fwd_df[fwd_df["month_num"].isin(used_months)]
            if bday_i >= 1:
                sub = sub[sub["bday_of_month"].astype(int) == int(bday_i)]
            else:
                sub = sub[sub["bday_from_end"].astype(int) == int(bday_i)]
            allowed_dates = set(sub["date_str"].astype(str).tolist())

        period_months = rule.get("period_months")
        try:
            period_i = int(period_months) if period_months is not None else int(freq_to_period.get(implied, 1))
        except Exception:
            period_i = int(freq_to_period.get(implied, 1))

        if not allowed_dates and strict:
            diag[f] = {
                "schedule_source": "release_calendar",
                "status": "error",
                "reason": "no_forward_updates_generated",
                "implied_frequency": str(implied),
                "period_months": int(period_i),
                "bday_of_month": int(bday_i),
                "months": ",".join([str(m) for m in used_months]) if used_months else "",
                "n_forward_updates": 0,
            }
            continue

        if allowed_dates:
            out[f] = allowed_dates

        diag[f] = {
            "schedule_source": "release_calendar",
            "status": "ok" if allowed_dates else "empty",
            "implied_frequency": str(implied),
            "period_months": int(period_i),
            "bday_of_month": int(bday_i),
            "months": ",".join([str(m) for m in used_months]) if used_months else "",
            "n_forward_updates": int(len(allowed_dates)),
            "block": str(block or ""),
        }

    return out, diag


def _parse_factor_block(factor: str) -> Optional[Tuple[str, str, int]]:
    m = _FACTOR_BLOCK_RE.match(str(factor))
    if not m:
        return None
    try:
        iso = str(m.group("iso"))
        block = str(m.group("block"))
        k = int(m.group("k"))
        return iso, block, k
    except Exception:
        return None


def _infer_lowfreq_factors_from_metadata(
    *,
    iso: str,
    factors: List[str],
    series_freq: Dict[str, str],
    pca_loadings_dir: Path,
    factor_constituents_dir: Optional[Path] = None,
    high_freq_labels: set[str] = _HIGH_FREQ_LABELS,
    min_high_share: float = 0.50,
) -> Tuple[set[str], Dict[str, Dict[str, Any]]]:
    """Classify low-frequency factors using series native-frequency metadata.

    For raw series factors: look up frequency directly.
    For block PC factors like "USA_external_fx_f1":
      - load block PCA loadings file
      - map f1 -> block_pc1 column
      - compute abs-loading-weighted share of high-frequency constituents

    Returns (low_set, diag_by_factor). Factors that cannot be classified are
    omitted from low_set and recorded with reason="unknown".
    """
    low: set[str] = set()
    diag: Dict[str, Dict[str, Any]] = {}

    # Cache PCA loadings per (iso, block)
    pca_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}

    # Cache factor constituents per (iso, block)
    fc_cache: Dict[Tuple[str, str], Optional[pd.DataFrame]] = {}

    def _get_pca(iso_: str, block_: str) -> Optional[pd.DataFrame]:
        key = (iso_, block_)
        if key in pca_cache:
            return pca_cache[key]
        path = pca_loadings_dir / f"{iso_}_{block_}_pca_loadings.csv"
        if not path.exists():
            pca_cache[key] = None
            return None

    def _get_factor_constituents(iso_: str, block_: str) -> Optional[pd.DataFrame]:
        if factor_constituents_dir is None:
            return None
        key = (iso_, block_)
        if key in fc_cache:
            return fc_cache[key]
        path = Path(factor_constituents_dir) / f"{iso_}_{block_}_factor_constituents.csv"
        if not path.exists():
            fc_cache[key] = None
            return None
        try:
            df = pd.read_csv(path)
            fc_cache[key] = df
            return df
        except Exception:
            fc_cache[key] = None
            return None

    def _weighted_frequency_stats(abs_weights: pd.Series) -> Tuple[Optional[str], float, float, Dict[str, float]]:
        """Return (dominant_label, dominant_share, high_share, mix_shares)."""
        freq_w: Dict[str, float] = {}
        for series_name, weight in abs_weights.items():
            sf = (series_freq.get(str(series_name)) or "").strip().lower()
            if not sf:
                continue
            freq_w[sf] = float(freq_w.get(sf, 0.0) + float(weight))
        total = float(sum(freq_w.values()))
        if total <= 0:
            return None, 0.0, 0.0, {}

        mix = {k: float(v) / total for k, v in freq_w.items()}
        dominant = max(mix.items(), key=lambda kv: kv[1])[0] if mix else None
        dominant_share = float(mix.get(dominant, 0.0)) if dominant else 0.0

        hf = {str(x).lower() for x in high_freq_labels}
        high_share = float(sum(v for k, v in mix.items() if str(k).lower() in hf))
        return dominant, dominant_share, high_share, mix
        try:
            df = pd.read_csv(path, index_col=0)
            pca_cache[key] = df
            return df
        except Exception:
            pca_cache[key] = None
            return None

    for f in factors:
        f = str(f)

        # Raw series factor
        freq = (series_freq.get(f) or "").strip().lower()
        if freq:
            is_high = freq in set(x.lower() for x in high_freq_labels)
            is_low = not is_high
            if is_low:
                low.add(f)
            diag[f] = {
                "source": "series_metadata",
                "series": f,
                "frequency": freq,
                "implied_frequency": freq,
                "is_low": bool(is_low),
            }
            continue

        # PC factor
        parsed = _parse_factor_block(f)
        if parsed is None:
            diag[f] = {"source": "metadata", "reason": "unrecognized_factor_name"}
            continue
        iso_f, block, k = parsed
        if str(iso_f).upper() != str(iso).upper():
            # Defensive; factors list should already be ISO-specific
            diag[f] = {"source": "metadata", "reason": "iso_mismatch", "iso": iso_f, "block": block, "k": k}
            continue

        pca = _get_pca(str(iso), block)
        pca_missing = pca is None or pca.empty

        col = f"{block}_pc{k}"
        w: pd.Series = pd.Series(dtype=float)
        used_cols: list[str] = []
        constituent_source = ""

        if not pca_missing:
            if col in pca.columns:
                used_cols = [col]
                w = pd.to_numeric(pca[col], errors="coerce").dropna()
                constituent_source = "pca_loadings"
            else:
                # Fallback: if the exact PC column isn't present (e.g., PCA only kept pc1
                # for a highly-collinear 2-series block), infer frequency from the block's
                # constituent series using available PC columns.
                pc_cols = [c for c in pca.columns if str(c).startswith(f"{block}_pc")]
                if pc_cols:
                    used_cols = pc_cols
                    try:
                        tmp = pca[pc_cols].apply(pd.to_numeric, errors="coerce")
                        w = tmp.abs().mean(axis=1).dropna()
                        constituent_source = "pca_loadings"
                    except Exception:
                        w = pd.Series(dtype=float)

        # Second source: per-block factor constituent map (if PCA loadings absent)
        if w.empty:
            fc = _get_factor_constituents(str(iso), block)
            if fc is not None and not fc.empty:
                try:
                    sub = fc[fc.get("factor").astype(str) == str(f)]
                    if not sub.empty and ("series" in sub.columns) and ("weight" in sub.columns):
                        sw = pd.to_numeric(sub["weight"], errors="coerce")
                        ss = sub["series"].astype(str)
                        tmp = pd.Series(sw.values, index=ss.values)
                        tmp = tmp.dropna()
                        if not tmp.empty:
                            w = tmp
                            used_cols = ["factor_constituents.weight"]
                            constituent_source = "factor_constituents"
                except Exception:
                    pass

        if w.empty:
            diag[f] = {
                "source": "metadata",
                "reason": "missing_pca_loadings_and_factor_constituents" if pca_missing else "empty_constituents",
                "block": block,
                "k": k,
                "requested_col": col,
            }
            continue

        abs_w = w.abs()
        dominant, dominant_share, high_share, mix = _weighted_frequency_stats(abs_w)
        total_weight = float(sum(abs_w.values))
        n_with_freq = int(sum(1 for s in abs_w.index if (series_freq.get(str(s)) or "").strip()))
        if not mix:
            diag[f] = {"source": "metadata", "reason": "no_constituent_frequencies", "block": block, "k": k}
            continue

        is_low = bool(high_share < float(min_high_share))
        if is_low:
            low.add(f)

        diag[f] = {
            "source": f"{constituent_source}+series_metadata" if constituent_source else "series_metadata",
            "block": block,
            "k": int(k),
            "constituent_source": str(constituent_source or ""),
            "n_constituents": int(len(abs_w)),
            "n_constituents_with_freq": int(n_with_freq),
            "total_abs_weight": float(total_weight),
            "high_freq_share": float(high_share),
            "min_high_share": float(min_high_share),
            "is_low": bool(is_low),
            "implied_frequency": str(dominant or ""),
            "implied_frequency_share": float(dominant_share),
            "requested_col": col,
            "used_cols": ",".join(used_cols) if used_cols else col,
        }

    return low, diag


def _infer_update_dates_from_dt(
    dt_csv: Path,
    *,
    factors: List[str],
    tol: float,
    asof_t0: Optional[str],
) -> Dict[str, set[str]]:
    """Return {factor -> set(YYYY-MM-DD)} for Dt_daily change dates (historical).

    NOTE: This is *historical* and not directly usable for forward gating.
    It is retained for diagnostics/backward-compat only.
    """
    df = pd.read_csv(dt_csv, parse_dates=["date"]).set_index("date").sort_index()
    if asof_t0:
        t0 = pd.to_datetime(asof_t0)
        df = df.loc[df.index <= t0]
    out: Dict[str, set[str]] = {}
    for f in factors:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce")
        if s.isna().all():
            continue
        changed = (s - s.shift(1)).abs()
        is_upd = (changed > float(tol)).fillna(False)
        dates = {pd.to_datetime(ix).strftime("%Y-%m-%d") for ix, v in is_upd.items() if bool(v)}
        if dates:
            out[str(f)] = dates
    return out


def _infer_forward_update_dates_from_dt(
    dt_csv: Path,
    *,
    factors: List[str],
    forward_dates: List[pd.Timestamp],
    tol: float,
    asof_t0: Optional[str],
) -> Tuple[Dict[str, set[str]], Dict[str, Dict[str, Any]]]:
    """Infer a *forward* update schedule from historical Dt change patterns.

    We need a forward-looking set of update dates (within the simulation horizon)
    to implement the "release-date shocks" policy. Historical Dt change dates do
    not overlap the forward horizon.

    Approach (simple + defensible):
    - Identify historical update events as Dt changes (abs diff > tol).
    - Infer a typical update periodicity in months (median spacing of update months).
    - Infer a typical business-day-of-month position (median bday index of update dates).
    - Project that schedule forward from the last observed update month.

    Returns:
      - forward_update_dates: factor -> set(YYYY-MM-DD) within forward_dates
      - diag: factor -> {period_months, bday_of_month, anchor_month, n_hist_updates, n_hist_update_months}
    """
    if not forward_dates:
        return {}, {}

    df = pd.read_csv(dt_csv, parse_dates=["date"]).set_index("date").sort_index()
    if asof_t0:
        t0 = pd.to_datetime(asof_t0)
        df = df.loc[df.index <= t0]

    fwd = pd.to_datetime(pd.Index(forward_dates)).sort_values()
    fwd_df = pd.DataFrame({"date": fwd})
    fwd_df["month"] = fwd_df["date"].dt.to_period("M")
    fwd_df["bday_of_month"] = fwd_df.groupby("month").cumcount() + 1
    fwd_df["date_str"] = fwd_df["date"].dt.strftime("%Y-%m-%d")

    first_fwd_month = fwd_df["month"].iloc[0]

    def _ym(p: Any) -> Tuple[int, int]:
        # Convert Period-like or string 'YYYY-MM' to (year, month)
        try:
            if isinstance(p, str):
                y, m = p.split("-")[:2]
                return int(y), int(m)
            # Period
            y = int(getattr(p, "year"))
            m = int(getattr(p, "month"))
            return y, m
        except Exception:
            # Fallback: parse via Timestamp
            t = pd.to_datetime(p)
            return int(t.year), int(t.month)

    def _prev_month(p: Any) -> pd.Period:
        y, m = _ym(p)
        if m > 1:
            return pd.Period(f"{y}-{m-1:02d}", freq="M")
        return pd.Period(f"{y-1}-12", freq="M")

    def _month_diff(m1: Any, m0: Any) -> int:
        y1, mm1 = _ym(m1)
        y0, mm0 = _ym(m0)
        return int((y1 - y0) * 12 + (mm1 - mm0))

    out: Dict[str, set[str]] = {}
    diag: Dict[str, Dict[str, Any]] = {}

    for f in factors:
        if f not in df.columns:
            continue
        s = pd.to_numeric(df[f], errors="coerce")
        if s.isna().all():
            continue

        changed = (s - s.shift(1)).abs()
        is_upd = (changed > float(tol)).fillna(False)
        hist_upd_dates = [pd.to_datetime(ix) for ix, v in is_upd.items() if bool(v)]
        hist_upd_dates = sorted({d.normalize() for d in hist_upd_dates})

        # Defaults: monthly on 1st business day
        period_months = 1
        bday_of_month = 1
        anchor_month = _prev_month(first_fwd_month)  # so first forward month triggers update

        if hist_upd_dates:
            # Infer typical business-day-of-month of updates
            bdays: List[int] = []
            months: List[pd.Period] = []
            for d in hist_upd_dates:
                m = d.to_period("M")
                months.append(m)
                month_start = pd.Timestamp(m.start_time).normalize()
                month_end = pd.Timestamp(m.end_time).normalize()
                bdr = pd.bdate_range(start=month_start, end=month_end)
                try:
                    pos = int(bdr.get_loc(d)) + 1
                    bdays.append(pos)
                except Exception:
                    continue

            if bdays:
                bday_of_month = int(np.clip(int(round(float(np.median(bdays)))), 1, 31))

            # Infer periodicity in months from update months
            uniq_months = sorted(set(months))
            if len(uniq_months) >= 2:
                diffs = [_month_diff(uniq_months[i], uniq_months[i - 1]) for i in range(1, len(uniq_months))]
                diffs = [d for d in diffs if d > 0]
                if diffs:
                    period_months = int(max(1, round(float(np.median(diffs)))))

            anchor_month = uniq_months[-1] if uniq_months else anchor_month

        # Project forward schedule
        allowed_dates: set[str] = set()
        for r in fwd_df.itertuples(index=False):
            m = r.month
            # update in months spaced by period_months from anchor_month
            dm = _month_diff(m, anchor_month)
            if dm <= 0:
                continue
            if dm % int(period_months) != 0:
                continue
            if int(r.bday_of_month) == int(bday_of_month):
                allowed_dates.add(str(r.date_str))

        if allowed_dates:
            out[str(f)] = allowed_dates

        diag[str(f)] = {
            "period_months": int(period_months),
            "bday_of_month": int(bday_of_month),
            "anchor_month": str(anchor_month),
            "n_hist_updates": int(len(hist_upd_dates)),
            "n_hist_update_months": int(len(sorted(set([d.to_period('M') for d in hist_upd_dates])))) if hist_upd_dates else 0,
            "n_forward_updates": int(len(allowed_dates)),
        }

    return out, diag


def _read_rt_row(rt_pairs_csv: Path, *, factors: List[str], asof_t0: pd.Timestamp) -> np.ndarray:
    # Header scan for quick column existence checks
    header = rt_pairs_csv.read_text(encoding="utf-8", errors="replace").splitlines()[0]
    cols = [c.strip() for c in header.split(",") if c.strip()]
    available = set(cols)

    # Build list of (i, j, colname) for available pairs
    pair_specs: List[Tuple[int, int, str]] = []
    for i in range(len(factors)):
        for j in range(i + 1, len(factors)):
            a, b = str(factors[i]), str(factors[j])
            c1, c2 = f"{a}_{b}", f"{b}_{a}"
            if c1 in available:
                pair_specs.append((i, j, c1))
            elif c2 in available:
                pair_specs.append((i, j, c2))

    usecols = ["date"] + [c for _, _, c in pair_specs]
    df = pd.read_csv(rt_pairs_csv, usecols=usecols, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    df = df.loc[df.index <= asof_t0]
    if df.empty:
        raise ValueError(f"No Rt rows <= asof_t0={asof_t0.date()} in {rt_pairs_csv}")

    row = df.iloc[-1]

    R = np.eye(len(factors), dtype=float)
    for i, j, col in pair_specs:
        try:
            v = float(row.get(col))
        except Exception:
            v = float("nan")
        if not np.isfinite(v):
            v = 0.0
        # Clip for numerical stability
        v = float(np.clip(v, -0.999, 0.999))
        R[i, j] = v
        R[j, i] = v

    return R


def _load_standardized_residuals(run_dir: Path, *, iso: str, factors: List[str]) -> pd.DataFrame:
    """Load standardized residuals (z-space) for an ISO from frozen scenario inputs."""
    path = run_dir / "inputs" / str(iso) / "covariance" / f"{iso}_standardized_residuals_daily.csv"
    if not path.exists():
        raise FileNotFoundError(str(path))
    df = pd.read_csv(path, parse_dates=["date"]).set_index("date").sort_index()
    keep = [f for f in factors if f in df.columns]
    if not keep:
        raise ValueError(f"No requested factors found in standardized residuals for {iso}")
    out = df[keep].copy()
    out = out.apply(pd.to_numeric, errors="coerce")
    return out


def _build_joint_corr(
    *,
    run_dir: Path,
    iso_factors: Dict[str, List[str]],
    iso_rt_blocks: Dict[str, np.ndarray],
    corr_shrinkage: float,
    corr_eig_floor: float,
) -> Tuple[List[Tuple[str, str]], np.ndarray]:
    """Build joint correlation matrix across all (iso, factor) dims.

    Diagonal ISO blocks are taken from Rt(t0) (iso_rt_blocks).
    Off-diagonal blocks are estimated empirically from standardized residuals.
    """
    dims: List[Tuple[str, str]] = []
    for iso in sorted(iso_factors.keys()):
        for f in iso_factors[iso]:
            dims.append((str(iso), str(f)))

    # Empirical correlation from standardized residuals
    frames: List[pd.DataFrame] = []
    for iso in sorted(iso_factors.keys()):
        df = _load_standardized_residuals(run_dir, iso=iso, factors=iso_factors[iso])
        df = df.rename(columns={c: f"{iso}::{c}" for c in df.columns})
        frames.append(df)

    merged = pd.concat(frames, axis=1, join="inner").dropna(axis=0, how="any")
    if merged.shape[0] < 50:
        # If too little overlap, fall back to block-diagonal independence.
        R_emp = np.eye(len(dims), dtype=float)
    else:
        R_emp = merged.corr().to_numpy(dtype=float)

    # Override diagonal blocks with Rt(t0)
    pos = 0
    for iso in sorted(iso_factors.keys()):
        k = len(iso_factors[iso])
        if k <= 0:
            continue
        block = iso_rt_blocks.get(iso)
        if block is not None and block.shape == (k, k):
            R_emp[pos : pos + k, pos : pos + k] = block
        pos += k

    # Shrink, sanitize, SPD project
    lam = float(corr_shrinkage)
    if not np.isfinite(lam) or lam < 0.0 or lam > 1.0:
        lam = 0.05
    R_emp = (1.0 - lam) * R_emp + lam * np.eye(R_emp.shape[0], dtype=float)
    R_emp = _nearest_spd_corr(R_emp, eig_floor=float(corr_eig_floor))
    return dims, R_emp


def _sample_correlated(
    rng: np.random.Generator,
    *,
    n: int,
    dim: int,
    chol: np.ndarray,
    family: str,
    df: Optional[float],
) -> np.ndarray:
    """Return array of shape (n, dim) with correlation implied by chol."""
    z = rng.standard_normal(size=(int(n), int(dim)))
    u = z @ chol.T

    family = str(family).strip().lower()
    if family in {"normal", "gaussian", "mvn"}:
        return u

    if family in {"student_t", "t", "student"}:
        nu = float(df or 7.0)
        if not np.isfinite(nu) or nu <= 2.0:
            raise ValueError("Student-t df must be > 2")
        g = rng.chisquare(df=nu, size=(int(n), 1))
        scale = np.sqrt(nu / np.clip(g, 1e-12, np.inf))
        return u * scale

    raise ValueError(f"Unknown family: {family}")


def _estimate_rows(*, n_draws: int, horizon_days: int, n_dims: int) -> int:
    try:
        return int(n_draws) * int(horizon_days) * int(n_dims)
    except Exception:
        return 0


def _write_daily_chunk_csv(
    *,
    out_path: Path,
    df: pd.DataFrame,
    header: bool,
    compress: bool,
) -> None:
    if compress:
        df.to_csv(out_path, index=False, mode="a", header=header, compression="gzip")
    else:
        df.to_csv(out_path, index=False, mode="a", header=header)


def _write_daily_chunk_parquet(*, out_path: Path, df: pd.DataFrame) -> None:
    # Requires pyarrow or fastparquet.
    df.to_parquet(out_path, index=False)


def _clear_daily_shards_dir(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        return
    # Only remove known shard file types.
    for p in path.rglob("*.parquet"):
        try:
            p.unlink()
        except Exception:
            pass
    for p in path.rglob("*.csv"):
        try:
            p.unlink()
        except Exception:
            pass
    for p in path.rglob("*.csv.gz"):
        try:
            p.unlink()
        except Exception:
            pass

    # Also remove stale per-ISO subdirectories (they can cause readers to
    # preferentially load old shards if left in place).
    for child in list(path.iterdir()):
        if child.is_dir():
            try:
                shutil.rmtree(child, ignore_errors=True)
            except Exception:
                pass


def _build_iso_long_df(
    *,
    run_name: str,
    iso: str,
    draw_ids: np.ndarray,
    dates: List[str],
    factors: List[str],
    shocks: np.ndarray,
) -> pd.DataFrame:
    """Build long DF for one ISO.

    shocks shape: (n_draw, horizon, k)
    """
    n_draw, horizon, k = shocks.shape
    draw_col = np.repeat(draw_ids.astype(int), horizon * k)
    date_col = np.tile(np.repeat(np.array(dates, dtype=object), k), n_draw)
    h_col = np.tile(np.repeat(np.arange(1, horizon + 1, dtype=int), k), n_draw)
    factor_col = np.tile(np.array(factors, dtype=object), n_draw * horizon)
    shock_col = shocks.reshape(-1).astype(float)

    return pd.DataFrame(
        {
            "run_id": run_name,
            "draw_id": draw_col,
            "iso": iso,
            "date": date_col,
            "h": h_col,
            "factor": factor_col,
            "shock": shock_col,
            "shock_units": "innovation",
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Step 12.0 — Monte Carlo scenarios (daily factors; mixed-frequency policy)")
    parser.add_argument("--run-id", default=None, help="Scenario run id under analysis_outputs/scenarios/<run_id> (default: latest)")
    parser.add_argument("--use-latest", action="store_true", help="Use analysis_outputs/scenarios/latest")

    parser.add_argument(
        "--output-subdir",
        type=str,
        default="monte_carlo",
        help=(
            "Output subdirectory under the scenario run directory (default: monte_carlo). "
            "Useful for smoke tests to avoid overwriting production outputs."
        ),
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help=(
            "Run a small deterministic smoke test (overrides n-draws/horizon/seed/output-subdir unless explicitly set). "
            "Writes to output-subdir 'monte_carlo_smoke' by default."
        ),
    )

    parser.add_argument("--n-draws", type=int, default=200, help="Number of Monte Carlo draws")
    parser.add_argument("--horizon-days", type=int, default=60, help="Forward horizon in business days")
    parser.add_argument("--seed", type=int, default=12345, help="RNG seed")

    parser.add_argument("--family", type=str, default="student_t", choices=["normal", "student_t"], help="Sampling family")
    parser.add_argument("--t-df", type=float, default=7.0, help="Degrees of freedom for student_t")

    parser.add_argument("--t0", default=None, help="Optional as-of date override (YYYY-MM-DD). Default: per-ISO latest Dt row")

    parser.add_argument("--corr-shrinkage", type=float, default=0.05, help="Shrink Rt(t0) toward identity")
    parser.add_argument("--corr-eig-floor", type=float, default=1e-6, help="Eigenvalue floor for SPD projection")

    parser.add_argument(
        "--cross-iso-mode",
        type=str,
        default="empirical",
        choices=["independent", "empirical"],
        help=(
            "How to couple ISOs. 'empirical' estimates cross-ISO blocks from frozen standardized residuals; "
            "'independent' keeps ISO blocks independent."
        ),
    )

    parser.add_argument(
        "--lowfreq-classifier",
        type=str,
        default="auto",
        choices=["auto", "dt", "metadata"],
        help=(
            "How to classify low-frequency factors. "
            "'dt' uses Dt_daily step-change frequency. "
            "'metadata' uses config/series_metadata.yaml plus PCA loadings and factor constituent maps. "
            "'auto' uses metadata if available, else falls back to dt."
        ),
    )
    parser.add_argument(
        "--lowfreq-metadata-strict",
        action="store_true",
        help=(
            "In metadata mode, error if any factor cannot be classified from metadata (no Dt fallback). "
            "Strongly recommended for governance/production runs."
        ),
    )
    parser.add_argument(
        "--lowfreq-update-frac-threshold",
        type=float,
        default=0.10,
        help="If Dt_daily changes on fewer than this fraction of days, treat factor as low-frequency.",
    )
    parser.add_argument(
        "--lowfreq-dt-change-tol",
        type=float,
        default=1e-9,
        help="Absolute Dt_daily change tolerance used when detecting update days.",
    )

    parser.add_argument(
        "--lowfreq-force",
        default=None,
        help="Comma-separated factor names to force as low-frequency (applies within each ISO).",
    )
    parser.add_argument(
        "--lowfreq-never",
        default=None,
        help="Comma-separated factor names to force as NOT low-frequency (applies within each ISO).",
    )

    parser.add_argument(
        "--lowfreq-force-blocks",
        default=None,
        help="Comma-separated block names to force as low-frequency (e.g. 'macro,public_finance').",
    )
    parser.add_argument(
        "--lowfreq-never-blocks",
        default=None,
        help="Comma-separated block names to force as NOT low-frequency (e.g. 'external_fx,financial_markets').",
    )

    parser.add_argument(
        "--update-schedule-mode",
        type=str,
        default="calendar",
        choices=["calendar", "dt_projection"],
        help=(
            "How to build the forward update-date schedule for low-frequency factors. "
            "'calendar' uses an explicit governed release calendar (no Dt). "
            "'dt_projection' projects historical Dt change patterns forward (deprecated; diagnostics/debug only)."
        ),
    )
    parser.add_argument(
        "--release-calendar-path",
        default=str(Path("config") / "release_calendar.yaml"),
        help=(
            "Path to release_calendar.yaml (explicit forward release-day schedule rules). "
            "Relative paths are resolved from project root."
        ),
    )
    parser.add_argument(
        "--release-calendar-strict",
        action="store_true",
        help="Error if any low-frequency factor cannot be scheduled from the release calendar.",
    )

    parser.add_argument(
        "--series-metadata-path",
        default=str(Path("config") / "series_metadata.yaml"),
        help="Path to series_metadata.yaml (native frequency mapping). Relative paths are resolved from project root.",
    )
    parser.add_argument(
        "--pca-loadings-dir",
        default=str(Path("analysis_outputs") / "factor_preparation"),
        help="Directory containing <ISO>_<block>_pca_loadings.csv. Relative paths are resolved from project root.",
    )
    parser.add_argument(
        "--factor-constituents-dir",
        default=str(Path("analysis_outputs") / "factor_preparation"),
        help=(
            "Directory containing <ISO>_<block>_factor_constituents.csv (per-factor constituent maps). "
            "Relative paths are resolved from project root."
        ),
    )
    parser.add_argument(
        "--lowfreq-metadata-min-high-share",
        type=float,
        default=0.50,
        help=(
            "In metadata mode, a PC factor is treated as high-frequency if the "
            "abs-loading-weighted share of high-frequency constituents is >= this threshold."
        ),
    )

    parser.add_argument(
        "--daily-output-mode",
        type=str,
        default="auto",
        choices=["auto", "single", "sharded"],
        help="Write daily draws as a single file or a sharded folder. Auto chooses sharded for large runs.",
    )
    parser.add_argument(
        "--daily-output-format",
        type=str,
        default="auto",
        choices=["auto", "csv", "parquet"],
        help="Daily draws output format. Parquet requires pyarrow/fastparquet; auto falls back to csv.",
    )
    parser.add_argument(
        "--daily-csv-gzip",
        action="store_true",
        help="If writing CSV, compress shards with gzip (.csv.gz).",
    )
    parser.add_argument(
        "--draw-chunk",
        type=int,
        default=25,
        help="Draw chunk size used for streaming writes (smaller uses less memory).",
    )

    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable caching. By default, Step 12 skips recompute if inputs+args signature matches an existing output manifest.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force recomputation even if cache signature matches.",
    )

    args = parser.parse_args()

    # Smoke preset: keep it fast and deterministic, and avoid clobbering the main outputs.
    if bool(getattr(args, "smoke", False)):
        if str(getattr(args, "output_subdir", "monte_carlo") or "monte_carlo") == "monte_carlo":
            args.output_subdir = "monte_carlo_smoke"
        args.n_draws = int(min(int(getattr(args, "n_draws", 200)), 200))
        args.horizon_days = int(min(int(getattr(args, "horizon_days", 90)), 90))
        args.seed = int(getattr(args, "seed", 12345) or 12345)
        # keep output small
        if str(getattr(args, "daily_output_mode", "auto")) == "auto":
            args.daily_output_mode = "single"
        args.draw_chunk = int(min(int(getattr(args, "draw_chunk", 25)), 25))

    run_dir = _find_run_dir(run_id=args.run_id, use_latest=bool(args.use_latest))
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise SystemExit(f"Missing scenario run manifest: {manifest_path}")

    manifest = _read_json(manifest_path)
    iso_inputs = manifest.get("iso_inputs") or {}
    if not isinstance(iso_inputs, dict) or not iso_inputs:
        raise SystemExit(f"Manifest has no iso_inputs: {manifest_path}")

    out_dir = _ensure_dir(run_dir / str(args.output_subdir))
    reps_dir = _ensure_dir(out_dir / "representatives")
    diag_dir = _ensure_dir(out_dir / "diagnostics")

    # Output manifest path is needed early for cache checks
    out_manifest_path = out_dir / "manifest.json"

    rng = np.random.default_rng(int(args.seed))

    # Optional metadata inputs for low-frequency classification
    series_freq = _load_series_frequency_map(series_metadata_path=_resolve_from_root(args.series_metadata_path))
    pca_loadings_dir = _resolve_from_root(args.pca_loadings_dir)
    factor_constituents_dir = _resolve_from_root(args.factor_constituents_dir)
    release_calendar_path = _resolve_from_root(args.release_calendar_path)
    release_calendar = _load_release_calendar(path=release_calendar_path)

    # Cache/signature gate (prevents expensive re-runs when nothing changed)
    sig_args = {
        "output_subdir": str(args.output_subdir),
        "n_draws": int(args.n_draws),
        "horizon_days": int(args.horizon_days),
        "seed": int(args.seed),
        "family": str(args.family),
        "t_df": float(args.t_df) if str(args.family) == "student_t" else None,
        "t0": str(args.t0) if args.t0 else None,
        "cross_iso_mode": str(args.cross_iso_mode),
        "corr_shrinkage": float(args.corr_shrinkage),
        "corr_eig_floor": float(args.corr_eig_floor),
        "lowfreq_classifier": str(args.lowfreq_classifier),
        "lowfreq_metadata_strict": bool(getattr(args, "lowfreq_metadata_strict", False)),
        "lowfreq_update_frac_threshold": float(args.lowfreq_update_frac_threshold),
        "lowfreq_dt_change_tol": float(args.lowfreq_dt_change_tol),
        "lowfreq_force": sorted(_parse_csv_list(args.lowfreq_force)),
        "lowfreq_never": sorted(_parse_csv_list(args.lowfreq_never)),
        "lowfreq_force_blocks": sorted(_parse_csv_list(args.lowfreq_force_blocks)),
        "lowfreq_never_blocks": sorted(_parse_csv_list(args.lowfreq_never_blocks)),
        "update_schedule_mode": str(args.update_schedule_mode),
        "release_calendar_path": str(release_calendar_path),
        "release_calendar_strict": bool(getattr(args, "release_calendar_strict", False)),
        "series_metadata_path": str(_resolve_from_root(args.series_metadata_path)),
        "pca_loadings_dir": str(pca_loadings_dir),
        "factor_constituents_dir": str(factor_constituents_dir),
        "metadata_min_high_share": float(args.lowfreq_metadata_min_high_share),
        "daily_output_mode": str(args.daily_output_mode),
        "daily_output_format": str(args.daily_output_format),
        "daily_csv_gzip": bool(args.daily_csv_gzip),
        "draw_chunk": int(args.draw_chunk),
        "smoke": bool(getattr(args, "smoke", False)),
    }

    blocks_needed: set[tuple[str, str]] = set()
    for iso, meta in (iso_inputs or {}).items():
        for f in (meta or {}).get("factors") or []:
            parsed = _parse_factor_block(str(f))
            if parsed is None:
                continue
            iso_f, block, _k = parsed
            blocks_needed.add((str(iso_f), str(block)))

    input_hashes: Dict[str, str] = {
        str(manifest_path): _file_sha256(manifest_path),
        str(_resolve_from_root(args.series_metadata_path)): _file_sha256(_resolve_from_root(args.series_metadata_path)),
        str(release_calendar_path): _file_sha256(release_calendar_path) if release_calendar_path.exists() else "",
        str(Path(__file__).resolve()): _file_sha256(Path(__file__).resolve()),
    }
    for iso_b, block_b in sorted(blocks_needed):
        p = pca_loadings_dir / f"{iso_b}_{block_b}_pca_loadings.csv"
        c = factor_constituents_dir / f"{iso_b}_{block_b}_factor_constituents.csv"
        if p.exists():
            input_hashes[str(p)] = _file_sha256(p)
        if c.exists():
            input_hashes[str(c)] = _file_sha256(c)

    signature_payload = {"args": sig_args, "input_hashes": input_hashes}
    signature = _stable_json_hash(signature_payload)

    if not bool(getattr(args, "force", False)) and not bool(getattr(args, "no_cache", False)):
        try:
            if out_manifest_path.exists():
                existing = _read_json(out_manifest_path)
                if str(existing.get("signature") or "") == str(signature):
                    daily_ok = (out_dir / "daily_draws.csv").exists() or (out_dir / "daily_draws").exists()
                    macro_ok = (out_dir / "macro_monthly_draws.csv").exists()
                    summary_ok = (out_dir / "summary.json").exists()
                    if daily_ok and macro_ok and summary_ok:
                        print(f"[SKIP] Outputs up-to-date (signature match): {out_dir}")
                        return 0
        except Exception:
            pass

    # Output files
    daily_path = out_dir / "daily_draws.csv"
    macro_path = out_dir / "macro_monthly_draws.csv"
    summary_path = out_dir / "summary.json"

    # (Re)create macro output
    if macro_path.exists():
        macro_path.unlink()

    # Prepare daily output (append mode)
    if daily_path.exists():
        daily_path.unlink()

    reps: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {
        "run_id": str(run_dir.name),
        "created_at": _utc_now_iso(),
        "n_draws": int(args.n_draws),
        "horizon_days": int(args.horizon_days),
        "family": str(args.family),
        "t_df": float(args.t_df) if str(args.family) == "student_t" else None,
        "isos": sorted(list(iso_inputs.keys())),
        "per_iso": {},
        "notes": [
            "This is an MVP Monte Carlo over daily factor innovations.",
            "Low-frequency macro is not simulated at daily frequency; macro_monthly_draws.csv is a placeholder.",
            "Correlation uses Rt(t0) and scaling uses Dt(t0).",
        ],
    }

    all_rep_scores: List[Tuple[float, str]] = []  # (score, joint_draw_id)

    # Precompute per-ISO pieces (factors, t0, vols, lowfreq classification, Rt blocks)
    iso_factors: Dict[str, List[str]] = {}
    iso_t0: Dict[str, pd.Timestamp] = {}
    iso_vols: Dict[str, Dict[str, float]] = {}
    iso_lowfreq: Dict[str, set[str]] = {}
    iso_update_fracs: Dict[str, Dict[str, float]] = {}
    iso_update_dates: Dict[str, Dict[str, set[str]]] = {}
    iso_update_schedule_diag: Dict[str, Dict[str, Dict[str, Any]]] = {}
    iso_rt_blocks: Dict[str, np.ndarray] = {}

    iso_lowfreq_classifier: Dict[str, str] = {}
    iso_lowfreq_metadata_diag: Dict[str, Dict[str, Dict[str, Any]]] = {}

    force_low = set(_parse_csv_list(args.lowfreq_force))
    never_low = set(_parse_csv_list(args.lowfreq_never))

    force_blocks = set(_parse_csv_list(args.lowfreq_force_blocks))
    never_blocks = set(_parse_csv_list(args.lowfreq_never_blocks))

    classifier_req = str(args.lowfreq_classifier).strip().lower()
    if classifier_req not in {"auto", "dt", "metadata"}:
        classifier_req = "auto"

    for iso in sorted(iso_inputs.keys()):
        meta = iso_inputs.get(iso) or {}
        factors = list(meta.get("factors") or [])
        if not factors:
            continue
        cov_dir = run_dir / "inputs" / str(iso) / "covariance"
        dt_csv = cov_dir / f"{iso}_Dt_daily.csv"
        rt_csv = cov_dir / f"{iso}_Rt_daily_pairs.csv"
        if not dt_csv.exists() or not rt_csv.exists():
            raise SystemExit(f"Missing covariance inputs for {iso}: {dt_csv} / {rt_csv}")

        t0_eff, vols = _read_dt_row(dt_csv, factors=factors, asof_t0=str(args.t0) if args.t0 else None)
        R_iso = _read_rt_row(rt_csv, factors=factors, asof_t0=t0_eff)

        low_dt, fracs = _infer_lowfreq_factors_from_dt(
            dt_csv,
            factors=factors,
            update_frac_threshold=float(args.lowfreq_update_frac_threshold),
            tol=float(args.lowfreq_dt_change_tol),
        )

        meta_low: set[str] = set()
        meta_diag: Dict[str, Dict[str, Any]] = {}
        can_use_metadata = bool(series_freq) and classifier_req in {"auto", "metadata"}
        if can_use_metadata:
            meta_low, meta_diag = _infer_lowfreq_factors_from_metadata(
                iso=str(iso),
                factors=[str(x) for x in factors],
                series_freq=series_freq,
                pca_loadings_dir=pca_loadings_dir,
                factor_constituents_dir=factor_constituents_dir,
                min_high_share=float(args.lowfreq_metadata_min_high_share),
            )

        if classifier_req == "metadata" and not can_use_metadata:
            raise SystemExit(
                "Requested --lowfreq-classifier metadata, but series metadata could not be loaded. "
                "Check --series-metadata-path and that pyyaml is installed."
            )

        # Choose low-frequency set (factor-by-factor) to allow fallback for unknown metadata
        low: set[str] = set()
        classifier_used = "dt"
        if classifier_req == "dt":
            low = set(low_dt)
            classifier_used = "dt"
        elif classifier_req == "metadata" and can_use_metadata:
            classifier_used = "metadata"
            unknown: List[Tuple[str, str]] = []
            for f in [str(x) for x in factors]:
                d = meta_diag.get(str(f)) or {}
                if "is_low" in d and d.get("is_low") is not None:
                    if bool(d.get("is_low")):
                        low.add(str(f))
                else:
                    unknown.append((str(f), str(d.get("reason") or "unknown")))

            if unknown and bool(getattr(args, "lowfreq_metadata_strict", False)):
                preview = ", ".join([f"{f}({r})" for f, r in unknown[:25]])
                more = "" if len(unknown) <= 25 else f" (+{len(unknown)-25} more)"
                raise SystemExit(
                    f"Metadata strict mode: {iso} has {len(unknown)} unclassifiable factors: {preview}{more}. "
                    "Provide series frequency labels and/or factor constituent maps to cover all factors."
                )

        elif classifier_req == "auto" and can_use_metadata:
            classifier_used = "auto(metadata+dt_fallback)"
            for f in [str(x) for x in factors]:
                d = meta_diag.get(str(f)) or {}
                if "is_low" in d and d.get("is_low") is not None:
                    if bool(d.get("is_low")):
                        low.add(str(f))
                else:
                    if str(f) in low_dt:
                        low.add(str(f))
        else:
            low = set(low_dt)
            classifier_used = "dt"

        iso_lowfreq_classifier[str(iso)] = classifier_used
        iso_lowfreq_metadata_diag[str(iso)] = {str(k): dict(v) for k, v in (meta_diag or {}).items()}

        # Apply allow/deny overrides
        if force_low:
            low |= {f for f in factors if f in force_low}
        if never_low:
            low -= {f for f in factors if f in never_low}

        if force_blocks or never_blocks:
            for f in [str(x) for x in factors]:
                parsed = _parse_factor_block(f)
                if parsed is None:
                    continue
                _iso_f, block, _k = parsed
                if block in force_blocks:
                    low.add(f)
                if block in never_blocks:
                    low.discard(f)

        # Historical update dates are not used for forward gating; we still keep
        # them for diagnostics and backward-compat.
        upd_dates = _infer_update_dates_from_dt(
            dt_csv,
            factors=factors,
            tol=float(args.lowfreq_dt_change_tol),
            asof_t0=str(args.t0) if args.t0 else None,
        )

        iso_factors[str(iso)] = [str(f) for f in factors]
        iso_t0[str(iso)] = t0_eff
        iso_vols[str(iso)] = {str(k): float(v) for k, v in vols.items()}
        iso_lowfreq[str(iso)] = set(low)
        iso_update_fracs[str(iso)] = {str(k): float(v) for k, v in fracs.items()}
        iso_update_dates[str(iso)] = {str(k): set(v) for k, v in upd_dates.items()}
        iso_rt_blocks[str(iso)] = R_iso

    if not iso_factors:
        raise SystemExit("No ISOs/factors found in manifest")

    # Shared forward daily date index: start from max(t0) to keep all ISOs aligned
    t0_global = max(iso_t0.values())
    dates = pd.bdate_range(start=t0_global + pd.Timedelta(days=1), periods=int(args.horizon_days))

    date_strs = [pd.to_datetime(d).strftime("%Y-%m-%d") for d in dates]

    # Build forward update schedules for low-frequency factors (per ISO).
    # Default: explicit release calendar rules (no Dt use).
    iso_forward_update_dates: Dict[str, Dict[str, set[str]]] = {}
    for iso in sorted(iso_factors.keys()):
        mode = str(getattr(args, "update_schedule_mode", "calendar") or "calendar").strip().lower()
        strict = bool(getattr(args, "release_calendar_strict", False)) or bool(getattr(args, "lowfreq_metadata_strict", False))

        if mode == "dt_projection":
            cov_dir = run_dir / "inputs" / str(iso) / "covariance"
            dt_csv = cov_dir / f"{iso}_Dt_daily.csv"
            if not dt_csv.exists():
                iso_forward_update_dates[str(iso)] = {}
                iso_update_schedule_diag[str(iso)] = {}
                continue
            fwd_map, f_diag = _infer_forward_update_dates_from_dt(
                dt_csv,
                factors=iso_factors[iso],
                forward_dates=list(pd.to_datetime(dates)),
                tol=float(args.lowfreq_dt_change_tol),
                asof_t0=str(args.t0) if args.t0 else None,
            )
            iso_forward_update_dates[str(iso)] = {str(k): set(v) for k, v in fwd_map.items()}
            iso_update_schedule_diag[str(iso)] = {str(k): dict(v) for k, v in f_diag.items()}
        else:
            fwd_map, f_diag = _build_forward_update_dates_from_release_calendar(
                iso=str(iso),
                factors=iso_factors[iso],
                forward_dates=list(pd.to_datetime(dates)),
                low_set=iso_lowfreq.get(iso) or set(),
                meta_diag=iso_lowfreq_metadata_diag.get(iso) or {},
                release_calendar=release_calendar,
                strict=bool(strict),
            )
            # If strict, treat any per-factor diag status=error as fatal.
            if strict:
                errs = [f for f, d in (f_diag or {}).items() if str((d or {}).get("status")) == "error"]
                if errs:
                    preview = ", ".join(errs[:25])
                    more = "" if len(errs) <= 25 else f" (+{len(errs)-25} more)"
                    raise SystemExit(
                        f"Release calendar strict mode: {iso} has {len(errs)} low-frequency factors with no schedule: {preview}{more}. "
                        "Update config/release_calendar.yaml and/or ensure metadata implies a frequency."
                    )
            iso_forward_update_dates[str(iso)] = {str(k): set(v) for k, v in (fwd_map or {}).items()}
            iso_update_schedule_diag[str(iso)] = {str(k): dict(v) for k, v in (f_diag or {}).items()}

    # Persist forward schedule diagnostics
    sched_rows: List[Dict[str, Any]] = []
    for iso in sorted(iso_factors.keys()):
        for f in iso_factors[iso]:
            d = (iso_update_schedule_diag.get(iso) or {}).get(f) or {}
            sched_rows.append({"iso": iso, "factor": f, **d})
    pd.DataFrame(sched_rows).to_csv(diag_dir / "lowfreq_forward_schedule.csv", index=False)

    # Build joint correlation across all (iso,factor) dims
    if str(args.cross_iso_mode).strip().lower() == "empirical":
        dims, R_joint = _build_joint_corr(
            run_dir=run_dir,
            iso_factors=iso_factors,
            iso_rt_blocks=iso_rt_blocks,
            corr_shrinkage=float(args.corr_shrinkage),
            corr_eig_floor=float(args.corr_eig_floor),
        )
    else:
        # Block-diagonal independence
        dims = [(iso, f) for iso in sorted(iso_factors.keys()) for f in iso_factors[iso]]
        R_joint = np.eye(len(dims), dtype=float)
        pos = 0
        for iso in sorted(iso_factors.keys()):
            k = len(iso_factors[iso])
            block = iso_rt_blocks.get(iso)
            if block is not None and block.shape == (k, k):
                R_joint[pos : pos + k, pos : pos + k] = block
            pos += k
        R_joint = _nearest_spd_corr(R_joint, eig_floor=float(args.corr_eig_floor))

    chol_joint = _chol_corr(R_joint, eig_floor=float(args.corr_eig_floor))

    # Diagonal vol scaling per dimension
    dim_vols: List[float] = []
    for iso, f in dims:
        dim_vols.append(float((iso_vols.get(iso) or {}).get(f, 1.0)))
    D_joint = np.diag(dim_vols).astype(float)

    # Diagnostics: input vs sample correlation
    n_check = int(min(30000, int(args.n_draws) * 100))
    u_check = _sample_correlated(
        rng,
        n=n_check,
        dim=len(dims),
        chol=chol_joint,
        family=str(args.family),
        df=float(args.t_df) if str(args.family) == "student_t" else None,
    )
    R_hat = np.corrcoef(u_check.T) if u_check.shape[0] > 5 else np.eye(len(dims))
    pd.DataFrame(R_joint).to_csv(diag_dir / "corr_joint_input.csv", index=False)
    pd.DataFrame(R_hat).to_csv(diag_dir / "corr_joint_sample.csv", index=False)
    pd.DataFrame({"iso": [i for i, _ in dims], "factor": [f for _, f in dims], "vol_t0": dim_vols}).to_csv(
        diag_dir / "dims.csv", index=False
    )

    # Record low-frequency classification
    lowfreq_rows: List[Dict[str, Any]] = []
    for iso in sorted(iso_factors.keys()):
        for f in iso_factors[iso]:
            meta = (iso_lowfreq_metadata_diag.get(iso) or {}).get(f) or {}
            lowfreq_rows.append(
                {
                    "iso": iso,
                    "factor": f,
                    "is_low_frequency": bool(f in (iso_lowfreq.get(iso) or set())),
                    "dt_update_frac": _safe_float((iso_update_fracs.get(iso) or {}).get(f)),
                    "classifier": str(iso_lowfreq_classifier.get(iso) or ""),
                    "metadata_source": str(meta.get("source") or ""),
                    "metadata_is_low": meta.get("is_low"),
                    "metadata_high_freq_share": _safe_float(meta.get("high_freq_share")),
                    "metadata_block": str(meta.get("block") or ""),
                }
            )
    pd.DataFrame(lowfreq_rows).to_csv(diag_dir / "lowfreq_classification.csv", index=False)

    meta_rows: List[Dict[str, Any]] = []
    for iso in sorted(iso_factors.keys()):
        d = iso_lowfreq_metadata_diag.get(iso) or {}
        for factor, payload in d.items():
            meta_rows.append({"iso": str(iso), "factor": str(factor), **(payload or {})})
    if meta_rows:
        pd.DataFrame(meta_rows).to_csv(diag_dir / "lowfreq_metadata_diag.csv", index=False)

    # Decide daily output mode/format
    est_rows = _estimate_rows(n_draws=int(args.n_draws), horizon_days=int(args.horizon_days), n_dims=len(dims))
    mode = str(args.daily_output_mode).strip().lower()
    if mode == "auto":
        mode = "sharded" if est_rows >= 5_000_000 else "single"

    fmt = str(args.daily_output_format).strip().lower()
    if fmt == "auto":
        fmt = "parquet" if _pyarrow_available() else "csv"
    if fmt == "parquet" and not _pyarrow_available():
        fmt = "csv"

    daily_shards_dir = out_dir / "daily_draws"
    if mode == "sharded":
        _ensure_dir(daily_shards_dir)
        _clear_daily_shards_dir(daily_shards_dir)
        if daily_path.exists():
            daily_path.unlink()
    else:
        # single
        if fmt == "parquet":
            _ensure_dir(daily_shards_dir)
            _clear_daily_shards_dir(daily_shards_dir)
        if daily_path.exists():
            daily_path.unlink()

    abs_vals: List[float] = []
    macro_header_written = False

    # Streaming diagnostics accumulators (per joint dim)
    n_obs_total = 0
    sum_u = np.zeros(len(dims), dtype=float)
    sumsq_u = np.zeros(len(dims), dtype=float)
    sum4_u = np.zeros(len(dims), dtype=float)

    sum_g = np.zeros(len(dims), dtype=float)
    sumsq_g = np.zeros(len(dims), dtype=float)
    sum4_g = np.zeros(len(dims), dtype=float)
    zero_g = np.zeros(len(dims), dtype=float)

    # Precompute dim positions per ISO for fast slicing
    iso_dim_pos: Dict[str, List[int]] = {}
    iso_dim_factors: Dict[str, List[str]] = {}
    for iso in sorted(iso_factors.keys()):
        pos = [i for i, (i_iso, _) in enumerate(dims) if i_iso == iso]
        iso_dim_pos[iso] = pos
        iso_dim_factors[iso] = [dims[i][1] for i in pos]

    chunk_size = int(args.draw_chunk)
    for draw_start in range(0, int(args.n_draws), chunk_size):
        draw_end = min(int(args.n_draws), draw_start + chunk_size)
        n_draw = draw_end - draw_start

        n_samples = int(n_draw) * int(args.horizon_days)
        u = _sample_correlated(
            rng,
            n=n_samples,
            dim=len(dims),
            chol=chol_joint,
            family=str(args.family),
            df=float(args.t_df) if str(args.family) == "student_t" else None,
        )
        shocks = u @ D_joint.T
        shocks = shocks.reshape((n_draw, int(args.horizon_days), len(dims)))

        flat_u = shocks.reshape((-1, len(dims))).astype(float, copy=False)
        n_obs_total += int(flat_u.shape[0])
        sum_u += np.sum(flat_u, axis=0)
        sumsq_u += np.sum(np.square(flat_u), axis=0)
        sum4_u += np.sum(np.square(np.square(flat_u)), axis=0)

        # Representative severity score per draw (max L2 over horizon across all dims)
        l2 = np.sqrt(np.sum(np.square(shocks), axis=2))
        score = np.max(l2, axis=1)
        for i in range(n_draw):
            all_rep_scores.append((float(score[i]), f"draw_{int(draw_start + i)}"))

        abs_vals.append(float(np.nanmax(np.abs(shocks))))

        # Stream-write per ISO (more efficient + enables sharding)
        draw_ids = np.arange(draw_start, draw_end, dtype=int)

        for iso in sorted(iso_factors.keys()):
            pos = iso_dim_pos.get(iso) or []
            if not pos:
                continue
            factors_iso = iso_dim_factors[iso]
            shocks_iso = shocks[:, :, pos]  # (n_draw, horizon, k)

            # Gate low-frequency factors to factor-specific update dates inferred from Dt_daily
            low_set = iso_lowfreq.get(iso) or set()
            # Use projected forward update schedule for gating
            upd_map = iso_forward_update_dates.get(iso) or {}
            if low_set:
                for j, f in enumerate(factors_iso):
                    if f not in low_set:
                        continue
                    upd_dates = upd_map.get(f) or set()
                    allowed = np.array([d in upd_dates for d in date_strs], dtype=bool)
                    if allowed.any():
                        shocks_iso[:, ~allowed, j] = 0.0
                    else:
                        shocks_iso[:, :, j] = 0.0

            flat_g = shocks_iso.reshape((-1, len(pos))).astype(float, copy=False)
            sum_g[pos] += np.sum(flat_g, axis=0)
            sumsq_g[pos] += np.sum(np.square(flat_g), axis=0)
            sum4_g[pos] += np.sum(np.square(np.square(flat_g)), axis=0)
            zero_g[pos] += np.sum(flat_g == 0.0, axis=0)

            # Write macro_monthly_draws rows for update dates only (append)
            if low_set:
                macro_parts: List[pd.DataFrame] = []
                for j, f in enumerate(factors_iso):
                    if f not in low_set:
                        continue
                    upd_dates = upd_map.get(f) or set()
                    if not upd_dates:
                        continue
                    upd_idx = [i for i, d in enumerate(date_strs) if d in upd_dates]
                    if not upd_idx:
                        continue
                    sub = shocks_iso[:, upd_idx, j]  # (n_draw, n_upd)
                    for k_idx, t_idx in enumerate(upd_idx):
                        update_date = date_strs[t_idx]
                        month = update_date[:7]
                        macro_parts.append(
                            pd.DataFrame(
                                {
                                    "run_id": str(run_dir.name),
                                    "draw_id": draw_ids,
                                    "iso": str(iso),
                                    "month": month,
                                    "update_date": update_date,
                                    "factor": str(f),
                                    "shock": sub[:, k_idx].astype(float),
                                    "shock_units": "innovation",
                                }
                            )
                        )
                if macro_parts:
                    macro_df = pd.concat(macro_parts, ignore_index=True)
                    macro_df.to_csv(macro_path, index=False, mode="a", header=not macro_header_written)
                    macro_header_written = True

            # Build daily long df and write
            df_iso = _build_iso_long_df(
                run_name=str(run_dir.name),
                iso=str(iso),
                draw_ids=draw_ids,
                dates=date_strs,
                factors=factors_iso,
                shocks=shocks_iso,
            )

            if mode == "single":
                if fmt == "parquet":
                    # single parquet: append via separate parts to avoid heavy concat
                    part = daily_shards_dir / f"part_{draw_start:06d}_{draw_end:06d}__{iso}.parquet"
                    _ensure_dir(daily_shards_dir)
                    _write_daily_chunk_parquet(out_path=part, df=df_iso)
                else:
                    _write_daily_chunk_csv(
                        out_path=daily_path,
                        df=df_iso,
                        header=not daily_path.exists(),
                        compress=bool(args.daily_csv_gzip),
                    )
            else:
                # sharded
                if fmt == "parquet":
                    iso_dir = _ensure_dir(daily_shards_dir / str(iso))
                    part = iso_dir / f"part_{draw_start:06d}_{draw_end:06d}.parquet"
                    _write_daily_chunk_parquet(out_path=part, df=df_iso)
                else:
                    iso_dir = _ensure_dir(daily_shards_dir / str(iso))
                    ext = ".csv.gz" if bool(args.daily_csv_gzip) else ".csv"
                    part = iso_dir / f"part_{draw_start:06d}_{draw_end:06d}{ext}"
                    # One part per chunk; write new file each time
                    if part.exists():
                        part.unlink()
                    if bool(args.daily_csv_gzip):
                        df_iso.to_csv(part, index=False, compression="gzip")
                    else:
                        df_iso.to_csv(part, index=False)

    # Ensure macro file exists even if no low-frequency factors are present
    if not macro_path.exists():
        pd.DataFrame(
            columns=["run_id", "draw_id", "iso", "month", "update_date", "factor", "shock", "shock_units"]
        ).to_csv(macro_path, index=False)

    # Summary per ISO
    for iso in sorted(iso_factors.keys()):
        summary["per_iso"][str(iso)] = {
            "asof_t0": str(iso_t0[iso].date()),
            "n_factors": int(len(iso_factors[iso])),
            "n_low_frequency": int(len(iso_lowfreq.get(iso) or set())),
        }

    # Representatives: top draw ids overall
    all_rep_scores.sort(key=lambda x: x[0], reverse=True)
    top = all_rep_scores[: min(10, len(all_rep_scores))]
    rep_payload = [
        {"rank": i + 1, "draw_id": str(draw_id), "severity_score": float(score)}
        for i, (score, draw_id) in enumerate(top)
    ]
    (reps_dir / "top_draws.json").write_text(json.dumps(rep_payload, indent=2), encoding="utf-8")

    # Minimal summary.json (MVP)
    _write_json(summary_path, summary)

    # Diagnostics: gating variance + basic innovation backtest checks
    if n_obs_total > 0:
        mu_u = sum_u / float(n_obs_total)
        m2_u = sumsq_u / float(n_obs_total)
        var_u = np.maximum(m2_u - np.square(mu_u), 0.0)
        std_u = np.sqrt(np.maximum(var_u, 0.0))

        mu_g = sum_g / float(n_obs_total)
        m2_g = sumsq_g / float(n_obs_total)
        var_g = np.maximum(m2_g - np.square(mu_g), 0.0)
        std_g = np.sqrt(np.maximum(var_g, 0.0))

        # Kurtosis (non-excess) using raw moments approximation
        m4_u = sum4_u / float(n_obs_total)
        m4_g = sum4_g / float(n_obs_total)
        kurt_u = np.where(var_u > 0, m4_u / np.square(var_u), np.nan)
        kurt_g = np.where(var_g > 0, m4_g / np.square(var_g), np.nan)

        gating_rows: List[Dict[str, Any]] = []
        for i, (iso, f) in enumerate(dims):
            is_low = bool(f in (iso_lowfreq.get(iso) or set()))
            upd = (iso_forward_update_dates.get(iso) or {}).get(f) or set()
            upd_days = int(len(upd)) if is_low else int(args.horizon_days)
            p_upd = float(upd_days) / float(max(1, int(args.horizon_days)))
            p0 = float(zero_g[i]) / float(n_obs_total)
            ratio = float(var_g[i] / var_u[i]) if float(var_u[i]) > 0 else np.nan

            # Basic pass/fail (very light-touch; primarily catches bugs)
            mean_tol = 0.05
            pass_mean = bool(np.isfinite(mu_u[i]) and np.isfinite(std_u[i]) and (abs(mu_u[i]) <= mean_tol * max(std_u[i], 1e-12)))

            gating_rows.append(
                {
                    "iso": str(iso),
                    "factor": str(f),
                    "is_low_frequency": bool(is_low),
                    "update_days_forward": int(upd_days),
                    "p_update": float(p_upd),
                    "mean_ungated": float(mu_u[i]),
                    "std_ungated": float(std_u[i]),
                    "var_ungated": float(var_u[i]),
                    "kurtosis_ungated": float(kurt_u[i]) if np.isfinite(kurt_u[i]) else None,
                    "mean_gated": float(mu_g[i]),
                    "std_gated": float(std_g[i]),
                    "var_gated": float(var_g[i]),
                    "kurtosis_gated": float(kurt_g[i]) if np.isfinite(kurt_g[i]) else None,
                    "var_ratio_gated_to_ungated": float(ratio) if np.isfinite(ratio) else None,
                    "p_zero_gated": float(p0),
                    "basic_backtest_pass": bool(pass_mean),
                }
            )
        pd.DataFrame(gating_rows).to_csv(diag_dir / "lowfreq_gating_variance.csv", index=False)

    # Diagnostics: update-calendar validation vs metadata implied frequency
    freq_to_period_months = {"monthly": 1, "quarterly": 3, "annual": 12, "yearly": 12}
    n_months_fwd = int(pd.to_datetime(pd.Index(dates)).to_period("M").nunique())
    cal_rows: List[Dict[str, Any]] = []
    for iso in sorted(iso_factors.keys()):
        for f in iso_factors[iso]:
            meta = (iso_lowfreq_metadata_diag.get(iso) or {}).get(f) or {}
            implied = str(meta.get("implied_frequency") or "").strip().lower()
            expected = freq_to_period_months.get(implied)
            sched = (iso_update_schedule_diag.get(iso) or {}).get(f) or {}
            inferred_period = _safe_float(sched.get("period_months"))
            bday = _safe_float(sched.get("bday_of_month"))
            upd = (iso_forward_update_dates.get(iso) or {}).get(f) or set()
            n_upd = int(len(upd))

            expected_updates = None
            if expected and expected > 0:
                expected_updates = int(math.ceil(float(n_months_fwd) / float(expected)))

            pass_period = None
            if expected and inferred_period is not None:
                pass_period = bool(int(round(float(inferred_period))) in {int(expected), int(expected) - 1, int(expected) + 1})

            pass_count = None
            if expected_updates is not None:
                pass_count = bool(abs(int(n_upd) - int(expected_updates)) <= 1)

            cal_rows.append(
                {
                    "iso": str(iso),
                    "factor": str(f),
                    "metadata_implied_frequency": implied,
                    "expected_period_months": expected,
                    "inferred_period_months": inferred_period,
                    "inferred_bday_of_month": bday,
                    "n_updates_forward": n_upd,
                    "expected_updates_forward": expected_updates,
                    "pass_period": pass_period,
                    "pass_count": pass_count,
                }
            )
    pd.DataFrame(cal_rows).to_csv(diag_dir / "lowfreq_update_calendar_validation.csv", index=False)

    # Audit manifest
    out_manifest = {
        "created_at": _utc_now_iso(),
        "signature": str(signature),
        "signature_payload": signature_payload,
        "inputs_manifest": str(manifest_path),
        "seed": int(args.seed),
        "family": str(args.family),
        "t_df": float(args.t_df) if str(args.family) == "student_t" else None,
        "horizon_days": int(args.horizon_days),
        "n_draws": int(args.n_draws),
        "corr_shrinkage": float(args.corr_shrinkage),
        "corr_eig_floor": float(args.corr_eig_floor),
        "t0_override": str(args.t0) if args.t0 else None,
        "lowfreq": {
            "classifier_requested": str(args.lowfreq_classifier),
            "classifier_used_by_iso": {str(k): str(v) for k, v in (iso_lowfreq_classifier or {}).items()},
            "metadata_strict": bool(getattr(args, "lowfreq_metadata_strict", False)),
            "update_frac_threshold": float(args.lowfreq_update_frac_threshold),
            "dt_change_tol": float(args.lowfreq_dt_change_tol),
            "force_factors": sorted(list(force_low)),
            "never_factors": sorted(list(never_low)),
            "force_blocks": sorted(list(force_blocks)),
            "never_blocks": sorted(list(never_blocks)),
            "series_metadata_path": str(_resolve_from_root(args.series_metadata_path)),
            "pca_loadings_dir": str(pca_loadings_dir),
            "factor_constituents_dir": str(factor_constituents_dir),
            "metadata_min_high_share": float(args.lowfreq_metadata_min_high_share),
            "high_freq_labels": sorted(list(_HIGH_FREQ_LABELS)),
        },
        "outputs": {
            "daily_draws": str(daily_path) if mode == "single" and fmt == "csv" else str(daily_shards_dir),
            "daily_draws_mode": mode,
            "daily_draws_format": fmt,
            "macro_monthly_draws": str(macro_path),
            "summary": str(summary_path),
            "representatives": str(reps_dir),
            "diagnostics": str(diag_dir),
        },
        "notes": [
            "Daily draws use static correlation over the horizon.",
            "Within-ISO correlation uses Rt(t0); cross-ISO correlation uses empirical standardized-residual correlation when enabled.",
            "Low-frequency macro factors are gated to monthly update days (non-update days are zero shocks).",
        ],
    }
    _write_json(out_manifest_path, out_manifest)

    if mode == "single" and fmt == "csv":
        print(f"[OK] Wrote: {daily_path}")
    else:
        print(f"[OK] Wrote: {daily_shards_dir}")
    print(f"[OK] Wrote: {macro_path}")
    print(f"[OK] Wrote: {summary_path}")
    print(f"[OK] Wrote: {out_manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
