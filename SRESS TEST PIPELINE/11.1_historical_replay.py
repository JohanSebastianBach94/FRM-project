"""11.1 - Historical replay scenario generator.

Builds literature-style historical replay shocks in factor/block space using
standardized residuals from the fitted DCC-GARCH blocks.

Outputs a run folder under analysis_outputs/historical_replay/ with per-episode,
per-block replayed innovations plus summary diagnostics.

Usage:
  python "SRESS TEST PIPELINE/11.1_historical_replay.py"
  python "SRESS TEST PIPELINE/11.1_historical_replay.py" --episodes config/historical_episodes.yaml --block-ids usa_real_estate,esp_financial_markets

Design choices (deliberate):
- Replay in standardized-residual space (post-GARCH) to avoid mixed-frequency
  artifacts (flat-then-jump) from forward-filled monthly series.
- Optionally rotate episode shocks into the current correlation regime.
- Optionally re-volatilize using current conditional volatilities.

This script is intentionally self-contained so it can run as a pipeline step.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None


ROOT = Path(__file__).resolve().parents[1]
DCC_RESULTS = ROOT / "DCC GARCH MODEL" / "results" / "blocks"
DEFAULT_EPISODES = ROOT / "config" / "historical_episodes.yaml"
SCENARIOS_DIR = ROOT / "analysis_outputs" / "scenarios"
OUT_BASE = ROOT / "analysis_outputs" / "historical_replay"
CATALOG_PATH = ROOT / "catalog.csv"
THRESHOLD_CONFIG_PATH = ROOT / "analysis_outputs" / "coverage_threshold_config.json"
FROZEN_BLOCK_DEF_PATH = ROOT / "outputs" / "country_block_definition.json"

_BLOCK_ID_RE = re.compile(r"[^a-z0-9]+")


def _slugify_block_id(iso_code: str, block_key: str | None) -> str:
    cleaned = f"{iso_code}_{block_key or 'block'}".lower()
    cleaned = _BLOCK_ID_RE.sub("_", cleaned)
    return cleaned.strip("_")


def _load_expected_block_ids() -> set[str] | None:
    """Load canonical block ids from the frozen block definition, if present.

    This is the authoritative taxonomy for which blocks *should* exist.
    """
    path = ROOT / "outputs" / "country_block_definition.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict) or not payload:
        return None

    expected: set[str] = set()
    for iso, entry in payload.items():
        if not iso:
            continue
        blocks = (entry or {}).get("blocks") if isinstance(entry, dict) else None
        if not isinstance(blocks, list):
            continue
        for b in blocks:
            if not isinstance(b, dict):
                continue
            key = str(b.get("key") or "block").strip()
            expected.add(_slugify_block_id(str(iso), key))
    return expected or None


def _is_iso_prefixed_block_id(block_id: str) -> bool:
    return bool(re.match(r"^[a-z]{3}_.+", str(block_id)))


def _safe_float(x: object) -> float | None:
    try:
        v = float(x)
        return v if np.isfinite(v) else None
    except Exception:
        return None


def _safe_str(x: object) -> str:
    try:
        return "" if x is None else str(x)
    except Exception:
        return ""


def _parse_boolish(x: object) -> bool:
    if x is None:
        return False
    if isinstance(x, bool):
        return bool(x)
    s = _safe_str(x).strip().lower()
    if not s:
        return False
    return s in {"1", "true", "t", "yes", "y", "do not use", "dont use", "do_not_use", "exclude"}


def _load_series_threshold(path: Path) -> float:
    if not path.exists():
        return 0.62
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0.62
    try:
        v = float(payload.get("series_threshold", 0.62))
        return v if (v > 0 and v <= 1.0) else 0.62
    except Exception:
        return 0.62


def _load_expected_series_codes(path: Path) -> set[str] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict) or not payload:
        return None
    out: set[str] = set()
    for _, entry in payload.items():
        if not isinstance(entry, dict):
            continue
        blocks = entry.get("blocks")
        if not isinstance(blocks, list):
            continue
        for b in blocks:
            if not isinstance(b, dict):
                continue
            for s in (b.get("series_codes") or []):
                ss = _safe_str(s).strip()
                if ss:
                    out.add(ss)
    return out or None


def _load_catalog_index(path: Path) -> dict[str, dict[str, object]]:
    """Return map series -> selected metadata from catalog.csv.

    Keys used here:
    - coverage_ratio
    - do_not_use
    - frequency_label
    - median_gap_days
    """
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if df.empty or "series" not in df.columns:
        return {}
    cov_col = "coverage_ratio" if "coverage_ratio" in df.columns else None
    dnu_col = "do_not_use" if "do_not_use" in df.columns else None
    freq_col = "frequency_label" if "frequency_label" in df.columns else None
    gap_col = "median_gap_days" if "median_gap_days" in df.columns else None

    out: dict[str, dict[str, object]] = {}
    for _, row in df.iterrows():
        series = _safe_str(row.get("series")).strip()
        if not series:
            continue
        cov = row.get(cov_col) if cov_col else None
        dnu = row.get(dnu_col) if dnu_col else None
        freq = row.get(freq_col) if freq_col else None
        gap = row.get(gap_col) if gap_col else None
        out[series] = {
            "coverage_ratio": cov,
            "do_not_use": dnu,
            "frequency_label": freq,
            "median_gap_days": gap,
        }
    return out


def _safe_lower(x: object) -> str:
    return _safe_str(x).strip().lower()


def _parse_str_list(x: object) -> list[str]:
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return [str(v).strip() for v in x if str(v).strip()]
    s = _safe_str(x).strip()
    if not s:
        return []
    # Accept either comma-separated strings or single token.
    parts = [p.strip() for p in s.split(",")]
    return [p for p in parts if p]


def _load_dt_daily_for_iso(inputs_root: Path, iso: str) -> pd.DataFrame | None:
    """Load Dt_daily (transformed daily panel) for an ISO, if available."""
    path = inputs_root / iso / "covariance" / f"{iso}_Dt_daily.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["date"])
    except Exception:
        return None
    if df.empty or "date" not in df.columns:
        return None
    df = df.set_index("date")
    # Ensure deterministic column naming.
    df.columns = [str(c) for c in df.columns]
    return df


def _apply_release_date_mask_to_low_frequency_z(
    z_ep: pd.DataFrame,
    *,
    block_id: str,
    catalog_index: dict[str, dict[str, object]],
    inputs_root: Path,
    dt_cache: dict[str, pd.DataFrame | None],
    low_freq_labels: set[str],
    change_tol: float,
    step_like_override: bool,
    update_frac_threshold: float,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Zero out low-frequency shocks on non-update dates.

    Update dates are inferred from the scenario input Dt_daily panel by
    detecting changes in the transformed series (i.e., value changes after
    forward-fill). This approximates "release-date" behavior.
    """
    if z_ep.empty:
        return z_ep, {"masked_series": []}

    iso = _safe_str(block_id).split("_", 1)[0].upper()
    if not iso or len(iso) != 3:
        return z_ep, {"masked_series": []}

    if iso not in dt_cache:
        dt_cache[iso] = _load_dt_daily_for_iso(inputs_root, iso)
    dt = dt_cache.get(iso)
    if dt is None or dt.empty:
        return z_ep, {"masked_series": [], "iso": iso, "dt_loaded": False}

    out = z_ep.copy()
    masked: list[str] = []
    scaled: dict[str, float] = {}

    # Align Dt_daily to the episode window.
    dt_ep = dt.reindex(out.index)

    tol = float(change_tol) if np.isfinite(change_tol) and change_tol >= 0 else 0.0
    override_on = bool(step_like_override)
    try:
        update_frac_thr = float(update_frac_threshold)
    except Exception:
        update_frac_thr = 0.2
    if not np.isfinite(update_frac_thr) or update_frac_thr <= 0:
        update_frac_thr = 0.2
    update_frac_thr = min(max(update_frac_thr, 0.001), 0.9)

    # Release/update inference:
    # - Prefer Dt_daily change-detection when it is informative.
    # - But some step-like series can have transformed Dt_daily values that vary
    #   day-to-day even though the effective shock is piecewise-constant. For
    #   these, infer update dates from changes in the z-shock itself.
    z_change_tol = 0.0

    for col in out.columns:
        meta = catalog_index.get(str(col), {})
        freq = _safe_lower(meta.get("frequency_label"))
        freq_is_low = freq in low_freq_labels

        # Candidate update indicators from Dt_daily and from z-shock changes.
        dt_update: pd.Series | None = None
        if col in dt_ep.columns:
            sdt = pd.to_numeric(dt_ep[col], errors="coerce")
            if not sdt.isna().all():
                changed = (sdt - sdt.shift(1)).abs()
                if tol > 0:
                    dt_update = changed > tol
                else:
                    dt_update = changed.ne(0)

                # If the series updates on very few days, treat it as effectively
                # low-frequency even if catalog frequency_label says 'daily'.
                if (not freq_is_low) and override_on:
                    try:
                        upd_frac = float(dt_update.fillna(False).mean())
                    except Exception:
                        upd_frac = 1.0
                    if np.isfinite(upd_frac) and upd_frac <= update_frac_thr:
                        freq_is_low = True

        if not freq_is_low:
            continue

        if not freq_is_low:
            continue

        sz = pd.to_numeric(out[col], errors="coerce")
        if sz.isna().all():
            continue

        z_changed = (sz - sz.shift(1)).abs()
        if z_change_tol > 0:
            z_update = z_changed > z_change_tol
        else:
            z_update = z_changed.ne(0)

        # Pick the sparser indicator (release-like) when both are available.
        is_update = z_update
        if dt_update is not None:
            try:
                frac_dt = float(dt_update.fillna(False).mean())
            except Exception:
                frac_dt = 1.0
            try:
                frac_z = float(z_update.fillna(False).mean())
            except Exception:
                frac_z = 1.0
            if np.isfinite(frac_dt) and np.isfinite(frac_z) and frac_dt < frac_z:
                is_update = dt_update

        is_update = is_update.fillna(False)
        # Keep first in-window date as update to avoid fully wiping a series.
        try:
            if len(is_update):
                is_update.iloc[0] = True
        except Exception:
            pass

        out[col] = out[col].where(is_update, 0.0)
        masked.append(str(col))

        # Scaling: daily-standardized residuals for low-frequency series can be
        # inflated by ~sqrt(gap_days). Downscale update-day z by sqrt(median_gap_days).
        gap = meta.get("median_gap_days") if isinstance(meta, dict) else None
        gap_f = _safe_float(gap)
        if gap_f is None or (not np.isfinite(gap_f)) or gap_f <= 1:
            gap_f = None
        scale = float(np.sqrt(gap_f)) if gap_f is not None else None
        if scale is not None and np.isfinite(scale) and scale > 1:
            out.loc[is_update, col] = out.loc[is_update, col] / scale
            scaled[str(col)] = float(scale)

    return out, {
        "masked_series": masked,
        "scaled_series": scaled,
        "iso": iso,
        "dt_loaded": True,
    }


def _flat_spike_metrics(
    s: pd.Series,
    *,
    abs_eps: float,
    p95_abs_threshold: float,
    near_zero_frac_threshold: float,
    spike_threshold: float,
) -> dict[str, Any]:
    """Heuristic detection of 'mostly flat then spikes' in standardized shocks.

    Intended to catch cases where a series is effectively constant/forward-filled
    for most of the window and then jumps, which can create huge standardized
    residuals unrelated to market dynamics.
    """
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return {
            "n": 0,
            "flat_spike_flag": False,
        }

    abs_x = x.abs()
    max_abs = float(abs_x.max())
    p95_abs = float(abs_x.quantile(0.95))
    p50_abs = float(abs_x.quantile(0.50))
    eps = float(abs_eps) if np.isfinite(abs_eps) and abs_eps > 0 else 1e-6
    near0_frac = float((abs_x <= eps).mean())

    flat_by_p95 = p95_abs <= float(p95_abs_threshold)
    flat_by_near0 = near0_frac >= float(near_zero_frac_threshold)
    spiky = max_abs >= float(spike_threshold)
    flag = bool(spiky and (flat_by_p95 or flat_by_near0))

    # Ratio can be informative but unstable if p50_abs is tiny.
    ratio = None
    if p50_abs and np.isfinite(p50_abs) and p50_abs > 0:
        ratio = float(max_abs / p50_abs)

    return {
        "n": int(len(x)),
        "max_abs": max_abs,
        "p95_abs": p95_abs,
        "p50_abs": p50_abs,
        "near_zero_frac": near0_frac,
        "max_over_p50": ratio,
        "flat_spike_flag": flag,
        "rule": {
            "spike_threshold": float(spike_threshold),
            "p95_abs_threshold": float(p95_abs_threshold),
            "near_zero_frac_threshold": float(near_zero_frac_threshold),
            "abs_epsilon": eps,
        },
    }


@dataclass(frozen=True)
class Episode:
    episode_id: str
    name: str
    start: pd.Timestamp
    end: pd.Timestamp
    tags: list[str]


def _utc_timestamp() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _load_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to run historical replay.")
    if not path.exists():
        raise FileNotFoundError(f"Missing episodes config: {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    if not isinstance(payload, dict):
        raise TypeError(f"Unexpected YAML schema in {path}")
    return payload


def _parse_episodes(payload: dict[str, Any]) -> tuple[list[Episode], dict[str, Any]]:
    raw = payload.get("episodes") or []
    if not isinstance(raw, list) or not raw:
        raise ValueError("episodes YAML must contain a non-empty 'episodes' list")

    episodes: list[Episode] = []
    for e in raw:
        if not isinstance(e, dict):
            continue
        episode_id = str(e.get("id") or "").strip()
        name = str(e.get("name") or episode_id).strip()
        start = pd.Timestamp(str(e.get("start")))
        end = pd.Timestamp(str(e.get("end")))
        tags = [str(x) for x in (e.get("tags") or [])]
        if not episode_id or pd.isna(start) or pd.isna(end) or start > end:
            raise ValueError(f"Invalid episode entry: {e}")
        episodes.append(Episode(episode_id=episode_id, name=name, start=start, end=end, tags=tags))

    settings = payload.get("settings") or {}
    if not isinstance(settings, dict):
        settings = {}
    return episodes, settings


def _nearest_spd_corr(R: np.ndarray, eig_floor: float = 1e-3) -> np.ndarray:
    R = np.asarray(R, dtype=float)
    if R.ndim != 2 or R.shape[0] != R.shape[1]:
        raise ValueError(f"Correlation matrix must be square, got shape={R.shape}")

    # Corr matrices can contain NaNs when some columns are constant in-window.
    R = np.nan_to_num(R, nan=0.0, posinf=0.0, neginf=0.0)
    R = (R + R.T) / 2.0
    np.fill_diagonal(R, 1.0)

    try:
        w, V = np.linalg.eigh(R)
    except np.linalg.LinAlgError:
        # Last-resort: identity (no cross-series correlation structure).
        return np.eye(R.shape[0], dtype=float)

    floor = float(eig_floor)
    if not np.isfinite(floor) or floor <= 0:
        floor = 1e-3
    w = np.maximum(w, floor)
    R_spd = (V * w) @ V.T

    d = np.sqrt(np.clip(np.diag(R_spd), floor, None))
    Dinv = np.diag(1.0 / d)
    R_corr = Dinv @ R_spd @ Dinv
    R_corr = np.nan_to_num(R_corr, nan=0.0, posinf=0.0, neginf=0.0)
    R_corr = (R_corr + R_corr.T) / 2.0
    np.fill_diagonal(R_corr, 1.0)
    return R_corr


def _chol_corr(R: np.ndarray) -> np.ndarray:
    try:
        return np.linalg.cholesky(R)
    except np.linalg.LinAlgError:
        return np.linalg.cholesky(_nearest_spd_corr(R))


def _read_csv_time_indexed(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[~df.index.isna()].sort_index()
    return df


def _load_block_artifacts(block_id: str) -> dict[str, Any]:
    base = DCC_RESULTS / block_id
    if not base.exists():
        raise FileNotFoundError(f"Missing block results folder: {base}")

    resids = _read_csv_time_indexed(base / "standardized_residuals.csv")
    vols = _read_csv_time_indexed(base / "conditional_volatilities.csv")

    # Unconditional correlation is a robust default when dynamic corr is harder to reconstruct.
    unc_path = base / "unconditional_correlation_matrix.csv"
    if not unc_path.exists():
        raise FileNotFoundError(f"Missing unconditional correlation matrix: {unc_path}")
    unc = pd.read_csv(unc_path, index_col=0)
    unc.index = unc.index.astype(str)
    unc.columns = unc.columns.astype(str)

    # Align column ordering.
    cols = [c for c in resids.columns if c in vols.columns]
    if len(cols) < 2:
        raise ValueError(f"Block {block_id} has <2 aligned series in residuals/vols")

    resids = resids[cols]
    vols = vols[cols]
    unc = unc.loc[cols, cols].to_numpy(dtype=float)
    unc = _nearest_spd_corr(unc)

    return {
        "block_id": block_id,
        "columns": cols,
        "resids": resids,
        "vols": vols,
        "uncorr": unc,
        "start": resids.index.min(),
        "end": resids.index.max(),
    }


def _filter_block_columns(
    cols: list[str],
    *,
    catalog_index: dict[str, dict[str, object]],
    expected_series: set[str] | None,
    min_coverage: float | None,
    respect_do_not_use: bool,
    drop_unknown: bool,
    drop_low_coverage_even_if_expected: bool,
) -> tuple[list[str], list[dict[str, object]]]:
    kept: list[str] = []
    dropped: list[dict[str, object]] = []
    for c in cols:
        meta = catalog_index.get(c)
        cov = _safe_float(meta.get("coverage_ratio")) if isinstance(meta, dict) else None
        dnu_raw = meta.get("do_not_use") if isinstance(meta, dict) else None
        dnu = _parse_boolish(dnu_raw)

        if respect_do_not_use and dnu:
            dropped.append({"series": c, "reason": "catalog_do_not_use", "coverage_ratio": cov, "do_not_use": _safe_str(dnu_raw)})
            continue

        # If the series is explicitly in the frozen block definition, keep it
        # unless strict low-coverage dropping is enabled.
        if expected_series is not None and c in expected_series and (not drop_low_coverage_even_if_expected):
            kept.append(c)
            continue

        if min_coverage is not None and cov is not None and np.isfinite(cov) and float(cov) < float(min_coverage):
            dropped.append({"series": c, "reason": "low_catalog_coverage", "coverage_ratio": cov, "do_not_use": _safe_str(dnu_raw)})
            continue

        if drop_unknown and meta is None:
            dropped.append({"series": c, "reason": "unknown_not_in_catalog", "coverage_ratio": None, "do_not_use": None})
            continue

        kept.append(c)

    return kept, dropped


def _slice_episode(
    df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
    *,
    min_col_coverage: float,
    min_series: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Episode slicing with sparsity-aware column pruning.

    Rationale: complete-case dropping across all series can produce 0 overlap
    for otherwise usable blocks when a single series is sparse.
    """
    sub = df.loc[(df.index >= start) & (df.index <= end)].copy()
    if sub.empty:
        return sub, {"kept_columns": [], "dropped_columns": list(df.columns)}

    cov = sub.notna().mean()
    try:
        thr = float(min_col_coverage)
    except Exception:
        thr = 0.90
    thr = min(max(thr, 0.0), 1.0)

    keep = [c for c in sub.columns if float(cov.get(c, 0.0)) >= thr]
    dropped = [c for c in sub.columns if c not in keep]

    sub = sub[keep]
    sub = sub.dropna(how="any")

    meta = {"kept_columns": keep, "dropped_columns": dropped, "min_col_coverage": thr}
    if sub.shape[1] < int(min_series):
        return sub.iloc[0:0], meta
    return sub, meta


def _build_replay_index(anchor_end: pd.Timestamp, length: int) -> pd.DatetimeIndex:
    if pd.isna(anchor_end):
        raise ValueError("anchor_end is NaT")
    # Use business-day index for comparability with trading-day conditional vols.
    idx = pd.bdate_range(end=anchor_end, periods=int(length))
    return idx


def _rotate_and_revolatilize(
    z_episode: pd.DataFrame,
    vols_current: pd.DataFrame,
    R_current: np.ndarray,
    rotate: bool,
    revol: bool,
    *,
    shrinkage_lambda: float,
    eig_floor: float,
    winsor_abs_quantile: float,
) -> pd.DataFrame:
    z = z_episode.to_numpy(dtype=float)
    if rotate:
        try:
            # Use a robust correlation estimate and sanitize NaNs/Inf.
            R_ep = pd.DataFrame(z, columns=z_episode.columns).corr().to_numpy(dtype=float)
            lam = float(shrinkage_lambda)
            if not np.isfinite(lam):
                lam = 0.0
            lam = min(max(lam, 0.0), 0.5)
            if lam > 0:
                R_ep = (1.0 - lam) * R_ep + lam * np.eye(R_ep.shape[0], dtype=float)

            R_ep = _nearest_spd_corr(R_ep, eig_floor=eig_floor)
            L_ep = _chol_corr(R_ep)
            L_cur = _chol_corr(_nearest_spd_corr(R_current, eig_floor=eig_floor))

            # Whiten: u = L_ep^{-1} z
            u = np.linalg.solve(L_ep, z.T).T

            # Optional winsorization in whitened space to prevent rare numerical spikes
            q = float(winsor_abs_quantile)
            if np.isfinite(q) and 0.5 < q < 1.0:
                caps = np.quantile(np.abs(u), q, axis=0)
                caps = np.where(np.isfinite(caps) & (caps > 0), caps, np.inf)
                u = np.clip(u, -caps, caps)

            # Re-correlate: z_tilde = L_cur u
            z = (L_cur @ u.T).T
        except Exception:
            # If rotation fails for numerical reasons (e.g., constant columns),
            # fall back to no-rotation rather than failing the whole run.
            pass

    if revol:
        V = vols_current.to_numpy(dtype=float)
        z = z * V

    return pd.DataFrame(z, index=vols_current.index, columns=vols_current.columns)


def _apply_rotation_only(
    z_episode: pd.DataFrame,
    *,
    R_current: np.ndarray,
    rotate: bool,
    shrinkage_lambda: float,
    eig_floor: float,
    winsor_abs_quantile: float,
) -> pd.DataFrame:
    """Return rotated standardized shocks (z-space) without re-volatilization."""
    dummy_vols = pd.DataFrame(1.0, index=z_episode.index, columns=z_episode.columns)
    z_rot = _rotate_and_revolatilize(
        z_episode=z_episode,
        vols_current=dummy_vols,
        R_current=R_current,
        rotate=rotate,
        revol=False,
        shrinkage_lambda=shrinkage_lambda,
        eig_floor=eig_floor,
        winsor_abs_quantile=winsor_abs_quantile,
    )
    return z_rot


def main() -> int:
    parser = argparse.ArgumentParser(description="11.1 Historical replay scenario generator")
    parser.add_argument("--episodes", default=str(DEFAULT_EPISODES), help="Path to YAML episode catalog")
    parser.add_argument(
        "--scenario-run-id",
        default=None,
        help=(
            "If provided, write outputs under analysis_outputs/scenarios/<run_id>/historical_replay/ "
            "instead of the standalone analysis_outputs/historical_replay/."
        ),
    )
    parser.add_argument(
        "--use-latest-scenario-run",
        action="store_true",
        help="Write outputs under analysis_outputs/scenarios/latest/historical_replay/.",
    )
    parser.add_argument(
        "--block-ids",
        default="",
        help="Optional comma-separated block ids to replay (e.g. 'usa_real_estate,esp_financial_markets').",
    )
    parser.add_argument(
        "--min-series-coverage",
        type=float,
        default=None,
        help=(
            "Optional catalog coverage_ratio floor applied when selecting series within blocks. "
            "If omitted, uses analysis_outputs/coverage_threshold_config.json series_threshold (default 0.62)."
        ),
    )
    parser.add_argument(
        "--respect-catalog-do-not-use",
        action="store_true",
        default=True,
        help="Drop series where catalog.csv do_not_use is truthy (default: on).",
    )
    parser.add_argument(
        "--ignore-catalog-do-not-use",
        action="store_true",
        help="Do not drop catalog.csv do_not_use series (overrides --respect-catalog-do-not-use).",
    )
    parser.add_argument(
        "--drop-unknown-series",
        action="store_true",
        help="Drop series not present in catalog.csv (default: keep).",
    )
    parser.add_argument(
        "--drop-low-coverage-even-if-expected",
        action="store_true",
        help=(
            "Also drop low-coverage series even if they are present in outputs/country_block_definition.json. "
            "Default keeps expected series to avoid dropping required governance drivers."
        ),
    )
    args = parser.parse_args()

    cfg_path = Path(args.episodes)
    payload = _load_yaml(cfg_path)
    episodes, settings = _parse_episodes(payload)

    min_obs = int(settings.get("min_obs", 60))
    min_col_coverage = float(settings.get("min_col_coverage", 0.90))
    min_series = int(settings.get("min_series", 2))
    anchor = str(settings.get("anchor", "end_of_sample")).strip().lower()
    rotate = bool(settings.get("rotate_to_current_correlation", True))
    revol = bool(settings.get("re_volatilize_with_current_vol", True))
    shrinkage_lambda = float(settings.get("corr_shrinkage_lambda", 0.10))
    eig_floor = float(settings.get("corr_eig_floor", 1e-3))
    winsor_abs_quantile = float(settings.get("winsor_abs_quantile", 0.99))

    # Low-frequency handling: approximate release-date shocks in replay z-space.
    # Default is ON for monthly/quarterly/annual series.
    release_date_low_freq = bool(settings.get("release_date_shocks_low_frequency", True))
    low_freq_labels = {
        _safe_lower(x)
        for x in (
            _parse_str_list(settings.get("release_date_shocks_frequency_labels"))
            or ["monthly", "quarterly", "annual"]
        )
        if _safe_lower(x)
    }
    release_change_tol = float(settings.get("release_date_change_tol", 0.0))

    # Optional: also treat step-like series (rarely changing levels) as effectively
    # low-frequency for replay masking (e.g., policy rates).
    release_step_like_override = bool(settings.get("release_date_shocks_step_like_override", True))
    release_update_frac_thr = float(settings.get("release_date_shocks_update_frac_threshold", 0.2))

    # Diagnostics controls
    diag_z_threshold = float(settings.get("diagnostics_z_threshold", 8.0))
    flat_p95_thr = float(settings.get("flat_spike_p95_abs_threshold", 0.5))
    flat_near0_thr = float(settings.get("flat_spike_near_zero_frac_threshold", 0.8))
    flat_eps = float(settings.get("flat_spike_abs_epsilon", 1e-6))

    wanted: set[str] | None = None
    if str(args.block_ids).strip():
        wanted = {x.strip() for x in str(args.block_ids).split(",") if x.strip()}

    blocks = [p.name for p in DCC_RESULTS.iterdir() if p.is_dir()]
    if wanted is not None:
        blocks = [b for b in blocks if b in wanted]
    else:
        expected = _load_expected_block_ids()
        before = list(blocks)
        # Default behavior: ignore non-ISO-prefixed folders (typically legacy/global).
        blocks = [b for b in blocks if _is_iso_prefixed_block_id(b)]
        ignored = [b for b in before if b not in set(blocks)]
        if ignored:
            print(
                f"[INFO] Ignoring {len(ignored)} non-ISO-prefixed block folders by default: {sorted(ignored)}. "
                f"Use --block-ids to include explicitly.",
                flush=True,
            )

        if expected is not None:
            missing_expected = sorted(expected - set(before))
            if missing_expected:
                print(
                    f"[WARN] {len(missing_expected)} expected blocks are not present under {DCC_RESULTS}: "
                    f"{missing_expected}",
                    flush=True,
                )
            unexpected_iso = sorted({b for b in before if _is_iso_prefixed_block_id(b)} - expected)
            if unexpected_iso:
                print(
                    f"[WARN] {len(unexpected_iso)} ISO-prefixed block folders are present but not in the frozen definition; keeping them: "
                    f"{unexpected_iso}",
                    flush=True,
                )

    if not blocks:
        raise SystemExit("No block ids found to replay")

    # Load catalog/threshold metadata once for series filtering.
    catalog_index = _load_catalog_index(CATALOG_PATH)
    expected_series = _load_expected_series_codes(FROZEN_BLOCK_DEF_PATH)
    min_cov = float(args.min_series_coverage) if args.min_series_coverage is not None else _load_series_threshold(THRESHOLD_CONFIG_PATH)
    respect_dnu = bool(args.respect_catalog_do_not_use) and (not bool(args.ignore_catalog_do_not_use))
    strict_low_cov = bool(args.drop_low_coverage_even_if_expected)

    # Scenario inputs root (used for Dt_daily-based release-date masks)
    if bool(args.use_latest_scenario_run):
        inputs_root = SCENARIOS_DIR / "latest" / "inputs"
    elif args.scenario_run_id:
        inputs_root = SCENARIOS_DIR / str(args.scenario_run_id) / "inputs"
    else:
        inputs_root = SCENARIOS_DIR / "latest" / "inputs"

    dt_cache: dict[str, pd.DataFrame | None] = {}

    run_id = f"replay_{_utc_timestamp()}"
    if bool(args.use_latest_scenario_run):
        out_root = SCENARIOS_DIR / "latest" / "historical_replay"
        scenario_run_id = "latest"
    elif args.scenario_run_id:
        out_root = SCENARIOS_DIR / str(args.scenario_run_id) / "historical_replay"
        scenario_run_id = str(args.scenario_run_id)
    else:
        out_root = OUT_BASE
        scenario_run_id = None

    run_dir = out_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "created_utc": datetime.utcnow().isoformat() + "Z",
        "episodes_config": str(cfg_path.as_posix()),
        "n_episodes": len(episodes),
        "n_blocks": len(blocks),
        "scenario_run_id": scenario_run_id,
        "settings": {
            "min_obs": min_obs,
            "anchor": anchor,
            "rotate_to_current_correlation": rotate,
            "re_volatilize_with_current_vol": revol,
            "catalog_series_filter": {
                "catalog_path": str(CATALOG_PATH.as_posix()),
                "respect_do_not_use": respect_dnu,
                "min_coverage_ratio": float(min_cov) if min_cov is not None else None,
                "drop_unknown_series": bool(args.drop_unknown_series),
                "expected_series_source": str(FROZEN_BLOCK_DEF_PATH.as_posix()) if expected_series is not None else None,
            },
            "low_frequency_replay": {
                "release_date_shocks_low_frequency": bool(release_date_low_freq),
                "release_date_shocks_frequency_labels": sorted(low_freq_labels),
                "release_date_change_tol": float(release_change_tol),
                "dt_daily_inputs_root": str(inputs_root.as_posix()),
            },
        },
        "blocks": blocks,
        "block_selection": {
            "explicit_block_ids": sorted(wanted) if wanted is not None else None,
            "expected_block_ids_source": str((ROOT / "outputs" / "country_block_definition.json").as_posix()),
        },
    }

    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Preload block artifacts once.
    block_artifacts: dict[str, dict[str, Any]] = {}
    block_series_filter_report: dict[str, Any] = {
        "created_utc": datetime.utcnow().isoformat() + "Z",
        "min_coverage_ratio": float(min_cov) if min_cov is not None else None,
        "respect_do_not_use": respect_dnu,
        "drop_unknown_series": bool(args.drop_unknown_series),
        "blocks": {},
    }
    for b in blocks:
        try:
            art = _load_block_artifacts(b)

            cols0 = list(art["columns"])
            kept, dropped = _filter_block_columns(
                cols0,
                catalog_index=catalog_index,
                expected_series=expected_series,
                min_coverage=min_cov,
                respect_do_not_use=respect_dnu,
                drop_unknown=bool(args.drop_unknown_series),
                drop_low_coverage_even_if_expected=strict_low_cov,
            )
            if len(kept) < 2:
                raise ValueError(f"<2 series remain after catalog filtering (kept={len(kept)}, dropped={len(dropped)})")

            # Keep correlation matrix aligned to kept ordering.
            try:
                pos = [cols0.index(c) for c in kept]
                art["uncorr"] = art["uncorr"][np.ix_(pos, pos)]
            except Exception as exc:
                raise ValueError(f"failed to align correlation matrix after filtering: {exc}")

            if dropped:
                block_series_filter_report["blocks"][b] = {
                    "n_before": int(len(cols0)),
                    "n_kept": int(len(kept)),
                    "n_dropped": int(len(dropped)),
                    "dropped": dropped,
                }

            art["columns"] = kept
            art["resids"] = art["resids"][kept]
            art["vols"] = art["vols"][kept]

            block_artifacts[b] = art
        except Exception as exc:
            print(f"[WARN] Skipping block {b}: {exc}")

    if not block_artifacts:
        raise SystemExit("No usable blocks (missing artifacts)")

    (run_dir / "block_series_filtering.json").write_text(
        json.dumps(block_series_filter_report, indent=2),
        encoding="utf-8",
    )

    for ep in episodes:
        ep_dir = run_dir / "episodes" / ep.episode_id
        (ep_dir / "block_z_shocks").mkdir(parents=True, exist_ok=True)
        (ep_dir / "block_innovations").mkdir(parents=True, exist_ok=True)

        summary_rows: list[dict[str, Any]] = []

        for block_id, art in block_artifacts.items():
            resids: pd.DataFrame = art["resids"]
            vols: pd.DataFrame = art["vols"]
            R_current: np.ndarray = art["uncorr"]

            z_ep, slice_meta = _slice_episode(
                resids,
                ep.start,
                ep.end,
                min_col_coverage=min_col_coverage,
                min_series=min_series,
            )
            if len(z_ep) < min_obs:
                summary_rows.append({
                    "episode_id": ep.episode_id,
                    "block_id": block_id,
                    "status": "skipped_insufficient_overlap",
                    "n_obs": int(len(z_ep)),
                    "n_series": int(z_ep.shape[1]) if hasattr(z_ep, "shape") else None,
                    "dropped_series": int(len(slice_meta.get("dropped_columns") or [])),
                })
                continue

            release_meta: dict[str, object] = {"masked_series": []}
            if release_date_low_freq:
                z_ep, release_meta = _apply_release_date_mask_to_low_frequency_z(
                    z_ep,
                    block_id=block_id,
                    catalog_index=catalog_index,
                    inputs_root=inputs_root,
                    dt_cache=dt_cache,
                    low_freq_labels=low_freq_labels,
                    change_tol=release_change_tol,
                    step_like_override=release_step_like_override,
                    update_frac_threshold=release_update_frac_thr,
                )

            # Anchor replay at end-of-sample per block to keep vols aligned.
            anchor_end = vols.index.max() if anchor == "end_of_sample" else vols.index.max()
            replay_index = _build_replay_index(anchor_end=anchor_end, length=len(z_ep))

            # Restrict to the series that survived episode pruning.
            cols_used = list(z_ep.columns)
            vols = vols[cols_used]
            vols_cur = vols.reindex(replay_index).ffill().bfill()
            # Ensure the episode shocks line up with replay length.
            z_ep = z_ep.tail(len(replay_index))
            z_ep = z_ep[vols_cur.columns]

            # Keep historical episode dates for interpretability, but use the
            # end-of-sample replay index only for fetching current volatilities.
            src_episode_start = pd.Timestamp(z_ep.index.min())
            src_episode_end = pd.Timestamp(z_ep.index.max())
            episode_index = z_ep.index

            # Export z-space replay shocks for comparability (sigma-multiples).
            z_replay = _apply_rotation_only(
                z_episode=z_ep,
                R_current=R_current,
                rotate=rotate,
                shrinkage_lambda=shrinkage_lambda,
                eig_floor=eig_floor,
                winsor_abs_quantile=winsor_abs_quantile,
            )
            z_out_path = ep_dir / "block_z_shocks" / f"{block_id}.csv"
            z_replay.to_csv(z_out_path, index_label="Date")

            # Unit-space innovations (optionally re-volatilized)
            innovations = _rotate_and_revolatilize(
                z_episode=z_ep,
                vols_current=vols_cur,
                R_current=R_current,
                rotate=rotate,
                revol=revol,
                shrinkage_lambda=shrinkage_lambda,
                eig_floor=eig_floor,
                winsor_abs_quantile=winsor_abs_quantile,
            )

            # Map replay-index innovations back onto the historical episode axis
            # (values are still scaled by current vols).
            innovations = innovations.copy()
            innovations.index = episode_index

            out_path = ep_dir / "block_innovations" / f"{block_id}.csv"
            innovations.to_csv(out_path, index_label="Date")

            # Basic diagnostics (robust to constant columns).
            corr_ep = pd.DataFrame(z_ep.to_numpy(dtype=float), columns=z_ep.columns).corr().to_numpy(dtype=float)
            corr_ep = _nearest_spd_corr(corr_ep, eig_floor=eig_floor)

            max_abs_z = float(np.nanmax(np.abs(z_ep.to_numpy(dtype=float))))
            max_abs_z_replay = float(np.nanmax(np.abs(z_replay.to_numpy(dtype=float))))
            summary_rows.append({
                "episode_id": ep.episode_id,
                "block_id": block_id,
                "status": "ok",
                "n_obs": int(len(z_ep)),
                "n_series": int(z_ep.shape[1]),
                "dropped_series": int(len(slice_meta.get("dropped_columns") or [])),
                "episode_start": str(src_episode_start.date()),
                "episode_end": str(src_episode_end.date()),
                "replay_start": str(replay_index.min().date()),
                "replay_end": str(replay_index.max().date()),
                "rotate": bool(rotate),
                "revol": bool(revol),
                "shrinkage_lambda": float(shrinkage_lambda),
                "eig_floor": float(eig_floor),
                "winsor_q": float(winsor_abs_quantile),
                "max_abs_z": max_abs_z,
                "max_abs_z_replay": max_abs_z_replay,
                "rotation_amplification": (max_abs_z_replay / max_abs_z) if (max_abs_z and np.isfinite(max_abs_z) and max_abs_z > 0) else np.nan,
                "mean_abs_corr_episode": float(np.mean(np.abs(corr_ep[np.triu_indices_from(corr_ep, k=1)]))) if corr_ep.shape[0] > 1 else np.nan,
                "mean_abs_corr_current": float(np.mean(np.abs(R_current[np.triu_indices_from(R_current, k=1)]))) if R_current.shape[0] > 1 else np.nan,
                "max_abs_innovation": float(np.nanmax(np.abs(innovations.to_numpy(dtype=float)))),
                "release_date_masked_series": ";".join(release_meta.get("masked_series") or []),
                "n_release_date_masked_series": int(len(release_meta.get("masked_series") or [])),
            })

        summary_df = pd.DataFrame(summary_rows)
        summary_df.to_csv(ep_dir / "episode_summary.csv", index=False)

        # Episode diagnostics: flag unusually large standardized shocks (z-space)
        flagged: list[dict[str, Any]] = []
        flat_spike_hits: list[dict[str, Any]] = []
        ok_df = summary_df.loc[summary_df["status"] == "ok"].copy()
        if not ok_df.empty:
            for _, row in ok_df.iterrows():
                block_id = str(row.get("block_id"))
                max_abs_z_replay = _safe_float(row.get("max_abs_z_replay"))
                if max_abs_z_replay is None or max_abs_z_replay <= diag_z_threshold:
                    continue

                z_path = ep_dir / "block_z_shocks" / f"{block_id}.csv"
                if not z_path.exists():
                    continue
                try:
                    z_df = pd.read_csv(z_path, index_col=0, parse_dates=True)
                except Exception:
                    continue
                if z_df.empty:
                    continue

                abs_df = z_df.abs()
                per_col = abs_df.max().sort_values(ascending=False)
                top_cols = []
                for col in per_col.head(8).index.tolist():
                    col_max = float(per_col.loc[col])
                    # Find a representative timestamp for the max (first occurrence)
                    try:
                        when = abs_df[col].idxmax()
                        when_s = when.date().isoformat() if hasattr(when, "date") else None
                    except Exception:
                        when_s = None

                    flat_meta = _flat_spike_metrics(
                        z_df[col],
                        abs_eps=flat_eps,
                        p95_abs_threshold=flat_p95_thr,
                        near_zero_frac_threshold=flat_near0_thr,
                        spike_threshold=diag_z_threshold,
                    )
                    if bool(flat_meta.get("flat_spike_flag")):
                        flat_spike_hits.append(
                            {
                                "block_id": block_id,
                                "series": str(col),
                                "date": when_s,
                                "metrics": flat_meta,
                            }
                        )
                    top_cols.append({"series": str(col), "max_abs_z": col_max, "date": when_s})

                flagged.append(
                    {
                        "block_id": block_id,
                        "max_abs_z_replay": float(max_abs_z_replay),
                        "n_obs": int(row.get("n_obs")) if str(row.get("n_obs", "")).strip() else None,
                        "n_series": int(row.get("n_series")) if str(row.get("n_series", "")).strip() else None,
                        "top_series": top_cols,
                    }
                )

        diagnostics = {
            "episode_id": ep.episode_id,
            "episode_name": ep.name,
            "episode_window": {"start": str(ep.start.date()), "end": str(ep.end.date())},
            "z_threshold": float(diag_z_threshold),
            "n_flagged_blocks": int(len(flagged)),
            "flagged_blocks": flagged,
            "flat_then_spike": {
                "p95_abs_threshold": float(flat_p95_thr),
                "near_zero_frac_threshold": float(flat_near0_thr),
                "abs_epsilon": float(flat_eps),
                "n_hits": int(len(flat_spike_hits)),
                "hits": flat_spike_hits,
            },
            "notes": {
                "interpretation": (
                    "Large z values can indicate genuine episode extremes, model misfit, "
                    "or data quality issues (e.g., stale/flat series creating spurious residual spikes)."
                )
            },
        }
        (ep_dir / "episode_diagnostics.json").write_text(
            json.dumps(diagnostics, indent=2), encoding="utf-8"
        )

        # Minimal plausibility report.
        ok = summary_df[summary_df["status"] == "ok"]
        skipped = summary_df[summary_df["status"] != "ok"]
        lines = [
            f"# Historical Replay Plausibility Report: {ep.name}",
            "",
            f"Episode: `{ep.episode_id}` ({ep.start.date()} to {ep.end.date()})",
            "",
            f"Blocks OK: {len(ok)} | Skipped: {len(skipped)} (min_obs={min_obs})",
            "",
        ]
        if not ok.empty:
            top = ok.sort_values("max_abs_innovation", ascending=False).head(10)
            lines.append("## Largest innovations (top 10 blocks)\n")
            for _, row in top.iterrows():
                lines.append(f"- {row['block_id']}: max_abs_innovation={row['max_abs_innovation']:.4f}")
            lines.append("")
        if not skipped.empty:
            lines.append("## Skipped blocks\n")
            for _, row in skipped.iterrows():
                lines.append(f"- {row['block_id']}: {row['status']} (n_obs={int(row.get('n_obs') or 0)})")
            lines.append("")

        (ep_dir / "plausibility_report.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"[OK] Historical replay outputs written to: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
