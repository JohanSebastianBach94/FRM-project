"""Innovation backtest gate for Step 12 Monte Carlo daily draws.

Reads:
- analysis_outputs/scenarios/<run_id>/<output_subdir>/daily_draws.csv OR /daily_draws/* shards
- diagnostics/dims.csv (for vol_t0)
- diagnostics/lowfreq_classification.csv (to identify low-frequency factors)
- manifest.json (to infer family/df)

Computes distributional/consistency checks on standardized shocks z = shock / vol_t0:
- mean near 0
- std near 1
- kurtosis roughly consistent with family
- tail exceedance rates
- optional (sampled) lag-1 autocorr of |z| and z^2 within draw (vol clustering proxy)

Writes:
- diagnostics/innovation_backtest_by_factor.csv
- diagnostics/innovation_backtest_summary.json

Exits non-zero if any high-frequency factor fails thresholds.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCENARIOS_DIR = PROJECT_ROOT / "analysis_outputs" / "scenarios"


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_run_dir(*, run_id: Optional[str], use_latest: bool) -> Path:
    if use_latest or (run_id is None):
        return SCENARIOS_DIR / "latest"
    return SCENARIOS_DIR / str(run_id)


def _iter_daily_draws_files(out_dir: Path) -> Tuple[str, List[Path]]:
    """Return (mode, files).

    mode:
      - "single_csv"
      - "sharded"
    """
    single = out_dir / "daily_draws.csv"
    if single.exists():
        return "single_csv", [single]

    shards = out_dir / "daily_draws"
    if shards.exists() and shards.is_dir():
        files = sorted([p for p in shards.rglob("*") if p.is_file()])
        files = [p for p in files if p.suffix.lower() in {".csv", ".gz", ".parquet"} or p.name.lower().endswith(".csv.gz")]
        if files:
            return "sharded", files

    raise FileNotFoundError(f"Could not find daily draws under: {out_dir}")


def _expected_kurtosis(*, family: str, df: Optional[float]) -> Optional[float]:
    fam = str(family).strip().lower()
    if fam == "normal":
        return 3.0
    if fam == "student_t":
        if df is None:
            return None
        d = float(df)
        if d <= 4:
            return None
        return 3.0 * (d - 2.0) / (d - 4.0)
    return None


def _expected_std(*, family: str, df: Optional[float]) -> Optional[float]:
    """Expected std of the base 1D draw used in Step 12 before vol scaling.

    Notes:
    - For normal: std=1.
    - For Student-t as typically sampled by numpy/scipy with scale=1: Var = df/(df-2)
      (for df>2), hence std = sqrt(df/(df-2)).

    If Step 12 later changes to variance-normalize t draws, this should be updated.
    """
    fam = str(family).strip().lower()
    if fam == "normal":
        return 1.0
    if fam == "student_t":
        if df is None:
            return None
        d = float(df)
        if d <= 2:
            return None
        return float(math.sqrt(d / (d - 2.0)))
    return None


@dataclass
class MomentAgg:
    n: int = 0
    s1: float = 0.0
    s2: float = 0.0
    s3: float = 0.0
    s4: float = 0.0
    n_zero: int = 0
    n_abs_gt3: int = 0
    n_abs_gt5: int = 0

    def update(self, x: np.ndarray) -> None:
        if x.size == 0:
            return
        xx = x.astype(float, copy=False)
        self.n += int(xx.size)
        self.s1 += float(np.sum(xx))
        self.s2 += float(np.sum(xx**2))
        self.s3 += float(np.sum(xx**3))
        self.s4 += float(np.sum(xx**4))
        self.n_zero += int(np.sum(xx == 0.0))
        self.n_abs_gt3 += int(np.sum(np.abs(xx) > 3.0))
        self.n_abs_gt5 += int(np.sum(np.abs(xx) > 5.0))

    def stats(self) -> Dict[str, Any]:
        if self.n <= 0:
            return {
                "n": 0,
                "mean": None,
                "std": None,
                "skew": None,
                "kurtosis": None,
                "p_zero": None,
                "p_abs_gt3": None,
                "p_abs_gt5": None,
            }
        n = float(self.n)
        mean = self.s1 / n
        m2 = self.s2 / n
        var = max(m2 - mean**2, 0.0)
        std = math.sqrt(max(var, 0.0))

        # central moments via raw moments (stable enough for near-zero mean z)
        m3 = (self.s3 / n) - 3 * mean * m2 + 2 * mean**3
        m4 = (self.s4 / n) - 4 * mean * (self.s3 / n) + 6 * (mean**2) * m2 - 3 * mean**4

        skew = (m3 / (std**3)) if std > 0 else None
        kurt = (m4 / (var**2)) if var > 0 else None

        return {
            "n": int(self.n),
            "mean": float(mean),
            "std": float(std),
            "skew": float(skew) if skew is not None and np.isfinite(skew) else None,
            "kurtosis": float(kurt) if kurt is not None and np.isfinite(kurt) else None,
            "p_zero": float(self.n_zero) / n,
            "p_abs_gt3": float(self.n_abs_gt3) / n,
            "p_abs_gt5": float(self.n_abs_gt5) / n,
        }


@dataclass
class LagAgg:
    n_pairs: int = 0
    s_xy: float = 0.0
    s_x: float = 0.0
    s_y: float = 0.0
    s_x2: float = 0.0
    s_y2: float = 0.0

    def update_pairs(self, x: np.ndarray, y: np.ndarray) -> None:
        if x.size == 0 or y.size == 0:
            return
        if x.size != y.size:
            m = min(x.size, y.size)
            x = x[:m]
            y = y[:m]
        self.n_pairs += int(x.size)
        self.s_xy += float(np.sum(x * y))
        self.s_x += float(np.sum(x))
        self.s_y += float(np.sum(y))
        self.s_x2 += float(np.sum(x**2))
        self.s_y2 += float(np.sum(y**2))

    def corr(self) -> Optional[float]:
        if self.n_pairs <= 3:
            return None
        n = float(self.n_pairs)
        cov = (self.s_xy / n) - (self.s_x / n) * (self.s_y / n)
        vx = (self.s_x2 / n) - (self.s_x / n) ** 2
        vy = (self.s_y2 / n) - (self.s_y / n) ** 2
        if vx <= 0 or vy <= 0:
            return None
        return float(cov / math.sqrt(vx * vy))


def _read_draws_in_chunks(
    files: List[Path],
    *,
    chunksize: int,
) -> Iterator[pd.DataFrame]:
    for path in files:
        name = path.name.lower()
        if name.endswith(".parquet"):
            df = pd.read_parquet(path)
            yield df
            continue

        compression = "gzip" if name.endswith(".gz") else None
        for chunk in pd.read_csv(path, chunksize=int(chunksize), compression=compression):
            yield chunk


def main() -> int:
    p = argparse.ArgumentParser(description="Innovation backtest gate for Step 12 daily draws")
    p.add_argument("--run-id", default=None)
    p.add_argument("--use-latest", action="store_true")
    p.add_argument("--output-subdir", default="monte_carlo")

    p.add_argument("--chunksize", type=int, default=250_000)
    p.add_argument("--acf-draw-id-max", type=int, default=100, help="Max draw_id included in ACF checks (keeps it cheap).")
    p.add_argument("--disable-acf", action="store_true", help="Skip lag-1 autocorr checks (fastest).")

    p.add_argument("--mean-abs-tol", type=float, default=0.05)
    p.add_argument("--std-tol", type=float, default=0.10)
    p.add_argument("--kurtosis-rel-tol", type=float, default=0.50)
    p.add_argument("--tail5-max", type=float, default=0.02, help="Max allowed P(|z|>5) for high-frequency factors")
    p.add_argument("--acf-abs-tol", type=float, default=0.10)
    p.add_argument("--acf-sq-tol", type=float, default=0.10)

    args = p.parse_args()

    run_dir = _find_run_dir(run_id=args.run_id, use_latest=bool(args.use_latest))
    out_dir = run_dir / str(args.output_subdir)
    diag_dir = out_dir / "diagnostics"

    out_manifest = _read_json(out_dir / "manifest.json")
    family = str(out_manifest.get("family") or "")
    df = out_manifest.get("t_df")

    expected_kurt = _expected_kurtosis(family=family, df=float(df) if df is not None else None)
    expected_std = _expected_std(family=family, df=float(df) if df is not None else None)

    dims_path = diag_dir / "dims.csv"
    if not dims_path.exists():
        raise SystemExit(f"Missing dims.csv: {dims_path}")
    dims = pd.read_csv(dims_path)
    if not {"iso", "factor", "vol_t0"}.issubset(set(dims.columns)):
        raise SystemExit(f"dims.csv missing required columns: {dims_path}")

    vol_map: Dict[Tuple[str, str], float] = {}
    for _, r in dims.iterrows():
        iso = str(r.get("iso"))
        fac = str(r.get("factor"))
        try:
            v = float(r.get("vol_t0"))
        except Exception:
            v = 1.0
        if not np.isfinite(v) or v <= 0:
            v = 1.0
        vol_map[(iso, fac)] = float(v)

    lowfreq_path = diag_dir / "lowfreq_classification.csv"
    low_set: set[Tuple[str, str]] = set()
    if lowfreq_path.exists():
        lf = pd.read_csv(lowfreq_path)
        if {"iso", "factor", "is_low_frequency"}.issubset(set(lf.columns)):
            for _, r in lf.iterrows():
                if bool(r.get("is_low_frequency")):
                    low_set.add((str(r.get("iso")), str(r.get("factor"))))

    mode, files = _iter_daily_draws_files(out_dir)

    # Accumulate moments for each factor, and ACF stats for sampled draws
    moments: Dict[Tuple[str, str], MomentAgg] = {}
    acf_abs: Dict[Tuple[str, str], LagAgg] = {}
    acf_sq: Dict[Tuple[str, str], LagAgg] = {}

    # Carry last obs between chunks for ACF calc per (iso,factor,draw_id)
    last_abs: Dict[Tuple[str, str, int], float] = {}
    last_sq: Dict[Tuple[str, str, int], float] = {}
    last_h: Dict[Tuple[str, str, int], int] = {}

    required_cols = {"iso", "factor", "shock", "draw_id"}
    for chunk in _read_draws_in_chunks(files, chunksize=int(args.chunksize)):
        if chunk is None or chunk.empty:
            continue
        if not required_cols.issubset(set(chunk.columns)):
            raise SystemExit(f"daily_draws missing required columns {sorted(list(required_cols))} in {mode}")

        # Standardize
        iso = chunk["iso"].astype(str)
        fac = chunk["factor"].astype(str)
        shock = pd.to_numeric(chunk["shock"], errors="coerce").astype(float)
        draw_id = pd.to_numeric(chunk["draw_id"], errors="coerce").fillna(-1).astype(int)

        vols = np.array([vol_map.get((i, f), 1.0) for i, f in zip(iso.values, fac.values)], dtype=float)
        vols = np.where((~np.isfinite(vols)) | (vols <= 0), 1.0, vols)
        z = shock.values / vols
        z = np.nan_to_num(z, nan=0.0, posinf=0.0, neginf=0.0)

        keys = list(zip(iso.values.tolist(), fac.values.tolist()))
        for (i, f), zz in zip(keys, z.tolist()):
            agg = moments.get((i, f))
            if agg is None:
                agg = MomentAgg()
                moments[(i, f)] = agg
            agg.update(np.array([zz], dtype=float))

        # Lag-1 autocorr checks (cheap sample): within each (iso,factor,draw_id) order by h
        if (not bool(args.disable_acf)) and ("h" in chunk.columns):
            sub = pd.DataFrame(
                {
                    "iso": iso,
                    "factor": fac,
                    "draw_id": draw_id,
                    "h": pd.to_numeric(chunk["h"], errors="coerce").fillna(-1).astype(int),
                    "z": z,
                }
            )
            sub = sub[sub["draw_id"] >= 0]
            if not sub.empty:
                sub = sub[sub["draw_id"] <= int(args.acf_draw_id_max)]
                sub = sub.sort_values(["iso", "factor", "draw_id", "h"])
                for (i, f, d), g in sub.groupby(["iso", "factor", "draw_id"], sort=False):
                    gz = g["z"].astype(float).values
                    gh = g["h"].astype(int).values
                    if gz.size <= 1:
                        continue

                    a = np.abs(gz)
                    s = gz**2

                    k3 = (str(i), str(f), int(d))
                    if k3 in last_abs:
                        # attempt to bridge if contiguous
                        if int(gh[0]) == int(last_h.get(k3, -999)) + 1:
                            a0 = float(last_abs[k3])
                            s0 = float(last_sq[k3])
                            a = np.concatenate([[a0], a])
                            s = np.concatenate([[s0], s])

                    # update carries
                    last_abs[k3] = float(a[-1])
                    last_sq[k3] = float(s[-1])
                    last_h[k3] = int(gh[-1])

                    x_abs = a[1:]
                    y_abs = a[:-1]
                    x_sq = s[1:]
                    y_sq = s[:-1]

                    if x_abs.size:
                        acf_abs.setdefault((str(i), str(f)), LagAgg()).update_pairs(x_abs, y_abs)
                        acf_sq.setdefault((str(i), str(f)), LagAgg()).update_pairs(x_sq, y_sq)

    # Build report rows
    rows: List[Dict[str, Any]] = []
    failures: List[str] = []

    for (i, f), agg in sorted(moments.items()):
        st = agg.stats()
        is_low = (i, f) in low_set
        mean = st.get("mean")
        std = st.get("std")
        kurt = st.get("kurtosis")
        p5 = st.get("p_abs_gt5")

        pass_mean = None
        pass_std = None
        pass_kurt = None
        pass_tail5 = None

        if (mean is not None) and (std is not None) and (not is_low):
            pass_mean = bool(abs(float(mean)) <= float(args.mean_abs_tol))
            if expected_std is not None:
                pass_std = bool(abs(float(std) - float(expected_std)) <= float(args.std_tol))
            else:
                pass_std = None
            if expected_kurt is not None and kurt is not None and np.isfinite(kurt):
                pass_kurt = bool(abs(float(kurt) - float(expected_kurt)) <= float(args.kurtosis_rel_tol) * float(expected_kurt))
            else:
                pass_kurt = None
            if p5 is not None:
                pass_tail5 = bool(float(p5) <= float(args.tail5_max))

        acf_a = acf_abs.get((i, f)).corr() if (i, f) in acf_abs else None
        acf_s = acf_sq.get((i, f)).corr() if (i, f) in acf_sq else None

        pass_acf_abs = None
        pass_acf_sq = None
        if (not bool(args.disable_acf)) and (not is_low):
            if acf_a is not None:
                pass_acf_abs = bool(abs(float(acf_a)) <= float(args.acf_abs_tol))
            if acf_s is not None:
                pass_acf_sq = bool(abs(float(acf_s)) <= float(args.acf_sq_tol))

        row = {
            "iso": str(i),
            "factor": str(f),
            "is_low_frequency": bool(is_low),
            "n": st.get("n"),
            "mean": mean,
            "std": std,
            "skew": st.get("skew"),
            "expected_std": float(expected_std) if expected_std is not None else None,
            "kurtosis": kurt,
            "expected_kurtosis": float(expected_kurt) if expected_kurt is not None else None,
            "p_zero": st.get("p_zero"),
            "p_abs_gt3": st.get("p_abs_gt3"),
            "p_abs_gt5": p5,
            "acf1_abs_z": acf_a,
            "acf1_z2": acf_s,
            "pass_mean": pass_mean,
            "pass_std": pass_std,
            "pass_kurtosis": pass_kurt,
            "pass_tail5": pass_tail5,
            "pass_acf1_abs_z": pass_acf_abs,
            "pass_acf1_z2": pass_acf_sq,
        }
        rows.append(row)

        if not is_low:
            # treat None passes as non-failures (e.g., expected kurtosis unavailable)
            for k, flag in [
                ("mean", pass_mean),
                ("std", pass_std),
                ("kurtosis", pass_kurt),
                ("tail5", pass_tail5),
                ("acf1_abs_z", pass_acf_abs),
                ("acf1_z2", pass_acf_sq),
            ]:
                if flag is False:
                    failures.append(f"{i}:{f}:{k}")

    df_out = pd.DataFrame(rows)
    diag_dir.mkdir(parents=True, exist_ok=True)
    out_csv = diag_dir / "innovation_backtest_by_factor.csv"
    df_out.to_csv(out_csv, index=False)

    summary = {
        "run_dir": str(run_dir),
        "output_subdir": str(args.output_subdir),
        "family": family,
        "t_df": float(df) if df is not None else None,
        "expected_std": float(expected_std) if expected_std is not None else None,
        "expected_kurtosis": float(expected_kurt) if expected_kurt is not None else None,
        "n_factors": int(df_out.shape[0]),
        "n_failures": int(len(failures)),
        "failures": failures[:200],
        "notes": [
            "z = shock / vol_t0 (from diagnostics/dims.csv).",
            "Low-frequency factors are excluded from pass/fail gating by default.",
            "ACF checks (if enabled) are computed on draw_id<=acf_draw_id_max only.",
        ],
    }
    out_json = diag_dir / "innovation_backtest_summary.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(f"[OK] Wrote: {out_csv}")
    print(f"[OK] Wrote: {out_json}")

    if failures:
        print(f"[FAIL] {len(failures)} backtest failures (showing up to 20):")
        for x in failures[:20]:
            print(f"  - {x}")
        return 2

    print("[PASS] Innovation backtest gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
