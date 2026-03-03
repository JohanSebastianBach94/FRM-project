"""Compare Step 11.8 series quality audits across replay runs.

Usage examples:
  python compare_step11_series_quality_audits.py \
    --new analysis_outputs/scenarios/latest/historical_replay/replay_20260216_182324 \
    --base analysis_outputs/scenarios/latest/historical_replay/replay_20260216_152419 \
    --out-md analysis_outputs/scenarios/latest/historical_replay/replay_20260216_182324/series_quality_audit_diff_vs_152419.md

Notes:
- Handles older/newer column name variants (episode_id vs episode, flag_* vs is_*).
- Produces a small markdown report suitable for commit-free tracking in outputs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


KEYS = ["episode", "block", "series"]


def _read_audit_csv(run_dir: Path) -> pd.DataFrame:
    csv_path = run_dir / "series_quality_audit.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing series_quality_audit.csv: {csv_path}")

    df = pd.read_csv(csv_path)

    # Normalize column names across versions.
    rename = {
        "episode_id": "episode",
        "block_id": "block",
        "flag_stale": "is_stale",
        "flag_spike": "is_spike",
        "flag_stale_spike": "stale_and_spike",
        "flag_flat_then_spike_step11_1": "flat_then_spike_step11_1",
        "unique_ratio": "unique_ratio",
        "consec_same_ratio": "consec_same_ratio",
        "max_abs_z": "max_abs_z",
        "p95_abs_z": "p95_abs_z",
    }
    df = df.rename(columns=rename)

    missing_keys = [k for k in KEYS if k not in df.columns]
    if missing_keys:
        raise KeyError(f"Missing keys {missing_keys} in {csv_path}")

    # Ensure expected boolean columns exist.
    for col in ["is_stale", "is_spike", "stale_and_spike", "flat_then_spike_step11_1"]:
        if col not in df.columns:
            df[col] = False

    return df


@dataclass(frozen=True)
class Rollup:
    rows: int
    stale: int
    stale_spike: int
    flat_then_spike: int
    max_abs_z: float


def _rollup(df: pd.DataFrame) -> Rollup:
    stale = int(df["is_stale"].fillna(False).astype(bool).sum())
    stale_spike = int(df["stale_and_spike"].fillna(False).astype(bool).sum())
    flat_then_spike = int(df["flat_then_spike_step11_1"].fillna(False).astype(bool).sum())
    max_abs = float(df["max_abs_z"].astype(float).max()) if "max_abs_z" in df.columns else float("nan")
    return Rollup(rows=int(len(df)), stale=stale, stale_spike=stale_spike, flat_then_spike=flat_then_spike, max_abs_z=max_abs)


def _episode_rollup(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    tmp["is_stale"] = tmp["is_stale"].fillna(False).astype(bool)
    tmp["stale_and_spike"] = tmp["stale_and_spike"].fillna(False).astype(bool)
    tmp["flat_then_spike_step11_1"] = tmp["flat_then_spike_step11_1"].fillna(False).astype(bool)

    out = (
        tmp.groupby("episode", dropna=False)
        .agg(
            series=("series", "size"),
            stale=("is_stale", "sum"),
            stale_spike=("stale_and_spike", "sum"),
            flat_then_spike=("flat_then_spike_step11_1", "sum"),
            max_abs_z=("max_abs_z", "max"),
        )
        .reset_index()
        .sort_values("episode")
    )
    return out


def _top_abs_changes(new: pd.DataFrame, base: pd.DataFrame, n: int) -> pd.DataFrame:
    idx_new = new.set_index(KEYS)
    idx_base = base.set_index(KEYS)
    inter = idx_new.index.intersection(idx_base.index)

    sub_new = idx_new.loc[inter]
    sub_base = idx_base.loc[inter]

    if "max_abs_z" not in sub_new.columns or "max_abs_z" not in sub_base.columns:
        return pd.DataFrame(columns=KEYS + ["max_abs_z_new", "max_abs_z_base", "abs_change"]).head(0)

    abs_change = (sub_new["max_abs_z"].astype(float) - sub_base["max_abs_z"].astype(float)).abs()
    abs_change = abs_change.sort_values(ascending=False).head(n)

    rows = []
    for key in abs_change.index:
        r_new = sub_new.loc[key]
        r_base = sub_base.loc[key]
        rows.append(
            {
                "episode": key[0],
                "block": key[1],
                "series": key[2],
                "max_abs_z_new": float(r_new["max_abs_z"]),
                "max_abs_z_base": float(r_base["max_abs_z"]),
                "abs_change": float(abs_change.loc[key]),
                "stale_and_spike_new": bool(r_new["stale_and_spike"]),
                "stale_and_spike_base": bool(r_base["stale_and_spike"]),
                "is_stale_new": bool(r_new["is_stale"]),
                "is_stale_base": bool(r_base["is_stale"]),
            }
        )

    return pd.DataFrame(rows)


def _flag_flips(new: pd.DataFrame, base: pd.DataFrame, flag_col: str, n: int) -> pd.DataFrame:
    idx_new = new.set_index(KEYS)
    idx_base = base.set_index(KEYS)
    inter = idx_new.index.intersection(idx_base.index)

    sub_new = idx_new.loc[inter]
    sub_base = idx_base.loc[inter]

    a = sub_new[flag_col].fillna(False).astype(bool)
    b = sub_base[flag_col].fillna(False).astype(bool)
    flips = a != b
    if not flips.any():
        return pd.DataFrame(columns=KEYS + [f"{flag_col}_new", f"{flag_col}_base"]).head(0)

    out = pd.DataFrame(index=sub_new.index[flips])
    out["episode"] = [i[0] for i in out.index]
    out["block"] = [i[1] for i in out.index]
    out["series"] = [i[2] for i in out.index]
    out[f"{flag_col}_new"] = a[flips].values
    out[f"{flag_col}_base"] = b[flips].values

    # Prefer showing the biggest max_abs_z changes within flips when available.
    if "max_abs_z" in sub_new.columns and "max_abs_z" in sub_base.columns:
        out["abs_change_max_abs_z"] = (
            (sub_new.loc[out.index, "max_abs_z"].astype(float) - sub_base.loc[out.index, "max_abs_z"].astype(float)).abs().values
        )
        out = out.sort_values("abs_change_max_abs_z", ascending=False)

    return out.reset_index(drop=True).head(n)


def _as_md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(none)"
    return df.to_markdown(index=False)


def build_report(new_run_dir: Path, base_run_dir: Path) -> str:
    new_df = _read_audit_csv(new_run_dir)
    base_df = _read_audit_csv(base_run_dir)

    new_r = _rollup(new_df)
    base_r = _rollup(base_df)

    same = False
    try:
        same = (
            new_df.sort_values(KEYS).reset_index(drop=True)
            .equals(base_df.sort_values(KEYS).reset_index(drop=True))
        )
    except Exception:
        same = False

    ep_new = _episode_rollup(new_df)
    ep_base = _episode_rollup(base_df)

    # Build episode delta table
    ep = ep_new.merge(ep_base, on="episode", how="outer", suffixes=("_new", "_base")).fillna(0)
    for col in ["series", "stale", "stale_spike", "flat_then_spike"]:
        ep[f"delta_{col}"] = ep[f"{col}_new"] - ep[f"{col}_base"]

    top_changes = _top_abs_changes(new_df, base_df, n=20)

    flips_stale_spike = _flag_flips(new_df, base_df, "stale_and_spike", n=25)
    flips_stale = _flag_flips(new_df, base_df, "is_stale", n=25)

    lines: list[str] = []
    lines.append("# Step 11 Series Quality Audit Diff")
    lines.append("")
    lines.append(f"New: `{new_run_dir}`")
    lines.append(f"Base: `{base_run_dir}`")
    lines.append("")
    lines.append(f"Exact CSV equality (after column normalization): `{same}`")
    lines.append("")

    lines.append("## Rollup")
    lines.append("")
    lines.append("| metric | new | base | delta |")
    lines.append("|---|---:|---:|---:|")
    lines.append(f"| rows | {new_r.rows} | {base_r.rows} | {new_r.rows - base_r.rows} |")
    lines.append(f"| stale | {new_r.stale} | {base_r.stale} | {new_r.stale - base_r.stale} |")
    lines.append(f"| stale+spike | {new_r.stale_spike} | {base_r.stale_spike} | {new_r.stale_spike - base_r.stale_spike} |")
    lines.append(f"| flat_then_spike | {new_r.flat_then_spike} | {base_r.flat_then_spike} | {new_r.flat_then_spike - base_r.flat_then_spike} |")
    lines.append(f"| max(max_abs_z) | {new_r.max_abs_z:.6g} | {base_r.max_abs_z:.6g} | {(new_r.max_abs_z - base_r.max_abs_z):.6g} |")
    lines.append("")

    lines.append("## Episode Deltas")
    lines.append("")
    lines.append(_as_md_table(ep[["episode", "series_new", "series_base", "delta_series", "stale_spike_new", "stale_spike_base", "delta_stale_spike"]]))
    lines.append("")

    lines.append("## Flag Flips: stale_and_spike")
    lines.append("")
    lines.append(_as_md_table(flips_stale_spike))
    lines.append("")

    lines.append("## Flag Flips: is_stale")
    lines.append("")
    lines.append(_as_md_table(flips_stale))
    lines.append("")

    lines.append("## Top abs(max_abs_z) Changes")
    lines.append("")
    lines.append(_as_md_table(top_changes))
    lines.append("")

    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--new", required=True, type=Path, help="Replay run dir containing series_quality_audit.csv")
    p.add_argument("--base", required=True, type=Path, help="Replay run dir containing series_quality_audit.csv")
    p.add_argument("--out-md", type=Path, default=None, help="Write markdown report to this path")
    args = p.parse_args()

    report = build_report(args.new, args.base)
    if args.out_md:
        args.out_md.parent.mkdir(parents=True, exist_ok=True)
        args.out_md.write_text(report, encoding="utf-8")

    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
