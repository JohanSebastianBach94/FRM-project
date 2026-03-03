import argparse
import json
from pathlib import Path

import pandas as pd


def _read_time_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    if df.empty:
        return pd.DataFrame()
    # Expect first column to be Date-like
    idx_col = df.columns[0]
    df[idx_col] = pd.to_datetime(df[idx_col], errors="coerce")
    df = df.dropna(subset=[idx_col]).set_index(idx_col)
    return df


def _safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="11.6 Step 11 full audit")
    ap.add_argument("--run-dir", required=True, help="Path to replay run directory")
    ap.add_argument(
        "--out",
        default="",
        help="Optional output markdown path (default: <run-dir>/step11_audit_report.md)",
    )
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    if not run_dir.exists():
        raise SystemExit(f"Run dir not found: {run_dir}")

    episodes_dir = run_dir / "episodes"
    if not episodes_dir.exists():
        raise SystemExit(f"Missing episodes dir: {episodes_dir}")

    out_path = Path(args.out) if args.out else (run_dir / "step11_audit_report.md")

    lines: list[str] = []
    lines.append(f"# Step 11 Audit")
    lines.append("")
    lines.append(f"Run: `{run_dir}`")

    manifest = run_dir / "manifest.json"
    lines.append(f"Manifest: {'OK' if manifest.exists() else 'MISSING'}")

    ep_dirs = [p for p in sorted(episodes_dir.iterdir()) if p.is_dir()]
    lines.append(f"Episodes: {len(ep_dirs)}")
    lines.append("")

    global_max_z = None
    global_max_info = None
    mismatches: list[str] = []

    for ep in ep_dirs:
        ep_id = ep.name
        z_dir = ep / "block_z_shocks"
        i_dir = ep / "block_innovations"
        diag_path = ep / "episode_diagnostics.json"
        summary_path = ep / "episode_summary.csv"
        plaus_path = ep / "plausibility_report.md"

        lines.append(f"## {ep_id}")
        lines.append("")
        lines.append(f"- episode_diagnostics.json: {'OK' if diag_path.exists() else 'MISSING'}")
        lines.append(f"- episode_summary.csv: {'OK' if summary_path.exists() else 'MISSING'}")
        lines.append(f"- plausibility_report.md: {'OK' if plaus_path.exists() else 'MISSING'}")
        lines.append(f"- block_z_shocks/: {'OK' if z_dir.exists() else 'MISSING'}")
        lines.append(f"- block_innovations/: {'OK' if i_dir.exists() else 'MISSING'}")

        diag = None
        if diag_path.exists():
            try:
                diag = json.loads(diag_path.read_text(encoding="utf-8"))
            except Exception as exc:
                lines.append(f"- diagnostics parse: FAIL ({exc})")

        if isinstance(diag, dict):
            n_flagged = diag.get("n_flagged_blocks")
            z_thr = diag.get("z_threshold")
            worst = None
            worst_block = None
            flagged = diag.get("flagged_blocks")
            if isinstance(flagged, list):
                for item in flagged:
                    if not isinstance(item, dict):
                        continue
                    v = _safe_float(item.get("max_abs_z_replay"))
                    if v is None:
                        continue
                    if worst is None or v > worst:
                        worst = v
                        worst_block = item.get("block_id")

            lines.append(f"- flagged blocks: {n_flagged} (z_threshold={z_thr})")
            lines.append(f"- worst max_abs_z_replay: {worst} (block={worst_block})")

        z_files = sorted(z_dir.glob("*.csv")) if z_dir.exists() else []
        i_files = sorted(i_dir.glob("*.csv")) if i_dir.exists() else []
        i_set = {p.name for p in i_files}

        lines.append(f"- z-shock block files: {len(z_files)}")
        lines.append(f"- innovation block files: {len(i_files)}")

        # Check block-by-block alignment and summarize maxima
        checked = 0
        for zf in z_files:
            checked += 1
            inf = i_dir / zf.name
            if inf.name not in i_set:
                mismatches.append(f"[{ep_id}] missing innovation for {zf.stem}")
                continue

            try:
                z = _read_time_csv(zf)
                inn = _read_time_csv(inf)
            except Exception as exc:
                mismatches.append(f"[{ep_id}] read error for {zf.stem}: {exc}")
                continue

            if z.empty or inn.empty:
                continue

            # Column overlap / index overlap checks
            common_cols = [c for c in z.columns if c in inn.columns]
            if not common_cols:
                mismatches.append(f"[{ep_id}] no common columns for {zf.stem}")
                continue

            # Align on intersection of dates for a fair sanity check
            common_idx = z.index.intersection(inn.index)
            if common_idx.empty:
                mismatches.append(f"[{ep_id}] no overlapping dates for {zf.stem}")
                continue

            zc = z.loc[common_idx, common_cols]
            max_abs = float(zc.abs().max().max())

            if global_max_z is None or max_abs > global_max_z:
                global_max_z = max_abs
                global_max_info = (ep_id, zf.stem)

        lines.append(f"- blocks checked: {checked}")
        lines.append("")

    lines.append("## Global")
    lines.append("")
    lines.append(f"- global max(|z|) across checked blocks: {global_max_z} (episode/block={global_max_info})")
    if mismatches:
        lines.append("- mismatches:")
        for m in mismatches[:50]:
            lines.append(f"  - {m}")
        if len(mismatches) > 50:
            lines.append(f"  - ... ({len(mismatches)-50} more)")
    else:
        lines.append("- mismatches: none")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[OK] Wrote: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
