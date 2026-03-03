"""11.4 - Plot audit (readability + completeness).

Checks that the Step 11.2 plot bundle is complete and readable:
- Required episode-level plots exist (heatmap + episode distribution)
- PNGs open successfully
- Image dimensions and file sizes are sane
- Flags likely-blank images (very low pixel variance)

Outputs:
  <plot_bundle>/plot_audit_report.md

Usage:
  python "SRESS TEST PIPELINE/11.4_plot_audit.py" --use-latest
  python "SRESS TEST PIPELINE/11.4_plot_audit.py" --plot-bundle "SRESS TEST PIPELINE/FHS Historical Replay Plots/replay_..."
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
PLOTS_BASE = ROOT / "SRESS TEST PIPELINE" / "FHS Historical Replay Plots"


@dataclass(frozen=True)
class PngCheck:
    path: Path
    ok: bool
    width: int | None
    height: int | None
    size_kb: float
    variance: float | None
    note: str


def _infer_latest_bundle() -> Path | None:
    if not PLOTS_BASE.exists():
        return None
    runs = [p for p in PLOTS_BASE.iterdir() if p.is_dir() and p.name.startswith("replay_")]
    if not runs:
        return None
    runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return runs[0]


def _try_open_png(path: Path) -> PngCheck:
    size_kb = path.stat().st_size / 1024.0
    try:
        from PIL import Image

        with Image.open(path) as im:
            im.load()
            w, h = im.size
            # Downsample for variance check
            arr = np.asarray(im.convert("L"))
            if arr.size > 2_000_000:
                arr = arr[::4, ::4]
            var = float(arr.var())

        note_parts = []
        if w < 640 or h < 360:
            note_parts.append("small-dim")
        if size_kb < 10:
            note_parts.append("tiny-file")
        if var < 2.0:
            note_parts.append("low-variance-blank?")

        return PngCheck(path=path, ok=True, width=w, height=h, size_kb=size_kb, variance=var, note=",".join(note_parts))
    except Exception as exc:
        return PngCheck(path=path, ok=False, width=None, height=None, size_kb=size_kb, variance=None, note=str(exc))


def main() -> int:
    ap = argparse.ArgumentParser(description="11.4 Plot audit")
    ap.add_argument("--use-latest", action="store_true")
    ap.add_argument("--plot-bundle", type=str, default=None)
    args = ap.parse_args()

    if args.plot_bundle:
        bundle = Path(args.plot_bundle)
        if not bundle.is_absolute():
            bundle = ROOT / bundle
    elif args.use_latest:
        bundle = _infer_latest_bundle()
        if bundle is None:
            raise SystemExit(f"No plot bundles found under {PLOTS_BASE}")
    else:
        raise SystemExit("Provide --use-latest or --plot-bundle")

    if not bundle.exists():
        raise SystemExit(f"Missing plot bundle: {bundle}")

    episodes = [p for p in bundle.iterdir() if p.is_dir() and p.name not in {"triage"}]
    episodes = sorted(episodes, key=lambda p: p.name)

    required_missing: list[str] = []
    checks: list[PngCheck] = []

    for ep in episodes:
        ep_id = ep.name
        heatmap = ep / "heatmaps" / f"{ep_id}_maxabsz_heatmap.png"
        dist = ep / "distributions" / f"{ep_id}_episode_distribution.png"
        if not heatmap.exists():
            required_missing.append(str(heatmap))
        if not dist.exists():
            required_missing.append(str(dist))

        for png in ep.rglob("*.png"):
            checks.append(_try_open_png(png))

    n_ok = sum(1 for c in checks if c.ok)
    n_bad = len(checks) - n_ok

    blank_like = [c for c in checks if c.ok and (c.variance is not None and c.variance < 2.0)]
    tiny = [c for c in checks if c.size_kb < 10]

    md = []
    md.append("# Plot audit report\n")
    md.append(f"Bundle: `{bundle}`\n")
    md.append(f"Episodes found: {len(episodes)}\n")
    md.append(f"PNGs checked: {len(checks)} (ok={n_ok}, failed={n_bad})\n")

    if required_missing:
        md.append("\n## Missing required episode-level plots\n")
        md.extend([f"- {p}" for p in required_missing[:200]])

    if n_bad:
        md.append("\n## PNGs that failed to open\n")
        for c in [x for x in checks if not x.ok][:200]:
            md.append(f"- {c.path} ({c.size_kb:.1f}KB): {c.note}")

    if blank_like:
        md.append("\n## Likely-blank (low variance)\n")
        for c in blank_like[:200]:
            md.append(f"- {c.path} ({c.width}x{c.height}, {c.size_kb:.1f}KB, var={c.variance:.2f}) {c.note}")

    if tiny:
        md.append("\n## Very small files (<10KB)\n")
        for c in tiny[:200]:
            md.append(f"- {c.path} ({c.size_kb:.1f}KB)")

    report_path = bundle / "plot_audit_report.md"
    report_path.write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"[OK] Wrote: {report_path}")
    if required_missing:
        print(f"[WARN] Missing {len(required_missing)} required plots")
    if n_bad:
        print(f"[WARN] {n_bad} PNGs failed to open")
    if blank_like:
        print(f"[WARN] {len(blank_like)} PNGs look blank")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
