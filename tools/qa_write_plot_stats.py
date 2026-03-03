"""Write basic file stats for key publish PNGs.

Used as a lightweight sanity check that regenerated plots were rewritten.
"""

from __future__ import annotations

import datetime
from pathlib import Path


def main() -> None:
    targets = [
        (
            "SEVERITY_RANKING__ISOS.png",
            Path("SRESS TEST PIPELINE/MC scenario plots/latest/SEVERITY_RANKING__ISOS.png"),
        ),
        (
            "CRISIS_DRIVERS__BLOCK_SHARES.png",
            Path("SRESS TEST PIPELINE/MC scenario plots/latest/CRISIS_DRIVERS__BLOCK_SHARES.png"),
        ),
        (
            "CONNECTEDNESS_BASELINE_VS_STRESS__TODAY_VS_P99PLUS.png",
            Path(
                "SRESS TEST PIPELINE/MC scenario plots/latest/CONNECTEDNESS DELTAS/CONNECTEDNESS_BASELINE_VS_STRESS__TODAY_VS_P99PLUS.png"
            ),
        ),
    ]

    out = Path("analysis_outputs/qa/regen_plot_stats.txt")
    out.parent.mkdir(parents=True, exist_ok=True)

    lines: list[str] = []
    for name, path in targets:
        st = path.stat()
        mtime = datetime.datetime.fromtimestamp(st.st_mtime).isoformat()
        lines.append(f"{name}\tsize={st.st_size}\tmtime={mtime}")

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
