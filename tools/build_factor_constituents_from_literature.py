"""Build per-ISO/per-block factor constituent maps from existing literature artifacts.

Uses:
- analysis_outputs/literature_factors/<ISO>_literature_manifest.json (block_details + factors)
- analysis_outputs/literature_factors/literature_pca_loadings.csv (weights for PCA factors)

Writes:
- analysis_outputs/factor_preparation/<ISO>_<block>_factor_constituents.csv

This is a one-time migration helper so Step 12 can use metadata-only classification
without re-running full factor preparation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    lit_dir = root / "analysis_outputs" / "literature_factors"
    out_dir = root / "analysis_outputs" / "factor_preparation"
    out_dir.mkdir(parents=True, exist_ok=True)

    loadings_path = lit_dir / "literature_pca_loadings.csv"
    df_loadings = None
    if loadings_path.exists():
        df_loadings = pd.read_csv(loadings_path, index_col=0)

    manifest_paths = sorted(lit_dir.glob("*_literature_manifest.json"))
    if not manifest_paths:
        raise SystemExit(f"No literature manifests under: {lit_dir}")

    written = 0
    for mp in manifest_paths:
        payload = json.loads(mp.read_text(encoding="utf-8"))
        iso = str(payload.get("iso") or mp.name.split("_")[0]).strip()
        block_details = payload.get("block_details") or {}
        if not isinstance(block_details, dict):
            continue

        for block_key, info in block_details.items():
            if not block_key or not isinstance(info, dict):
                continue
            method = str(info.get("method") or "")
            factors = info.get("factors") or []
            if not isinstance(factors, list) or not factors:
                continue

            series_post = info.get("series_post_dedupe") or []
            if not isinstance(series_post, list):
                series_post = []

            rows = []
            if method == "pca" and df_loadings is not None and not df_loadings.empty:
                sub = df_loadings[(df_loadings.get("iso") == iso) & (df_loadings.get("block") == block_key)]
                if not sub.empty:
                    for f in factors:
                        if f not in sub.columns:
                            continue
                        w = pd.to_numeric(sub[f], errors="coerce").dropna()
                        for series, weight in w.items():
                            rows.append(
                                {
                                    "iso": iso,
                                    "block": block_key,
                                    "factor": str(f),
                                    "series": str(series),
                                    "weight": float(weight),
                                    "method": method,
                                }
                            )

            # Fallback: equal weights using post-dedupe series list
            if not rows and series_post:
                for f in factors:
                    if method == "single_series":
                        if len(series_post) >= 1:
                            rows.append(
                                {
                                    "iso": iso,
                                    "block": block_key,
                                    "factor": str(f),
                                    "series": str(series_post[0]),
                                    "weight": 1.0,
                                    "method": method,
                                }
                            )
                    else:
                        w = 1.0 / float(len(series_post))
                        for s in series_post:
                            rows.append(
                                {
                                    "iso": iso,
                                    "block": block_key,
                                    "factor": str(f),
                                    "series": str(s),
                                    "weight": float(w),
                                    "method": method,
                                }
                            )

            if not rows:
                continue

            df_out = pd.DataFrame(rows)
            out_path = out_dir / f"{iso}_{block_key}_factor_constituents.csv"
            df_out.to_csv(out_path, index=False)
            written += 1

    print(f"Wrote {written} factor constituent map files to: {out_dir}")


if __name__ == "__main__":
    main()
