"""Generate diagnostics for literature-mode block PCA inputs.

Writes CSVs under analysis_outputs/diagnostics:
- literature_pc_input_series_by_iso_block.csv
    Compact/legacy: one row per (ISO, block) with the key series lists used for PCA.
- literature_pc_input_series_by_iso_block_detail.csv
    Expanded: includes additional stages (configured/present/dropped) for full transparency.
- literature_pc_input_series_long.csv
    Long form: one row per (ISO, block, stage, series_code) with the block method.

These diagnostics are designed to make governance vs data availability explicit:
- 'frozen_series' comes from outputs/country_block_definition.json
- 'configured_series' is frozen series minus catalog.csv do_not_use exclusions
- 'present_in_panel' indicates which configured series exist in the combined panel
- 'dropped_by_filters' indicates present series removed by coverage/min-obs
- 'pre_series_used' / 'post_series_used' are the actual literature PCA inputs

If a block ends up with zero usable series, it is still emitted with method='empty'.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _semi(values: list[str]) -> str:
    return ";".join([str(v) for v in values if str(v)])


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    frozen_path = root / "outputs" / "country_block_definition.json"
    manifests_dir = root / "analysis_outputs" / "literature_factors"
    out_dir = root / "analysis_outputs" / "diagnostics"
    out_dir.mkdir(parents=True, exist_ok=True)

    frozen = json.loads(frozen_path.read_text(encoding="utf-8"))

    rows: list[dict[str, object]] = []
    long_rows: list[dict[str, object]] = []

    for iso in sorted(frozen.keys()):
        iso_payload = frozen.get(iso) or {}
        blocks = iso_payload.get("blocks") or []

        block_order = {str(b.get("key")): i for i, b in enumerate(blocks) if b.get("key")}

        manifest_path = manifests_dir / f"{iso}_literature_manifest.json"
        block_details: dict[str, dict[str, object]] = {}
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                block_details = manifest.get("block_details") or {}
            except Exception:
                block_details = {}

        for block_obj in blocks:
            block_key = str(block_obj.get("key") or "")
            frozen_series = [str(s) for s in (block_obj.get("series_codes") or [])]
            details = block_details.get(block_key) or {}

            method = str(details.get("method") or "missing_manifest")
            factors = [str(s) for s in (details.get("factors") or [])]

            configured_series = [str(s) for s in (details.get("series_configured") or [])]
            present_in_panel = [str(s) for s in (details.get("series_present_in_panel") or [])]
            missing_in_panel = [str(s) for s in (details.get("series_missing_in_panel") or [])]
            dropped_by_filters = [str(s) for s in (details.get("series_dropped_by_filters") or [])]
            pre_series_used = [str(s) for s in (details.get("series_pre_dedupe") or [])]
            post_series_used = [str(s) for s in (details.get("series_post_dedupe") or [])]

            frozen_set = set(frozen_series)
            post_used_and_in_frozen = [s for s in post_series_used if s in frozen_set]

            rows.append(
                {
                    "iso": iso,
                    "block": block_key,
                    "method": method,
                    "n_frozen_series": int(len(frozen_series)),
                    "frozen_series": _semi(frozen_series),
                    "n_pre_coverage": int(len(pre_series_used)),
                    "pre_series_used": _semi(pre_series_used),
                    "n_post_dedupe": int(len(post_series_used)),
                    "post_series_used": _semi(post_series_used),
                    "n_post_used_and_in_frozen": int(len(post_used_and_in_frozen)),
                    "post_used_and_in_frozen": _semi(post_used_and_in_frozen),
                }
            )

            # Expanded row for full transparency.
            rows.append(
                {
                    "iso": iso,
                    "block": block_key,
                    "method": method,
                    "factors": _semi(factors),
                    "n_frozen_series": int(len(frozen_series)),
                    "frozen_series": _semi(frozen_series),
                    "n_configured_series": int(len(configured_series)),
                    "configured_series": _semi(configured_series),
                    "n_present_in_panel": int(len(present_in_panel)),
                    "present_in_panel": _semi(present_in_panel),
                    "n_missing_in_panel": int(len(missing_in_panel)),
                    "missing_in_panel": _semi(missing_in_panel),
                    "n_dropped_by_filters": int(len(dropped_by_filters)),
                    "dropped_by_filters": _semi(dropped_by_filters),
                    "n_pre_coverage": int(len(pre_series_used)),
                    "pre_series_used": _semi(pre_series_used),
                    "n_post_dedupe": int(len(post_series_used)),
                    "post_series_used": _semi(post_series_used),
                    "n_post_used_and_in_frozen": int(len(post_used_and_in_frozen)),
                    "post_used_and_in_frozen": _semi(post_used_and_in_frozen),
                    "__row_type": "detail",
                }
            )

            # Long-form: one row per series per stage.
            stages: list[tuple[str, list[str]]] = [
                ("frozen", frozen_series),
                ("configured", configured_series),
                ("present_in_panel", present_in_panel),
                ("missing_in_panel", missing_in_panel),
                ("dropped_by_filters", dropped_by_filters),
                ("pre_dedupe", pre_series_used),
                ("post_dedupe", post_series_used),
            ]
            for stage, series_list in stages:
                for series_code in series_list:
                    long_rows.append(
                        {
                            "iso": iso,
                            "block": block_key,
                            "stage": stage,
                            "series_code": series_code,
                            "method": method,
                        }
                    )

    df_all = pd.DataFrame(rows)
    # Split compact vs detail (detail rows carry the '__row_type' marker).
    if "__row_type" in df_all.columns:
        df_detail = df_all[df_all["__row_type"].fillna("") == "detail"].drop(columns=["__row_type"], errors="ignore")
        df_compact = df_all[df_all.get("__row_type").isna()].copy()
    else:
        df_compact = df_all.copy()
        df_detail = pd.DataFrame()
    df_long = pd.DataFrame(long_rows)

    # Preserve within-ISO block order as in the frozen artifact.
    def _order(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        # Recompute block order per ISO using the frozen artifact keys.
        iso_to_order: dict[str, dict[str, int]] = {}
        for iso_key in sorted(frozen.keys()):
            bs = (frozen.get(iso_key) or {}).get("blocks") or []
            iso_to_order[iso_key] = {str(b.get("key")): i for i, b in enumerate(bs) if b.get("key")}
        tmp = df.copy()
        tmp["__block_order"] = [iso_to_order.get(str(i), {}).get(str(b), 10_000) for i, b in zip(tmp["iso"], tmp["block"]) ]
        tmp = tmp.sort_values(["iso", "__block_order", "block"]).drop(columns=["__block_order"]).reset_index(drop=True)
        return tmp

    df_compact = _order(df_compact)
    df_detail = _order(df_detail)
    df_long = df_long.sort_values(["iso", "block", "stage", "series_code"]).reset_index(drop=True)

    compact_cols = [
        "iso",
        "block",
        "method",
        "n_frozen_series",
        "frozen_series",
        "n_pre_coverage",
        "pre_series_used",
        "n_post_dedupe",
        "post_series_used",
        "n_post_used_and_in_frozen",
        "post_used_and_in_frozen",
    ]
    # Keep only the compact columns (in order) for readability.
    df_compact_out = df_compact[[c for c in compact_cols if c in df_compact.columns]].copy()

    (out_dir / "literature_pc_input_series_by_iso_block.csv").write_text(
        df_compact_out.to_csv(index=False),
        encoding="utf-8",
    )
    if not df_detail.empty:
        (out_dir / "literature_pc_input_series_by_iso_block_detail.csv").write_text(
            df_detail.to_csv(index=False),
            encoding="utf-8",
        )
    (out_dir / "literature_pc_input_series_long.csv").write_text(
        df_long.to_csv(index=False),
        encoding="utf-8",
    )

    print(f"Wrote {len(df_compact_out)} rows: analysis_outputs/diagnostics/literature_pc_input_series_by_iso_block.csv")
    if not df_detail.empty:
        print(
            f"Wrote {len(df_detail)} rows: analysis_outputs/diagnostics/literature_pc_input_series_by_iso_block_detail.csv"
        )
    print(f"Wrote {len(df_long)} rows: analysis_outputs/diagnostics/literature_pc_input_series_long.csv")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
