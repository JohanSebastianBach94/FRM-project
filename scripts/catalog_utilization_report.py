from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class Paths:
    root: Path
    catalog_csv: Path
    block_def_json: Path
    dcc_results_blocks_dir: Path
    dcc_block_fit_summary_csv: Path
    latest_replay_dir: Path | None
    latest_quality_audit_csv: Path | None
    out_dir: Path
    out_md: Path
    out_block_csv: Path
    out_series_csv: Path


def _find_latest_replay_dir(historical_replay_dir: Path) -> Path | None:
    if not historical_replay_dir.exists():
        return None

    # replay_YYYYMMDD_HHMMSS
    pat = re.compile(r"^replay_(\d{8})_(\d{6})$")
    candidates: list[tuple[str, Path]] = []
    for child in historical_replay_dir.iterdir():
        if not child.is_dir():
            continue
        m = pat.match(child.name)
        if not m:
            continue
        key = f"{m.group(1)}_{m.group(2)}"
        candidates.append((key, child))
    if not candidates:
        return None
    return max(candidates, key=lambda x: x[0])[1]


def _parse_semicolon_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []
    s = str(value).strip()
    if not s:
        return []
    return [x.strip() for x in s.split(";") if x.strip()]


def _load_catalog(catalog_csv: Path) -> pd.DataFrame:
    catalog = pd.read_csv(catalog_csv)
    catalog["series"] = catalog["series"].astype(str)
    catalog = catalog.drop_duplicates(subset=["series"]).set_index("series", drop=False)
    return catalog


def _load_block_def(block_def_json: Path) -> dict[str, list[str]]:
    raw = json.loads(block_def_json.read_text(encoding="utf-8"))

    by_block: dict[str, list[str]] = {}
    for iso, entry in raw.items():
        blocks = entry.get("blocks") or []
        for b in blocks:
            key = b.get("key")
            if not key:
                continue
            block_id = f"{iso.lower()}_{key}"
            series_codes = b.get("series_codes") or []
            by_block[block_id] = [str(x) for x in series_codes]
    return by_block


def _load_valued_series_by_block(dcc_results_blocks_dir: Path) -> dict[str, list[str]]:
    valued: dict[str, list[str]] = {}
    if not dcc_results_blocks_dir.exists():
        return valued

    for block_dir in dcc_results_blocks_dir.iterdir():
        if not block_dir.is_dir():
            continue
        block_id = block_dir.name
        if block_id == "miscellaneous":
            continue

        params_csv = block_dir / "dcc_garch_parameters.csv"
        if not params_csv.exists():
            continue
        df = pd.read_csv(params_csv, index_col=0)
        series = [str(x) for x in df.index.tolist()]
        valued[block_id] = series
    return valued


def _load_fit_summary_reasons(dcc_block_fit_summary_csv: Path) -> pd.DataFrame:
    if not dcc_block_fit_summary_csv.exists():
        return pd.DataFrame(columns=["block_id", "missing_series", "already_assigned"]).set_index("block_id")
    df = pd.read_csv(dcc_block_fit_summary_csv)
    if "block_id" not in df.columns:
        # Fallback; older file schema
        return pd.DataFrame(columns=["block_id", "missing_series", "already_assigned"]).set_index("block_id")
    df = df[[c for c in ["block_id", "missing_series", "already_assigned"] if c in df.columns]].copy()
    return df.set_index("block_id", drop=False)


def _load_quality_audit(latest_quality_audit_csv: Path | None) -> pd.DataFrame | None:
    if latest_quality_audit_csv is None or (not latest_quality_audit_csv.exists()):
        return None
    return pd.read_csv(latest_quality_audit_csv)


def build_paths(root: Path) -> Paths:
    historical_replay_dir = root / "analysis_outputs" / "scenarios" / "latest" / "historical_replay"
    latest_replay_dir = _find_latest_replay_dir(historical_replay_dir)
    latest_quality_audit_csv = None
    if latest_replay_dir is not None:
        candidate = latest_replay_dir / "series_quality_audit.csv"
        if candidate.exists():
            latest_quality_audit_csv = candidate

    out_dir = root / "analysis_outputs" / "diagnostics"
    return Paths(
        root=root,
        catalog_csv=root / "catalog.csv",
        block_def_json=root / "outputs" / "country_block_definition.json",
        dcc_results_blocks_dir=root / "DCC GARCH MODEL" / "results" / "blocks",
        dcc_block_fit_summary_csv=root / "DCC GARCH MODEL" / "results" / "block_fit_summary.csv",
        latest_replay_dir=latest_replay_dir,
        latest_quality_audit_csv=latest_quality_audit_csv,
        out_dir=out_dir,
        out_md=out_dir / "catalog_utilization_report.md",
        out_block_csv=out_dir / "catalog_utilization_by_block.csv",
        out_series_csv=out_dir / "catalog_utilization_by_series.csv",
    )


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    p = build_paths(root)
    p.out_dir.mkdir(parents=True, exist_ok=True)

    catalog = _load_catalog(p.catalog_csv)
    configured_by_block = _load_block_def(p.block_def_json)
    valued_by_block = _load_valued_series_by_block(p.dcc_results_blocks_dir)
    fit_summary = _load_fit_summary_reasons(p.dcc_block_fit_summary_csv)
    audit = _load_quality_audit(p.latest_quality_audit_csv)

    valued_by_block_configured = {k: v for k, v in valued_by_block.items() if k in configured_by_block}
    extra_valued_blocks = sorted(set(valued_by_block) - set(configured_by_block))

    all_configured = sorted({s for series in configured_by_block.values() for s in series})
    all_valued_configured = sorted({s for series in valued_by_block_configured.values() for s in series})
    all_valued_all = sorted({s for series in valued_by_block.values() for s in series})

    # Series-level audit stats (optional)
    audit_series_stats = None
    if audit is not None and "series" in audit.columns:
        audit = audit.copy()
        # Normalize boolean-like columns in case they come in as strings
        for col in ["flag_stale", "flag_spike", "flag_stale_spike"]:
            if col in audit.columns:
                if audit[col].dtype == object:
                    audit[col] = audit[col].astype(str).str.lower().isin(["true", "1", "yes"])
        audit_series_stats = (
            audit.groupby("series", dropna=False)
            .agg(
                audit_rows=("series", "size"),
                stale_spike_rows=("flag_stale_spike", "sum") if "flag_stale_spike" in audit.columns else ("series", "size"),
                stale_rows=("flag_stale", "sum") if "flag_stale" in audit.columns else ("series", "size"),
                spike_rows=("flag_spike", "sum") if "flag_spike" in audit.columns else ("series", "size"),
            )
            .reset_index()
        )

    # Block-level utilization
    block_rows: list[dict[str, object]] = []
    for block_id, configured_series in sorted(configured_by_block.items()):
        configured_set = set(configured_series)
        valued_set = set(valued_by_block_configured.get(block_id, []))

        missing_in_fit = sorted(configured_set - valued_set)
        extra_in_fit = sorted(valued_set - configured_set)

        missing_declared = []
        already_assigned = []
        if block_id in fit_summary.index:
            missing_declared = _parse_semicolon_list(fit_summary.loc[block_id].get("missing_series"))
            already_assigned = _parse_semicolon_list(fit_summary.loc[block_id].get("already_assigned"))

        # Where do duplicates end up?
        reassigned_to: dict[str, list[str]] = {}
        for s in already_assigned:
            holders = [b for b, series in valued_by_block.items() if s in set(series)]
            # only report reassignment targets that are configured blocks
            reassigned_to[s] = sorted([h for h in holders if h in configured_by_block])

        block_rows.append(
            {
                "block_id": block_id,
                "configured_n": len(configured_set),
                "valued_n": len(valued_set),
                "missing_in_fit_n": len(missing_in_fit),
                "extra_in_fit_n": len(extra_in_fit),
                "configured_series": ";".join(sorted(configured_set)),
                "valued_series": ";".join(sorted(valued_set)),
                "missing_in_fit": ";".join(missing_in_fit),
                "missing_declared": ";".join(missing_declared),
                "already_assigned": ";".join(already_assigned),
                "already_assigned_reassigned_to": json.dumps(reassigned_to, ensure_ascii=False),
            }
        )

    block_df = pd.DataFrame(block_rows)
    block_df.to_csv(p.out_block_csv, index=False)

    # Series-level utilization
    series_rows: list[dict[str, object]] = []
    all_series = sorted(set(all_configured) | set(all_valued_all))
    valued_blocks_by_series: dict[str, list[str]] = {}
    configured_blocks_by_series: dict[str, list[str]] = {}
    for block_id, series in configured_by_block.items():
        for s in series:
            configured_blocks_by_series.setdefault(s, []).append(block_id)
    for block_id, series in valued_by_block.items():
        for s in series:
            valued_blocks_by_series.setdefault(s, []).append(block_id)

    for s in all_series:
        cat = catalog.loc[s] if s in catalog.index else None
        series_rows.append(
            {
                "series": s,
                "in_catalog": bool(cat is not None),
                "frequency_label": None if cat is None else cat.get("frequency_label"),
                "median_gap_days": None if cat is None else cat.get("median_gap_days"),
                "do_not_use": None if cat is None else cat.get("do_not_use"),
                "configured_blocks_n": len(set(configured_blocks_by_series.get(s, []))),
                "valued_blocks_n": len(set(valued_blocks_by_series.get(s, []))),
                "valued_blocks_n_configured": len(set([b for b in valued_blocks_by_series.get(s, []) if b in configured_by_block])),
                "configured_blocks": ";".join(sorted(set(configured_blocks_by_series.get(s, [])))),
                "valued_blocks": ";".join(sorted(set(valued_blocks_by_series.get(s, [])))),
                "configured_not_valued_anywhere": (s in configured_blocks_by_series) and (s not in valued_blocks_by_series),
            }
        )

    series_df = pd.DataFrame(series_rows)
    if audit_series_stats is not None:
        series_df = series_df.merge(audit_series_stats, on="series", how="left")
        series_df[["audit_rows", "stale_spike_rows", "stale_rows", "spike_rows"]] = series_df[
            ["audit_rows", "stale_spike_rows", "stale_rows", "spike_rows"]
        ].fillna(0)
    series_df.to_csv(p.out_series_csv, index=False)

    # Markdown report
    n_cfg = len(set(all_configured))
    n_val = len(set(all_valued_configured))
    cfg_not_val = series_df[series_df["configured_not_valued_anywhere"]].copy()

    # Frequency / stale+spike focus
    monthly_stale_spike = pd.DataFrame()
    if "stale_spike_rows" in series_df.columns:
        monthly_stale_spike = (
            series_df[(series_df["frequency_label"] == "monthly") & (series_df["stale_spike_rows"] > 0)]
            .sort_values(["stale_spike_rows", "configured_blocks_n"], ascending=[False, False])
            .head(20)
        )

    lines: list[str] = []
    lines.append("# Catalog Utilization Report\n")
    lines.append("This report compares three things:\n")
    lines.append("- **Configured** series per block from `outputs/country_block_definition.json`\n")
    lines.append("- **Valued (fit-used)** series per block from `DCC GARCH MODEL/results/blocks/*/dcc_garch_parameters.csv`\n")
    if p.latest_quality_audit_csv is not None:
        lines.append(
            f"- **Replay quality flags** from `{p.latest_quality_audit_csv.relative_to(root).as_posix()}` (stale/spike/stale+spike)\n"
        )
    else:
        lines.append("- **Replay quality flags**: not available (no latest Step 11.8 audit found)\n")

    lines.append("## Summary\n")
    lines.append(f"- Unique configured series across blocks: **{n_cfg}**\n")
    lines.append(f"- Unique valued series across configured blocks: **{n_val}**\n")
    if n_cfg:
        lines.append(f"- Configured→valued coverage (unique series): **{n_val / n_cfg:.1%}**\n")
    lines.append(f"- Blocks configured: **{len(configured_by_block)}**\n")
    lines.append(f"- Configured blocks with fit outputs found: **{len(valued_by_block_configured)}**\n")
    if extra_valued_blocks:
        lines.append(f"- Extra fitted blocks not in block definition JSON: **{len(extra_valued_blocks)}**\n")
        lines.append(f"  - {', '.join(extra_valued_blocks[:12])}{' …' if len(extra_valued_blocks) > 12 else ''}\n")
    lines.append("\n")

    if not cfg_not_val.empty:
        lines.append("## Configured But Not Valued Anywhere\n")
        lines.append(
            "These series are present in `country_block_definition.json` but were not used in any fitted block (often because they are missing in the cleaned panel):\n"
        )
        preview = cfg_not_val.sort_values(["configured_blocks_n"], ascending=False).head(40)
        for _, r in preview.iterrows():
            freq = r.get("frequency_label")
            gap = r.get("median_gap_days")
            lines.append(
                f"- `{r['series']}` (freq={freq}, median_gap_days={gap}) configured_in={r['configured_blocks_n']} blocks\n"
            )
        if len(cfg_not_val) > len(preview):
            lines.append(f"- (… plus {len(cfg_not_val) - len(preview)} more)\n")
        lines.append("\n")
    else:
        lines.append("## Configured But Not Valued Anywhere\n")
        lines.append("- None detected.\n\n")

    lines.append("## Monthly Series With Stale+Spike Flags (Top 20)\n")
    if monthly_stale_spike.empty:
        lines.append("- None (or no Step 11.8 audit available).\n\n")
    else:
        for _, r in monthly_stale_spike.iterrows():
            lines.append(
                f"- `{r['series']}` stale+spike_rows={int(r['stale_spike_rows'])}, configured_blocks={int(r['configured_blocks_n'])}, valued_blocks={int(r['valued_blocks_n'])}\n"
            )
        lines.append("\n")

    lines.append("## Outputs\n")
    lines.append(f"- Block-level CSV: `{p.out_block_csv.relative_to(root).as_posix()}`\n")
    lines.append(f"- Series-level CSV: `{p.out_series_csv.relative_to(root).as_posix()}`\n")
    lines.append("\n")

    p.out_md.write_text("".join(lines), encoding="utf-8")
    print(f"Wrote: {p.out_md}")
    print(f"Wrote: {p.out_block_csv}")
    print(f"Wrote: {p.out_series_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
