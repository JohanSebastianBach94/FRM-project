import csv
from collections import defaultdict
from pathlib import Path


def load(path: Path) -> dict[tuple[str, str], dict]:
    rows: dict[tuple[str, str], dict] = {}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            iso = r.get("iso")
            factor = r.get("factor")
            if iso is None or factor is None:
                continue
            v = r.get("is_low_frequency")
            if v is None:
                continue
            is_low = v.strip().lower() == "true"
            r = dict(r)
            r["is_low_frequency"] = is_low
            rows[(iso, factor)] = r
    return rows


def main() -> None:
    root = Path(r"c:\Users\frank\Documents\FRM project")
    diag = root / "analysis_outputs" / "scenarios" / "latest" / "monte_carlo" / "diagnostics"

    baseline_path = diag / "lowfreq_classification.bak_20260223_221023.csv"
    auto_path = diag / "lowfreq_classification.auto_20260223_232739.csv"
    meta_path = diag / "lowfreq_classification.meta_neverblocks_20260223_232739.csv"

    out_csv = diag / "lowfreq_threeway_diff_20260223_232739.csv"
    out_counts = diag / "lowfreq_threeway_counts_20260223_232739.csv"

    baseline = load(baseline_path)
    auto = load(auto_path)
    meta = load(meta_path)

    keys = sorted(set(baseline) | set(auto) | set(meta))

    counts = defaultdict(
        lambda: {
            "baseline_low": 0,
            "baseline_total": 0,
            "auto_low": 0,
            "auto_total": 0,
            "meta_low": 0,
            "meta_total": 0,
        }
    )

    report_rows: list[dict] = []
    for iso, factor in keys:
        b = baseline.get((iso, factor))
        a = auto.get((iso, factor))
        m = meta.get((iso, factor))

        b_low = None if b is None else b["is_low_frequency"]
        a_low = None if a is None else a["is_low_frequency"]
        m_low = None if m is None else m["is_low_frequency"]

        if b_low is not None:
            counts[iso]["baseline_total"] += 1
            counts[iso]["baseline_low"] += int(bool(b_low))
        if a_low is not None:
            counts[iso]["auto_total"] += 1
            counts[iso]["auto_low"] += int(bool(a_low))
        if m_low is not None:
            counts[iso]["meta_total"] += 1
            counts[iso]["meta_low"] += int(bool(m_low))

        flip_b_to_a = (b_low is not None and a_low is not None and b_low != a_low)
        flip_a_to_m = (a_low is not None and m_low is not None and a_low != m_low)
        flip_b_to_m = (b_low is not None and m_low is not None and b_low != m_low)

        src_row = m or a
        report_rows.append(
            {
                "iso": iso,
                "factor": factor,
                "metadata_block": None if src_row is None else src_row.get("metadata_block"),
                "metadata_source": None if src_row is None else src_row.get("metadata_source"),
                "metadata_is_low": None if src_row is None else src_row.get("metadata_is_low"),
                "metadata_high_freq_share": None if src_row is None else src_row.get("metadata_high_freq_share"),
                "baseline_is_low": b_low,
                "auto_is_low": a_low,
                "meta_neverblocks_is_low": m_low,
                "flip_baseline_to_auto": flip_b_to_a,
                "flip_auto_to_meta": flip_a_to_m,
                "flip_baseline_to_meta": flip_b_to_m,
            }
        )

    # write diff report
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        fieldnames = list(report_rows[0].keys()) if report_rows else []
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(report_rows)

    # write counts
    with out_counts.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "iso",
            "baseline_low",
            "baseline_total",
            "auto_low",
            "auto_total",
            "meta_low",
            "meta_total",
            "baseline_low_share",
            "auto_low_share",
            "meta_low_share",
            "delta_auto_minus_baseline_low",
            "delta_meta_minus_auto_low",
            "delta_meta_minus_baseline_low",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for iso in sorted(counts.keys()):
            c = counts[iso]
            bshare = (c["baseline_low"] / c["baseline_total"]) if c["baseline_total"] else None
            ashare = (c["auto_low"] / c["auto_total"]) if c["auto_total"] else None
            mshare = (c["meta_low"] / c["meta_total"]) if c["meta_total"] else None
            w.writerow(
                {
                    "iso": iso,
                    "baseline_low": c["baseline_low"],
                    "baseline_total": c["baseline_total"],
                    "auto_low": c["auto_low"],
                    "auto_total": c["auto_total"],
                    "meta_low": c["meta_low"],
                    "meta_total": c["meta_total"],
                    "baseline_low_share": bshare,
                    "auto_low_share": ashare,
                    "meta_low_share": mshare,
                    "delta_auto_minus_baseline_low": c["auto_low"] - c["baseline_low"],
                    "delta_meta_minus_auto_low": c["meta_low"] - c["auto_low"],
                    "delta_meta_minus_baseline_low": c["meta_low"] - c["baseline_low"],
                }
            )

    flip_counts = {
        "baseline_to_auto": sum(1 for r in report_rows if r["flip_baseline_to_auto"]),
        "auto_to_meta": sum(1 for r in report_rows if r["flip_auto_to_meta"]),
        "baseline_to_meta": sum(1 for r in report_rows if r["flip_baseline_to_meta"]),
    }

    print("Wrote:", out_csv)
    print("Wrote:", out_counts)
    print("Flip counts:", flip_counts)

    # show override-effect flips (auto -> meta)
    override_flips = [r for r in report_rows if r["flip_auto_to_meta"]]
    if override_flips:
        print("\nAuto->Meta flips (override effect):")
        for r in override_flips:
            print(
                r["iso"],
                r["factor"],
                "auto=",
                r["auto_is_low"],
                "meta=",
                r["meta_neverblocks_is_low"],
                "block=",
                r.get("metadata_block"),
                "md_is_low=",
                r.get("metadata_is_low"),
                "md_high_share=",
                r.get("metadata_high_freq_share"),
            )


if __name__ == "__main__":
    main()
