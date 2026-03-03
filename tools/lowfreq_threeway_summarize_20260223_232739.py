import csv
from collections import Counter, defaultdict
from pathlib import Path


def main() -> None:
    root = Path(r"c:\Users\frank\Documents\FRM project")
    diag = root / "analysis_outputs" / "scenarios" / "latest" / "monte_carlo" / "diagnostics"

    diff_path = diag / "lowfreq_threeway_diff_20260223_232739.csv"
    out_flip_baseline_auto = diag / "lowfreq_fliplist_baseline_to_auto_20260223_232739.csv"
    out_flip_auto_meta = diag / "lowfreq_fliplist_auto_to_meta_20260223_232739.csv"

    rows: list[dict] = []
    with diff_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # normalize booleans stored as strings
            for k in [
                "baseline_is_low",
                "auto_is_low",
                "meta_neverblocks_is_low",
                "flip_baseline_to_auto",
                "flip_auto_to_meta",
                "flip_baseline_to_meta",
            ]:
                if k in r and r[k] != "":
                    v = r[k].strip().lower()
                    if v in {"true", "false"}:
                        r[k] = v == "true"
            rows.append(r)

    flips_ba = [r for r in rows if r.get("flip_baseline_to_auto") is True]
    flips_am = [r for r in rows if r.get("flip_auto_to_meta") is True]

    # write flip lists
    def write_flip_list(path: Path, flip_rows: list[dict], a_key: str, b_key: str) -> None:
        with path.open("w", newline="", encoding="utf-8") as f:
            fieldnames = [
                "iso",
                "factor",
                "metadata_block",
                "metadata_source",
                "metadata_is_low",
                "metadata_high_freq_share",
                a_key,
                b_key,
                "direction",
            ]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in flip_rows:
                a = r.get(a_key)
                b = r.get(b_key)
                direction = None
                if isinstance(a, bool) and isinstance(b, bool):
                    direction = "low->high" if (a is True and b is False) else ("high->low" if (a is False and b is True) else "")
                w.writerow(
                    {
                        "iso": r.get("iso"),
                        "factor": r.get("factor"),
                        "metadata_block": r.get("metadata_block"),
                        "metadata_source": r.get("metadata_source"),
                        "metadata_is_low": r.get("metadata_is_low"),
                        "metadata_high_freq_share": r.get("metadata_high_freq_share"),
                        a_key: a,
                        b_key: b,
                        "direction": direction,
                    }
                )

    write_flip_list(out_flip_baseline_auto, flips_ba, "baseline_is_low", "auto_is_low")
    write_flip_list(out_flip_auto_meta, flips_am, "auto_is_low", "meta_neverblocks_is_low")

    # summarize baseline->auto by direction and block
    dir_counts = Counter()
    by_block = Counter()
    by_iso = Counter()
    by_iso_block = Counter()

    for r in flips_ba:
        iso = r.get("iso")
        block = r.get("metadata_block") or "(unknown)"
        b = r.get("baseline_is_low")
        a = r.get("auto_is_low")
        if isinstance(b, bool) and isinstance(a, bool):
            dir_label = "low->high" if (b is True and a is False) else "high->low"
        else:
            dir_label = "unknown"
        dir_counts[dir_label] += 1
        by_block[block] += 1
        if iso:
            by_iso[iso] += 1
            by_iso_block[(iso, block)] += 1

    print("Baseline->Auto flip direction counts:", dict(dir_counts))
    print("Baseline->Auto flips by block (top):")
    for block, n in by_block.most_common(20):
        print(" ", block, n)

    print("\nAuto->Meta flips (override effect):")
    for r in flips_am:
        print(" ", r.get("iso"), r.get("factor"), "auto=", r.get("auto_is_low"), "meta=", r.get("meta_neverblocks_is_low"), "block=", r.get("metadata_block"))

    print("\nWrote:", out_flip_baseline_auto)
    print("Wrote:", out_flip_auto_meta)


if __name__ == "__main__":
    main()
