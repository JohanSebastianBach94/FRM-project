import argparse, json
from pathlib import Path

def load_episode_diagnostics(run_dir: Path):
    eps_dir = run_dir / "episodes"
    out = {}
    for ep_path in sorted(eps_dir.glob("*/episode_diagnostics.json")):
        episode = ep_path.parent.name
        out[episode] = json.loads(ep_path.read_text(encoding="utf-8"))
    return out


def extract_block_max_abs_z(diag: dict):
    # Current layout: only flagged blocks are present
    # diag["flagged_blocks"] = [ {"block_id": ..., "max_abs_z_replay": ...}, ... ]
    flagged = diag.get("flagged_blocks")
    if isinstance(flagged, list):
        res = {}
        for item in flagged:
            if not isinstance(item, dict):
                continue
            block_id = item.get("block_id")
            value = item.get("max_abs_z_replay")
            if isinstance(block_id, str) and isinstance(value, (int, float)):
                res[block_id] = float(value)
        if res:
            return res

    # Older layouts (keep for robustness)
    if isinstance(diag.get("blocks"), dict):
        res = {}
        for block_id, b in diag["blocks"].items():
            if isinstance(b, dict) and "max_abs_z_replay" in b:
                res[block_id] = b["max_abs_z_replay"]
        if res:
            return res

    if isinstance(diag.get("block_metrics"), dict):
        res = {}
        for block_id, b in diag["block_metrics"].items():
            if isinstance(b, dict) and "max_abs_z_replay" in b:
                res[block_id] = b["max_abs_z_replay"]
        if res:
            return res

    return {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old", required=True, help="Old replay run id (e.g. replay_YYYYMMDD_HHMMSS)")
    ap.add_argument("--new", required=True, help="New replay run id")
    ap.add_argument("--root", default="analysis_outputs/scenarios/latest/historical_replay", help="Replay root dir")
    ap.add_argument("--block-ids", default="", help="Comma-separated block ids to report")
    args = ap.parse_args()

    root = Path(args.root)
    old_dir = root / args.old
    new_dir = root / args.new

    if not old_dir.exists():
        raise SystemExit(f"Old run not found: {old_dir}")
    if not new_dir.exists():
        raise SystemExit(f"New run not found: {new_dir}")

    wanted = [b.strip() for b in args.block_ids.split(",") if b.strip()]
    old_eps = load_episode_diagnostics(old_dir)
    new_eps = load_episode_diagnostics(new_dir)

    episodes = sorted(set(old_eps) & set(new_eps))
    if not episodes:
        raise SystemExit("No overlapping episodes found between runs")

    print(f"Comparing runs:\n  old={args.old}\n  new={args.new}\n  episodes={len(episodes)}")

    for ep in episodes:
        od = old_eps[ep]
        nd = new_eps[ep]

        om = extract_block_max_abs_z(od)
        nm = extract_block_max_abs_z(nd)

        old_n_flagged_total = od.get("n_flagged_blocks")
        new_n_flagged_total = nd.get("n_flagged_blocks")

        if wanted:
            old_subset = {b: om.get(b) for b in wanted if b in om}
            new_subset = {b: nm.get(b) for b in wanted if b in nm}
            old_n_flagged = len(old_subset)
            new_n_flagged = len(new_subset)
            old_worst = max(old_subset.values()) if old_subset else None
            new_worst = max(new_subset.values()) if new_subset else None
        else:
            old_n_flagged = old_n_flagged_total
            new_n_flagged = new_n_flagged_total
            old_worst = max(om.values()) if om else None
            new_worst = max(nm.values()) if nm else None

        if wanted:
            blocks = wanted
        else:
            blocks = sorted(set(om) | set(nm))

        rows = []
        for b in blocks:
            o = om.get(b)
            n = nm.get(b)
            if o is None and n is None:
                continue
            rows.append((b, o, n))

        # Sort by old then new, descending (None last)
        def sort_key(t):
            _b, o, n = t
            o_key = o if isinstance(o, (int, float)) else float("-inf")
            n_key = n if isinstance(n, (int, float)) else float("-inf")
            return (-o_key, -n_key, _b)

        rows.sort(key=sort_key)

        print("\n" + ep)
        if wanted:
            print(
                f"  flagged blocks (subset): old={old_n_flagged} new={new_n_flagged} "
                f"| totals: old={old_n_flagged_total} new={new_n_flagged_total}"
            )
        else:
            print(f"  flagged blocks: old={old_n_flagged} new={new_n_flagged}")
        print(f"  worst max_abs_z_replay: old={old_worst} new={new_worst}")

        for b, o, n in rows:
            if isinstance(o, (int, float)) and isinstance(n, (int, float)):
                print(f"  {b:28s}  old={o:12.4f}  new={n:12.4f}  delta={(n - o):12.4f}")
            else:
                print(f"  {b:28s}  old={o!s:>12}  new={n!s:>12}")

if __name__ == "__main__":
    main()
