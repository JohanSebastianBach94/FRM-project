"""Diagnose missing DLL dependencies for Graphviz on Windows conda-prefix env.

This script recursively inspects PE import tables (via pefile) starting from
`gvplugin_pango.dll` and reports imported DLLs that cannot be found in the conda
prefix's `Library/bin`.

Run (from repo root):
    python tools/graphviz_dll_depcheck.py
    python tools/graphviz_dll_depcheck.py --start ".conda/Library/bin/dot.exe"
"""

from __future__ import annotations

import argparse
from pathlib import Path


def _norm(name: str) -> str:
    return name.strip().lower()


def main() -> None:
    try:
        import pefile
    except Exception as exc:  # pragma: no cover
        raise SystemExit("Missing dependency: pefile. Install with `pip install pefile`.") from exc

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start",
        type=Path,
        default=Path(".conda/Library/bin/gvplugin_pango.dll"),
        help="Start binary/DLL to inspect (relative to repo root by default)",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    prefix = repo_root / ".conda"
    bin_dir = prefix / "Library" / "bin"

    start = args.start
    if not start.is_absolute():
        start = (repo_root / start).resolve()
    if not start.exists():
        raise SystemExit(f"Not found: {start}")

    # Build a lookup of available DLLs in the env
    available = { _norm(p.name): p for p in bin_dir.glob("*.dll") }

    seen: set[str] = set()
    queue: list[Path] = [start]
    missing: set[str] = set()

    while queue:
        dll_path = queue.pop(0)
        key = _norm(dll_path.name)
        if key in seen:
            continue
        seen.add(key)

        pe = pefile.PE(str(dll_path))
        imports = []
        if hasattr(pe, "DIRECTORY_ENTRY_IMPORT"):
            for entry in pe.DIRECTORY_ENTRY_IMPORT:
                name = entry.dll.decode("utf-8", "ignore")
                imports.append(name)

        for dep in imports:
            dep_key = _norm(dep)
            # Skip Windows system DLLs and CRT API sets
            if dep_key.endswith(".dll") and (
                dep_key.startswith("api-ms-win-")
                or dep_key.startswith("ext-ms-")
                or dep_key in {"kernel32.dll", "user32.dll", "gdi32.dll", "advapi32.dll", "shell32.dll", "ole32.dll", "oleaut32.dll", "ws2_32.dll"}
            ):
                continue

            if dep_key in available:
                queue.append(available[dep_key])
            else:
                missing.add(dep)

    print(f"Start: {start}")
    print(f"Scanned DLLs: {len(seen)}")
    if missing:
        print("\nMissing (not found in .conda/Library/bin):")
        for m in sorted(missing, key=lambda s: s.lower()):
            print(f"- {m}")
    else:
        print("\nNo missing DLLs detected in .conda/Library/bin.")


if __name__ == "__main__":
    main()
