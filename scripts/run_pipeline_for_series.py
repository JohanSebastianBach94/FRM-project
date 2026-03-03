"""Run match -> download -> annotate -> normalize for a single catalog series.

This script filters existing `analysis_outputs/bis_matches.csv` and
`analysis_outputs/bis_matches_normalized.csv` for the specified `catalog_series`.
If no matches exist for that series, it will run the full matcher (`match_bis.py`) to
regenerate match outputs, then proceed.

The script backs up original match files, writes filtered copies so the existing
`download_bis_series.py` can be used unchanged, runs the downloader, then runs
the annotator and normalizer. Finally it restores the original match files.

Usage: python scripts/run_pipeline_for_series.py <SERIES_ID>
Example: python scripts/run_pipeline_for_series.py ECBESTRVOLWGTTRMDMNRT
"""

import sys
from pathlib import Path
import shutil
import subprocess
import csv
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
OUT = BASE / 'analysis_outputs'
MATCH_RAW = OUT / 'bis_matches.csv'
MATCH_NORM = OUT / 'bis_matches_normalized.csv'
MATCH_RAW_BAK = OUT / 'bis_matches.csv.bak'
MATCH_NORM_BAK = OUT / 'bis_matches_normalized.csv.bak'

SCRIPTS = {
    'matcher': BASE / 'scripts' / 'match_bis.py',
    'downloader': BASE / 'scripts' / 'download_bis_series.py',
    'annotate': BASE / 'scripts' / 'annotate_bis_with_matches.py',
    'normalize': BASE / 'scripts' / 'normalize_bis_catalog.py',
}


def run_cmd(cmd):
    print('RUN:', ' '.join(cmd))
    subprocess.run(cmd, check=True)


def backup_matches():
    if MATCH_RAW.exists():
        shutil.copy2(MATCH_RAW, MATCH_RAW_BAK)
    if MATCH_NORM.exists():
        shutil.copy2(MATCH_NORM, MATCH_NORM_BAK)


def restore_matches():
    if MATCH_RAW_BAK.exists():
        shutil.copy2(MATCH_RAW_BAK, MATCH_RAW)
        MATCH_RAW_BAK.unlink()
    if MATCH_NORM_BAK.exists():
        shutil.copy2(MATCH_NORM_BAK, MATCH_NORM)
        MATCH_NORM_BAK.unlink()


def filter_matches_for_series(series_id: str) -> bool:
    """Filter existing match files for the given catalog_series. Returns True if any row written."""
    any_written = False
    # filter raw matches
    if MATCH_RAW.exists():
        df = pd.read_csv(MATCH_RAW, dtype=str).fillna('')
        sel = df[df['catalog_series'] == series_id]
        if not sel.empty:
            sel.to_csv(MATCH_RAW, index=False)
            any_written = True
    # filter normalized matches
    if MATCH_NORM.exists():
        dfn = pd.read_csv(MATCH_NORM, dtype=str).fillna('')
        seln = dfn[dfn['catalog_series'] == series_id]
        if not seln.empty:
            seln.to_csv(MATCH_NORM, index=False)
            any_written = True
    return any_written


def ensure_matches_for_series(series_id: str):
    # if filtering didn't find matches, run full matcher to regenerate, then filter
    found = filter_matches_for_series(series_id)
    if found:
        return True
    print('No existing matches for', series_id, '- regenerating matcher outputs')
    run_cmd([sys.executable, str(SCRIPTS['matcher'])])
    # try again
    return filter_matches_for_series(series_id)


def run_pipeline_for(series_id: str):
    backup_matches()
    try:
        ok = ensure_matches_for_series(series_id)
        if not ok:
            print('No matches available for', series_id)
            return 1

        # Run downloader (it reads analysis_outputs/bis_matches_normalized.csv)
        run_cmd([sys.executable, str(SCRIPTS['downloader'])])

        # Re-run annotator to update annotated catalog
        run_cmd([sys.executable, str(SCRIPTS['annotate'])])

        # Re-run normalizer to reshape final catalog
        run_cmd([sys.executable, str(SCRIPTS['normalize'])])

        print('Pipeline run complete for', series_id)
        return 0
    finally:
        # restore original match files so other work is unaffected
        restore_matches()


def main():
    if len(sys.argv) < 2:
        print('Usage: run_pipeline_for_series.py <SERIES_ID>')
        raise SystemExit(2)
    series_id = sys.argv[1]
    rc = run_pipeline_for(series_id)
    raise SystemExit(rc)


if __name__ == '__main__':
    main()
