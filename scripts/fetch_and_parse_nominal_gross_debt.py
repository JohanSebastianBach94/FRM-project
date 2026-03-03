#!/usr/bin/env python3
"""
Combined runner to fetch Eurostat/OECD payloads then parse them locally.

Usage (on a connected machine):
  pip install -r requirements.txt
  python scripts\fetch_and_parse_nominal_gross_debt.py

This simply invokes the fetch script and then the parser. It leaves raw payloads and canonical CSVs in `data_repository/raw/macro/`.
"""
import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

SCRIPTS = [
    'scripts/fetch_nominal_gross_debt_providers.py',
    'scripts/parse_nominal_gross_debt_payloads.py',
]

def run(cmd):
    print('Running:', cmd)
    res = subprocess.run([sys.executable, cmd], cwd=BASE_DIR)
    if res.returncode != 0:
        print('Command failed with exit code', res.returncode)
        sys.exit(res.returncode)


def main():
    for s in SCRIPTS:
        run(s)
    print('\nAll done. Check data_repository/raw/macro for outputs.')

if __name__ == '__main__':
    main()
