#!/usr/bin/env python3
"""Extract BIS LBS household-loans rows from the attached BIS CSV.

This is defensive: it streams the large CSV in chunks, finds rows that contain
case-insensitive matches for 'household' and 'loan' in any text column, and
writes a compact CSV of matches plus per-country timeseries CSVs when possible.

Usage:
  python scripts/extract_bis_household_loans.py

Writes:
  - data_repository/processed/bis_lbs_household_matches.csv
  - data_repository/processed/bis_lbs_household_<ISO>.csv  (one per country if pivotable)
"""
import os
import re
import csv
import sys
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / 'data_repository' / 'raw' / 'structural'
INFILE = RAW / 'bis_lbs_d_pub.csv'
OUT_DIR = BASE / 'data_repository' / 'processed'
OUT_DIR.mkdir(parents=True, exist_ok=True)

if not INFILE.exists():
    print(f"Input file not found: {INFILE}\nPlease ensure the BIS LBS CSV is present at that path.")
    sys.exit(2)

pattern = re.compile(r'household', flags=re.IGNORECASE)
loan_pattern = re.compile(r'loan', flags=re.IGNORECASE)

matches_file = OUT_DIR / 'bis_lbs_household_matches.csv'

print(f"Scanning {INFILE} for household-loans rows (streaming, schema-aware)...")
reader = pd.read_csv(INFILE, dtype=str, low_memory=False, chunksize=200000)
first_write = True
matched_rows = 0
for i, chunk in enumerate(reader):
    # ensure string dtype
    chunk = chunk.astype(str)

    # Typical BIS LBS layout: code column then label column for many fields.
    # We prefer to test the code columns (short names) if present, else the verbose labels.
    cp_sector_code = None
    cp_sector_label = None
    instr_code = None
    instr_label = None
    rep_cty_code = None
    rep_cty_label = None

    for c in chunk.columns:
        lc = c.lower()
        if lc == 'l_cp_sector':
            cp_sector_code = c
        if lc == 'counterparty sector' or lc == 'l_cp_sector_label' or 'counterparty' in lc and 'sector' in lc:
            cp_sector_label = c
        if lc == 'l_instr':
            instr_code = c
        if lc == 'type of instruments' or lc == 'l_instr_label' or 'instrument' in lc:
            instr_label = c
        if lc == 'l_rep_cty':
            rep_cty_code = c
        if lc == 'reporting country' or lc == 'l_rep_cty_label' or 'reporting' in lc and 'country' in lc:
            rep_cty_label = c

    # Build mask: counterparty = households AND instrument mentions loans/credit
    mask_hh = False
    if cp_sector_code and cp_sector_code in chunk.columns:
        mask_hh = chunk[cp_sector_code].str.strip().str.upper() == 'H'
    if (isinstance(mask_hh, bool) and not mask_hh) or (not isinstance(mask_hh, pd.Series)):
        mask_hh = pd.Series(False, index=chunk.index)
    if cp_sector_label and cp_sector_label in chunk.columns:
        mask_hh = mask_hh | chunk[cp_sector_label].str.contains('household', case=False, na=False)

    mask_instr = pd.Series(False, index=chunk.index)
    if instr_code and instr_code in chunk.columns:
        # codes like G (Loans and deposits) or B (Credit (loans & debt securities))
        mask_instr = mask_instr | chunk[instr_code].str.strip().str.upper().isin(['G', 'B'])
    if instr_label and instr_label in chunk.columns:
        mask_instr = mask_instr | chunk[instr_label].str.contains('loan', case=False, na=False) | chunk[instr_label].str.contains('credit', case=False, na=False)

    # final mask
    mask = mask_hh & mask_instr

    sel = chunk[mask]
    if sel.empty:
        continue
    # Append matches to a compact CSV for inspection
    if first_write:
        sel.to_csv(matches_file, index=False, mode='w')
        first_write = False
    else:
        sel.to_csv(matches_file, index=False, header=False, mode='a')
    matched_rows += len(sel)
    print(f"  chunk {i}: found {len(sel)} candidate rows (total {matched_rows})")

if matched_rows == 0:
    print("No candidate rows matched 'household' or 'loan' keywords. Review CSV schema manually.")
    sys.exit(1)

print(f"Wrote candidate rows to {matches_file} ({matched_rows} rows). Attempting to pivot per-country where possible.")

# Try to pivot into timeseries for each country if the CSV has PERIOD/DATE style columns or Year/Quarter/Value
df = pd.read_csv(matches_file, dtype=str)

# Heuristics: find wide-format time columns that look like 'YYYY-Qn'
time_columns = [c for c in df.columns if re.match(r'^\d{4}-Q\d+$', c)]
if time_columns:
    print(f"Detected wide-format time columns: {len(time_columns)} columns")
    # Prefer reporting country code column 'L_REP_CTY' if present
    country_col = 'L_REP_CTY' if 'L_REP_CTY' in df.columns else None
    if country_col is None:
        # try label name
        for candidate in ['Reporting country', 'reporting country', 'REPORTING COUNTRY', 'reporting_area']:
            if candidate in df.columns:
                country_col = candidate
                break

    if country_col is None:
        print("Could not detect country column automatically; leaving matched rows for manual inspection.")
    else:
        # Melt and aggregate per reporting country and period
        long = df.melt(id_vars=[country_col], value_vars=time_columns, var_name='period', value_name='value')
        long = long[long['value'].notna() & (long['value'] != '')]
        # coerce numeric values
        long['value'] = pd.to_numeric(long['value'].str.replace(',', ''), errors='coerce')
        long = long.dropna(subset=['value'])
        grouped = long.groupby(country_col)
        for iso, g in grouped:
            out = OUT_DIR / f"bis_lbs_household_{iso}.csv"
            out_df = g[['period', 'value']].sort_values('period')
            out_df.to_csv(out, index=False)
            print(f"  wrote {out} ({len(out_df)} rows)")
else:
    # Try long-form with Year/Quarter/Value columns
    # find year, quarter, value and country columns
    cols = {c.lower(): c for c in df.columns}
    year_col = cols.get('year') or cols.get('time') or cols.get('period')
    quarter_col = cols.get('quarter')
    value_col = cols.get('value') or cols.get('observation') or cols.get('obs')
    country_col = cols.get('country') or cols.get('reporting economy')
    if value_col and (year_col or quarter_col) and country_col:
        df2 = df[[country_col, year_col if year_col else quarter_col, value_col]]
        # create period if needed
        if year_col and quarter_col:
            df2['period'] = df2[year_col].astype(str) + '-Q' + df2[quarter_col].astype(str)
        else:
            df2 = df2.rename(columns={year_col or quarter_col: 'period'})
        for iso, g in df2.groupby(country_col):
            out = OUT_DIR / f"bis_lbs_household_{iso}.csv"
            g[['period', value_col]].to_csv(out, index=False, header=['period', 'value'])
            print(f"  wrote {out} ({len(g)} rows)")
    else:
        print("Could not automatically pivot matched rows into time series (unknown schema). Matched rows saved for manual inspection.")

print("Done.")
