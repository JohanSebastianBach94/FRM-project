#!/usr/bin/env python3
"""Generalized extractor for BIS LBS counterparty series (e.g. Private NFC).

This script streams the large `bis_lbs_d_pub.csv`, filters rows by
counterparty (e.g. 'Non-financial') and instrument patterns (e.g. 'loans', 'total'),
melts wide time columns, aggregates by reporting country and period, and writes
per-country canonical CSVs in `data_repository/processed/`.

Usage examples:
  python scripts/extract_bis_counterparty.py --counterparty "Non-financial" --instruments loans,total

Outputs (examples):
  data_repository/processed/BIS_LBS_Private_NFC_Loans_DEU.csv
  data_repository/processed/BIS_LBS_Private_NFC_Total_DEU.csv
"""
import argparse
import re
from pathlib import Path
import sys
import pandas as pd


BASE = Path(__file__).resolve().parents[1]
RAW = BASE / 'data_repository' / 'raw' / 'structural'
DEFAULT_INFILE = RAW / 'bis_lbs_d_pub.csv'
OUT_DIR = BASE / 'data_repository' / 'processed'
OUT_DIR.mkdir(parents=True, exist_ok=True)


def find_candidate_columns(cols):
    lower = [c.lower() for c in cols]
    def find(*candidates):
        for cand in candidates:
            for i, lc in enumerate(lower):
                if cand in lc:
                    return cols[i]
        return None

    cp_code = find('l_cp_sector', 'cp_sector', 'cp_sector_code')
    cp_label = find('counterparty', 'l_cp_sector_label', 'cp_sector_label')
    instr_code = find('l_instr', 'instr', 'instrument_code')
    instr_label = find('instrument', 'l_instr_label', 'type of instruments')
    rep_cty = find('l_rep_cty', 'reporting country', 'reporting', 'reporting_area')
    return cp_code, cp_label, instr_code, instr_label, rep_cty


def detect_time_columns(df):
    # Look for columns like '1977-Q4' or '1990-01-31' or '1990-01'
    time_cols = [c for c in df.columns if re.match(r'^\d{4}-Q\d+$', c) or re.match(r'^\d{4}-\d{2}(-\d{2})?$', c)]
    return time_cols


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--counterparty', required=True, help="Counterparty text to match (case-insensitive), e.g. 'Non-financial')")
    p.add_argument('--instruments', required=True, help="Comma-separated instrument name patterns, e.g. 'loans,total'")
    p.add_argument('--infile', default=str(DEFAULT_INFILE), help='Path to bis_lbs_d_pub.csv')
    p.add_argument('--outdir', default=str(OUT_DIR), help='Output directory for processed CSVs')
    p.add_argument('--chunksize', type=int, default=200000, help='CSV chunk size')
    return p.parse_args()


def main():
    args = parse_args()
    infile = Path(args.infile)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not infile.exists():
        print(f"Input file not found: {infile}")
        sys.exit(2)

    instrument_patterns = [s.strip().lower() for s in args.instruments.split(',') if s.strip()]
    cp_pattern = args.counterparty.lower()

    print(f"Scanning {infile} for counterparty containing '{cp_pattern}' and instruments {instrument_patterns} (streaming)...")

    reader = pd.read_csv(infile, dtype=str, low_memory=False, chunksize=args.chunksize)

    # We'll collect matching rows into separate temporary CSVs by instrument pattern
    temp_matches = {pat: outdir / f'bis_lbs_matches_{pat}.csv' for pat in instrument_patterns}
    first_write = {pat: True for pat in instrument_patterns}
    total_found = {pat: 0 for pat in instrument_patterns}

    for i, chunk in enumerate(reader):
        chunk = chunk.astype(str)
        if i == 0:
            cp_code, cp_label, instr_code, instr_label, rep_cty = find_candidate_columns(chunk.columns)
            print('Detected columns ->', dict(cp_code=cp_code, cp_label=cp_label, instr_code=instr_code, instr_label=instr_label, rep_cty=rep_cty))

        # Build counterparty mask
        mask_cp = pd.Series(False, index=chunk.index)
        if cp_label and cp_label in chunk.columns:
            mask_cp = chunk[cp_label].str.contains(cp_pattern, case=False, na=False)
        if cp_code and cp_code in chunk.columns:
            # sometimes codes exist; try match common non-financial sector codes (S11 family) if label match failed
            mask_cp = mask_cp | chunk[cp_code].str.contains('2240|224|S.11|S11', case=False, na=False)

        if not mask_cp.any():
            # nothing in this chunk
            continue

        # For each instrument pattern, build instrument mask and write matches
        for pat in instrument_patterns:
            mask_instr = pd.Series(False, index=chunk.index)
            if instr_label and instr_label in chunk.columns:
                mask_instr = chunk[instr_label].str.contains(pat, case=False, na=False)
            if instr_code and instr_code in chunk.columns:
                # heuristics: loans often coded with letters like 'G' in BIS LBS; include those if label match fails
                mask_instr = mask_instr | chunk[instr_code].str.contains('G|B|L', case=False, na=False)

            sel = chunk[mask_cp & mask_instr]
            if sel.empty:
                continue
            total_found[pat] += len(sel)
            tm = temp_matches[pat]
            if first_write[pat]:
                sel.to_csv(tm, index=False, mode='w')
                first_write[pat] = False
            else:
                sel.to_csv(tm, index=False, header=False, mode='a')
            print(f"  chunk {i}: pattern '{pat}' found {len(sel)} rows (total {total_found[pat]})")

    for pat, count in total_found.items():
        if count == 0:
            print(f"No rows found for instrument pattern '{pat}'.")
        else:
            print(f"Wrote {count} matched rows for pattern '{pat}' to {temp_matches[pat]}")

    # For each instrument pattern, try to pivot/melt and aggregate per reporting country
    for pat, tm in temp_matches.items():
        if not tm.exists():
            continue
        df = pd.read_csv(tm, dtype=str)
        time_cols = detect_time_columns(df)
        # detect country column heuristics
        rep_col = None
        for c in df.columns:
            lc = c.lower()
            if 'l_rep_cty' == lc or 'reporting' in lc and 'country' in lc:
                rep_col = c
                break
        if rep_col is None:
            # fallback to any column containing 'rep' or 'country'
            for c in df.columns:
                if 'rep' in c.lower() or 'country' in c.lower():
                    rep_col = c
                    break

        if time_cols:
            # melt and aggregate
            long = df.melt(id_vars=[rep_col] if rep_col else [], value_vars=time_cols, var_name='period', value_name='value')
            long = long[long['value'].notna() & (long['value'] != '')]
            long['value'] = pd.to_numeric(long['value'].str.replace(',', ''), errors='coerce')
            long = long.dropna(subset=['value'])
            if rep_col:
                grouped = long.groupby(rep_col)
                for iso, g in grouped:
                    out_name = outdir / f'BIS_LBS_Private_NFC_{pat.capitalize()}_{iso}.csv'
                    out_df = g[['period', 'value']].sort_values('period')
                    out_df.to_csv(out_name, index=False)
                    print(f"  wrote {out_name} ({len(out_df)} rows)")
            else:
                # no country column, write single aggregated file
                out_name = outdir / f'BIS_LBS_Private_NFC_{pat.capitalize()}_ALL.csv'
                agg = long.groupby('period')['value'].sum().reset_index()
                agg.to_csv(out_name, index=False)
                print(f"  wrote {out_name} ({len(agg)} rows)")
        else:
            # long-form fallback
            cols = {c.lower(): c for c in df.columns}
            year_col = cols.get('year') or cols.get('time') or cols.get('period')
            quarter_col = cols.get('quarter')
            value_col = cols.get('value') or cols.get('observation') or cols.get('obs')
            country_col = None
            for k in ('reporting country', 'reporting', 'l_rep_cty', 'country'):
                if k in cols:
                    country_col = cols[k]
                    break
            if value_col and (year_col or quarter_col):
                df2 = df[[country_col, year_col if year_col else quarter_col, value_col]]
                if year_col and quarter_col:
                    df2['period'] = df2[year_col].astype(str) + '-Q' + df2[quarter_col].astype(str)
                else:
                    df2 = df2.rename(columns={year_col or quarter_col: 'period'})
                if country_col:
                    for iso, g in df2.groupby(country_col):
                        out_name = outdir / f'BIS_LBS_Private_NFC_{pat.capitalize()}_{iso}.csv'
                        g[['period', value_col]].to_csv(out_name, index=False, header=['period', 'value'])
                        print(f"  wrote {out_name} ({len(g)} rows)")
                else:
                    out_name = outdir / f'BIS_LBS_Private_NFC_{pat.capitalize()}_ALL.csv'
                    df2[['period', value_col]].to_csv(out_name, index=False, header=['period', 'value'])
                    print(f"  wrote {out_name} ({len(df2)} rows)")
            else:
                print(f"Could not pivot matches for pattern '{pat}' (unknown time schema). Matched rows: {len(df)}")

    print('Extraction complete.')


if __name__ == '__main__':
    main()
