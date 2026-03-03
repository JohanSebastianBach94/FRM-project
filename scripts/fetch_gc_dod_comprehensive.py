#!/usr/bin/env python3
"""
Comprehensive GC.DOD fetcher - tries multiple providers in sequence.

Attempts (in order):
1. World Bank WDI API (GC.DOD.TOTL.GD.ZS)
2. IMF IFS via SDMX (Government debt)
3. Eurostat (gov_10_gdp - Government consolidated gross debt)
4. OECD (GOV_DEBT)

Run this script on a machine with Internet access:
    python scripts/fetch_gc_dod_comprehensive.py

Output: Updates data_repository/raw/macro/wb_GC.DOD.TOTL.GD.ZS_{ISO}.json
        and writes general_government_gross_debt_pct_gdp_{ISO}.csv
"""
import urllib.request
import urllib.error
import json
import csv
import os
import time
import argparse
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
MACRO_DIR = os.path.join(BASE_DIR, 'data_repository', 'raw', 'macro')
os.makedirs(MACRO_DIR, exist_ok=True)

COUNTRIES = {
    'USA': {'wb': 'USA', 'iso2': 'US', 'eurostat': 'US', 'oecd': 'USA'},
    'DEU': {'wb': 'DEU', 'iso2': 'DE', 'eurostat': 'DE', 'oecd': 'DEU'},
    'FRA': {'wb': 'FRA', 'iso2': 'FR', 'eurostat': 'FR', 'oecd': 'FRA'},
    'ITA': {'wb': 'ITA', 'iso2': 'IT', 'eurostat': 'IT', 'oecd': 'ITA'},
    'ESP': {'wb': 'ESP', 'iso2': 'ES', 'eurostat': 'ES', 'oecd': 'ESP'},
}


def parse_args():
    parser = argparse.ArgumentParser(description='Fetch GC.DOD data from multiple providers.')
    parser.add_argument('--iso', nargs='+', metavar='ISO3',
                        help='Optional list of ISO3 country codes to process (default: all).')
    return parser.parse_args()


def select_countries(args):
    if not args.iso:
        return COUNTRIES
    requested = {iso.upper() for iso in args.iso}
    filtered = {iso: data for iso, data in COUNTRIES.items() if iso in requested}
    unknown = requested - set(filtered.keys())
    if unknown:
        print(f'Warning: ignoring unknown ISO3 codes: {",".join(sorted(unknown))}')
    if not filtered:
        print('No matching ISO3 codes found; nothing to do.')
    return filtered
    return filtered

def fetch_url(url, timeout=20, retries=3, backoff=2):
    """Fetch URL with retry/backoff to handle transient network issues."""
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'FRM-fetch/1.0'})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode('utf-8'), None
        except Exception as e:
            last_err = str(e)
            if attempt < retries:
                delay = backoff * attempt
                print(f'    fetch attempt {attempt}/{retries} failed, retrying in {delay}s...')
                time.sleep(delay)
            else:
                print(f'    fetch attempt {attempt}/{retries} failed, no more retries')
    return None, last_err

def fetch_worldbank(iso3):
    """Fetch from World Bank WDI API."""
    url = f'https://api.worldbank.org/v2/country/{iso3}/indicator/GC.DOD.TOTL.GD.ZS?format=json&per_page=2000&date=1960:2025'
    data_str, err = fetch_url(url)
    if err:
        return {}, f'WB fetch failed: {err}'
    
    try:
        data = json.loads(data_str)
        # Save raw JSON
        out_json = os.path.join(MACRO_DIR, f'wb_GC.DOD.TOTL.GD.ZS_{iso3}.json')
        with open(out_json, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        # Parse to year->value dict
        if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
            obs_list = data[1]
            result = {}
            for obs in obs_list:
                year = obs.get('date')
                val = obs.get('value')
                if val is not None and year:
                    try:
                        result[int(year)] = float(val)
                    except:
                        pass
            return result, f'WB OK: {len(result)} observations from JSON'
        return {}, 'WB returned unexpected format'
    except Exception as e:
        return {}, f'WB parse error: {e}'

def fetch_imf_ifs(iso3):
    """Attempt IMF IFS via SDMX - government gross debt."""
    # IMF IFS CompactData pattern (example for USA)
    # https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/A.{country}.GGXWDG_GDP?startPeriod=1980
    # GGXWDG_GDP = General government gross debt as % of GDP
    url = f'https://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/A.{iso3}.GGXWDG_GDP?startPeriod=1980'
    data_str, err = fetch_url(url, timeout=30)
    if err:
        return {}, f'IMF fetch failed: {err}'
    
    try:
        data = json.loads(data_str)
        # IMF SDMX structure varies; attempt to extract observations
        result = {}
        # Try to navigate CompactData > DataSet > Series > Obs
        if 'CompactData' in data:
            dataset = data['CompactData'].get('DataSet')
            if dataset and 'Series' in dataset:
                series = dataset['Series']
                if not isinstance(series, list):
                    series = [series]
                for s in series:
                    obs_list = s.get('Obs', [])
                    if not isinstance(obs_list, list):
                        obs_list = [obs_list]
                    for obs in obs_list:
                        year_str = obs.get('@TIME_PERIOD', obs.get('@TIME', ''))
                        val_str = obs.get('@OBS_VALUE', '')
                        if year_str and val_str:
                            try:
                                result[int(year_str)] = float(val_str)
                            except:
                                pass
        if result:
            return result, f'IMF OK: {len(result)} observations'
        return {}, 'IMF returned no parseable observations'
    except Exception as e:
        return {}, f'IMF parse error: {e}'

def fetch_eurostat(iso2):
    """Attempt Eurostat gov_10_gdp (Government consolidated gross debt)."""
    # Eurostat SDMX endpoint for gov_10_gdp
    # https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/gov_10q_ggdebt/?format=JSON&...
    # Conservative attempt: use simpler REST API pattern
    # Example: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10_gdp?geo={iso2}&unit=PC_GDP
    # Note: Eurostat may not have USA/non-EU countries
    if iso2 in ['US']:
        return {}, 'Eurostat does not cover USA'
    
    url = f'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/gov_10_gdp?geo={iso2}&unit=PC_GDP&format=JSON'
    data_str, err = fetch_url(url, timeout=30)
    if err:
        return {}, f'Eurostat fetch failed: {err}'
    
    try:
        data = json.loads(data_str)
        result = {}
        # Eurostat JSON structure varies; attempt basic extraction
        # This is a placeholder - real structure parsing needed
        # For now return empty with note
        return {}, 'Eurostat fetch attempted but parsing not implemented (manual check recommended)'
    except Exception as e:
        return {}, f'Eurostat parse error: {e}'

def fetch_oecd(iso3):
    """Attempt OECD Government debt data."""
    # OECD SDMX endpoint for GOV_DEBT
    # Example: https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/GOV_DEBT/{iso3}/all?format=compact_v2
    url = f'https://stats.oecd.org/restsdmx/sdmx.ashx/GetData/GOV_DEBT/{iso3}/all'
    data_str, err = fetch_url(url, timeout=30)
    if err:
        return {}, f'OECD fetch failed: {err}'
    
    try:
        # OECD returns XML SDMX; parsing XML not implemented here (needs lxml or xml.etree)
        # Return placeholder
        return {}, 'OECD fetch attempted but XML parsing not implemented (manual check recommended)'
    except Exception as e:
        return {}, f'OECD parse error: {e}'

def merge_sources(wb_data, imf_data, eurostat_data, oecd_data):
    """Merge multiple sources, preferring WB > IMF > Eurostat > OECD."""
    merged = {}
    for year in range(1960, datetime.utcnow().year + 1):
        val = None
        source = None
        if year in wb_data:
            val = wb_data[year]
            source = 'worldbank'
        elif year in imf_data:
            val = imf_data[year]
            source = 'imf'
        elif year in eurostat_data:
            val = eurostat_data[year]
            source = 'eurostat'
        elif year in oecd_data:
            val = oecd_data[year]
            source = 'oecd'
        
        if val is not None:
            merged[year] = {'value': val, 'source': source}
    return merged

def write_csv(iso3, merged_data):
    """Write merged data to CSV."""
    out_csv = os.path.join(MACRO_DIR, f'general_government_gross_debt_pct_gdp_{iso3}.csv')
    rows = []
    for year in sorted(merged_data.keys()):
        rows.append({
            'country': iso3,
            'year': year,
            'value': merged_data[year]['value'],
            'indicator': 'GC.DOD.TOTL.GD.ZS',
            'source': merged_data[year]['source']
        })
    
    with open(out_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['country', 'year', 'value', 'indicator', 'source'])
        writer.writeheader()
        writer.writerows(rows)
    return out_csv

def main():
    args = parse_args()
    targets = select_countries(args)

    print('Starting comprehensive GC.DOD fetch...\n')
    if args.iso:
        print(f'Processing subset: {",".join(targets.keys())}\n')

    if not targets:
        return
    
    for iso3, codes in targets.items():
        print(f'--- {iso3} ---')
        
        # Try World Bank
        wb_data, wb_msg = fetch_worldbank(codes['wb'])
        print(f'  World Bank: {wb_msg}')
        
        # Try IMF
        imf_data, imf_msg = fetch_imf_ifs(codes['wb'])
        print(f'  IMF IFS: {imf_msg}')
        
        # Try Eurostat (EU countries only)
        eurostat_data, eurostat_msg = fetch_eurostat(codes['iso2'])
        print(f'  Eurostat: {eurostat_msg}')
        
        # Try OECD
        oecd_data, oecd_msg = fetch_oecd(codes['oecd'])
        print(f'  OECD: {oecd_msg}')
        
        # Merge and write
        merged = merge_sources(wb_data, imf_data, eurostat_data, oecd_data)
        if merged:
            csv_path = write_csv(iso3, merged)
            years = sorted(merged.keys())
            print(f'  Merged: {len(merged)} observations ({min(years)}–{max(years)}) -> {csv_path}')
        else:
            print(f'  No data retrieved for {iso3}')
        print()
    
    print('Done. Check data_repository/raw/macro/ for outputs.')

if __name__ == '__main__':
    main()
