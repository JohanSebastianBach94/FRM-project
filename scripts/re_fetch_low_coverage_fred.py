"""Automate FRED re-fetches for low-coverage series and log attempts."""
import csv
import datetime
import os
from pathlib import Path

import pandas as pd
from fredapi import Fred


def load_env(path='.env'):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith('#'):
            continue
        if '=' not in stripped:
            continue
        key, value = stripped.split('=', 1)
        os.environ.setdefault(key.strip(), value.strip())


def fetch_low_coverage_targets(low_cov_path, output_dir):
    load_env()
    fred_key = os.getenv('FRED_API_KEY')
    if not fred_key:
        raise SystemExit('FRED_API_KEY is missing. Unable to fetch via fredapi.')

    fred = Fred(api_key=fred_key)
    low_cov = pd.read_csv(low_cov_path)
    targets = (
        low_cov[low_cov['provider'] == 'fred']
        .loc[low_cov['fetch_method'] == 'fredapi']
        .sort_values('recomputed_cov_tradingday_realobs')
    )
    if targets.empty:
        print('No FRED targets found in', low_cov_path)
        return []

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    end_date = datetime.date.today().isoformat()

    log_entries = []
    for _, row in targets.iterrows():
        series = row['series']
        storage_path = output_dir / f"{series}.csv"
        old_obs = int(row['real_obs_count']) if not pd.isna(row['real_obs_count']) else 0
        success = False
        new_obs = old_obs
        notes = ''
        try:
            print('Fetching', series)
            series_data = fred.get_series(
                series, observation_start='1900-01-01', observation_end=end_date
            )
            if series_data is None or series_data.empty:
                notes = 'fredapi returned no data'
            else:
                df = series_data.to_frame(series)
                df.index.name = 'DATE'
                df.to_csv(storage_path)
                new_obs = int(df[series].dropna().shape[0])
                success = True
                notes = f'fetched {new_obs} rows via fredapi'
        except Exception as exc:
            notes = f'fredapi error: {exc}'
        log_entries.append([
            series,
            'fred',
            'fredapi',
            str(storage_path),
            old_obs,
            True,
            success,
            new_obs,
            notes,
        ])
    return log_entries


def append_log(log_path, entries):
    log_path = Path(log_path)
    if not log_path.exists():
        raise SystemExit(f'Log file {log_path} not found.')
    with log_path.open('a', newline='', encoding='utf-8') as fh:
        writer = csv.writer(fh)
        for entry in entries:
            writer.writerow(entry)


def main():
    base = Path(__file__).resolve().parent.parent
    low_cov_path = base / 'analysis_outputs' / 'low_coverage_with_catalog.csv'
    log_path = base / 'analysis_outputs' / 'refetch_attempts.csv'
    output_dir = base / 'data_repository' / 'raw' / 'fred'
    entries = fetch_low_coverage_targets(low_cov_path, output_dir)
    if entries:
        append_log(log_path, entries)
    print(f'Appended {len(entries)} rows to {log_path}')


if __name__ == '__main__':
    main()
