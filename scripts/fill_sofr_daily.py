import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd


BASE = Path(__file__).resolve().parents[1]
SOFR_PATH = BASE / "data_repository" / "raw" / "providers" / "derived_risk_drivers" / "SOFR_3m.csv"
CATALOG_PATH = BASE / "catalog.csv"


def backup(p: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    target = p.with_name(p.name + f".bak_fill_{ts}")
    shutil.copy2(p, target)
    return target


def fill_daily():
    if not SOFR_PATH.exists():
        raise SystemExit(f"Missing source file: {SOFR_PATH}")
    print(f"Backing up {SOFR_PATH}")
    backup(SOFR_PATH)

    df = pd.read_csv(SOFR_PATH)
    if 'date' not in df.columns:
        raise SystemExit("SOFR CSV missing 'date' column")
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df = df.sort_values('date').drop_duplicates(subset=['date'])

    start = df['date'].min()
    end = df['date'].max()
    print(f"Filling business days from {start.date()} to {end.date()}")

    bidx = pd.bdate_range(start=start, end=end)

    # set index and reindex to business days, forward-fill monthly values
    df2 = df.set_index('date')
    # if there are extra columns, keep only the data column (assume second column is value)
    value_col = [c for c in df2.columns if c.lower() != 'date'][0]
    series = df2[[value_col]].reindex(bidx)
    series = series.ffill()
    series.index.name = 'date'
    out = series.reset_index()
    out['date'] = out['date'].dt.strftime('%Y-%m-%d')

    # write back
    out.to_csv(SOFR_PATH, index=False)
    total_obs = len(out)
    print(f"Wrote filled SOFR series with {total_obs} rows")

    # update catalog total_obs for series 'SOFR_3m'
    update_catalog_total_obs('SOFR_3m', total_obs, end.date())


def update_catalog_total_obs(series_id: str, total_obs: int, last_date):
    # create a backup of catalog
    print(f"Backing up catalog {CATALOG_PATH}")
    backup(CATALOG_PATH)
    rows = []
    with CATALOG_PATH.open(newline='') as f:
        reader = pd.read_csv(f)
    # reader is dataframe; update matching row
    dfc = reader
    mask = dfc['series'] == series_id
    if not mask.any():
        print(f"Series {series_id} not found in catalog; skipping catalog update")
        return
    dfc.loc[mask, 'total_obs'] = int(total_obs)
    # update last_observation to last_date if later
    dfc.loc[mask, 'last_observation'] = pd.to_datetime(dfc.loc[mask, 'last_observation']).dt.strftime('%Y-%m-%dT00:00:00')
    # write back
    dfc.to_csv(CATALOG_PATH, index=False)
    print(f"Updated catalog total_obs for {series_id} -> {total_obs}")


if __name__ == '__main__':
    fill_daily()
