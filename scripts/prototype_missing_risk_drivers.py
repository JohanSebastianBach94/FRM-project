"""Prototypes for filling the missing risk driver gaps listed in country_blocks_extended.yaml."""
import io
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
BOND_DIR = ROOT / 'data_repository' / 'raw' / 'market_data'
DERIVED_DIR = ROOT / 'data_repository' / 'raw' / 'providers' / 'derived_risk_drivers'
DERIVED_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = ROOT / 'logs' / 'derived_risk_drivers'
LOG_DIR.mkdir(parents=True, exist_ok=True)
FRED_DIR = ROOT / 'data_repository' / 'raw' / 'fred'
FRED_YIELD_SERIES_BY_ISO = {
    'USA': 'IRLTLT01USM156N',
    'DEU': 'IRLTLT01DEM156N',
    'FRA': 'IRLTLT01FRM156N',
    'ITA': 'IRLTLT01ITM156N',
    'ESP': 'IRLTLT01ESM156N',
}

ZCB_STRIP_DIR = ROOT / 'data' / 'ZCB STRIPS'
ZCB_STRIP_FILES = {
    'USA': 'usa_historical_strips_20251014_140448.csv',
    'DEU': 'deu_historical_strips_20251014_140448.csv',
    'FRA': 'fra_historical_strips_20251014_140448.csv',
    'ITA': 'ita_historical_strips_20251014_140448.csv',
    'ESP': 'esp_historical_strips_20251014_140448.csv',
}

BOND_LABELS: Dict[str, str] = {
    'USA': 'United',
    'FRA': 'France',
    'ITA': 'Italy',
    'ESP': 'Spain',
    'DEU': 'Germany',
}

EUROSTAT_COUNTRIES = ['DE', 'FR', 'IT', 'ES']
EUROSTAT_BASE_URL = 'https://api.europa.eu/eurostat/api/dissemination/statistics/1.0/data'

logger = logging.getLogger('prototype_missing_risk_drivers')
if not logger.handlers:
    fh = logging.FileHandler(LOG_DIR / f'derived_risk_drivers_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(sh)
    logger.setLevel(logging.INFO)


def _export(df: pd.DataFrame, label: str) -> None:
    if df.empty:
        logger.warning('nothing to export for %s', label)
        return
    path = DERIVED_DIR / f'{label}.csv'
    df.to_csv(path)
    logger.info('exported %s (%s rows)', path, len(df))


def _load_bond(iso: str, tenor: str) -> Optional[pd.DataFrame]:
    label = BOND_LABELS.get(iso)
    if not label:
        logger.warning('no bond label configured for %s', iso)
        return None
    path = BOND_DIR / f'BOND_{label}_{tenor}.csv'
    if not path.exists():
        logger.warning('missing bond file %s', path.name)
        return None
    df = pd.read_csv(path, parse_dates=['Date'])
    df = df.rename(columns={'Date': 'date', 'Value': 'yield'})
    df.set_index('date', inplace=True)
    return df


def _tenor_to_years(tenor: str) -> Optional[float]:
    if not tenor:
        return None
    tenor = tenor.upper().strip()
    if tenor.endswith('Y'):
        try:
            return float(tenor[:-1])
        except ValueError:
            return None
    return None


def _load_zcb_monthly_series(iso: str, tenor: str) -> Tuple[Optional[pd.Series], Optional[str]]:
    years = _tenor_to_years(tenor)
    if years is None:
        return None, None
    iso = iso.upper()
    file_name = ZCB_STRIP_FILES.get(iso)
    if not file_name:
        logger.debug('no ZCB strip mapped for %s', iso)
        return None, None
    path = ZCB_STRIP_DIR / file_name
    if not path.exists():
        logger.warning('missing ZCB strip %s', path.name)
        return None, None
    df = pd.read_csv(path, parse_dates=['date'])
    if 'maturity_years' not in df.columns or 'country' not in df.columns:
        logger.warning('unexpected schema in ZCB strip %s', path.name)
        return None, None
    mask = (
        df['country'].str.upper() == iso
    ) & (
        df['maturity_years'].round(6) == years
    )
    df = df.loc[mask]
    if df.empty:
        logger.debug('no %s %s records in %s', iso, tenor, path.name)
        return None, None
    series = df.set_index('date')['yield_percent'].dropna()
    if series.empty:
        return None, None
    series = series.resample('ME').last().dropna()
    return series, 'ZCB strips'


def _load_fred_yield(iso: str) -> Optional[pd.DataFrame]:
    series_id = FRED_YIELD_SERIES_BY_ISO.get(iso)
    if not series_id:
        logger.debug('no FRED yield series mapped for %s', iso)
        return None
    path = FRED_DIR / f'{series_id}.csv'
    if not path.exists():
        logger.warning('missing FRED file %s for %s', path.name, iso)
        return None
    df = pd.read_csv(path)
    date_col = next((col for col in df.columns if 'date' in col.lower()), None)
    value_col = next((col for col in df.columns if col != date_col), None)
    if not date_col or not value_col:
        logger.warning('unexpected schema for %s', path.name)
        return None
    df = df[[date_col, value_col]].dropna()
    df = df.rename(columns={date_col: 'date', value_col: 'yield'})
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df = df.set_index('date')
    return df


def _load_monthly_series(iso: str, tenor: str) -> Tuple[Optional[pd.Series], Optional[str]]:
    series, source = _load_zcb_monthly_series(iso, tenor)
    if series is not None:
        return series, source

    df = _load_fred_yield(iso)
    if df is not None:
        monthly = df['yield'].resample('ME').last().dropna()
        if not monthly.empty:
            return monthly, 'FRED'

    df = _load_bond(iso, tenor)
    if df is not None:
        monthly = df['yield'].resample('ME').last().dropna()
        if not monthly.empty:
            return monthly, 'investing.com'

    return None, None


def derive_sovereign_spread(target_iso: str, base_iso: str = 'DEU', tenor: str = '10Y') -> None:
    target = _load_bond(target_iso, tenor)
    base = _load_bond(base_iso, tenor)
    used_base = base_iso
    if base is None:
        logger.info('base %s unavailable; falling back to USA for spread calculation', base_iso)
        base = _load_bond('USA', tenor)
        used_base = 'USA'
    if target is None or base is None:
        logger.warning('skipping spread for %s (target=%s base=%s)', target_iso, target is not None, base is not None)
        return
    merged = target.join(base, lsuffix='_target', rsuffix='_base', how='inner')
    if merged.empty:
        logger.warning('no overlapping dates for spread %s vs %s', target_iso, used_base)
        return
    merged['spread'] = merged['yield_target'] - merged['yield_base']
    monthly = merged['spread'].resample('ME').last().dropna().to_frame('spread')
    _export(monthly, f'spread_{target_iso}_vs_{used_base}_{tenor}')


def derive_cds_proxy(target_iso: str, base_iso: str = 'DEU', tenor: str = '5Y') -> None:
    target_series, target_source = _load_monthly_series(target_iso, tenor)
    base_series, base_source = _load_monthly_series(base_iso, tenor)
    used_base = base_iso
    if base_series is None:
        logger.info('base for CDS proxy %s unavailable; falling back to USA', base_iso)
        base_series, base_source = _load_monthly_series('USA', tenor)
        used_base = 'USA'
    if target_series is None or base_series is None:
        logger.warning('skipping CDS proxy for %s (missing source data)', target_iso)
        return
    target_series = target_series.rename('yield_target')
    base_series = base_series.rename('yield_base')
    merged = pd.concat([target_series, base_series], axis=1, join='inner')
    if merged.empty:
        logger.warning('no overlapping dates for CDS proxy %s vs %s', target_iso, used_base)
        return
    merged['cds_proxy'] = merged['yield_target'] - merged['yield_base']
    monthly = merged['cds_proxy'].to_frame('cds_proxy')
    source_note = f"{target_source or 'unknown'}/{base_source or 'unknown'} feeds"
    logger.info('CDS proxy %s vs %s built using %s', target_iso, used_base, source_note)
    _export(monthly, f'cds_proxy_{target_iso}_vs_{used_base}_{tenor}')


def fetch_eurostat(dataset: str, extra_params: Optional[Dict[str, str]] = None) -> Optional[pd.DataFrame]:
    params = {'format': 'CSV', 'precision': '1', 'geo': ','.join(EUROSTAT_COUNTRIES)}
    if extra_params:
        params.update(extra_params)
    url = f'{EUROSTAT_BASE_URL}/{dataset}'
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning('Eurostat request failed for %s: %s', dataset, exc)
        return None
    df = pd.read_csv(io.StringIO(resp.text))
    time_col = next((c for c in df.columns if c.lower().startswith('time')), None)
    value_col = next((c for c in df.columns if c.lower().startswith('value')), None)
    geo_col = next((c for c in df.columns if c.lower() == 'geo'), None)
    if not {time_col, value_col, geo_col}.issubset(df.columns):
        logger.warning('unexpected Eurostat schema for %s (%s/%s/%s)', dataset, time_col, value_col, geo_col)
        return None
    df = df[[time_col, geo_col, value_col]].dropna()
    df = df.rename(columns={time_col: 'date', geo_col: 'geo', value_col: 'value'})
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    pivot = df.pivot_table(index='date', columns='geo', values='value')
    return pivot


def derive_real_estate_ratios() -> None:
    price = fetch_eurostat('prc_hpi', {'unit': 'I5', 'freq': 'Q'})
    income = fetch_eurostat('earn_gr', {'freq': 'Q'})
    if price is None or income is None:
        logger.warning('price/income series unavailable, skipping ratio derivation')
        return
    common = price.index.intersection(income.index)
    if common.empty:
        logger.warning('no common timestamps for real estate ratios')
        return
    ratio = price.loc[common].divide(income.loc[common])
    _export(ratio, 'price_to_income_ratios_eurozone')


def fetch_yfinance_series(ticker: str, label: str, freq: str = '1mo') -> None:
    df = yf.download(ticker, start='1990-01-01', interval=freq, progress=False, auto_adjust=False)
    if df.empty:
        logger.warning('no data returned for %s', ticker)
        return
    series = df[['Close']].rename(columns={'Close': ticker})
    series.index.name = 'date'
    _export(series, label)


def fetch_move_daily_vol(window: int = 20, interval: str = '1d', start: str = '1990-01-01') -> None:
    df = yf.download('^MOVE', start=start, interval=interval, progress=False, auto_adjust=False)
    if df.empty:
        logger.warning('no daily MOVE data returned')
        return
    df = df[['Adj Close']].rename(columns={'Adj Close': 'adj_close'})
    df.index.name = 'date'
    df['log_ret'] = np.log(df['adj_close']).diff()
    df['realized_vol_20'] = df['log_ret'].rolling(window).std() * np.sqrt(252)
    realized = df[['realized_vol_20']].dropna()
    _export(realized, 'move_index_daily')


def fetch_v2x_index(start: str = '1990-01-01', interval: str = '1d') -> None:
    _download_vol_index('^V2X', 'v2x_index', start, interval)


def fetch_vxst_index(start: str = '1990-01-01', interval: str = '1d') -> None:
    _download_vol_index('^VXST', 'vxst_index', start, interval)


def _download_vol_index(ticker: str, label: str, start: str, interval: str) -> None:
    df = yf.download(ticker, start=start, interval=interval, progress=False, auto_adjust=False)
    if df.empty:
        logger.warning('no data returned for %s', ticker)
        return
    if 'Close' not in df.columns:
        logger.warning('no Close column returned for %s', ticker)
        return
    series = df[['Close']].dropna().rename(columns={'Close': 'value'})
    series.columns = ['value']
    if series.empty:
        logger.warning('%s download yielded no close values', ticker)
        return
    series.index.name = 'date'
    _export(series, label)


def fetch_fred_series(series_id: str, label: str) -> None:
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv'
    try:
        resp = requests.get(url, params={'id': series_id, 'cosd': '1990-01-01', 'freq': 'm'}, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning('FRED download failed for %s: %s', series_id, exc)
        return
    df = pd.read_csv(io.StringIO(resp.text))
    date_col = next((col for col in df.columns if 'date' in col.lower()), None)
    if date_col is None:
        logger.warning('no date column found for %s', series_id)
        return
    value_col = next((col for col in df.columns if col != date_col), None)
    if value_col is None:
        logger.warning('no value column found for %s', series_id)
        return
    df = df[[date_col, value_col]].dropna()
    df = df.rename(columns={date_col: 'date', value_col: series_id})
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df.set_index('date', inplace=True)
    df = df.dropna()
    _export(df, label)


def main() -> None:
    logger.info('Starting missing risk driver prototypes run')
    for iso in ['USA', 'FRA', 'ITA', 'ESP', 'DEU']:
        derive_sovereign_spread(iso)
        derive_cds_proxy(iso)
    derive_real_estate_ratios()
    fetch_yfinance_series('^MOVE', 'move_index_monthly')
    fetch_move_daily_vol()
    fetch_v2x_index()
    fetch_vxst_index()
    fetch_fred_series('TEDRATE', 'ted_rate_monthly')
    logger.info('Missing risk driver prototypes run complete')


if __name__ == '__main__':
    main()
