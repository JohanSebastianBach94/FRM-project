import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / 'data_repository' / 'raw' / 'providers' / 'bank_indices'
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_META_DIR = OUT_DIR / 'meta'
OUT_META_DIR.mkdir(parents=True, exist_ok=True)
CATALOG_PATH = ROOT / 'catalog.csv'
PROXY_PANEL_PATH = ROOT / 'data' / 'stress_indicators_expanded.csv'
LOG_DIR = ROOT / 'logs' / 'fetch_bank_equity_indexes'
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_TS = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
LOG_FILE = LOG_DIR / f'fetch_bank_equity_indexes_{LOG_TS}.log'

logger = logging.getLogger('fetch_bank_equity_indexes')
if not logger.handlers:
    handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

logger.info('Starting bank equity fetch run (log: %s)', LOG_FILE)


PROXY_TICKERS = {
    # Used only to backfill earlier history when the composite starts later.
    # These columns are expected to be present in data/stress_indicators_expanded.csv.
    'USA': '^GSPC',
    'DEU': '^GDAXI',
    'FRA': '^FCHI',
    'ITA': '^FTSEMIB',
    'ESP': '^IBEX',
}

def _fetch_market_cap(ticker):
    ticker_obj = yf.Ticker(ticker)
    try:
        info = ticker_obj.info
    except Exception as exc:
        logger.debug('metadata unavailable for %s: %s', ticker, exc)
        return None
    market_cap = info.get('marketCap')
    if market_cap:
        return market_cap
    shares = info.get('sharesOutstanding')
    price = info.get('currentPrice') or info.get('regularMarketPrice')
    if shares and price:
        return shares * price
    return None


INDEX_SPECS = {
    'USA': {
        'constituents': ['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS', '^BKX'],
        'enhancements': ['KBE', 'KRE'],
        'description': 'Large US banks plus BKX benchmark with bank ETFs (KBE, KRE) for diversification',
        'method': 'Market-cap and volatility-adjusted weights with ETF augmentation',
    },
    'DEU': {
        'constituents': ['DBK.DE', 'CBK.DE', 'ARL.DE', 'SX7E'],
        'enhancements': ['EWG'],
        'description': 'Major German banks plus Euro STOXX Banks index and German ETF proxy (EWG)',
        'method': 'Market-cap and volatility-adjusted weights with Euro STOXX Banks / ETF aid',
    },
    'FRA': {
        'constituents': ['BNP.PA', 'GLE.PA', 'ACA.PA', 'SGO.PA', 'BPCE.PA', 'SX7E'],
        'enhancements': ['EWQ'],
        'description': 'French banks complemented by Euro STOXX Banks plus France ETF proxy (EWQ)',
        'method': 'Market-cap and volatility-adjusted weights with diversification from ETF/index proxies',
    },
    'ITA': {
        'constituents': ['ISP.MI', 'UCG.MI', 'BAMI.MI', 'BPER.MI', 'MPS.MI', 'SX7E'],
        'enhancements': ['EWI'],
        'description': 'Italian banks plus Euro STOXX Banks benchmark and Italy ETF proxy (EWI)',
        'method': 'Market-cap and volatility-adjusted weights using ETFs/indices for stability',
    },
    'ESP': {
        'constituents': ['SAN.MC', 'BBVA.MC', 'CABK.MC', 'BKT.MC', 'SAB.MC', 'BPOP.MC', 'SX7E'],
        'enhancements': ['EWP'],
        'description': 'Spanish banks stacked with Euro STOXX Banks index and Spanish ETF proxy (EWP)',
        'method': 'Market-cap and volatility-adjusted weights plus ETF/index augmentation',
    },
}

START_DATE = '1990-01-01'
END_DATE = datetime.utcnow().strftime('%Y-%m-%d')


def _download_monthly_close(ticker):
    try:
        df = yf.download(ticker, start=START_DATE, end=END_DATE, interval='1mo', progress=False)
    except Exception as exc:
        logger.warning('failed to download %s: %s', ticker, exc)
        return None
    if df.empty:
        logger.warning('no data for %s', ticker)
        return None
    series = df[['Close']].rename(columns={'Close': ticker})
    return series


def _load_proxy_monthly_close(iso: str) -> pd.Series | None:
    """Load a monthly proxy close series from the local stress-indicator panel.

    The panel is daily; we take month-end values and index them at month-start
    to align with the 1mo index convention used by yfinance.
    """
    proxy_col = PROXY_TICKERS.get(iso)
    if not proxy_col:
        return None
    if not PROXY_PANEL_PATH.exists():
        return None
    try:
        df = pd.read_csv(PROXY_PANEL_PATH, usecols=['Date', proxy_col], parse_dates=['Date'])
    except Exception:
        return None
    if df.empty:
        return None
    df = df.rename(columns={'Date': 'date'}).set_index('date')
    s = pd.to_numeric(df[proxy_col], errors='coerce').dropna()
    if s.empty:
        return None
    s = s.sort_index()
    monthly = s.resample('M').last()
    monthly.index = monthly.index.to_period('M').to_timestamp(how='start')
    monthly.name = proxy_col
    return monthly


def _splice_with_proxy(composite: pd.Series, proxy: pd.Series) -> pd.Series:
    if composite is None or proxy is None:
        return composite
    composite = composite.dropna().sort_index()
    proxy = proxy.dropna().sort_index()
    if composite.empty or proxy.empty:
        return composite

    common = proxy.index.intersection(composite.index)
    if common.empty:
        return composite

    anchor = common.min()
    try:
        scale = float(composite.loc[anchor]) / float(proxy.loc[anchor])
    except Exception:
        return composite
    if not np.isfinite(scale) or scale <= 0:
        return composite

    proxy_scaled = proxy * scale
    start = composite.index.min()
    extended = pd.concat([proxy_scaled.loc[proxy_scaled.index < start], composite])
    extended = extended[~extended.index.duplicated(keep='last')].sort_index()
    return extended


def _rebased_index(series: pd.Series) -> pd.Series:
    series = series.dropna().sort_index()
    if series.empty:
        return series
    base = float(series.iloc[0])
    if not np.isfinite(base) or base == 0:
        return series
    return series / base * 100.0


def _expected_monthly_obs(last_date: pd.Timestamp) -> int:
    base = pd.Period(pd.Timestamp(1990, 2, 1), freq='M')
    end = pd.Period(last_date, freq='M')
    if end < base:
        return 0
    return end.ordinal - base.ordinal + 1


def _update_catalog_row(series_id: str, df_series: pd.DataFrame) -> None:
    if not CATALOG_PATH.exists():
        return
    if df_series.empty:
        return
    if 'date' not in df_series.columns or 'value' not in df_series.columns:
        return
    last_date = pd.to_datetime(df_series['date'], errors='coerce').dropna().max()
    if pd.isna(last_date):
        return
    base_date = pd.Timestamp(1990, 2, 1)
    df_series = df_series.copy()
    df_series['date'] = pd.to_datetime(df_series['date'], errors='coerce')
    df_series = df_series[df_series['date'].notna() & (df_series['date'] >= base_date)]

    total_obs = int(pd.to_numeric(df_series['value'], errors='coerce').dropna().shape[0])
    denom = _expected_monthly_obs(pd.Timestamp(last_date))
    if denom <= 0:
        return
    coverage_ratio = float(total_obs) / float(denom)

    try:
        cat = pd.read_csv(CATALOG_PATH)
    except Exception:
        return
    if 'series' not in cat.columns:
        return
    mask = cat['series'].astype(str) == str(series_id)
    if not mask.any():
        return

    cat.loc[mask, 'frequency_label'] = 'monthly'
    cat.loc[mask, 'last_observation'] = pd.Timestamp(last_date).strftime('%Y-%m-%dT00:00:00')
    if 'window_obs' in cat.columns:
        cat.loc[mask, 'window_obs'] = int(denom)
    if 'total_obs' in cat.columns:
        cat.loc[mask, 'total_obs'] = int(total_obs)
    if 'coverage_ratio' in cat.columns:
        cat.loc[mask, 'coverage_ratio'] = float(coverage_ratio)

    cat.to_csv(CATALOG_PATH, index=False)


parser = argparse.ArgumentParser(description='Fetch and build bank equity index composites')
parser.add_argument(
    '--extend-only',
    action='store_true',
    help='Do not fetch from yfinance; only extend existing bank index CSVs using local proxy series and refresh catalog fields.',
)
args = parser.parse_args()


for iso, spec in INDEX_SPECS.items():
    csv_path = OUT_DIR / f'bank_equity_index_{iso}.csv'
    series_id = f'Bank_equity_index_{iso}'

    if args.extend_only:
        if not csv_path.exists():
            logger.warning('extend-only: missing %s', csv_path)
            continue
        try:
            existing = pd.read_csv(csv_path, parse_dates=['date'])
        except Exception:
            logger.warning('extend-only: failed to read %s', csv_path)
            continue
        value_col = 'value' if 'value' in existing.columns else 'close' if 'close' in existing.columns else None
        if value_col is None:
            logger.warning('extend-only: unexpected columns in %s', csv_path)
            continue
        series_existing = pd.to_numeric(existing[value_col], errors='coerce')
        series_existing.index = pd.to_datetime(existing['date'], errors='coerce')
        series_existing = series_existing.dropna().sort_index()
        proxy = _load_proxy_monthly_close(iso)
        if proxy is not None:
            series_existing = _splice_with_proxy(series_existing, proxy)
        series_existing = _rebased_index(series_existing)
        out_df = series_existing.rename('value').to_frame()
        out_df.index.name = 'date'
        out_df.to_csv(csv_path)
        _update_catalog_row(series_id, out_df.reset_index())
        logger.info('extended %s (%d rows)', csv_path, len(out_df))
        continue

    tickers = list(dict.fromkeys(spec.get('constituents', []) + spec.get('enhancements', [])))
    frames = []
    ticker_metadata = {}
    for ticker in tickers:
        downloaded = _download_monthly_close(ticker)
        if downloaded is None:
            continue
        frames.append(downloaded)
        ticker_metadata[ticker] = {'market_cap': _fetch_market_cap(ticker)}
    if not frames:
        logger.warning('no constituents downloaded for %s', iso)
        continue
    df_all = pd.concat(frames, axis=1)
    if df_all.dropna(how='all').empty:
        logger.warning('no valid data for %s', iso)
        continue
    df_all.sort_index(inplace=True)
    pct_change = df_all.pct_change()
    volatility = pct_change.std(skipna=True)
    scores = []
    constituent_info = []
    normalized = pd.DataFrame(index=df_all.index)
    valid_tickers = []
    for ticker in df_all.columns:
        series = df_all[ticker]
        valid = series.dropna()
        if valid.empty:
            logger.debug('skipping %s because it has no valid prices', ticker)
            continue
        last_price = valid.iloc[-1]
        first_price = valid.iloc[0]
        normalized[ticker] = series.div(first_price).mul(100)
        mc = ticker_metadata.get(ticker, {}).get('market_cap')
        fallback_mc = last_price * 1e7 if last_price > 0 else 1e8
        base_mc = mc if mc and mc > 0 else fallback_mc
        vol = float(volatility.get(ticker, 0.0))
        vol_safe = vol if vol > 1e-8 else 1e-4
        score = base_mc / vol_safe
        scores.append(score)
        constituent_info.append({'ticker': ticker, 'market_cap': mc, 'volatility': round(vol, 6)})
        valid_tickers.append(ticker)
    weights = np.array(scores, dtype=float)
    if weights.sum() == 0:
        weights = np.ones_like(weights)
    weights /= weights.sum()
    weights_series = pd.Series(weights, index=valid_tickers)
    normalized = normalized[weights_series.index]
    weighted = normalized.multiply(weights_series, axis=1)
    available = normalized.notna().astype(float).multiply(weights_series, axis=1)
    denom = available.sum(axis=1)
    composite = weighted.sum(axis=1).where(denom != 0, np.nan).div(denom.replace(0, np.nan))

    proxy = _load_proxy_monthly_close(iso)
    if proxy is not None:
        composite = _splice_with_proxy(composite, proxy)
    composite = _rebased_index(composite)

    df_out = composite.to_frame(name='value')
    df_out.index.name = 'date'
    for idx, entry in enumerate(constituent_info):
        entry['weight'] = round(float(weights[idx]), 6)
    df_out.to_csv(csv_path)
    _update_catalog_row(series_id, df_out.reset_index())
    meta = {
        'created_at': datetime.utcnow().isoformat() + 'Z',
        'composition': constituent_info,
        'description': spec['description'],
        'method': spec['method'],
        'augmentation': spec.get('enhancements', []),
        'weighting': 'Market cap / volatility adjusted',
        'source': 'Yahoo Finance via yfinance',
        'rows': len(df_out),
        'first_date': df_out.index[0].strftime('%Y-%m-%d'),
        'last_date': df_out.index[-1].strftime('%Y-%m-%d'),
    }
    with open(OUT_META_DIR / f'bank_equity_index_{iso}.meta.json', 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)
    logger.info('wrote %s (%d rows)', csv_path, len(df_out))
