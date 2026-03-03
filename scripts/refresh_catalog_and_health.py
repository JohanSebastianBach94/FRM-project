"""
Refresh catalog coverage fields and risk factor health using a fixed window
from 1990-02-01 to today. Writes updated `catalog.csv` and
`analysis_outputs/risk_factor_health.csv` (backing up the previous catalog).

Usage:
    python scripts/refresh_catalog_and_health.py

This script expects `data/stress_indicators_expanded.csv` and `catalog.csv`
to exist in the project root.
"""
from pathlib import Path
from datetime import datetime
import sys
import pandas as pd
import numpy as np

try:
    import yaml
except Exception:  # pragma: no cover
    yaml = None

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_pipeline.calendar_utils import (
    count_weekend_gaps,
    expected_dates_between,
    load_series_metadata,
    normalize_frequency_label,
    weekend_dates_between,
)


CATALOG = ROOT / 'catalog.csv'
OUT_DIR = ROOT / 'analysis_outputs'
OUT_DIR.mkdir(exist_ok=True)

CALENDAR_GAP_LOG = OUT_DIR / 'calendar_gap_log.csv'

CONFIG = ROOT / 'config' / 'country_blocks_extended.yaml'
SERIES_METADATA = load_series_metadata(ROOT)

WINDOW_START = pd.Timestamp('1990-02-01')
WINDOW_END = pd.Timestamp(datetime.utcnow().date())
WEEKEND_SLOTS = len(weekend_dates_between(WINDOW_START, WINDOW_END))

# The merged panel pipeline now lives under data_pipeline and contains the dpreads columns we need.
MERGED_CANDIDATES = [
    (ROOT / 'data_pipeline' / 'data' / 'stress_indicators_expanded.csv', 'data_pipeline/data/stress_indicators_expanded.csv'),
    (ROOT / 'data' / 'stress_indicators_expanded.csv', 'data/stress_indicators_expanded.csv'),
]
MERGED_REQUIRED_COLUMNS = {'BTP_Bund_Spread', 'Bonos_Bund_Spread', 'OAT_Bund_Spread'}
MERGED_STORAGE_PATH = 'data/stress_indicators_expanded.csv'


# Some legacy catalog entries use non-canonical names that don't match the merged panel columns.
# Canonicalize them here so coverage_ratio and health fields populate correctly.
SERIES_CANONICAL_ALIASES = {
    # Stored as a combined label in older catalogs; panel column is TTF_GAS.
    'TTF_GAS / PNGASEUUSDM': 'TTF_GAS',
    # Older catalog used a generic name without the ISO suffix.
    'BIS_LBS_Household_Loans': 'BIS_LBS_Household_Loans_USA',
    # Older catalog stored the index without the caret.
    'FTSEMIB': '^FTSEMIB',
    # Legacy generic label; the pipeline uses FRED MORTGAGE30US.
    'Mortgage_rate_USA': 'MORTGAGE30US',
    # Older catalog used the generic BIS task name without ISO suffix; panel column is BIS_LBS_Private_NFC_<ISO>.
    'BIS_LBS_Private_NFC_Total': 'BIS_LBS_Private_NFC_USA',
}

# Series we no longer want to track in catalog because they were replaced by
# panel-built spreads used in config/country_blocks_extended.yaml.
DEPRECATED_SERIES = {
    'Sovereign_spread_vs_Germany_ESP',
    'Sovereign_spread_vs_Germany_FRA',
    'Sovereign_spread_vs_Germany_ITA',
}


# Explicit suppression flags that should persist even after a refresh.
# These series may exist in the merged panel but are intentionally excluded from modeling.
DO_NOT_USE_SERIES = {
    # Known-bad/placeholder mortgage proxies (legacy)
    # NOTE: EU mortgage rates are now fetched from ECB MIR via scripts/fetch_mortgage_and_rent_proxies.py
    # and should not be suppressed here.
    # Redundant long-term yields when DNSS betas are used
    'IRLTLT01DEM156N',
    'IRLTLT01ESM156N',
    'IRLTLT01FRM156N',
    'IRLTLT01ITM156N',
    'IRLTLT01USM156N',
    # ECB €STR volume proxy (not a good stress indicator)
    'ECBESTRVOLWGTTRMDMNRT',
    # Cross-country equity ETF proxies removed from country-specific blocks
    'FXI',
    'EWC',
    'EWJ',
    'EWT',
    'EWU',
    'EWW',
    'EWY',
    'EWZ',
    'EZU',
}


def _load_timeseries_from_csv(path: Path, series_name: str | None = None) -> pd.Series:
    if not path.exists():
        return pd.Series(dtype=float)
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.Series(dtype=float)

    if df.empty:
        return pd.Series(dtype=float)

    date_col = next((c for c in df.columns if str(c).lower() in {"date", "datetime", "time"}), None)
    if date_col is None:
        date_col = next((c for c in df.columns if "date" in str(c).lower()), None)
    if date_col is None:
        return pd.Series(dtype=float)

    candidate_cols = [c for c in df.columns if c != date_col]
    value_col = None
    if series_name:
        for c in candidate_cols:
            if str(c).strip() == series_name:
                value_col = c
                break
    if value_col is None:
        for preferred in ("value", "Value", "close", "Close"):
            if preferred in candidate_cols:
                value_col = preferred
                break
    if value_col is None and candidate_cols:
        value_col = candidate_cols[0]

    out = df[[date_col, value_col]].copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    out = out.dropna(subset=[date_col, value_col]).sort_values(date_col)
    if out.empty:
        return pd.Series(dtype=float)
    s = out.set_index(date_col)[value_col]
    s.index = pd.DatetimeIndex(s.index)
    s.name = series_name or str(value_col)
    return s


def expected_count_between(start, end, freq_label):
    if pd.isna(start) or pd.isna(end):
        return None
    if freq_label == 'daily':
        return (end - start).days + 1
    if freq_label == 'weekly':
        return ((end - start).days // 7) + 1
    if freq_label == 'monthly':
        return (end.year - start.year) * 12 + (end.month - start.month) + 1
    if freq_label == 'quarterly':
        months = (end.year - start.year) * 12 + (end.month - start.month)
        return (months // 3) + 1
    if freq_label == 'annual':
        return end.year - start.year + 1
    if freq_label == 'trading':
        if start > end:
            return 0
        try:
            # Much faster than building a full DatetimeIndex.
            return int(np.busday_count(start.date(), end.date()) + 1)
        except Exception:
            return len(pd.bdate_range(start=start, end=end))
    return None


def infer_series_frequency(index, fallback):
    if fallback:
        fallback = str(fallback).lower()
    else:
        fallback = 'daily'
    if isinstance(index, pd.DatetimeIndex):
        idx = index.sort_values()
        if len(idx) >= 2:
            diffs = idx.to_series().diff().dropna()
            if not diffs.empty:
                median_gap = float(diffs.dt.total_seconds().abs().median() / 86400.0)
                weekends_present = idx.weekday.isin([5, 6]).any()
                if median_gap <= 1.5:
                    return 'trading' if not weekends_present else 'daily'
                # Weekly series often arrive with ~7-day spacing (not <=4).
                if median_gap <= 10:
                    return 'weekly'
                if median_gap <= 45:
                    return 'monthly'
                if median_gap <= 120:
                    return 'quarterly'
                return 'annual'
    return fallback


def coverage_bucket(cov):
    if pd.isna(cov):
        return ''
    if cov >= 0.9:
        return 'full_>90'
    if cov >= 0.62:
        return 'partial_62_90'
    return 'lt_62'


def _panel_contains_required_columns(path: Path, required: set[str]) -> bool:
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            header = f.readline()
    except OSError:
        return False
    if not header:
        return False
    columns = {col.strip().strip('"') for col in header.split(',') if col.strip()}
    return required <= columns


def _read_panel_columns(path: Path) -> set[str]:
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            header = f.readline()
    except OSError:
        return set()
    if not header:
        return set()
    return {col.strip().strip('"') for col in header.split(',') if col.strip()}


def _select_merged_panel() -> tuple[Path, str]:
    # Choose the candidate that contains required spread columns *and* best matches the configured
    # series universe. This avoids accidentally selecting a narrower panel (e.g., data_pipeline/data)
    # and then misreporting coverage/last_obs for structural series that exist in the richer panel.
    configured = _extract_series_from_config(CONFIG)
    best: tuple[int, int, Path, str] | None = None  # (overlap, total_cols, path, rel)

    for path, rel in MERGED_CANDIDATES:
        if not path.exists():
            continue
        if not _panel_contains_required_columns(path, MERGED_REQUIRED_COLUMNS):
            continue
        cols = _read_panel_columns(path)
        # Count overlap with configured series; ignore the index column placeholder.
        overlap = len((cols - {''}) & configured)
        score = (overlap, len(cols))
        if best is None or score > (best[0], best[1]):
            best = (overlap, len(cols), path, rel)

    if best is not None:
        return best[2], best[3]

    # Fallback: first existing candidate.
    for path, rel in MERGED_CANDIDATES:
        if path.exists():
            return path, rel
    raise FileNotFoundError(f"merged panel not found in any of {[str(p) for p, _ in MERGED_CANDIDATES]}")


def _extract_series_from_config(path: Path) -> set[str]:
    if yaml is None:
        return set()
    if not path.exists():
        return set()
    try:
        cfg = yaml.safe_load(path.read_text(encoding='utf-8'))
    except Exception:
        return set()

    out: set[str] = set()
    for country in (cfg or {}).get('country_blocks', []) if isinstance(cfg, dict) else []:
        for block in country.get('blocks', []) if isinstance(country, dict) else []:
            for key in ('series_codes', 'optional_series_codes'):
                codes = block.get(key, []) if isinstance(block, dict) else []
                if isinstance(codes, list):
                    for c in codes:
                        if isinstance(c, str) and c.strip():
                            out.add(c.strip())
            local = block.get('local_series_files', {}) if isinstance(block, dict) else {}
            if isinstance(local, dict):
                for series_name in local.keys():
                    if isinstance(series_name, str) and series_name.strip():
                        out.add(series_name.strip())

    return out


def _append_missing_catalog_rows(df_cat: pd.DataFrame, needed: set[str]) -> pd.DataFrame:
    if not needed:
        return df_cat
    existing = {str(value) for value in df_cat['series'].astype(str).tolist()} if 'series' in df_cat else set()
    missing = [series for series in sorted(needed) if series not in existing]
    if not missing:
        return df_cat

    template_columns = df_cat.columns.tolist()
    new_rows: list[dict[str, str]] = []
    for series in missing:
        row = {col: '' for col in template_columns}
        row['series'] = series
        meta = SERIES_METADATA.get(series, {})
        freq_label = meta.get('frequency', 'daily')
        normalized_freq = normalize_frequency_label(freq_label)
        if 'frequency_label' in row:
            row['frequency_label'] = normalized_freq
        if 'storage_path' in row:
            row['storage_path'] = MERGED_STORAGE_PATH
        for key, value in meta.items():
            if key in row and not row[key]:
                row[key] = value
        if 'title' in row and not row['title']:
            row['title'] = series
        new_rows.append(row)

    if not new_rows:
        return df_cat

    return pd.concat([df_cat, pd.DataFrame(new_rows, columns=template_columns)], ignore_index=True, sort=False)
def _autofill_eur_fx_metadata(entry: dict) -> None:
    series = str(entry.get('series', '') or '')
    if not (series.startswith('EUR_') and series.endswith('_XR')):
        return

    # Example: EUR_CHF_XR -> quote CHF
    parts = series.split('_')
    if len(parts) < 3:
        return
    quote = parts[1]

    if not entry.get('entity'):
        entry['entity'] = f"EUR/{quote} Exchange Rate"
    if not entry.get('country_code'):
        entry['country_code'] = 'EUR'
    if not entry.get('instrument'):
        entry['instrument'] = 'FX'
    if not entry.get('topic_keywords'):
        entry['topic_keywords'] = 'fx,exchange_rate,eur'
    if not entry.get('extra_keywords'):
        entry['extra_keywords'] = f"eur,{quote.lower()},cross_rate"
    if not entry.get('source'):
        entry['source'] = 'Panel Derived' if not entry.get('storage_path') else entry.get('source', '')
    if not entry.get('source_group'):
        entry['source_group'] = 'panel'
    if not entry.get('source_detail'):
        entry['source_detail'] = 'merged_panel'
    if not entry.get('provider'):
        entry['provider'] = 'panel'
    if not entry.get('fetch_method'):
        entry['fetch_method'] = 'merged_panel'
    if not entry.get('storage_path'):
        entry['storage_path'] = MERGED_STORAGE_PATH


def _autofill_eur_funding_metadata(entry: dict) -> None:
    series = str(entry.get('series', '') or '')
    if series != 'EURIBOR_ESTR_SPREAD':
        return

    if not entry.get('entity'):
        entry['entity'] = 'EUR Funding Stress – Euribor 3M minus €STR'
    if not entry.get('country_code'):
        entry['country_code'] = 'EUR'
    if not entry.get('instrument'):
        entry['instrument'] = 'Spread'
    if not entry.get('topic_keywords'):
        entry['topic_keywords'] = 'funding,liquidity,stress,euribor,estr'
    if not entry.get('extra_keywords'):
        entry['extra_keywords'] = 'euribor_estr_spread,funding_stress'
    if not entry.get('source'):
        entry['source'] = 'Panel Derived'
    if not entry.get('source_group'):
        entry['source_group'] = 'panel'
    if not entry.get('source_detail'):
        entry['source_detail'] = 'merged_panel'
    if not entry.get('provider'):
        entry['provider'] = 'panel'
    if not entry.get('fetch_method'):
        entry['fetch_method'] = 'merged_panel'
    if not entry.get('storage_path'):
        entry['storage_path'] = MERGED_STORAGE_PATH


def main():
    merged_path, merged_rel = _select_merged_panel()
    global MERGED_STORAGE_PATH
    MERGED_STORAGE_PATH = merged_rel

    if not CATALOG.exists():
        raise FileNotFoundError(f"catalog.csv not found at {CATALOG}")
    if not merged_path.exists():
        raise FileNotFoundError(f"merged panel not found at {merged_path}")

    df_cat = pd.read_csv(CATALOG, dtype=str)
    df_cat = df_cat.fillna('')
    if 'series' in df_cat.columns:
        df_cat = df_cat[~df_cat['series'].isin(DEPRECATED_SERIES)].copy()

    # Ensure the catalog includes all series referenced in the config so new additions
    # (e.g., EUR_*_XR crosses) appear in catalog.csv.
    needed = _extract_series_from_config(CONFIG)
    df_cat = _append_missing_catalog_rows(df_cat, needed)

    panel = pd.read_csv(merged_path, parse_dates=True, index_col=0)
    panel.index.name = 'Date'

    # Augment the preferred merged panel with any columns still only present in the
    # legacy export so existing coverage ratios do not disappear.
    canonical_series = {
        SERIES_CANONICAL_ALIASES.get(series, series)
        for series in df_cat.get('series', [])
        if series
    }
    missing_series = {series for series in canonical_series if series not in panel.columns}
    fallback_path = None
    for path, _ in MERGED_CANDIDATES:
        if path == merged_path:
            continue
        if path.exists():
            fallback_path = path
            break

    if fallback_path and missing_series:
        header = pd.read_csv(fallback_path, nrows=0).columns.tolist()
        if header:
            index_label = header[0]
            fallback_candidates = [c for c in header[1:] if c in missing_series]
            if fallback_candidates:
                fallback_data = pd.read_csv(
                    fallback_path,
                    parse_dates=True,
                    index_col=0,
                    usecols=[index_label] + fallback_candidates,
                )
                fallback_data = fallback_data.reindex(panel.index)
                panel = pd.concat([panel, fallback_data], axis=1)
                panel.index.name = 'Date'

    # Prepare result columns
    results = []
    calendar_gap_rows: list[dict] = []
    for idx, row in df_cat.iterrows():
        raw_series = row['series']
        series = SERIES_CANONICAL_ALIASES.get(raw_series, raw_series)
        freq = row.get('frequency_label', 'daily') or 'daily'
        entry = row.to_dict()

        storage_path = str(row.get('storage_path', '') or '').strip()
        local_series: pd.Series | None = None
        if storage_path:
            # For some provider-derived series, the merged panel may contain stale exports;
            # prefer the explicitly versioned CSV pointed to by storage_path.
            normalized = storage_path.replace('\\', '/').lstrip('/')
            prefer_storage_prefixes = (
                'data_repository/raw/providers/derived_risk_drivers/',
                'data_repository/raw/providers/bank_indices/',
            )
            if normalized.startswith(prefer_storage_prefixes):
                candidate = ROOT / storage_path
                local_series = _load_timeseries_from_csv(candidate, series)

        if series != raw_series:
            entry['series'] = series
            # Make the display title consistent with the ISO-suffixed series.
            if series == 'BIS_LBS_Household_Loans_USA':
                title = entry.get('title', '')
                if title and 'USA' not in title:
                    entry['title'] = f"USA {title}".strip()
            if series == 'BIS_LBS_Private_NFC_USA':
                title = entry.get('title', '')
                if title and 'USA' not in title:
                    entry['title'] = f"USA {title}".strip()

        if local_series is None and series not in panel.columns:
            # mark as no data
            entry.update({
                'coverage_ratio': '',
                'coverage_bucket': coverage_bucket(np.nan),
                'median_gap_days': '',
                'last_observation': '',
                'window_obs': '',
                'total_obs': '0',
                'has_data': 'False'
            })
            results.append(entry)
            continue

        if local_series is not None and not local_series.empty:
            ser = local_series.dropna()
            total_obs = int(ser.notna().sum())
        else:
            ser = panel[series].dropna()
            total_obs = int(panel[series].notna().sum())
        if ser.empty:
            entry.update({
                'coverage_ratio': 0.0,
                'coverage_bucket': coverage_bucket(0.0),
                'median_gap_days': '',
                'last_observation': '',
                'window_obs': 0,
                'total_obs': total_obs,
                'has_data': 'False'
            })
            results.append(entry)
            continue

        # restrict to window
        window_ser = ser[(ser.index >= WINDOW_START) & (ser.index <= WINDOW_END)]
        inferred_freq = infer_series_frequency(ser.index, freq)
        inferred_norm = normalize_frequency_label(inferred_freq)
        expected = expected_count_between(WINDOW_START, WINDOW_END, inferred_norm)
        obs_in_window = int(window_ser.notna().sum())
        expected_dates = expected_dates_between(WINDOW_START, WINDOW_END, inferred_norm)
        expected_slots = len(expected_dates)
        observed_window = window_ser[window_ser.index.isin(expected_dates)]
        observed_slots = int(observed_window.notna().sum())
        weekend_gaps = count_weekend_gaps(WINDOW_START, WINDOW_END, window_ser.index)
        calendar_gap_rows.append({
            'series': series,
            'freq_label': inferred_norm,
            'expected_slots': expected_slots,
            'observed_slots': observed_slots,
            'weekend_slots': WEEKEND_SLOTS,
            'weekend_gaps': weekend_gaps,
        })
        cov = float(obs_in_window / expected) if expected and expected > 0 else np.nan
        if not pd.isna(cov):
            # Guard against mis-inferred frequencies and duplicates: coverage is a fraction.
            cov = float(min(max(cov, 0.0), 1.0))

        # median gap in days within the window (fallback to full-ser gaps)
        if len(window_ser) >= 2:
            diffs = window_ser.index.to_series().diff().dropna().dt.total_seconds() / 86400.0
            median_gap = float(max(diffs.median(), 1.0))
        else:
            diffs_all = ser.index.to_series().diff().dropna().dt.total_seconds() / 86400.0
            median_gap = float(diffs_all.median()) if not diffs_all.empty else float(365)

        last_obs = ser.index.max().isoformat()

        entry.update({
            'coverage_ratio': round(float(cov), 6) if not pd.isna(cov) else '',
            'coverage_bucket': coverage_bucket(cov),
            'median_gap_days': round(float(median_gap), 3),
            'last_observation': last_obs,
            'window_obs': int(expected) if expected else '',
            'total_obs': total_obs,
            'has_data': 'True'
        })

        if 'frequency_label' in entry:
            entry['frequency_label'] = inferred_norm

        # Autofill metadata for EUR crosses if missing.
        _autofill_eur_fx_metadata(entry)
        _autofill_eur_funding_metadata(entry)
        results.append(entry)

    if calendar_gap_rows:
        pd.DataFrame(calendar_gap_rows).to_csv(CALENDAR_GAP_LOG, index=False)

    out_df = pd.DataFrame(results)

    # If canonicalization caused duplicates (same series listed multiple times),
    # keep the row with the most information.
    if 'series' in out_df.columns and not out_df.empty:
        tmp = out_df.replace('', np.nan)
        filled = tmp.notna().sum(axis=1)
        cov = pd.to_numeric(out_df.get('coverage_ratio', ''), errors='coerce').fillna(-1)
        total = pd.to_numeric(out_df.get('total_obs', ''), errors='coerce').fillna(-1)
        out_df = out_df.assign(_filled=filled, _cov=cov, _total=total)
        out_df = out_df.sort_values(['series', '_filled', '_cov', '_total'], ascending=[True, False, False, False])
        out_df = out_df.drop_duplicates(subset=['series'], keep='first')
        out_df = out_df.drop(columns=['_filled', '_cov', '_total'])

    # Apply suppression flags deterministically (do not carry forward legacy values).
    out_df['do_not_use'] = ''
    out_df.loc[out_df['series'].isin(DO_NOT_USE_SERIES), 'do_not_use'] = 'DO NOT USE'

    # Backup existing catalog
    backup = CATALOG.with_suffix('.csv.bak')
    if backup.exists():
        from datetime import datetime

        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup = CATALOG.with_suffix(f'.csv.bak_{stamp}')
    CATALOG.rename(backup)
    out_df.to_csv(CATALOG, index=False)
    print(f"Wrote updated catalog to {CATALOG} (backup saved to {backup})")

    # Produce risk_factor_health.csv using the same window
    health_rows = []
    for col in panel.columns:
        ser = panel[col].loc[(panel.index >= WINDOW_START) & (panel.index <= WINDOW_END)].dropna()
        observed = int(ser.notna().sum())
        # find frequency; try to read from updated catalog
        freq = 'daily'
        match = out_df[out_df['series'] == col]
        if not match.empty:
            freq = match.iloc[0].get('frequency_label') or 'daily'
        freq_index = ser.index if not ser.empty else panel[col].index
        inferred_freq = infer_series_frequency(freq_index, freq)
        expected = expected_count_between(WINDOW_START, WINDOW_END, inferred_freq)
        coverage = float(observed / expected) if expected and expected > 0 else 0.0

        if ser.empty:
            mean = std = minv = maxv = np.nan
            flags = 'no_data'
        else:
            mean = float(ser.mean())
            std = float(ser.std())
            minv = float(ser.min())
            maxv = float(ser.max())
            flags_list = []
            if coverage < 0.62:
                flags_list.append('low_coverage')
            if np.isclose(std, 0.0):
                flags_list.append('flat_series')
            if 'spread' in col.lower() and minv < -1e-6:
                flags_list.append('spread_negative')
            if 'gdp' in col.lower() and minv <= 0:
                flags_list.append('gdp_nonpositive')
            flags = '|'.join(flags_list)

        health_rows.append({
            'series': col,
            'coverage': coverage,
            'mean': mean,
            'std': std,
            'min': minv,
            'max': maxv,
            'flags': flags,
            'flagged': bool(flags)
        })

    health_df = pd.DataFrame(health_rows)
    health_df.to_csv(OUT_DIR / 'risk_factor_health.csv', index=False)
    print(f"Wrote refreshed health to {OUT_DIR / 'risk_factor_health.csv'}")


if __name__ == '__main__':
    main()
