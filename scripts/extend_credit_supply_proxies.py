#!/usr/bin/env python3
"""Extend corporate credit supply series with long-history public proxies.

The project uses `BIS_LBS_Private_NFC_<ISO>` as a corporate credit supply driver.
For several countries this BIS LBS-derived series starts only in 2013/2014,
which pushes its catalog `coverage_ratio` below the project `series_threshold`.

This script backfills those series using free public proxies:
- Euro area countries (DEU/FRA/ITA/ESP): ECB Data Portal (BSI) loans to NFCs.
- USA: FRED (fredgraph CSV) Commercial & Industrial Loans (BUSLOANS).

Method:
1) Fetch proxy series.
2) Convert to month-end frequency.
3) Scale proxy to match the BIS series over the overlap window
   (median ratio on overlapping non-null observations).
4) Fill missing BIS history with the scaled proxy.
5) Write updated columns back into any existing stress panel copies.

Usage:
	python scripts/extend_credit_supply_proxies.py

After running, refresh catalog coverage fields:
	python scripts/refresh_catalog_and_health.py
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parents[1]

BIS_LBS_MONTHLY_DIR = (
	ROOT / "data_repository" / "raw" / "providers" / "bis_lbs" / "monthly"
)

PANEL_CANDIDATES: list[Path] = [
	ROOT / "stress_indicators_expanded.csv",
	ROOT / "data" / "stress_indicators_expanded.csv",
	ROOT / "data_pipeline" / "data" / "stress_indicators_expanded.csv",
]

ECB_AREA_BY_ISO3 = {
	"DEU": "DE",
	"FRA": "FR",
	"ITA": "IT",
	"ESP": "ES",
}


@dataclass(frozen=True)
class SeriesBackfillSpec:
	iso3: str
	target_column: str


def load_bis_lbs_monthly_series(series_name: str) -> pd.Series:
	path = BIS_LBS_MONTHLY_DIR / f"{series_name}.csv"
	if not path.exists():
		return pd.Series(dtype=float)
	try:
		df = pd.read_csv(path)
	except Exception:
		return pd.Series(dtype=float)

	if df.empty:
		return pd.Series(dtype=float)

	date_col = next((c for c in df.columns if str(c).lower() in {"date", "time"}), None)
	value_col = next((c for c in df.columns if str(c).lower() in {"value"}), None)
	if date_col is None:
		date_col = df.columns[0]
	if value_col is None:
		value_col = next((c for c in df.columns if c != date_col), None)
	if value_col is None:
		return pd.Series(dtype=float)

	df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
	df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
	df = df.dropna(subset=[date_col, value_col]).sort_values(date_col)
	if df.empty:
		return pd.Series(dtype=float)

	s = df.set_index(date_col)[value_col]
	s.index = pd.DatetimeIndex(s.index)
	s.name = series_name
	s = s[~s.index.duplicated(keep="last")]
	return s


def _to_month_end_index(dates: Iterable[str]) -> pd.DatetimeIndex:
	periods = pd.PeriodIndex([str(x) for x in dates], freq="M")
	return periods.to_timestamp(how="end")


def fetch_ecb_bsi_loans_to_nfc(
	area: str,
	start_period: str = "1990-01",
	end_period: str = "2025-12",
) -> pd.Series:
	"""Fetch ECB BSI loans to non-financial corporations (stock).

	Series key pattern (ECB BSI):
		M.<REF_AREA>.N.A.A20.A.1.U2.2240.Z01.E

	REF_AREA uses 2-letter codes (DE, FR, IT, ES, ...).
	"""

	key = f"M.{area}.N.A.A20.A.1.U2.2240.Z01.E"
	url = f"https://data-api.ecb.europa.eu/service/data/BSI/{key}"
	params = {
		"startPeriod": start_period,
		"endPeriod": end_period,
		"format": "csvdata",
		"detail": "dataonly",
	}
	resp = requests.get(url, params=params, timeout=45)
	resp.raise_for_status()

	reader = csv.DictReader(StringIO(resp.text))
	dates: list[str] = []
	values: list[float] = []
	for row in reader:
		t = (row.get("TIME_PERIOD") or "").strip()
		v = (row.get("OBS_VALUE") or "").strip()
		if not t or not v or v in {".", "NaN"}:
			continue
		try:
			dates.append(t)
			values.append(float(v))
		except ValueError:
			continue

	if not dates:
		return pd.Series(dtype=float)

	idx = _to_month_end_index(dates)
	s = pd.Series(values, index=idx, name=f"ECB_BSI_NFC_LOANS_{area}").sort_index()
	s = s[~s.index.duplicated(keep="last")]
	return s


def fetch_fred_fredgraph_series(series_id: str) -> pd.Series:
	"""Fetch a FRED series without an API key via the fredgraph CSV endpoint."""
	url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
	resp = requests.get(url, params={"id": series_id}, timeout=45)
	resp.raise_for_status()

	df = pd.read_csv(StringIO(resp.text))
	if df.empty:
		return pd.Series(dtype=float)

	date_col = None
	for candidate in ("DATE", "observation_date", "Observation_Date", "date"):
		if candidate in df.columns:
			date_col = candidate
			break
	if date_col is None:
		return pd.Series(dtype=float)

	df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
	value_col = next((c for c in df.columns if c != date_col), None)
	if value_col is None:
		return pd.Series(dtype=float)

	df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
	df = df.dropna(subset=[date_col, value_col]).sort_values(date_col)
	if df.empty:
		return pd.Series(dtype=float)

	s = df.set_index(date_col)[value_col]
	s.index = pd.DatetimeIndex(s.index)
	s.name = series_id
	return s


def _scale_proxy_to_target(proxy: pd.Series, target: pd.Series) -> float:
	overlap = target.dropna().index.intersection(proxy.dropna().index)
	if len(overlap) < 12:
		if len(overlap) == 0:
			return 1.0
		ratios = (target.loc[overlap] / proxy.loc[overlap]).replace(
			[np.inf, -np.inf], np.nan
		).dropna()
		return float(ratios.iloc[-1]) if not ratios.empty else 1.0

	ratios = (target.loc[overlap] / proxy.loc[overlap]).replace(
		[np.inf, -np.inf], np.nan
	).dropna()
	if ratios.empty:
		return 1.0
	return float(ratios.median())


def backfill_series(target: pd.Series, proxy: pd.Series) -> pd.Series:
	proxy = proxy.sort_index()
	target = target.sort_index()

	if proxy.empty:
		return target

	if not isinstance(proxy.index, pd.DatetimeIndex):
		proxy.index = pd.to_datetime(proxy.index, errors="coerce")
	proxy = proxy.dropna()

	# Resample proxy to month-end (ffill within month if higher frequency).
	proxy_monthly = proxy.resample("ME").last().ffill()
	proxy_aligned = proxy_monthly.reindex(target.index)

	scale = _scale_proxy_to_target(proxy_aligned, target)
	proxy_scaled = proxy_aligned * scale

	combined = target.astype(float).copy()
	fill_mask = combined.isna() & proxy_scaled.notna()
	combined.loc[fill_mask] = proxy_scaled.loc[fill_mask]
	return combined


def update_panel(path: Path, specs: list[SeriesBackfillSpec]) -> bool:
	if not path.exists():
		return False

	df = pd.read_csv(path, index_col=0, parse_dates=True)
	df.sort_index(inplace=True)

	updates = 0
	for spec in specs:
		if spec.target_column not in df.columns:
			print(f"[SKIP] {path.name}: missing column {spec.target_column}")
			continue

		target = pd.to_numeric(df[spec.target_column], errors="coerce")

		if spec.iso3 in ECB_AREA_BY_ISO3:
			area = ECB_AREA_BY_ISO3[spec.iso3]
			proxy_raw = fetch_ecb_bsi_loans_to_nfc(area)
		elif spec.iso3 == "USA":
			proxy_raw = fetch_fred_fredgraph_series("BUSLOANS")
		else:
			print(f"[SKIP] No proxy configured for {spec.iso3}")
			continue

		if proxy_raw.empty:
			print(
				f"[WARN] Proxy empty for {spec.iso3}; leaving {spec.target_column} unchanged"
			)
			continue

		combined = backfill_series(target=target, proxy=proxy_raw)
		df[spec.target_column] = combined
		updates += 1
		first = combined.first_valid_index()
		last = combined.last_valid_index()
		print(
			f"[OK] {path.name}: backfilled {spec.target_column} ({spec.iso3}) | "
			f"{first.date() if first is not None else None} -> {last.date() if last is not None else None}"
		)

	household_isos = ["DEU", "FRA", "ITA", "ESP", "USA"]
	for iso3 in household_isos:
		col = f"BIS_LBS_Household_Loans_{iso3}"
		if col not in df.columns:
			continue

		target = pd.to_numeric(df[col], errors="coerce")
		source = load_bis_lbs_monthly_series(col)
		if source.empty:
			print(f"[WARN] BIS raw monthly series missing/empty for {col}")
			continue

		# Align the month-end observations into the daily-index panel.
		source_aligned = source.reindex(df.index)
		combined = target.astype(float).copy()
		fill_mask = combined.isna() & source_aligned.notna()
		if int(fill_mask.sum()) == 0:
			continue
		combined.loc[fill_mask] = source_aligned.loc[fill_mask]
		df[col] = combined
		updates += 1
		first = combined.first_valid_index()
		last = combined.last_valid_index()
		print(
			f"[OK] {path.name}: synced {col} from BIS monthly raw | "
			f"{first.date() if first is not None else None} -> {last.date() if last is not None else None}"
		)

	if updates:
		tmp = path.with_suffix(
			f".tmp_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
		)
		df.to_csv(tmp, index_label=df.index.name or "Date")
		tmp.replace(path)
		print(f"[SAVED] {path} (updated {updates} columns)")
	return bool(updates)


def main() -> int:
	specs = [
		SeriesBackfillSpec("DEU", "BIS_LBS_Private_NFC_DEU"),
		SeriesBackfillSpec("FRA", "BIS_LBS_Private_NFC_FRA"),
		SeriesBackfillSpec("ITA", "BIS_LBS_Private_NFC_ITA"),
		SeriesBackfillSpec("ESP", "BIS_LBS_Private_NFC_ESP"),
		SeriesBackfillSpec("USA", "BIS_LBS_Private_NFC_USA"),
	]

	any_updates = False
	for path in PANEL_CANDIDATES:
		updated = update_panel(path, specs)
		any_updates = any_updates or updated

	if not any_updates:
		print("No panel updates applied (no files found or no matching columns).")
	else:
		print("\nNext: run `python scripts/refresh_catalog_and_health.py` to recompute coverage_ratio.")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())