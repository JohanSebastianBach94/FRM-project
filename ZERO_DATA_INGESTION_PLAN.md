# Zero-Data Series Ingestion Plan

This document tracks the implementation path for clearing `catalog.csv` rows with `has_data=False`. Each section defines the upstream data source, desired sampling rules, pipeline touchpoints, and validation/QA gates.

## 1. BIS LBS Household & Private NFC Loans
- **Source files**: `data_repository/processed/BIS_LBS_Household_Loans_{ISO}.csv` and `BIS_LBS_Private_NFC_{suffix}_{ISO}.csv` (quarterly levels from BIS statistics).
- **Action**: build a converter that parses the quarterly `period` column, converts to timestamp (quarter end), and resamples to month-end with forward-fill. Persist to
  `data_repository/raw/providers/bis_lbs/monthly/BIS_LBS_Household_Loans_{ISO}.csv` and matching files for `Private_NFC_{ISO}`.
- **Pipeline integration**: update `config/country_blocks_extended.yaml` and `catalog.csv` so the manual loader references the new monthly files.
- **QA**: expect ~360 observations since 1995; median gap ≤ 31 days. Plot YoY growth vs. original quarterly source to confirm no drifts.

## 2. NPL Proxy Series (DEU/ESP/FRA/ITA/USA)
- **Source files**: `data_repository/raw/providers/npl_proxies/npl_proxy_{ISO}.csv` (quarterly proxy derived from BIS via `build_npl_proxy_from_bis.py`).
- **Action**: extend/resample to month-end (forward-fill) and store under `data_repository/raw/providers/npl_proxies/monthly/`. Propagate metadata (`.meta.json`) with sampling + provenance.
- **Pipeline integration**: point `country_blocks_extended` `local_series_files` to the monthly versions and ensure `collect_industry_data.py` picks them up.
- **QA**: coverage ratio ≥ 0.95 over the trailing 10 yrs; compare monthly proxy to quarterly original (aggregation check) and document in `validation_log.txt`.

## 3. Real HPI (DEU/FRA/ITA/ESP)
- **Source**: Eurostat `prc_hpi` dataset for nominal HPI plus Eurostat HICP (`prc_hicp_midx`, `coicop=CP00`, `unit=I15`).
- **Action**: fetch quarterly price index and monthly CPI, align to quarterly, compute `real_hpi = price_index / (cpi / 100)` and normalize to 2015=100. Resample to month-end via forward-fill and save as `data_repository/raw/providers/real_estate/{ISO}_HPI_REAL.csv`.
- **Pipeline integration**: add `local_series_files` entries and update catalog rows (source=`Eurostat`, frequency=`monthly`).
- **QA**: verify YoY growth vs. OECD/BIS published real HPI; ensure last obs within 1 quarter of present; run volatility sanity check (std dev < nominal HPI).

## 4. Affordability Ratios (Price-to-Income & Rent-to-Income)
- **Source**: reuse existing `scripts/derive_real_estate_ratios.py` to pull Eurostat earnings + price/rent indices. Extend script to allow ISO filtering and ensure outputs land at `data_repository/raw/providers/derived_risk_drivers/Price_to_income_ratio_{ISO}.csv` and `Rent_to_income_ratio_{ISO}.csv` for ISO in `{DEU,FRA,ITA,ESP}`.
- **Action**: add CLI flags to regenerate all countries and produce metadata (rows, date range). Replace catalog rows to point to the new CSVs.
- **QA**: compare ratios to OECD housing data; check for monotone drift; run unit tests on resampling helper.

## 5. GC.DOD.TOTL.GD.ZS_DEU (General Government Gross Debt % GDP)
- **Source**: World Bank WDI API (primary) with IMF IFS fallback (GGXWDG_GDP).
- **Action**: add a lightweight fetcher function that requests `GC.DOD.TOTL.GD.ZS` for DEU, writes `data_repository/raw/macro/general_government_gross_debt_pct_gdp_DEU.csv`, and logs raw JSON to `wb_GC.DOD.TOTL.GD.ZS_DEU.json` for audit.
- **Pipeline integration**: rerun `collect_industry_data.py` → `merge_industry_data.py`, refresh `catalog.csv` entry with updated coverage stats.
- **QA**: cross-check last value vs. Eurostat government debt (difference < 1.5pp). Ensure at least 10 consecutive annual observations ending ≥2024.

## Orchestration Script
Implement `scripts/ingest_zero_data_series.py` to wrap the steps above:
1. `generate_bis_monthly_series()`
2. `generate_npl_monthly_series()`
3. `fetch_affordability_ratios()` (delegates to `derive_real_estate_ratios`)
4. `build_real_hpi()`
5. `fetch_gc_dod_deu()`
Each sub-step should emit structured logs, write metadata next to CSV outputs, and optionally produce a summary JSON of completed actions to feed `PIPELINE_INTEGRATION_SUMMARY.txt`.

## Validation & Catalog Refresh
- After running the orchestrator, rerun `collect_industry_data.py`, `merge_industry_data.py`, and `scripts/check_block_coverage.py`.
- Use `scripts/check_local_series_presence.py` to confirm the new manual files resolve.
- Update `catalog.csv` rows with fresh coverage stats (window_obs, median_gap_days, last_observation, coverage_bucket) and flip `has_data=True` once validation logs are captured.
