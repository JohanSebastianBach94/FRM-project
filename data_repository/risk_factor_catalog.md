# Risk Factor Catalog

This catalog tracks the risk factors we already ingest for the stress/sovereign blocks and the ones we still need to source (or derive) so the pipeline can cover the full set described in `data_repository/block_catalog.yaml`.

## Risk factors currently ingested

| Block | Risk factor | Series / file | Source | Notes |
| --- | --- | --- | --- | --- |
| Macro | Real GDP level | `GDPC1`, `NAEXKP01<ISO>Q661S` etc. | FRED/Eurostat (stress indicator pack) stored under `data_repository/raw/risk_indicators/pipeline/stress_indicators_expanded.csv` | Covers USA + Italy/France/Germany/Spain; feeds `cleaned_monthly_panel` via `data_collection`. |
| Macro | Nominal GDP | `data_repository/raw/macro/wb_NY.GDP.MKTP.CD_<ISO>.json` | World Bank WDI API (via `scripts/extend_fetch_structural_data.py`) | Canonical nominal levels for denominators and derived deflators. |
| Prices & activity | CPI (`CPIAUCSL`, `FRACPIALLMINMEI`, etc.), unemployment | FRED & Eurostat via expanded stress pack | Same CSV as above | Used for inflation/U-rate risk exposures described in the macro block. |
| Credit & banking | BIS LBS locational aggregates (household + NFC loans, total loans) | `data_repository/raw/structural/bis_lbs_d_pub.csv` | BIS LBS public CSV (manually downloaded when API 404s) | Supplies credit-to-economy, household mortgage debt, and real-estate banking proxies. |
| Real estate | Housing price indexes (`CSUSHPISA`, `DEU_HPI_NOMINAL`, etc.) | FRED national series + derived proxies | FRED / national releases stored in the stress pack and metadata | Nominal HPI proxies used in each country block; German proxy now flows through `analysis_outputs/factor_preparation/DEU_factors.csv`. |
| Real estate | Mortgage rates | `MORTGAGE30US` (Freddie Mac) + country rate proxies | Freddie Mac, ECB (for EUR mortgage rates) | Captured for real-estate stress block; optional `MORTGAGE_RATE_DEU` etc. |
| Public finance | NSS/DNSS betas and government yields | `analysis_outputs/factor_preparation/<ISO>_factors.csv` + NSS beta outputs under `nss_models` | Internal DNSS pipeline (`reestimate_dnss_monthly.py`) | Beta factors already treated as risk factors for yield-risk exposures; yields drawn from NSS and Investing.com archives. |
| External / FX | EUR/USD | `data_repository/raw/market_data/FX_EURUSD_EURUSD_X.csv` + ECB reference rate | Investing.com + ECB SDW | Provides FX risk for every block that references `EUR_USD`. |

## Risk factors still needed (status: planned)

| Block | Risk factor | Target source | Notes / rationale |
| --- | --- | --- | --- |
| Macro | GDP components (C, I, G, NX) | IMF SDMX / OECD national accounts | Block catalog notes `IMF SDMX` as the planned provider; these components would live in the same structural area once ingested via `scripts/extend_fetch_structural_data.py` or a dedicated SDMX fetcher. |
| Macro | Labor force participation, wage growth | IMF / OECD labor tables | Needed to complete the employment/wage coverage flagged as `planned` in the macro block. |
| Macro | Manufacturing & services PMI | IHS Markit / S&P Global PMI releases | Planned to improve activity coverage; these monthly prints eventually feed the macro block when accessible. |
| Prices | CPI core, PPI, inflation expectations | IMF / Eurostat / Fed (survey data) | Marked as `planned` to complement headline CPI; would sit in the prices sub-block once obtained. |
| Real estate | Real HPI (deflated), price-to-income, price-to-rent, real-house price growth | Derived from nominal HPIs + GDP deflator + rent/income datasets (national statistical offices) | Catalog lists these as `planned` derived metrics; acquiring national rent/income series is the gating factor. |
| Real estate | BIS DSR table (debt-service ratio) | BIS database (DSR CSV or API) | Needed for the `credit_dsr` indicator in the real-estate block once downloaded and parsed. |
| Credit / External | BIS CDIS (IIP counterpart detail) | BIS CDIS public CSV (`bis_cdis_d_pub.csv`) | The helper still hits 404s; need to continue manual download attempts or request the data directly from BIS so we capture the cross-border investment positions flagged under the external / real-estate categories. |
| Public finance | IMF GFS deficits, primary balance, interest payments | IMF Government Finance Statistics (SDMX) | Planned to round out the public finance block (deficit-to-GDP, primary balance, interest burden). |
| External / FX | BIS/OECD NEER & REER | BIS SDMX / OECD | Listed as `planned` under the FX sub-block; enriches FX risk coverage beyond EUR/USD. |
| Banking / Liquidity | Additional BIS liquidity metrics (deposit stocks, DSR, sectoral funding) | BIS banking statistics (SDR,Liquidity) | Supports the banking system block and funds the optional `BIS_LBS_*` series noted in each country definition. |

## Next steps for completion

1. **Document missing sources** in `data_repository/raw/structural/structural_metadata.csv` each time a manual download or new provider is added (e.g., manual BIS bulk ZIP). 2. **Automate the planned SDMX pulls** for IMF/OECD data so the metadata, catalog, and `cleaned_monthly_panel.parquet` reflect the new fields. 3. **Clarify derivations** in `config/series_metadata.yaml` once we compute the derived risk factors (real-price growth, price-to-income, etc.) so `scripts/prepare_country_blocks.py` can confirm metadata coverage.
