# Phase 3 Asset Inventory
 
## Step 4 – Lasso mappings & Diagnostics
- 4.0_lasso_pipeline.py (clean + diagnostics-aware Lasso runner)
- 4.1_lasso_mapping_trainer.py (wrapper for 4.0_lasso_pipeline.py)
- 4.2_lasso_mapping_daily_trainer.py (wrapper for scripts/step4_lasso_pipeline_daily.py)
- config/country_blocks_extended.yaml (configuration copied into phase folder)
- config/series_metadata.yaml (metadata driving ingestion)
- catalog.csv (stress catalog referenced by health guards)

## Step 1 – Data Health Diagnostics
- 1.1_data_health_diagnostics.py (wrapper for scripts/data_health_checks.py)
- 1.2_coverage_threshold_optimizer.py (records the 0.62 coverage threshold so later optimizers can bootstrap from that baseline)
- additional diagnostics produced under analysis_outputs (risk_factor_health, risk_factor_holes_feed, etc.)

> Coverage filtering and health guards now use a 62% cutoff; lower-coverage candidates still appear in `data/cleaned_monthly_panel_full.parquet` so phased diagnostics can see the full pre-filter universe.

## Step 2 – Country Block Definition & Tagging
- 2.1_country_block_definer.py (wrapper for scripts/prepare_country_blocks.py)

## Step 3 – Factor Preparation (Block-level)
- 3.1_country_factor_preparer.py (wrapper for scripts/prepare_country_factors.py)
- 3.2_daily_factors_builder.py (wrapper for scripts/build_daily_factors_for_st.py)
- 3.3_daily_panel_builder.py (wrapper for scripts/run_daily_for_st_pipeline.py)

## Step 4 – Lasso mappings & Diagnostics
- 4.0_lasso_pipeline.py (clean + diagnostics-aware Lasso runner)
- 4.1_lasso_mapping_trainer.py (wrapper for 4.0_lasso_pipeline.py)
- 4.2_lasso_mapping_daily_trainer.py (wrapper for scripts/step4_lasso_pipeline_daily.py)

## Step 5 – Feature Shortlist & Collinearity
- 5.1_collinearity_shortlist.py (wrapper for scripts/step5_shortlist_collinearity.py)
- 5.2_collinearity_shortlist_daily.py (wrapper for scripts/step5_shortlist_collinearity_daily.py)

## Step 6 – Diagnostics, Validation & Backtesting Layer
- 6.1_daily_chain_runner.py (wrapper sequencing daily_factor_preparation, daily_elasticnet_mapping, and daily_shortlist_builder)
- 6.2_backtest_daily_runner.py (wrapper for daily_adcc_prep within this folder)
- 6.3_iso_adcc_runner.py (wrapper for iso_adcc_diagnostics within this folder)
- 6.4_verification_with_logging.py (wrapper for scripts/run_phase3_with_logging.py)

## Step 7 – Threshold watchdog and volatility diagnostics
- 7.0_coverage_threshold_watchdog.py (runs the optimizer once diagnostics exist and adjusts the 62% baseline if eigenvalues/persistence are out of range)
- 7.1_volatility_mean_reversion_runner.py (wrapper for Volatility_MeanReversion/run_pipeline.py)
- 7.2_dcc_garch_trainer.py (wrapper for DCC GARCH MODEL/fit_dcc_garch.py and helpers)
- scripts/coverage_threshold_optimizer.py (used by the watchdog to calibrate the threshold)

## Orchestration
- run_phase3_pipeline.py (executes every numbered wrapper in order so the folder runs Phase 3 end-to-end)

> Notes: Daily routines output diag_corr_daily, diag_corr, diagnostics JSONs, etc. The Phase 3 pipeline also relies on core datasets stored under `analysis_outputs/`, `models/`, and root-level configs (e.g., configs, catalog, series metadata). We'll replicate or reference these assets inside `SRESS TEST PIPELINE` where practical.
