Project: FRM data pipeline

This repository contains a Jupyter notebook `data gathering + clean.ipynb` and helper scripts in `tools/`.

Key tools:
- tools/prepare_datasets.py  - creates per-nation prepared par-yield CSVs in `Data/prepared/`.
- tools/bootstrap_ns_runner.py - prototype runner that bootstraps par-yields per date and writes zero curves to `Output/curves/`.

Workflow:
1. Run the notebook `data gathering + clean.ipynb` to produce cleaned canonical CSVs in `Output/`.
2. Use `tools/prepare_datasets.prepare_all()` to generate `Data/prepared/BOND_<nation>_par_yields.csv`.
3. Use `tools/bootstrap_ns_runner.run_for_all()` to produce per-date bootstrap outputs and a zeros matrix.

Dependencies: pandas, numpy, scipy (optional for smoothing/NS), yfinance (optional for fetching).

## BIS SDMX RESTful API pulls

- Use `fetchers/fetch_bis_api.py list-flows` to inspect which BIS dataflows (LBS, CDIS, DSR, NEER, etc.) are exposed by the documented SDMX v1.4.0 service.
- Run `fetchers/fetch_bis_api.py fetch --flow <FLOW_ID> --key <KEY> [--start-period 1990 --end-period 2025]` to download the matching cube region into `data_repository/raw/bis_api`. The command also logs each run to `bis_api_metadata.csv` so you can track when a flow was last updated.
- After new raw XML payloads land, regenerate `analysis_outputs/bis_series_components.csv` via `scripts/extract_bis_series_components.py` so you can quickly identify the component combinations for each `Series` entry.
- When you want to download the subset that was previously captured, use `scripts/run_bis_fetch_with_components.py` with `--flow`/`--title-contains` and `--key` to replay the fetcher with the exact `--component` flags captured in the CSV, or add `--dry-run` to preview the commands before execution.
- This helper workflow replaces brute-force wildcard hits by mirroring the component filters that successfully produced each cached payload; rerun the extractor each time you ingest a new payload so the inventory stays fresh.
