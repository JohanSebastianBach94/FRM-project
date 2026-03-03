# Series triage report

Replay run: `replay_20260212_153430`

Episodes scanned: 3

Catalog join: OK


## Top 25 suspect series

| series | triage_score | max_abs_shock | episodes_total | hits_total | frequency_label | median_gap_days | recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PWHEAMTUSDM | 218.3 | 67 | 3 | 25 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PIORECRUSDM | 163.3 | 50.11 | 3 | 25 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PALUMUSDM | 121.7 | 37.8 | 3 | 24 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PMAIZMTUSDM | 91.34 | 28.03 | 3 | 25 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| lme_proxy_equal | 86.16 | 44.28 | 3 | 6 | nan |  | fix resampling; treat as level/step (no daily returns) |
| PCOPPUSDM | 80.61 | 24.74 | 3 | 25 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| ECBASSETS | 52.18 | 14.05 | 3 | 40 | daily | 1 | review; winsorize/shrink only if justified |
| Bank_equity_index_USA | 46.28 | 23.78 | 3 | 6 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| COMM_PAPER_SPREAD_EUR | 43.75 | 17.06 | 3 | 12 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| EUR_CHF | 40.39 | 18.38 | 2 | 8 | daily | 1 | review; winsorize/shrink only if justified |
| FAO_AG_INDEX | 35.81 | 10.99 | 3 | 25 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| COMM_PAPER_SPREAD_USA | 31.4 | 22.65 | 3 | 3 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| OAT_Bund_Spread | 25.58 | 15.9 | 3 | 4 | trading | 1 | fix resampling; treat as level/step (no daily returns) |
| PSOYBUSDQ | 25.25 | 11.49 | 1 | 8 | quarterly | 91.5 | fix resampling; treat as level/step (no daily returns) |
| MYAGM1EZM196N | 20.89 | 8.406 | 2 | 11 | daily | 1 | ok / monitor |
| SWAPTION_VOL_USA | 20.01 | 14.43 | 3 | 3 | trading | 1 | fix resampling; treat as level/step (no daily returns) |
| EUR_JPY | 17.58 | 7.999 | 2 | 8 | daily | 1 | ok / monitor |
| WALCL | 16.24 | 9.061 | 3 | 5 | daily | 1 | ok / monitor |
| ECBDFR | 15.72 | 8.078 | 2 | 6 | daily | 1 | ok / monitor |
| MYAGM2EZM196N | 14.74 | 7.575 | 2 | 6 | daily | 1 | ok / monitor |
| ESP_GDP_EUROSTAT | 14.24 | 12.96 | 1 | 2 | quarterly | 91.5 | fix resampling; treat as level/step (no daily returns) |
| SWAPTION_VOL_DEU | 12.33 | 4.807 | 3 | 12 | trading | 1 | fix resampling; treat as level/step (no daily returns) |
| Sovereign_spread_vs_Germany_USA | 10.79 | 15.57 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| EUR_GBP_XR | 10.46 | 4.761 | 2 | 8 | trading | 1 | fix resampling; treat as level/step (no daily returns) |
| FRA_GDP_EUROSTAT | 10.36 | 9.43 | 1 | 2 | quarterly | 91.5 | fix resampling; treat as level/step (no daily returns) |


Full CSV: `series_triage.csv`

