# Series triage report

Replay run: `replay_20260212_152321`

Episodes scanned: 3

Catalog join: OK


## Top 25 suspect series

| series | triage_score | max_abs_shock | episodes_total | hits_total | frequency_label | median_gap_days | recommendation |
| --- | --- | --- | --- | --- | --- | --- | --- |
| COMM_PAPER_SPREAD_USA | 31.4 | 22.65 | 3 | 3 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| SWAPTION_VOL_USA | 20.01 | 14.43 | 3 | 3 | trading | 1 | fix resampling; treat as level/step (no daily returns) |
| Bank_equity_index_DEU | 7.125 | 10.28 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PCOPPUSDM | 6.099 | 8.799 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PWHEAMTUSDM | 5.455 | 7.87 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| FAO_AG_INDEX | 4.127 | 5.954 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PIORECRUSDM | 4.024 | 5.805 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| PMAIZMTUSDM | 2.933 | 4.231 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| DCOILBRENTEU | 2.913 | 4.203 | 1 | 1 | daily | 1 | ok / monitor |
| WTI_Crude_Futures | 2.64 | 3.809 | 1 | 1 | daily | 1 | ok / monitor |
| DCOILWTICO | 2.621 | 3.781 | 1 | 1 | daily | 1 | ok / monitor |
| BIS_LBS_Household_Loans_DEU | 2.399 | 3.461 | 1 | 1 | monthly | 31 | fix resampling; treat as level/step (no daily returns) |
| EURIBOR_3m | 0.8693 | 1.254 | 1 | 1 | daily | 1 | ok / monitor |


Full CSV: `series_triage.csv`

