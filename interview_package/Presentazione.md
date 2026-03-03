# Presentazione Progetto FRM

## 1. Sintesi
Struttura progetto: 
- raccolta dati pulita
- modellazione curva yield (NSS/DNSS con Kalman/RTS)
- modelli di volatilità (GARCH → FIGARCH → HAR‑RV)
- correlazioni dinamiche (DCC vs ADCC) 
- visualizzazioni di crisi (heatmap Lehman, COVID)
 - aggregazione per paese e "country risk blocks" (blocchi di rischio nazione usati per heatmap, DCC/ADCC e stress testing)
- stress testing (MonteCarlo + EBA)
- reverse stress testing
L'obiettivo è fornire una vista coerente dei risk factors, volatilità e co‑movimenti in stress.

**Obiettivo**: valutazine modelli adatti a stress testing multifattoriale cross-country

**Sintesi risultati chiave**:
- DNSS mensile + smoothing giornaliero migliora stabilità dei fattori senza perdere segnali di shock
- HAR‑RV supera GARCH su errori di breve periodo; FIGARCH utile solo in presenza di memoria lunga marcata
- ADCC cattura onset crisi più rapido rispetto DCC (picchi correlativi downside)
- Heatmap mostrano convergenza rapida delle correlazioni inter‑paese nei periodi Lehman e COVID

Diagramma pipeline (schema ASCII):
```
Raw Data → QA & Merge → DNSS Factors → Volatility Models → DCC/ADCC Correlations → Crisis Heatmaps → Insights
```

---

## 2. Pipeline Dati e Controlli Qualità
### Obiettivo
Costruire dataset integrato coerente, affidabile e tracciabile per modelli successivi.

### Why?
Errori o outlier non gestiti distorcono stima di volatilità e correlazioni; la qualità dei fattori curva dipende da densità e pulizia.

### Metodo
1. Ingest multi‑fonte (FRED, Yahoo, ETF settoriali, FX, commodity) → staging.
2. Allineamento calendario business‑day con regole conservative di riempimento.
3. Standardizzazione meta‑dati (country, asset class, bucket scadenza).
4. QA su outlier (z‑score dinamico + IQR) e gap di missing.
5. Segmentazione eventi crisi (timestamp Lehman, COVID) per etichettare stress.

### Codice implementazione
Implementato in: [`collect_industry_data.py`](collect_industry_data.py), [`merge_industry_data.py`](merge_industry_data.py). Report: `industry_data_summary.json`, `merge_report.json`.


### Dataset prodotto e fattori di rischio
Qui sotto una tabella semplificata: per ogni categoria riporto esempi di nomi storici (in forma leggibile, senza codici) che compaiono nel dataset finale, e la colonna "Macro‑rischio" che classifica il rischio principale associato.

| Categoria | Serie (esempi, nomi leggibili) | Macro‑rischio |
|---|---|---|
| Indice azionario globale | SP500 (S&P 500), MSCI World | Equity / Market risk |
| ETF settoriali | Financials, Energy, Healthcare, Technology, Industrials | Equity / Market risk |
| ETF / Indici paese | Japan equity, UK equity, Germany equity, France equity, Italy equity | Equity / Country risk |
| Credit spreads / indici credito | US AAA spread, US A spread, High Yield spread | Credit risk |
| ETF credito | High Yield ETF, Investment Grade ETF | Credit risk |
| Titoli di stato e rendimenti | Rendimenti 1Y–30Y, 10Y yield (paesi principali) | Interest rate / Rate risk |
| Spread sovrani | BTP‑Bund spread, OAT‑Bund spread | Sovereign credit / Country risk |
| Commodities | Brent, WTI, Natural Gas, Gold, Copper, Wheat, Corn, Soybeans | Commodity price risk |
| FX (tassi di cambio) | EUR/USD, JPY/USD, USD/GBP, CHF/USD, BRL/USD, MXN/USD | FX / Currency risk |
| Indicatori di liquidità / stress di mercato | TED spread, funding spreads, central bank balance sheet proxy | Liquidity / Funding risk |
| Fattori curva (DNSS / NSS) | Level, Slope, Curvature (fattori latenti curva) | Interest rate / Term structure risk |
| Volatilità realizzata / proxy | RV bipower, implied vol proxy (VIX proxy) | Volatility risk |
| Indicatori macro (resampled) | CPI, GDP (upsampled), unemployment (forward‑filled) | Macro / Inflation & growth risk |

---
## 3. Modellazione Curva dei Rendimenti (NSS → DNSS → Kalman/RTS)

### Obiettivo

Estrarre fattori latenti livello, pendenza e curvature per descrivere dinamica della curva dei rendimenti.

### Why?

Fattori curva fungono da driver comuni nei modelli di rischio e migliorano interpretazione di shock macro/finanziari.

### Metodo
Formula NSS (Svensson):
$$
\begin{aligned}
y(t) &= \beta_0 + \beta_1 \frac{1 - e^{-t/\lambda_1}}{t/\lambda_1}
        + \beta_2 \left(\frac{1 - e^{-t/\lambda_1}}{t/\lambda_1} - e^{-t/\lambda_1}\right)
        + \beta_3 \left(\frac{1 - e^{-t/\lambda_2}}{t/\lambda_2} - e^{-t/\lambda_2}\right) \\
H_\tau(\lambda) &= \big[1, h_1(\tau), h_2(\tau), h_3(\tau) \big]
\end{aligned}
$$
dove $h_1, h_2, h_3$ sono i termini di carico definiti dalla trasformazione esponenziale.

### Regressione WLS per la stima dei parametri NSS
Per stimare i parametri $\beta = [\beta_0,\beta_1,\beta_2,\beta_3]'$ della curva NSS si usa comunemente una regressione pesata (Weighted Least Squares, WLS). Questo approccio è preferibile al semplice OLS quando gli errori sui diversi tenori hanno varianze diverse (eteroschedasticità) o quando si vuole dare peso maggiore a osservazioni più affidabili (es. tenori con maggiore liquidità).

Modello lineare per ogni istante temporale (vectorizzazione sui tenori $\tau$):
$$
y = H\beta + \varepsilon,
\qquad \mathbb{E}[\varepsilon]=0, \quad \mathrm{Var}(\varepsilon)=\Sigma
$$
dove $y$ è il vettore dei rendimenti osservati sui vari tenori, $H$ è la matrice dei carichi $H_\tau(\lambda)$ valutata sui tenori, e $\Sigma$ è la matrice di covarianza degli errori (diagonale se si assume indipendenza cross‑tenor).

Se assumiamo $\Sigma=\sigma^2 W^{-1}$ (con $W$ matrice diagonale di pesi), lo stimatore WLS è:
$$
\hat{\beta}_{WLS} = (H' W H)^{-1} H' W y
$$
che minimizza la somma pesata dei residui $\sum_i w_i (y_i - h_i'\beta)^2$.

Scelta dei pesi
- peso proporzionale a $1/\widehat{\mathrm{Var}}(\varepsilon_i)$: se disponiamo di una stima della varianza per ogni tenore (ad es. da volatilità storica o qualità dei dati) usare l'inverso come peso riduce l'influenza di osservazioni rumorose.
- peso basato sulla liquidità: pesare di più i tenori con maggiore volume/attività di mercato (proxy di affidabilità osservazione).
- peso basato su maturità: a volte si preferisce stabilizzare la stima penalizzando tenori molto corti o molto lunghi.

Stima iterativa (IRLS) e relazione con GLS
- Se $\Sigma$ è sconosciuta, si può procedere per passi: inizializzare con OLS, stimare i residui, costruire $W$ come funzione dei residui (ad es. $w_i = 1/\hat{\varepsilon}_i^2$ o funzione robusta), ricalcolare WLS e ripetere fino a convergenza (Iteratively Reweighted Least Squares, IRLS).
- Quando $\Sigma$ non è diagonale (errori correlati tra tenori), il caso generale è la Generalized Least Squares (GLS): $\hat{\beta}_{GLS}=(H'\Sigma^{-1}H)^{-1}H'\Sigma^{-1}y$.

Errori standard e inference
- Varianza stimata dei parametri (sotto ipotesi classiche):
$$
\widehat{\mathrm{Var}}(\hat{\beta}_{WLS}) = (H' W H)^{-1} \hat{\sigma}^2
$$
con $\hat{\sigma}^2 = \frac{1}{n-p} (y-H\hat{\beta}_{WLS})' W (y-H\hat{\beta}_{WLS})$, dove $p$ è il numero di parametri.
- Se si vuole robustezza rispetto a violazioni di $\Sigma$, usare covarianza robusta tipo "sandwich" (heteroskedasticity‑consistent) adattata ai pesi.

Punti pratici di implementazione
- Normalizzare i tenori: talvolta si scala $\tau$ e le funzioni $h_j(\tau)$ per migliorare condizionamento numerico.
- Controllare multicollinearità: i termini NSS possono essere fortemente correlati per alcuni insiemi di tenori; considerare regolarizzazione (ridge) o riduzione di dimensione se necessario.
- Peso e outlier: scegliere una funzione pesata robusta (es. Huber‑style) se sono presenti outlier occasionali che distorcono la stima.
- Validazione: valutare RMSE per tenore e confrontare i residui in funzione della maturità per assicurare che i pesi abbiano l'effetto desiderato.

Esempio di algoritmo (pseudo‑codice):
1. Costruire $H$ valutando $H_\tau(\lambda)$ sui tenori osservati.
2. Stimare inizialmente $\beta^{(0)}$ con OLS.
3. Calcolare residui $\hat{\varepsilon}^{(0)} = y - H\beta^{(0)}$ e stimare $\hat{\sigma}_i^2$ (es. finestra storica o funzione dei residui).
4. Costruire $W = \mathrm{diag}(1/\hat{\sigma}_i^2)$ e calcolare $\beta^{(1)} = (H' W H)^{-1} H' W y$.
5. Ripetere i passi 3–4 fino alla convergenza (||\beta^{(k+1)}-\beta^{(k)}|| < tol).

Questa procedura fornisce stime dei parametri NSS più robuste in presenza di eteroschedasticità tra tenori e consente di integrare informazioni di qualità dei dati (liquidità) all'interno della stima della curva.

---

Stato‑spazio DNSS:
$$
\begin{aligned}
x_t &= A x_{t-1} + \eta_t, & \eta_t \sim \mathcal{N}(0,Q) \\
y_t &= H x_t + \varepsilon_t, & \varepsilon_t \sim \mathcal{N}(0,R)
\end{aligned}
$$
Filtro di Kalman:
$$
\begin{aligned}
\hat{x}_{t|t-1} &= A\hat{x}_{t-1|t-1}, & P_{t|t-1} = A P_{t-1|t-1} A' + Q \\
K_t &= P_{t|t-1} H' (H P_{t|t-1} H' + R)^{-1} \\
\hat{x}_{t|t} &= \hat{x}_{t|t-1} + K_t (y_t - H\hat{x}_{t|t-1}) \\
P_{t|t} &= (I - K_t H) P_{t|t-1}
\end{aligned}
$$
Smussatore RTS:
$$
\begin{aligned}
J_t &= P_{t|t} A' P_{t+1|t}^{-1} \\
\hat{x}_{t|T} &= \hat{x}_{t|t} + J_t (\hat{x}_{t+1|T} - \hat{x}_{t+1|t})
\end{aligned}
$$
Selezione tenori (ottimizzazione):
$$
\min_{S \subseteq \mathcal{T}} \; \mathrm{RMSE}(S) + \lambda\,\log\kappa\big(H_S(\lambda)\big)
$$

### Codice implementazione
File e moduli principali per l'implementazione DNSS/NSS nel repository:

- `reestimate_dnss_monthly.py` — routine batch che ristima i parametri NSS a frequenza mensile (ottimizzazione su griglia, output parametri per paese).
- `estimate_dnss_kalman_daily.py` — esecuzione giornaliera: costruisce matrice di osservazione H, applica filtro di Kalman e RTS smoother per ottenere fattori DNSS giornalieri.
- `analyze_dnss_creation.py` — script di analisi e diagnostica della pipeline di creazione DNSS (QA, scelta tenori, valutazione RMSE e condizionamento).
- `nss_models/` package:
        - `nss_models/core.py` — funzioni core: `nelson_siegel_svensson_institutional`, `optimize_nss_institutional`, `kalman_nss_filter`, `nss_observation_matrix`.
        - `nss_models/visualization.py` — utilità per valutazione grafica della bontà di fit e pannelli di diagnostics.
- `data_pipeline/nss_betas.py` — helper che estrae, normalizza e salva i betas NSS (usato da `DCC GARCH MODEL/build_complete_34series.py` e dai runner DCC).
- Notebooks utili per approfondimento e riproducibilità:
        - `NSS_Core_Implementation.ipynb` — walkthrough della stima NSS e verifiche numeriche.
        - `NSS_Visualization.ipynb` — notebook per generare i pannelli immagine usati nella presentazione.

### Figure principali 
Di seguito le figure NSS e esito fit

![Yield evolution by maturity](images/Image1_Yield_Evolution_by_Maturity.png)

![Yield evolution - long term](images/Image2_Yield_Evolution_Long_Term.png)

![Yield evolution 5y part 1](images/Image3_Yield_Evolution_5Years_Part1.png)

![Yield evolution 5y part 2](images/Image4_Yield_Evolution_5Years_Part2.png)

![NSS params Beta0 & Beta1](images/Image5_NSS_Parameters_Beta0_Beta1.png)

![NSS params Beta0 & Beta1 ITA vs USA](images/Image5_NSS_Parameters_Beta0_Beta1_ITA_USA.png)

![NSS params Beta2 & Beta3](images/Image6_NSS_Parameters_Beta2_Beta3.png)

![NSS params Beta2 & Beta3 ITA vs USA](images/Image6_NSS_Parameters_Beta2_Beta3_ITA_USA.png)

![Model overview](images/Image7_Version4A_Model.png)

![Data overview](images/Image7_Version4B_Data.png)

![FRED components](images/Image7_Version4C_FRED.png)

![Investing NSS summary](images/Image7_Version4D_Investing.png)

### Interpretazione
DNSS mensile offre fattori stabili; layer giornaliero cattura transizioni rapide. Smoothing riduce rumore e supporta comparazione cross‑asset.

### Limiti / Trade-off
- Kalman giornaliero più costoso computazionalmente.
- Scelta $\lambda_1, \lambda_2$ sensibile alla griglia di ottimizzazione.

---
## 4. Modellazione della Volatilità (GARCH, FIGARCH, HAR‑RV, varianti asimmetriche)
### Obiettivo
Produrre previsioni di volatilità robuste a differenti strutture (mean reversion, memoria lunga, multi‑scala, asimmetrie).

### Why?
volatility forecasts accurati migliorano allocazione rischio, margini e input per modelli di correlazione dinamica.

### Metodo
GARCH(1,1):
$$
\sigma_t^2 = \omega + \alpha \, \epsilon_{t-1}^2 + \beta \, \sigma_{t-1}^2, \quad \alpha+\beta<1
$$
Varianza incondizionata:
$$
\frac{\omega}{1-\alpha-\beta}
$$

FIGARCH(1,d,1):
$$
\phi(L)(1-L)^d (\epsilon_t^2 - \sigma^2) = \omega + [1-\beta(L)]\nu_t, \quad 0<d<1
$$

HAR‑RV:
$$
\begin{aligned}
RV_t &= c + \alpha_d RV_{t-1}^{(d)} + \alpha_w RV_{t-1}^{(w)} + \alpha_m RV_{t-1}^{(m)} + \varepsilon_t \\
RV_{t-1}^{(w)} &= \frac{1}{5}\sum_{i=1}^5 RV_{t-i}, \quad RV_{t-1}^{(m)} = \frac{1}{22}\sum_{i=1}^{22} RV_{t-i}
\end{aligned}
$$

Metriche confronto:
$$
\mathrm{MAE} = \frac{1}{T}\sum_t |\hat{\sigma}_t - \sigma_t|, \quad \mathrm{RMSE} = \sqrt{\frac{1}{T}\sum_t (\hat{\sigma}_t-\sigma_t)^2}
$$
$$
\mathrm{QLIKE} = \frac{1}{T}\sum_t \Big( \log \hat{\sigma}_t^2 + \frac{\epsilon_t^2}{\hat{\sigma}_t^2} \Big)
$$

### Codice implementazione
HAR‑RV & pipeline: [`../Volatility_MeanReversion/run_pipeline.py`](../Volatility_MeanReversion/run_pipeline.py). Modello HAR: [`../Volatility_MeanReversion/models/har_rv.py`](../Volatility_MeanReversion/models/har_rv.py). Varianti GARCH/FIGARCH integrate nello stesso flusso.

### Figure principali
![US10Y realized vs forecast](images/us_10y_panel_realized_vs_forecasts.png)

![France OAT‑Bund realized vs forecast](images/france_oat_bund_panel_realized_vs_forecasts.png)

![Italy BTP‑Bund realized vs forecast](images/italy_btp_bund_panel_realized_vs_forecasts.png)

### Interpretazione
HAR‑RV riduce errore di breve; FIGARCH cattura persistenza ma è più pesante; GARCH resta baseline semplice e stabile. Asimmetrie (GJR/E‑GARCH) marginali nei dataset esaminati.

### Limiti / Trade-off
- FIGARCH: costo computazionale + stima sensibile a inizializzazioni.
- HAR‑RV richiede serie realized solida (rolling window di qualità).

---
## 5. Correlazione Dinamica (DCC vs ADCC)
### Obiettivo
Stimare correlazioni condizionate tempo‑variabili e asimmetrie downside per analizzare contagio e co‑movimenti.

### Why?
Correlazioni dinamiche guidano stress scenario multi‑asset e misure di diversificazione effettiva.

### Metodo
Residui standardizzati: $\epsilon_t = D_t^{-1} u_t$.
Aggiornamento DCC:
$$
Q_t = (1-a-b)\bar{Q} + a\epsilon_{t-1}\epsilon_{t-1}' + b Q_{t-1}
$$
Normalizzazione:
$$
R_t = D(Q_t)^{-1/2} Q_t D(Q_t)^{-1/2}
$$
ADCC (asimmetria):
$$
Q_t = (1-a-b-\tfrac{g}{2})\bar{Q} + a\epsilon_{t-1}\epsilon_{t-1}' + b Q_{t-1} + g n_{t-1} n_{t-1}'
$$
Betas DNSS inclusi tra le variabili per arricchire struttura comune.

### Codice implementazione
Confronto numerico: [`dcc_vs_adcc_gap_summary.csv`](dcc_vs_adcc_gap_summary.csv).
Codice e script principali (cartella `DCC GARCH MODEL`):
- `DCC GARCH MODEL/fit_dcc_garch.py` — stima DCC/ADCC e componenti di likelihood.
- `DCC GARCH MODEL/run_dcc_34series.py` / `run_dcc_34series_CORRECTED.py` — runner per le serie selezionate.
- `DCC GARCH MODEL/build_complete_34series.py` — prepara input (34 series) e file di configurazione per DCC.
- `DCC GARCH MODEL/extract_country_data.py` / `extract_country_correlations_34series.py` — estrazione dati per blocchi paese e matrici di correlazione.
- `DCC GARCH MODEL/run_full_pipeline.py` — pipeline end‑to‑end (preprocessing → DCC → heatmaps).

Esempio: per rieseguire il flusso DCC completo usare `DCC GARCH MODEL/run_full_pipeline.py` che chiama la preparazione degli input e le routine di fitting.

### Figure principali

![France OAT‑Bund rolling correlation](images/france_oat_bund_panel_rolling_correlation.png)

![Italy BTP‑Bund rolling correlation](images/italy_btp_bund_panel_rolling_correlation.png)

![US 10Y rolling correlation](images/us_10y_panel_rolling_correlation.png)

### Interpretazione
ADCC risponde più rapidamente agli shock negativi (flight‑to‑quality). DCC sufficiente in periodi normali. Inclusione betas curva aumenta coerenza cross‑asset.

### Limiti / Trade-off
- Parametri sensibili alla scelta pre‑stima delle volatilità univariate.
- ADCC aggiunge complessità e possibile overfitting fuori dalle crisi.

---
## 6. Country Risk Blocks

### Obiettivo
Definire blocchi di rischio (risk blocks) per ciascuna nazione che aggregano indicatori omogenei (rendimenti sovrani, spread sovrani, fattori curva DNSS, credit/financial ETFs, volatilità realizzata) in componenti utilizzabili nei modelli di correlazione e negli heatmap di crisi.

### Perché
I risk block consentono di ridurre dimensionalità e rumore, facilitano l'interpretazione per area/paese e forniscono input stabili per DCC/ADCC e stress testing (es. shock di paese). Aggregare logicamente evita che singole serie illiquide dominino la misura di contagio.

### Metodo proposto (passi)
1. Selezione universale: per ogni paese P costruire l'universo U_P composto da:
        - rendimenti governativi (10Y, 1Y–30Y se disponibili),
        - spread sovrani (vs Bund/Bund‑proxy),
        - fattori DNSS (level/slope/curvature) se calcolati per il paese,
        - principali ETF/indici di credito e banking locali,
        - realized volatility del benchmark e proxy di liquidità (bid‑ask, TED, funding spread) quando presenti.
2. Standardizzazione: ciascuna serie in U_P viene rescaled (zscore rolling su finestra T_standard, p.es. 252 giorni) per rendere omogenee le scale prima dell'aggregazione.
3. Clustering / block assignment:
        - Opzione semplice: aggregazione per regole ad albero (Govt yields + spreads + DNSS betas + credito) con pesi predefiniti.
        - Opzione data‑driven: applicare clustering gerarchico o k‑means su una matrice di distanza costruita da correlazioni vettoriali (window rolling). Scegliere k in funzione di var explained (scree) e stabilità temporale.
4. Costruzione del block metric: per ogni block B_P calcolare metriche:
        - media pesata dei segnali standardizzati (weights by liquidity or inverse variance),
        - mediana e percentili per robustezza agli outlier,
        - tail‑co‑movement: fraction of series in U_P with returns below their 5th percentile (indicatore stress sincronizzato).
5. Thresholding e smoothing: applicare una soglia minima di copertura (es. almeno 60% delle serie in U_P non missing) e smoothing esponenziale per evitare salto‑rumore.

### Uso operativo
- Input a DCC/ADCC: utilizzare i block metrics (uno per paese) come variabili addizionali o come aggregati sostitutivi per ridurre dimensionalità.
- Heatmap & crisis panels: mostrare sia la block‑level correlation matrix che la decomposition per componenti (govt vs credito vs vol) per interpretabilità.
- Stress testing: shockare il block metric (ad es. +200bps sul block spread) e rimappare su serie sottostanti per misurare impatti di portafoglio.

### Implementazione (script e note)
- Script suggeriti: `create_country_blocks.py` (ingest U_P → standardize → cluster/aggregate → dump `country_blocks.csv`), `apply_block_shocks.py` (map from block shock to component series), `plot_country_block_panels.py` (visualizzazione heatmaps e tail metrics).
- Persistenza: salvare block metrics giornalieri in `data_pipeline/country_blocks.csv` con metadati (`coverage`, `n_series`, `weights_used`).
- Robustezze: preferire mediana o trimmed‑mean quando coverage è bassa; registrare warning automatici se coverage < threshold.
### Implementazione (script e note)
- Script suggeriti (helper): `create_country_blocks.py` (ingest U_P → standardize → cluster/aggregate → dump `country_blocks.csv`), `apply_block_shocks.py` (map from block shock to component series), `plot_country_block_panels.py` (visualizzazione heatmaps e tail metrics).  
- Script esistenti utili nel repo:
        - `DCC GARCH MODEL/extract_country_data.py` — estrazione e normalizzazione delle serie paese.  
        - `DCC GARCH MODEL/extract_country_correlations_34series.py` / `extract_country_correlations_CORRECTED.py` — costruzione matrici di correlazione a livello paese.  
        - `DCC GARCH MODEL/transform_and_validate.py` — utility per trasformazioni e validazione dati.
- Persistenza: salvare block metrics giornalieri in `data_pipeline/country_blocks.csv` con metadati (`coverage`, `n_series`, `weights_used`).
- Robustezze: preferire mediana o trimmed‑mean quando coverage è bassa; registrare warning automatici se coverage < threshold.

### Limitazioni e punti di attenzione
- Aggregare nasconde dispersione interna: sempre fornire una decomposizione per componenti quando si presentano shock.
- Cambiamenti strutturali: ricalcolare clustering periodicamente (quarterly) e monitorare stability metrics (Adjusted Rand Index vs previous clustering).
- Peso delle serie illiquide: evitare pesi eccessivi su serie con scarsa qualità; usare regole di downweight automatico.

### Esempio rapido (pseudo‑workflows)
1. `create_country_blocks.py --input industry_data_raw.csv --window 252 --method rule_based --out data_pipeline/country_blocks.csv`
2. `run_dcc.py --assets data_pipeline/country_blocks.csv --config dcc_config.yaml`

---

## 7. Heatmap di Crisi e Interpretazione per Regime
### Obiettivo
Visualizzare la struttura di correlazione in diverse fasi (pre‑crisi, onset, post‑shock) e per blocchi paese.

### Why?
Le heatmap facilitano identificazione rapida di cluster e contagio sistemico, supportando stress test e gestione rischio.

### Metodo
Generazione da script dedicati (cartella `DCC GARCH MODEL`): `create_correlation_heatmaps.py`, `create_country_heatmaps.py`, `create_regime_covariance_heatmaps.py`, `create_regime_heatmaps_v2.py`, `create_scaled_heatmaps.py`.
Blocco logico: Dati fattori + residui standardizzati → Matrici Q_t → Normalizzazione → Rendering heatmap per finestre evento.

### Heatmaps per crisi (Lehman / Dotcom / COVID)
Questa sezione presenta heatmap separate per eventi di riferimento: Lehman, Dotcom e COVID — utili per comparare pattern di contagio e co‑movimenti per regime.

### Figure principali

#### Lehman
![ADCC Lehman - blocks](images/adcc_lehman_bankruptcy_heatmapcore_panel.png)

#### Dotcom
![ADCC Dotcom - blocks](images/adcc_dotcom_bubble_peak_heatmapcore_panel.png)

#### COVID
![ADCC COVID - blocks](images/adcc_covid_pandemic_heatmapcore_panel.png)

### Interpretazione
Onset crisi: forte convergenza cluster; post‑shock dispersione parziale ma fattori curva restano elevati → persistenza rischio sovrano. Blocchi paese mostrano sincronizzazione rapida EU durante COVID.

### Limiti / Trade-off
- Scelta finestra evento impatta intensità visualizzata.
- Scaling improprio può mascherare differenze minori.

---
## 8. Insight Finali e Raccomandazione di Modello
### Insight chiave
- Architettura integrata migliora coerenza tra curva, volatilità e correlazioni.
- DNSS mensile + layer Kalman giornaliero bilancia stabilità e reattività.
- HAR‑RV preferibile per forecast breve; FIGARCH solo in presenza di persistenza marcata; GARCH baseline.
- ADCC utile per analisi onset crisi; DCC rimane standard operativo.
- Heatmap confermano contagio rapido cross‑country in eventi sistemici.

### Raccomandazioni
Base operativa: DNSS mensile + HAR‑RV + DCC; attivare ADCC e FIGARCH solo in fasi di stress diagnostico.
Includere betas curva sempre nelle correlazioni per robustezza strutturale.

### Estensioni possibili
- Regime switching sui betas DNSS.
- Copule per tail dependence selettiva.
- Rolling/adaptive parameters per DCC/ADCC in presenza di break strutturali.

### Limiti
- Complessità computazionale cresce con FIGARCH e ADCC.
- Dipendenza qualità realized volatility (HAR‑RV).

