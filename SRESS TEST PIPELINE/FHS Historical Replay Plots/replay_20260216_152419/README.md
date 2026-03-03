# FHS Historical Replay Plots — Guide

This folder contains a ‘literature-style’ plot bundle for the Step 11.1 historical replay run.

- Source run dir: `C:/Users/frank/Documents/FRM project/analysis_outputs/scenarios/latest/historical_replay/replay_20260216_152419`
- Output root: `C:/Users/frank/Documents/FRM project/SRESS TEST PIPELINE/FHS Historical Replay Plots/replay_20260216_152419`

## Folder map (per episode)
Each episode subfolder contains the same plot categories:

- `severity/`
- `drivers/`
- `z_vs_innov/`
- `correlations/`
- `distributions/`
- `heatmaps/`
- `plot_exclusions.json` (if present)

## What each category means
### Severity (folder: `severity/`)
What it shows: how ‘big’ the episode is through time in each block, using z-shocks (standardized residuals).

Two severity measures are plotted:
- $S_{max}(t)=\max_i |z_{t,i}|$ (single-largest driver at time $t$)
- $S_{L2}(t)=\sqrt{\sum_i z_{t,i}^2}$ (broad stress energy at time $t$)

Why it’s useful: best first plot to rank ‘what blew up’ and when; it distinguishes broad stress (high $S_{L2}$) vs single-driver spikes (high $S_{max}$ only).

### Drivers (folder: `drivers/`)
What it shows: time series of the top-K driver series (by peak $|z|$) within each block during the episode.

Why it’s useful: answers ‘which risk drivers explain the severity plot,’ and whether it’s rates/credit/equity/FX/commodities doing the work.

### Z vs innov (folder: `z_vs_innov/`)
What it shows: side-by-side for top drivers: (a) z-shocks and (b) ‘unit innovations / replayed residuals’ (the replay output series).

Why it’s useful: great for diagnosing ‘fake stress’ from data artifacts. If z spikes but innovations are weird/step-like, you likely have a stale/step series issue.

### Correlations (folder: `correlations/`)
What it shows: correlation heatmap of z-shocks among the top ~20 series in that block during the episode window.

Why it’s useful: tells you if stress is a coherent regime move (many series moving together) vs fragmented. Economically, coherent correlation is what makes an episode feel like ‘a macro regime’ rather than noise.

### Distributions (folder: `distributions/`)
What it shows: compares the episode’s $|z|$ distribution vs a baseline $|z|$ distribution from pre-episode history (ECDF + box comparison).

Why it’s useful: quantifies tail-thickening. If the episode ECDF shifts right vs baseline, you’re seeing a real stress regime, not just one-off spikes.

### Heatmaps (folder: `heatmaps/`)
What it shows: an episode-level matrix of max$|z|$ with rows=blocks and columns=series (top-N series overall).

Why it’s useful: fastest ‘system map’ of which block/series combinations are responsible for episode severity.

### Plot exclusions (file: `plot_exclusions.json`)
What it is: audit trail of series excluded from driver selection/heatmaps due to ‘flat-then-spike’ or ‘stale-spike’ flags.

Why it’s useful: if a series disappears from drivers/heatmaps (and per-block plots), this explains it (robustness guard; not a silent drop).

Note: per-block plots in `severity/`, `drivers/`, `z_vs_innov/`, `correlations/` are generated using the filtered z-shocks (i.e., with excluded series removed when possible).

## Economic interpretation cheat sheet (episodes)
- **gfc_2008**: banking/systemic stress + broad risk repricing. Prioritize `severity/`, `heatmaps/`, `distributions/`.
- **eurozone_2011**: sovereign/bank loop + cross-country propagation. Prioritize `correlations/`, `drivers/`, `heatmaps/`.
- **covid_2020**: shock speed + volatility regime shift. Prioritize `severity/` (timing), `z_vs_innov/` (artifact check), `distributions/` (regime jump).
