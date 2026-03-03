# Step 5 (Shortlist + Collinearity) — Soundness Review (Draft)

Date: 2026-01-18

## What Step 5 does today (monthly)
Implemented in [scripts/step5_shortlist_collinearity.py](scripts/step5_shortlist_collinearity.py) and wrapped by [SRESS TEST PIPELINE/5.1_collinearity_shortlist.py](SRESS%20TEST%20PIPELINE/5.1_collinearity_shortlist.py).

Inputs
- Step 4 diagnostics: `analysis_outputs/feature_contributions_<ISO>.csv`
- Feature panels:
  - PCA components: `analysis_outputs/factor_preparation/<ISO>_pca_components.csv`
  - Full feature panel (lags/etc, if produced): `analysis_outputs/factor_preparation/<ISO>_factors.csv`
  - FCI: `analysis_outputs/FCI_<ISO>.csv` (if present)
- Clean panel (coverage-filtered): `data/cleaned_monthly_panel.parquet` (optional, used only if feature names match)

Outputs
- Shortlist table: `analysis_outputs/feature_shortlist/factors_<ISO>.csv`
- Audit manifest: `analysis_outputs/feature_shortlist/manifest.json`

Core logic
1) Aggregate Step 4 information into per-feature scores:
   - Mean absolute coefficient magnitude across targets
   - Stability-selection frequency (if Step 4 produced stability files)
   - Coverage = fraction of targets in which feature appears
   - Weighted score = 0.5 contribution + 0.3 stability + 0.2 coverage (with fallback when stability is absent)
2) Choose an initial shortlist of size 8–12.
3) Force-include:
   - “Important” PCA components (explained variance ratio > 0.10, when `<ISO>_*pca*_explained.csv` exists)
   - Anything with `fci` in the feature name
   - Step 4 “top contributors” (derived from `top_contributions` strings)
4) Prune collinearity using:
   - VIF threshold (default 8)
   - Pairwise absolute correlation threshold (default 0.85)
   - When dropping, prefer keeping forced features; otherwise keep higher-scoring feature.

## What we validated in this session
- Step 4 Lasso trains and writes artifacts with the new Eurostat targets (GDP + unemployment).
- Step 5 runs and produces updated shortlist outputs.

## Key soundness observations

### 1) Step 5 is conceptually reasonable as a governance layer
The “Step 4 → rank features → enforce diversity/forced includes → prune collinearity” approach is a practical pattern:
- It acknowledges Lasso/ElasticNet selection instability.
- It separates *predictive mapping* (Step 4) from *final driver set* (Step 5).
- The manifest output is a strong auditability artifact.

### 2) Current scoring is heuristic and can drift from modeling goals
Today’s score uses coefficient magnitude + stability frequency + target coverage.
Potential pitfalls:
- Coefficient magnitudes can be misleading under correlated features: Lasso arbitrarily picks one of a group.
- Coverage across targets may overweight “generic” features (e.g., global risk proxies) at the expense of target-specific drivers.
- If Step 4 stability diagnostics are skipped or fail, stability terms become 0 everywhere; the score can unintentionally collapse.

Suggested direction:
- Treat “feature utility” as a blend of (i) out-of-sample contribution and (ii) selection stability, not raw coefficient size.
  - For example: average out-of-sample incremental $\Delta R^2$ when feature is included, or permutation importance computed once on the held-out set (not full permutation tests).

### 3) Collinearity pruning can be fragile with small samples
VIF and correlation computed after `dropna()` can leave few rows if any feature has gaps, making VIF/corr noisy.
Mitigations:
- Standardize a fixed date window per ISO (e.g., last N years) to stabilize correlation estimates.
- Prefer shrinkage correlation (e.g., Ledoit–Wolf) for high-dimensional panels.
- Consider clustering-based pruning: keep best feature per correlation cluster rather than pairwise greedy drops.

### 4) PCA + FCI mixing needs explicit intent
Step 5 forces:
- PCA components above an explained-variance threshold, plus
- any FCI-like features.

That can be correct, but should be explicit:
- PCA components are orthogonal by construction (within a block), so collinearity pruning across PCA components usually shouldn’t drop them unless you’re mixing across blocks or mixing with transformed panels.
- If FCI is itself built from similar underlying series, forcing both FCI and multiple PCs can reintroduce redundancy.

Suggested direction:
- Decide whether FCI is:
  - a required “headline” state variable (keep it, prune overlapping PCs), or
  - an alternative summary representation (use FCI *or* PCs).

### 5) Time-series methodology: beware leakage and unstable CV
Step 4 already tries to be time-series-safe (e.g., no contemporaneous leakage, CV via TimeSeriesSplit / walk-forward, moving-block bootstrap for stability).
Step 5 should maintain the same standards:
- Avoid using full-sample correlation/VIF if final evaluation is rolling or walk-forward.
- Ideally compute collinearity on the *training window only* (or within each fold) and aggregate decisions.

## Literature / practice comparison (high-level)
- Lasso selection instability under correlation is well-known; stability selection is a standard response.
  - Recommended reference: Meinshausen & Bühlmann (2010), “Stability Selection”.
- For time series, block bootstrap/subsampling is commonly used to respect dependence.
  - Moving-block bootstrap is a reasonable approximation.
- In macro/finance factor selection, common patterns are:
  - ElasticNet (groups of correlated predictors)
  - Group Lasso / sparse group lasso when predictors have natural groupings (blocks)
  - Post-selection refit (post-lasso OLS / ridge) to stabilize coefficients
  - Cluster representatives (choose 1 feature per highly-correlated cluster)

## Concrete upgrade proposals (prioritized)

### P0 — Make Step 5 consistent with Step 4 feature source
- Ensure the collinearity matrix is built from the same panel Step 4 used.
  - If Step 4 used `*_factors.csv` (lags, transforms), Step 5 should load it.

### P0 — Configurability
- Move Step 5 thresholds/weights to a config (YAML) so they are governed and comparable across runs:
  - `CORRELATION_THRESHOLD`, `VIF_THRESHOLD`, min/max shortlist, and weights.

### P1 — Better stability integration
- If stability selection data is available, incorporate it with an explicit cutoff:
  - e.g., “keep features with selection probability ≥ 0.6, then fill remaining slots by score.”
- If stability is unavailable, reweight to contribution+coverage (already partially handled).

### P1 — Cluster-based collinearity pruning
- Replace greedy pairwise drops with:
  - hierarchical clustering of features by correlation distance,
  - then keep the top-scoring feature per cluster.
This is more stable and easier to explain.

### P2 — Use performance-aware ranking
- Incorporate Step 4 test $R^2$ / CV stability per target into scoring.
- Consider ranking on *median* performance across targets to reduce sensitivity.

## Open questions (to decide before larger refactors)
- Do we want Step 5 to output a fixed number of factors per ISO (e.g., exactly 12), or a variable number based on stability/coverage?
- Should PCA components be considered “non-droppable” within each block, with pruning happening only across blocks/representations?
- How do we want to trade off interpretability vs. predictive performance for the shortlist?
