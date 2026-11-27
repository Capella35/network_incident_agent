# Results Log

One row per measured run. Step 6 rows (2026-11-25) were logged to a local MLflow file store (Docker down); run IDs refer to that store. Step 6b rows (2026-11-26) are in the stack's MLflow (`detector-tuning`). Mark whether the split was validation or test (test weeks are touched once).

| Date | Split | Dataset snapshot | Config | Model version | Detection % | Median delay (intervals) | False incidents / day | Days measured | Root node #1 % | Mislabel % |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1 (windows 3, 6); M2 Step 6 | zscore study a4052f7b (no feasible trial) | n/a (best possible within budget: none; at z ≥ 20: 24.6% with 11.9/day) | n/a | n/a | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1; M2 Step 6; alarm share 2.1e-4, threshold 0.743 | iforest study 3686254b, trial 13 | 10.5% (6/57) | not reached | 0.93 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1; M2 Step 6; alarm share 1.7e-3, threshold 24.4 | autoencoder study e7f7a36a, trial 20 | 24.6% (14/57) | not reached (CI low 27) | 0.29 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-26 | validation | 52a97ac6 (fault schedule 62d292f4) | tune.yaml v1, features.yaml Step 6b (log1p real KPIs, latency vs load, drop_rate not detected); M2 Step 6b | zscore study 1ba17f05 (no feasible trial) | n/a (at z ≥ 20: 61.4% with 1.79/day) | n/a | n/a | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-26 | validation | 52a97ac6 (fault schedule 62d292f4) | tune.yaml v1, features.yaml Step 6b; M2 Step 6b; alarm share 2.6e-3, threshold 0.716 | iforest study 6a2b8311, trial 32 | 56.1% (32/57) | 10 [2, not reached] | 0.93 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-26 | validation | 52a97ac6 (fault schedule 62d292f4) | tune.yaml v1, features.yaml Step 6b; M2 Step 6b; alarm share 1.2e-2, threshold 35.7 | autoencoder study 90ccf60f, trial 0 | 56.1% (32/57) | 9 [6, not reached] | 0.00 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-26 | validation | 52a97ac6 (fault schedule 62d292f4) | features.yaml Step 6b; M2 Step 6c; union of iforest 6a2b8311 + autoencoder 90ccf60f at their own tuned thresholds (nothing re-tuned) | ensemble run 267381d9 | 66.7% (38/57) | 6 [2, 14] (KM); detected only: median 0, worst 17 | 0.86 | 2013-12-02 → 12-15 (14) | – | – |

## Agent results

| Date | Split | LLM model | Agent config | Questions | Correct % | Hallucinated-entity % | Constraint-violation % | Ruled-out accuracy % | Unverified % | Tokens used |
|---|---|---|---|---|---|---|---|---|---|---|
