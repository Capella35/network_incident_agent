# Results Log

One row per measured run. Step 6 rows (2026-11-25) were logged to a local MLflow file store (Docker down); run IDs refer to that store until `make train` is re-run against the stack. Mark whether the split was validation or test (test weeks are touched once).

| Date | Split | Dataset snapshot | Config | Model version | Detection % | Median delay (intervals) | False incidents / day | Days measured | Root node #1 % | Mislabel % |
|---|---|---|---|---|---|---|---|---|---|---|
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1 (windows 3, 6); M2 Step 6 | zscore study a4052f7b (no feasible trial) | n/a (best possible within budget: none; at z ≥ 20: 24.6% with 11.9/day) | n/a | n/a | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1; M2 Step 6; alarm share 2.1e-4, threshold 0.743 | iforest study 3686254b, trial 13 | 10.5% (6/57) | not reached | 0.93 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-25 | validation | f48a4046 (fault schedule 62d292f4) | tune.yaml v1, features.yaml v1; M2 Step 6; alarm share 1.7e-3, threshold 24.4 | autoencoder study e7f7a36a, trial 20 | 24.6% (14/57) | not reached (CI low 27) | 0.29 | 2013-12-02 → 12-15 (14) | – | – |

## Agent results

| Date | Split | LLM model | Agent config | Questions | Correct % | Hallucinated-entity % | Constraint-violation % | Ruled-out accuracy % | Unverified % | Tokens used |
|---|---|---|---|---|---|---|---|---|---|---|
