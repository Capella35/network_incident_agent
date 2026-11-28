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
| 2026-11-27 | validation | 80c52048 (fault schedule 62d292f4) | tune.yaml v1, features.yaml Step 6d (+ level.modeled: load_ratio); M2 Step 6d | zscore study 6321919a, trial 38 | 64.9% (37/57) | 0 [0, 18] | 0.86 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-27 | validation | 80c52048 (fault schedule 62d292f4) | features.yaml Step 6d; M2 Step 6d | iforest study bf3e9e4c, trial 19 | 40.4% (23/57) | not reached [10, n/a] | 0.79 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-27 | validation | 80c52048 (fault schedule 62d292f4) | features.yaml Step 6d; M2 Step 6d (re-run study 2d41d0aa reproduces 40f15f2b) | autoencoder study 2d41d0aa, trial 35 | 71.9% (41/57) | 1 [0, 9] | 0.29 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-27 | validation | 52a97ac6 (fault schedule 62d292f4) | ablation: features.yaml Step 6d **without** level.modeled (= Step 6b features); same seeded tuning | autoencoder study a80c102f, trial 0 | 56.1% (32/57) | 9 [6, n/a] | 0.00 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-27 | validation | 80c52048 (fault schedule 62d292f4) | features.yaml Step 6d; union of zscore 6321919a + iforest bf3e9e4c + autoencoder 40f15f2b at their own thresholds | ensemble run 7cc0541d (806 MiB) | 75.4% (43/57) | 0 [0, 5] | **1.64 (over the 1.0 cap)** | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-27 | validation | 80c52048 (fault schedule 62d292f4) | features.yaml Step 6d; M2 Step 7 promotion gate (≤ 1.0/day; first champion: cap only); package = model + threshold + alarm settings + feature config + fitted reference | **`detector` v1 = champion** (autoencoder study 2d41d0aa, re-scored) | 71.9% (41/57) | 1 [0, 9] | 0.29 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-28 | validation | 80c52048 (fault schedule 62d292f4) | **M2 Step 11 validation report** (`make validation-report`): champion re-scored from its registered package, on the frame the trials used | **`detector` v1 (champion)**, run 3180d8e5 (MLflow `detector-evaluation`) | 71.9% (41/57) | 1 [0, 9] (KM); detected only: median 0, worst 34 | 0.29 | 2013-12-02 → 12-15 (14) | – | – |
| 2026-11-28 | validation | 80c52048 (fault schedule 62d292f4) | M2 Step 11 validation report: baseline = latest feasible z-score study at its tuned threshold | zscore study 6321919a, run 3180d8e5 | 64.9% (37/57) | 0 [0, 18] (KM); detected only: median 0, worst 18 | 0.86 | 2013-12-02 → 12-15 (14) | – | – |

## Milestone 2 validation report (2026-11-28)

MLflow run `3180d8e5` (experiment `detector-evaluation`, role `validation-report`; artifacts `report.md`, `champion_fault_outcomes.csv`, `baseline_fault_outcomes.csv`). Validation weeks 2013-12-02 → 2013-12-15 (14 days), **SYNTHETIC faults** (schedule fingerprint 62d292f4, injector seed 20131202), detector seed 20131104, data snapshot 80c52048, champion `detector` v1 (autoencoder study 2d41d0aa), baseline z-score study 6321919a, code `ad1e2a0` + the Step 11 changes. Topology and latency / load / drop KPIs are SYNTHETIC or MODELED. These numbers are **optimistic**: the detectors were tuned on these days; the honest numbers come once, from the test weeks, at Milestone 6. Both detectors are still short of the v1 bars (≥ 90% detected; congestion is the main miss).

| Fault type | Faults | Champion detected | Champion KM delay [CI] | Z-score detected | Z-score KM delay [CI] |
|---|---:|---:|---|---:|---|
| backhaul | 8 | 8 (100%) | 0 [0, n/a] | 7 (87.5%) | 0 [0, n/a] |
| cell_outage | 11 | 10 (90.9%) | 0 [0, n/a] | 10 (90.9%) | 0 [0, n/a] |
| config_push | 9 | 5 (55.6%) | 19 [8, n/a] | 1 (11.1%) | not reached |
| congestion | 12 | 1 (8.3%) | not reached | 2 (16.7%) | not reached [18, n/a] |
| site_power | 17 | 17 (100%) | 0 [0, 0] | 17 (100%) | 0 [0, 0] |
| **overall** | 57 | **41 (71.9%)** | **1 [0, 9]** | 37 (64.9%) | 0 [0, 18] |
| Incidents / false (per day) | | 81 / 4 (0.29) | | 51 / 12 (0.86) | |

Delay: Kaplan-Meier median in 10-min intervals from onset to the first matching alarm, 95% CI, missed faults censored. Incidents use the provisional M2 grouping (M3 replaces it).

## Agent results

| Date | Split | LLM model | Agent config | Questions | Correct % | Hallucinated-entity % | Constraint-violation % | Ruled-out accuracy % | Unverified % | Tokens used |
|---|---|---|---|---|---|---|---|---|---|---|
