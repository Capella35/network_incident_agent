# streaming/

Spark Structured Streaming job: windowed per-cell features, scoring with the promoted detector, alarms out.

- `features.py` (+ `features.yaml`): causal per-cell features for every real and MODELED KPI (z-score against the seasonal baseline, bad-direction oriented score, rolling means, rate of change); no features on data gaps. Imports only `common`, so the Spark job can use it.
- `features_check.py`: `make features ARGS="--split validation --inject"` computes the features for a split and reports size, time, and peak memory.
- `baseline.py`: seasonal mean/std per cell, weekday, hour (Europe/Rome) from the clean training weeks → `data/processed/baseline.parquet` (`make baseline`).

The Spark job (Step 8) calls the same feature code per cell.
