# models/

Detector candidates (seasonal z-score, Isolation Forest, autoencoder), Optuna search (step 6), and the XGBoost incident classifier (milestone 4).

- `detectors.py` (+ `detectors.yaml`): one interface (`fit`, `score`, `alarms`) and the three candidates; seeded defaults, tuned in step 6. Alarms carry the contributing KPIs.
- `tracking.py`: logs a fitted detector to MLflow as a pyfunc model (input: feature rows; output: scores).
- `candidates.py`: `make candidates` trains each candidate on the clean training weeks in its own process (memory budget), scores one validation day with SYNTHETIC faults, and logs it.
