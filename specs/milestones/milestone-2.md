# Milestone 2 — Fault injection + detection

**Goal:** Injector with ground-truth log; Spark feature job; candidate detectors tuned with Optuna and tracked in MLflow; best detector promoted and scoring the stream; detection-rate / delay / false-alarm report.

**Done when** *(confirmed by the user 2026-11-23)*: the promoted detector scores a replayed validation day **in Spark**, its alarms are **identical** to the offline scoring of the same day (same keys, same scores), and the validation report (detection rate, median delay, false incidents per day; overall and per fault type; promoted detector vs the z-score baseline) is generated from MLflow-tracked runs. The v1 quality bars are judged on test weeks at Milestone 6; M2 reports validation numbers honestly, whatever they are.

**Domain model:** `faults.yaml` (fault types, root levels, severities, teams, signatures) shared with the injector; the injector must not use signature thresholds (Rule 15).

**Guardrails due this milestone** (`specs/08-guardrails.md`): R3, R4; re-check R1 (new Spark container, Optuna memory), S5 (new image + dependencies), P4 (ground truth and models never committed).

**Specs:** `specs/03-detection.md` (behavior), `specs/09-evals.md` (matching, splits), `specs/01-domain-model.md` §4 (faults), `specs/02-telemetry.md` (message).

## Detailed plan

*Status: confirmed by the user 2026-11-23 (whole plan, with the decisions below). Estimate: ~14–20 h.*

Each step ends with its own check. One commit per step; one `BUILD_LOG.md` entry per session.

### Design in one paragraph
Detection is built **offline first and streamed second**. All feature and model code is plain Python (polars / scikit-learn) and runs on the processed parquet files, so tuning takes minutes, not replays, and CI can test it. The Spark job calls the **same** feature and model code per cell (`applyInPandasWithState`), and Step 9 proves the two paths give identical alarms. Faults are injected as physical effects on traffic (and, for backhaul, latency) before the modeled KPIs are computed, from the injector's own parameters. The ground-truth log is written only by the injector and read only by `src/evaluation/`.

### Step 1 — Fault types in the domain model (~1 h) — *done 2026-11-23*
- `src/common/domain/faults.yaml` + loader: `cell_outage`, `site_power`, `backhaul`, `congestion`, `config_push`, `no_fault`; root level (an entity type), severity, team (from `entities.yaml`), and the signature predicates with their default thresholds (for M3), as in `specs/01-domain-model.md` §4. Loader checks every reference.
- ChangeEvent stays an entity (`CHG-<n>`); `change_events` table in Postgres (operational data the system may see; not ground truth).
- **Tests:** loader validation; an **import-boundary test** that fails if anything under `src/producer/` (the injector) imports the signature section, and if anything outside `src/evaluation/` (and the injector, which writes it) imports the ground-truth module (Rules 6, 15).
- **Done when:** `load_domain().fault(name)` works for all six; the boundary test passes and fails on a planted violation.
- **Verified 2026-11-23:** all six fault types load (root levels, `Severity` enum ordered minor < major < critical, team); the five real faults each have a signature in a **separate** `fault_signatures.yaml` / `common.domain.signatures` (checked against the fault types in the same directory). `tests/test_import_boundaries.py`: 0 violations in `src/`; catches planted violations by import (direct, relative, and for Rule 15 transitively through a helper) and by file name in any source or config file (`fault_signatures`, `ground_truth` outside their owners). `change_events` table via `infra/db/002_change_events.sql` + `make db-migrate` (derived list; idempotency enforced by `tests/test_db_migrations.py`), applied to the running stack twice. `make check` 311 passed; integration 24 passed. Reviewed with `/simplify` (4 reviewers) and `/code-review` (9 findings, all fixed).

### Step 2 — Fault injector + ground-truth log (~2.5 h)
- `src/producer/injector.py` + `injector_config.yaml` (own parameters, seeded): a **schedule** per split: fault type, root node (chosen from the topology at the right level), onset, duration (3–36 intervals), magnitude, seed. Effects:
  - `cell_outage`: one cell's traffic × 0.00–0.05; `site_power`: all cells of a site the same; `backhaul`: sites under one aggregation node, traffic × 0.3–0.7 and extra latency; `congestion`: traffic × 1.5–2.5 on a cell or site (latency and drop then follow the KPI model); `config_push`: gradual drift over ≥ 3 intervals, starting after a logged ChangeEvent.
  - **Decoy ChangeEvents** with no fault (~2 per fault), so a change event alone is not a label.
- Schedules: **validation** (seed V, ~4 faults/day, ~55 over 14 days, all five types); **training** (seed T, for the M4 classifier later; detectors train on clean training weeks); **test** (seed X, disjoint locations/timings/magnitudes) is *defined* but generated only with `--final-eval` (Rule 7).
- Ground truth → `data/processed/ground_truth/<split>-<seed>.parquet` (git-ignored, hash in the manifest); change events → Postgres `change_events`.
- `make replay ARGS="… --inject validation"` streams the faulted telemetry; `make inject` writes the schedule and ground truth.
- **Tests:** same seed → identical schedule and output; effects land only on the scheduled nodes and intervals; magnitudes within their ranges; no fault on a data gap; test schedule refused without `--final-eval`; the injector never imports signatures.
- **Done when:** a validation schedule is generated, applied to the 14 validation days, and spot-checked against the ground truth (e.g., a site-power fault shows all three sector cells at ~0 for exactly its window).

### Step 3 — Features (~2 h)
- `src/streaming/features.py` (pure Python, polars): per-cell, **causal** features (interval *t* uses only ≤ *t*): rolling means over a window, deviation from a **seasonal baseline** (same hour of day and weekday, Europe/Rome), rate of change, per KPI from the catalog (bad direction respected). Data gaps produce no features (no alarm on a gap).
- Seasonal baseline fitted on training weeks only (`splits.require(day, TRAIN)`) → `data/processed/baseline.parquet` (`make baseline`).
- **Tests:** causality (changing the future never changes the past); gap handling; determinism; KPI names only from `kpis.yaml`.
- **Done when:** features for all 14 validation days computed in < 1 min and memory < 1 GB.

### Step 4 — Detector candidates (~2.5 h)
- `src/models/`: one interface (`fit(train_features)`, `score(features) -> scores`, `alarms(scores, threshold)`), three candidates:
  1. **Seasonal z-score** (the baseline);
  2. **Isolation Forest** (scikit-learn) on multi-KPI windows;
  3. **Autoencoder** as a small scikit-learn MLP trained to reconstruct its input (no PyTorch: keeps the laptop install and the Spark image small).
- Trained on **clean training weeks** only; saved as MLflow models.
- **Tests:** fit/score on hand-made fixtures; a planted anomaly scores above normal; deterministic with a seed; alarms carry the contributing KPIs.
- **Done when:** each candidate trains on the 28 training days within its memory budget and scores a validation day.

### Step 5 — Evaluation (~2 h)
- `src/evaluation/`: ground-truth reader (the only one); **matching** per `specs/09-evals.md` (overlap ± 1 interval); metrics: detection rate (overall, per fault type), median detection delay (intervals), false incidents per day.
- **Provisional incident grouping (M2 only):** alarms within ± 1 interval that share a site or aggregation node form one incident. Needed because the "false incident" metric counts incidents, and the real correlator is Milestone 3, which replaces this rule and re-measures.
- Report: a table (Markdown + MLflow artifact), per fault type, plus the days of data measured.
- **Tests:** hand-made fixtures for every case (hit, late hit, miss, false incident, two faults overlapping, a fault on a gap).
- **Done when:** the report runs on validation ground truth with a dummy detector and every number is checked by hand on a 1-day fixture.

### Step 6 — Optuna tuning + MLflow tracking (~2 h)
- `src/models/tune.py` + `make train`: one Optuna study per candidate on **validation weeks with injected faults**; window lengths, thresholds, contamination, hidden size, etc.
- Objective: **maximize detection rate, then minimize median delay, subject to ≤ 1 false incident per day** (Optuna constraint; infeasible trials never win).
- **Budget (R4):** fixed trials and wall-clock timeout per study, e.g. 40 trials / 15 min (`tune.yaml`).
- MLflow: one parent run per study, nested runs per trial with params, metrics, data snapshot hashes (manifest), config, seeds, code version.
- **Tests:** a tiny study (3 trials, fixture data) runs, respects the budget, logs to a local file store.
- **Done when:** three studies finish within budget; the comparison table (candidate × detection rate, delay, false/day) is in MLflow and `evals/results-log.md`.

### Step 7 — Promotion gate (~1 h)
- `make promote`: registers the best feasible candidate as MLflow model `detector`, alias `champion`, **only if** it meets ≤ 1 false incident/day on validation **and** beats the z-score baseline on the objective (Definition of done). Otherwise the z-score baseline is champion and the report says so.
- `make rollback`: moves `champion` back to the previous version. Retraining is a manual, reviewed event.
- **Tests:** gate refuses a candidate that is infeasible or not better than the baseline; rollback restores the previous version.
- **Done when:** a champion is registered with its validation metrics attached.

### Step 8 — Spark streaming job (~3 h)
- **Own Spark image** (`infra/spark/Dockerfile`): pinned Python 3.11 base, Java 17, PySpark 4.2.1, the Kafka connector jar (pinned version + checksum), and this package installed from `uv.lock`. Reason: the stock Spark image ships Python 3.10; the laptop has Java 8 (Spark 4 needs 17).
- `src/streaming/job.py`: reads `telemetry.raw`; `applyInPandasWithState` per cell keeps the last *W* intervals and calls the Step 3 features and the champion model (loaded from MLflow); writes alarms to Kafka `alarms` and Postgres `alarms`; checkpointing; a heartbeat row per micro-batch.
- Alarm schema in `src/common/alarms.py` (`ALM-<uuid>`, deterministic from run, cell, interval, model version; KPIs, score, detector version) and `specs/03-detection.md` updated.
- Compose: the job under the `spark` profile, memory ≤ 1.5 GiB (R1); `alarms` topic with bounded retention (R2).
- **Done when:** the job runs against a replay without errors, restarts from its checkpoint without duplicate alarms, and stays within its memory limit.

### Step 9 — Parity and end-to-end check (~1.5 h) — *the milestone's Done when*
- Replay one validation day with injected faults; compare the Spark job's alarms with the offline scoring of the same day: **identical alarm keys and scores** (tolerance 0). Then run the Step 5 report on the streamed alarms: it must equal the offline report.
- `make detect-check` (like `make replay-check`).

### Step 10 — Health, cleanup, dashboards (~1.5 h)
- **Dead detector alert:** Grafana alert rule (provisioned as code): telemetry arriving but no detector heartbeat for N intervals (N in config).
- Grafana panels (generated, like Step 9 of M1): alarms vs telemetry, false alarms/day, detection delay.
- **R3:** `make clean` also removes Spark checkpoints, old MLflow runs beyond a kept number, and logs (never `data/`).

### Step 11 — Close-out (~1 h)
- Walk R3, R4 (+ R1, S5, P4 re-checks); record pass/fail in `BUILD_LOG.md`.
- Validation report into `evals/results-log.md` (with the days, seeds, model versions); README status; `specs/03-detection.md` and `specs/09-evals.md` (M2) markers resolved; `DECISIONS.md`; handoff → Milestone 3.

### Decisions (confirmed 2026-11-23)
| Item | Proposal | Why |
|---|---|---|
| Incident grouping in M2 | Provisional rule (± 1 interval, same site or aggregation node); M3 replaces it and re-measures. **Confirmed by the user 2026-11-23.** | "False incidents per day" needs incidents before the correlator exists |
| Autoencoder | scikit-learn MLP, no PyTorch. **Confirmed by the user 2026-11-23.** | Small install on 8 GB; small Spark image |
| Ground-truth storage | Git-ignored parquet, read only by `src/evaluation/` (import-boundary test) | Simple; the rule is enforced in code and tests |
| Fault rate (validation) | ~4/day, ~55 over 14 days, all five types, 3–36 intervals | Enough per type to measure; not so many that everything alarms |
| Detector training data | Clean training weeks; faults only in validation (tuning) and test (final) | Unsupervised detectors learn "normal"; labeled training faults are for the M4 classifier |
| Streaming design | Same Python code offline and in Spark; parity proven in Step 9 | One implementation, testable in CI |
| Spark image | Own image: Python 3.11 + Java 17 + PySpark 4.2.1, pinned | Stock image has Python 3.10 |
| MLflow client | `mlflow-skinny` pinned to the server's version (3.17.2) | Client/server match; lighter than full `mlflow` |

### Known risks
- **Milan holidays in validation week 1** (Sat 2013-12-07 Sant'Ambrogio, Sun 12-08 Immaculate Conception): real, unlabeled anomalies; they will cost false alarms. Useful as a realism check, not something to tune away.
- **Busy peaks:** 0.23% of training rows already have `load_ratio` > 0.9; the seasonal baseline must absorb them.
- **Tuning and reporting on the same validation weeks** gives optimistic M2 numbers; the honest numbers come once, from test weeks, at M6.
- **Spark on 8 GB:** 1.5 GiB cap, local[2]; if state per cell is too large, shorten *W*.

### Out of scope for this milestone
Correlation and root-node ranking (M3, beyond the provisional grouping), classifier and noisy reports (M4), console, agent (M5). Test-week data is not downloaded, injected, or touched.

## Status

Plan confirmed 2026-11-23. Step 1 done 2026-11-23 (fault types, separate signatures, import/name boundaries for Rules 6 and 15, `change_events`). Next: Step 2 (fault injector + ground-truth log). Data ready: all training and validation weeks processed and calibrated (2026-11-22).
