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

### Step 2 — Fault injector + ground-truth log (~2.5 h) — *done 2026-11-23*
- `src/producer/injector.py` + `injector_config.yaml` (own parameters, seeded): a **schedule** per split: fault type, root node (chosen from the topology at the right level), onset, duration (3–36 intervals), magnitude, seed. Effects:
  - `cell_outage`: one cell's traffic × 0.00–0.05; `site_power`: all cells of a site the same; `backhaul`: sites under one aggregation node, traffic × 0.3–0.7 and extra latency; `congestion`: traffic × 1.5–2.5 on a cell or site (latency and drop then follow the KPI model); `config_push`: gradual drift over ≥ 3 intervals, starting after a logged ChangeEvent.
  - **Decoy ChangeEvents** with no fault (~2 per fault), so a change event alone is not a label.
- Schedules: **validation** (seed V, ~4 faults/day, ~55 over 14 days, all five types); **training** (seed T, for the M4 classifier later; detectors train on clean training weeks); **test** (seed X, disjoint locations/timings/magnitudes) is *defined* but generated only with `--final-eval` (Rule 7).
- Ground truth → `data/processed/ground_truth/<split>-<seed>.parquet` (git-ignored, hash in the manifest); change events → Postgres `change_events`.
- `make replay ARGS="… --inject validation"` streams the faulted telemetry; `make inject` writes the schedule and ground truth.
- **Tests:** same seed → identical schedule and output; effects land only on the scheduled nodes and intervals; magnitudes within their ranges; no fault on a data gap; test schedule refused without `--final-eval`; the injector never imports signatures.
- **Done when:** a validation schedule is generated, applied to the 14 validation days, and spot-checked against the ground truth (e.g., a site-power fault shows all three sector cells at ~0 for exactly its window).
- **Verified 2026-11-23:** `make inject ARGS="--split validation"` → 57 SYNTHETIC faults (site_power 17, congestion 12, cell_outage 11, config_push 9, backhaul 8), fingerprint `62d292f4…`. Applied to all 14 validation days through `replay.telemetry_for` (the one assembly path for the replay and offline code): of 290,304 rows exactly the 5,425 cell-intervals inside fault windows changed, 0 mismatches against multipliers computed independently from the ground-truth file; spot check SITE-7 site_power: all 3 cells ×0.038 for exactly its 9 intervals. `make replay ARGS="--start 2013-12-02 --speed 0 --inject"`: 3 faults, 4 change events in Postgres, schedule matched its ground truth, 20,736 sent/sunk. Design changes from review: `--inject` is a flag (split from the days); `telemetry_for` never returns the faults (Rule 6; `producer.injector` importable only by `producer`/`evaluation`); the ground-truth sidecar holds a schedule fingerprint that the replay checks; the topology index moved to `topology.Index`.

### Step 3 — Features (~2 h) — *done 2026-11-23*
- `src/streaming/features.py` (pure Python, polars): per-cell, **causal** features (interval *t* uses only ≤ *t*): rolling means over a window, deviation from a **seasonal baseline** (same hour of day and weekday, Europe/Rome), rate of change, per KPI from the catalog (bad direction respected). Data gaps produce no features (no alarm on a gap).
- Seasonal baseline fitted on training weeks only (`splits.require(day, TRAIN)`) → `data/processed/baseline.parquet` (`make baseline`).
- **Tests:** causality (changing the future never changes the past); gap handling; determinism; KPI names only from `kpis.yaml`.
- **Done when:** features for all 14 validation days computed in < 1 min and memory < 1 GB.
- **Verified 2026-11-23:** `make baseline` → 24,192 buckets (144 cells × 7 weekdays × 24 hours) from the 28 training days, clean. `make features ARGS="--split validation --inject"` → 14 days + 57 SYNTHETIC faults, 290,304 rows × 30 features (6 KPIs × z, bad, mean over 3 and 6 intervals, delta), features **0.3 s**, whole run ~1 s, peak memory **376–415 MiB**; training weeks 580,608 rows, 0.7 s, 663 MiB. Under injection exactly 5,421 rows' features change (the injector changed 5,425 cell-intervals; 4 stay equal after clipping). `streaming.features` imports only `common` (checked), so the Spark job can call it. Reviewed with `/simplify` (4 reviewers) and `/code-review` (7 findings; 1 important, fixed: a stale baseline was used silently).

### Step 4 — Detector candidates (~2.5 h) — *done 2026-11-24*
- `src/models/`: one interface (`fit(train_features)`, `score(features) -> scores`, `alarms(scores, threshold)`), three candidates:
  1. **Seasonal z-score** (the baseline);
  2. **Isolation Forest** (scikit-learn) on multi-KPI windows;
  3. **Autoencoder** as a small scikit-learn MLP trained to reconstruct its input (no PyTorch: keeps the laptop install and the Spark image small).
- Trained on **clean training weeks** only; saved as MLflow models.
- **Tests:** fit/score on hand-made fixtures; a planted anomaly scores above normal; deterministic with a seed; alarms carry the contributing KPIs.
- **Done when:** each candidate trains on the 28 training days within its memory budget and scores a validation day.
- **Verified 2026-11-24:** `make candidates` (one process per candidate, budget 1,536 MiB fixed in `detectors.yaml` before measuring): zscore 881 MiB, iforest 895 MiB, autoencoder 926 MiB; each trained on 580,608 rows (28 clean training days; features 2.5 s, fit 0.007 / 0.36 / 4.5 s) and scored 2013-12-02 (20,736 rows, SYNTHETIC faults injected); each logged to MLflow `detector-candidates` as a pyfunc model that reloads and scores like the detector (test). Also done here (user's decision 2026-11-24, option b): real KPIs' z is level-adjusted over the previous 7 days; clean validation week 2 internet `bad > 3` 12.2% → 1.6%. Reviewed with `/simplify` (4 reviewers) and `/code-review` (4 important findings fixed).

### Step 5 — Evaluation (~2 h) — *done 2026-11-25*
- `src/evaluation/`: ground-truth reader (the only one); **matching** per `specs/09-evals.md` (overlap ± 1 interval); metrics: detection rate (overall, per fault type), median detection delay (intervals; **Kaplan–Meier** with missed faults censored, + 95% CI, user's choice 2026-11-25), false incidents per day.
- **Provisional incident grouping (M2 only):** alarms within ± 1 interval that share a site or aggregation node form one incident. Needed because the "false incident" metric counts incidents, and the real correlator is Milestone 3, which replaces this rule and re-measures.
- Report: a table (Markdown + MLflow artifact), per fault type, plus the days of data measured.
- **Tests:** hand-made fixtures for every case (hit, late hit, miss, false incident, two faults overlapping, a fault on a gap).
- **Done when:** the report runs on validation ground truth with a dummy detector and every number is checked by hand on a 1-day fixture.

### Step 6 — Optuna tuning + MLflow tracking (~2 h) — *done 2026-11-25*
- `src/models/tune.py` + `make train`: one Optuna study per candidate on **validation weeks with injected faults**; thresholds (as the share of validation intervals that alarm), Isolation Forest size, autoencoder hidden size and iterations. *Window lengths not tuned (user's choice 2026-11-25, `DECISIONS.md`).*
- Objective: **maximize detection rate, then minimize median delay, subject to ≤ 1 false incident per day** (Optuna constraint; infeasible trials never win).
- **Budget (R4):** fixed trials and wall-clock timeout per study, e.g. 40 trials / 15 min (`tune.yaml`).
- MLflow: one parent run per study, nested runs per trial with params, metrics, data snapshot hashes (manifest), config, seeds, code version.
- **Tests:** a tiny study (3 trials, fixture data) runs, respects the budget, logs to a local file store.
- **Done when:** three studies finish within budget; the comparison table (candidate × detection rate, delay, false/day) is in MLflow and `evals/results-log.md`.

### Step 6b — KPI deviation features (~2–3 h) — *added 2026-11-25* — *done 2026-11-26*
- Why: Step 6 measured that with today's features the z-score cannot reach ≤ 1 false incident/day at any threshold, and the tuned ML candidates stay within budget only by catching 6–14 of 57 faults. The false incidents at the cap are MODELED drop_rate / latency exploding at real busy hours (`DECISIONS.md`, "Skewed KPI deviations").
- ~~Planned:~~ *(replaced 2026-11-26, `DECISIONS.md`)* `log1p` for real KPIs before the baseline and z; for MODELED KPIs an empirical tail z (training-week distribution of deviations → the normal z with the same tail probability); refit the baseline; re-check Step 3's feature checks.
- **Built (revised design, the user's proposal 2026-11-26):** each KPI judged against what explains its normal swings: real traffic KPIs on `log1p` against the clock (7-day level as an additive log offset over hours with traffic); MODELED latency against load (training-week log-latency curve over 100 equal-count `load_ratio` bins); MODELED drop rate not detected (a function of load only); `load_ratio` against the clock as before. Empirical tail z dropped: thresholds are already set by measured false incidents. Refit the reference (`make baseline`); re-check Step 3's feature checks.
- **Done when:** `make train` re-run on the new features; the comparison table (before / after) in MLflow and `evals/results-log.md`; keep the change only if it detects more faults within the false-incident budget (else record why and revert).

### Step 6c — Recall: diagnose the misses, one change (~2 h) — *added and done 2026-11-26*
- Why: after Step 6b the false-incident budget is met but detection is 56% (bar ≥ 90%) and the KM median delay 9–10 (bar ≤ 2).
- `make diagnose` (`src/evaluation/misses.py`): per-fault table of the latest tuned detectors (type, duration, cells, worst KPI deviation, peak score / threshold) and the delay of the caught faults; Rule 6: the ground truth stays in `evaluation`.
- One change from the diagnosis: `make union` (`src/models/ensemble.py`): a cell-interval alarms if any member reaches its own tuned threshold; measures each member alone and the union (the ablation).
- **Done when:** the diagnosis is recorded and the union measured before / after on the validation weeks, kept only if ≥ 36/57 within ≤ 1 false incident/day with a KM median no worse than 9 (criteria set before measuring). **Result:** 38/57, 0.86/day, KM median 6 [2, 14]: kept.

### Step 7 — Promotion gate (~1 h)
- `make promote`: registers the best feasible candidate as MLflow model `detector`, alias `champion`, **only if** it meets ≤ 1 false incident/day on validation **and** beats the z-score baseline on the objective (Definition of done). Otherwise the z-score baseline is champion and the report says so.
- `make rollback`: moves `champion` back to the previous version. Retraining is a manual, reviewed event.
- **Tests:** gate refuses a candidate that is infeasible or not better than the baseline; rollback restores the previous version.
- **Done when:** a champion is registered with its validation metrics attached.

### Step 8 — Spark streaming job (~3 h)
- **Own Spark image** (`infra/spark/Dockerfile`): pinned Python 3.11 base, Java 17, PySpark 4.2.1, the Kafka connector jar (pinned version + checksum), and this package installed from `uv.lock`. Reason: the stock Spark image ships Python 3.10; the laptop has Java 8 (Spark 4 needs 17).
- `src/streaming/job.py`: reads `telemetry.raw`; `applyInPandasWithState` per cell keeps the last *W* intervals **plus the level-offset state (per cell, hour and real KPI: the last 7 local days' deviation sums, row counts and day counts; found in Step 4, changed in Step 6b) and the load curve (100 bins, from `make baseline`)** and calls the Step 3 features and the champion model (loaded from MLflow); writes alarms to Kafka `alarms` and Postgres `alarms`; checkpointing; a heartbeat row per micro-batch.
- Alarm schema in `src/common/alarms.py` (`ALM-<uuid>`, deterministic from run, cell, interval, model version; KPIs, score, detector version) and `specs/03-detection.md` updated.
- Compose: the job under the `spark` profile, memory ≤ 1.5 GiB (R1); `alarms` topic with bounded retention (R2).
- **Done when:** the job runs against a replay without errors, restarts from its checkpoint without duplicate alarms, and stays within its memory limit.

### Step 9 — Parity and end-to-end check (~1.5 h) — *the milestone's Done when*
- Replay one validation day with injected faults, **preceded by its 7 history days under the same rule as `streaming.offline.features_for`** (earlier split clean, same split injected), so level factors match; compare the Spark job's alarms with the offline scoring of the same day: **identical alarm keys and scores** (tolerance 0). Then run the Step 5 report on the streamed alarms: it must equal the offline report.
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
- **Real internet level shift in December (found in Step 3):** from 2013-12-03/04, 18 of 144 cells carry < 60% of their November internet traffic for the rest of the validation weeks (31 cells < 80%); voice is unchanged (1 cell < 60%). Example CELL-7-1: ~950 → ~330 per weekday. Real, unlabeled data behavior, not an injected fault: on clean validation days `traffic_internet_bad > 3` on 10.7% of rows (training's last week: ~0%). Unhandled, these cells alarm all day and will dominate false incidents/day in Steps 4–6. How to handle it is open (see `DECISIONS.md`).
- **Milan holidays in validation week 1** (Sat 2013-12-07 Sant'Ambrogio, Sun 12-08 Immaculate Conception): real, unlabeled anomalies; they will cost false alarms. Useful as a realism check, not something to tune away.
- **Busy peaks:** 0.23% of training rows already have `load_ratio` > 0.9; the seasonal baseline must absorb them.
- **Tuning and reporting on the same validation weeks** gives optimistic M2 numbers; the honest numbers come once, from test weeks, at M6.
- **Spark on 8 GB:** 1.5 GiB cap, local[2]; if state per cell is too large, shorten *W*.

### Out of scope for this milestone
Correlation and root-node ranking (M3, beyond the provisional grouping), classifier and noisy reports (M4), console, agent (M5). Test-week data is not downloaded, injected, or touched.

## Status

Plan confirmed 2026-11-23. Step 6c done 2026-11-26 (miss diagnosis: misses are long, large deviations the scores rank too low, congestion dominates; union of Isolation Forest + autoencoder: 38/57 at 0.86/day, KM median 6 [2, 14]; still short of ≥ 90% and ≤ 2). Step 6b done 2026-11-26 (revised design: real KPIs `log1p` against the clock, MODELED latency against load, MODELED drop rate not detected; `make train` on the validation weeks: Isolation Forest 6 → 32/57 at 0.93/day, autoencoder 14 → 32/57 at 0.00/day, z-score still infeasible (35/57 at 1.79/day at the cap); congestion still weak, 0–2/12). Step 6 done 2026-11-25 (`make train`: one Optuna study per candidate, ≤ 1 false incident/day constraint, threshold inside the logged model; z-score infeasible at any threshold, tuned Isolation Forest / autoencoder feasible but catch few faults; Step 6b added for the KPI deviation features). Step 5 done 2026-11-25 (`src/evaluation/`: ground-truth reader, provisional incidents, matching with a shared cell, Kaplan–Meier delay + CI, unobservable faults, `make evaluate`; hand-checked 1-day fixture; z-score dummy at threshold 4 on the validation weeks: 56/57 detected, 61 false incidents/day). Step 4 done 2026-11-24. Step 3 done 2026-11-23 (seasonal baseline, causal features, stale-baseline check; real December internet level shift found). Step 2 done 2026-11-23 (seeded injector, ground truth + fingerprint, `replay --inject`, change events). Step 1 done 2026-11-23 (fault types, separate signatures, import/name boundaries for Rules 6 and 15, `change_events`). Step 4 done 2026-11-24 (three candidates behind one interface, MLflow pyfunc models, memory budget checked before logging; level-adjusted z for real KPIs, the user's option b). Next: Step 7 (promotion gate). Data ready: all training and validation weeks processed and calibrated (2026-11-22).
