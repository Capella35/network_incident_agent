# 03 — Detection

Turns `telemetry.raw` into per-cell **alarms**. Built in Milestone 2 (`milestones/milestone-2.md`). Drawn from `PROJECT.md` → What is being built (items 4–5) and `README.md` → Models; every detail once marked **(M2)** was decided in the Milestone 2 plan and is recorded below (none left open, 2026-11-28).

## Behavior
- **Spark Structured Streaming job** (`src/streaming/`) reads `telemetry.raw`, computes windowed per-cell features, scores them with the **promoted** detector, and writes alarms to Kafka `alarms` and Postgres. Checkpointing gives exactly-once output.
- **Features:** rolling means, deviation from a reference, rates of change. KPI names, units, and bad directions come from the KPI catalog (`01-domain-model.md` §3), never literals. Each detected KPI is judged against what explains its normal swings (Step 6b, `DECISIONS.md` 2026-11-26):
  - **against the clock**: real traffic KPIs (on a `log1p` scale, with a 7-day level adjustment) and MODELED `load_ratio`: deviation from the seasonal baseline for that cell, hour of day, and weekday (Europe/Rome);
  - **against the load**: MODELED `latency_ms`, as log latency vs what the training weeks show at the same `load_ratio` (100 equal-count load bins, one curve for all cells since load is capacity-normalized; a real network would need per-cell curves). A busy hour's queueing delay is normal; latency high for its load is not;
  - **not detected**: MODELED `drop_rate`. It is a function of load only (no injected fault moves it off its curve), so it adds nothing beyond load and false-alarms at busy hours.
- Data gaps (`data_gap = true`, `02-telemetry.md`) are not faults: the detector must not alarm on a gap alone.
- **An alarm** is raised on one cell for one interval, with the KPI(s), score, and detector version (`ALM-<uuid>`, `01-domain-model.md` §2). Message (`src/common/alarms.py`, Kafka `alarms` keyed by `alarm_id`, Postgres `alarms`; Step 8): `schema_version`, `alarm_id`, `replay_run_id`, `cell_id`, `site_id`, `interval_start` (UTC), `score`, `kpis` (contributing KPIs, largest first), `detector_version` (`detector/<registered version>`). `alarm_id` is a UUID5 of (run, cell, interval, detector version), so re-scoring the same input gives the same ID.

## Streaming job (Step 8)
- `src/streaming/job.py`, image `infra/spark/Dockerfile` (Python 3.11, Java 17, PySpark 4.2.1), Compose profile `spark`, `local[1]`, 1.5 GiB (`make detect` / `make detect-stop`). Loads the champion package once at start (by version); a promotion takes effect after `make detect-reset` (the checkpoint is pinned to the detector version and the state layout).
- **State per (replay run, cell)** (`applyInPandasWithState`, `src/streaming/scoring.py`): the raw rows of the last `level.days` + 1 local days (what a day's level offsets and windows need) and the rows still waiting. The same `features.compute` as offline runs on each local day's rows plus that history, so streamed features equal the offline ones.
- **Arrival order:** `telemetry.raw` is keyed by cell *and* interval (`02-telemetry.md`), so one cell's intervals sit in different partitions and a fast replay delivers them out of order. Rows wait until every earlier interval of their cell has arrived (a run starts at the local midnight of its first day: a replay sends whole days); a hole is given up only once every Kafka partition has delivered a later row of the cell (each partition holds a run's rows in order, so the missing row cannot come any more; no clock, Step 9); a row arriving after a later interval was scored is counted as late and dropped (in practice: re-sent duplicates).
- **Output per micro-batch:** alarms to Kafka first, then Postgres `alarms` + one `detector_heartbeats` row (rows scored, rows late, alarms, latest interval) in one transaction. After a restart Spark re-runs the unfinished batch from its checkpoint with the same state and the same alarm IDs; alarms already stored are not sent again. Postgres gets each alarm once; Kafka is at-least-once (a crash between the send and the commit re-sends that batch under the same keys).
- A run silent for `state_timeout_minutes` (60) is dropped from the state; rows still waiting behind a lost message are scored then.
- **Scores do not depend on batch size:** the autoencoder scores in fixed 128-row chunks (`detectors.SCORE_BLOCK`), so the stream (a few rows per cell per micro-batch) and offline (whole days) compute every row the same way.
- **Parity check (Step 9):** `make detect-check ARGS="--day …"` replays a validation day after its history, waits for the job's commit log, and compares the streamed alarms with offline scoring of the same rows in the same image: IDs, scores (exact) and KPIs, then the validation report. 2013-12-03: MATCH (757 = 757).

## Candidates and selection
Trained offline on **training weeks** (`common.splits`, Rule 7):
1. **Seasonal baseline + z-score**: deviation from the same hour and weekday; the simplest baseline.
2. **Isolation Forest** on windowed multi-KPI features.
3. **Autoencoder**: reconstruction error on windowed features.

Optuna tunes window lengths, thresholds, contamination, latent size, and other hyperparameters on **validation weeks**. **Union (Step 6c):** the tuned Isolation Forest and autoencoder combined, a cell-interval alarming if either reaches its own threshold (`make union`); their misses are mostly different faults. Objective: **maximize detection rate and minimize delay, subject to ≤ 1 false incident per day**. Every trial is logged to MLflow; the trial budget and wall-clock timeout are fixed per study (guardrail R4).

## Promotion
- A **named** candidate (never picked automatically) is registered in MLflow as model `detector`, alias `champion`, only through an **evaluation gate** on the **validation weeks**: **≤ 1 false incident/day AND more faults caught than the incumbent** champion, both scored on the same validation days, fault schedule, and data snapshot; the first champion needs the cap only (`make promote ARGS="--candidate …"`; changed 2026-11-27, `00-overview.md` Rule 5 note). The test weeks are never used by the gate (Rule 7).
- A registered version is one **package**: fitted detector, alarm threshold and settings, feature config, and the fitted reference the features are computed from (`features.Spec`), so a promotion or rollback moves all of them together.
- Rollback (`make rollback`) moves `champion` to the version the current one replaced, and is logged; retraining is a reviewed event, never silent.
- A **dead detector alerts** (no scores while telemetry flows), shown on the detector-health dashboard (*decided 2026-11-28*, Step 10): a Grafana rule generated as code (`make dashboards`; `src/common/grafana.yaml`) fires when telemetry stored in `telemetry_raw` has waited **more than N = 5 minutes of wall-clock time** with no `detector_heartbeats` row after it, held 1 minute, evaluated every minute. Wall-clock minutes, not data intervals: a replay's speed decouples data time from wall time. It stays up until the detector reports again. Limits: it sees telemetry only through the sink (`make sink`); telemetry stored long after the detector went idle fires it; a restart with a backlog can fire it briefly (champion load + first batch took ~6 min on 2026-11-28). Measured 2026-11-28: live detector during a backlog, longest wait 1.9 min; detector stopped → fired 6.3 min after the telemetry arrived; restarted → cleared 10 s after the first heartbeat.
- Detection quality on the dashboard comes only from `make detect-check`, which stores each checked day's totals in `detection_reports` (faults, detected, Kaplan-Meier delay, false incidents, parity; no fault labels). Grafana never reads the ground truth (Rule 6).

## Must never
- Read the ground-truth fault log. Tuning scores (detection rate, delay, false incidents) are computed by `src/evaluation/` on validation weeks and handed to Optuna as numbers (Rule 6).
- Tune on test weeks (Rule 7).

## Known issue to handle
About 0.22% of real training rows already have `load_ratio` > 0.9 at busy peaks (`BUILD_LOG.md` 2026-11-20). They will look like congestion and count against the false-alarm budget unless the detector or the signature accounts for them. Step 6b handled it for latency and drop rate (judged against load / not detected); `load_ratio` itself is still judged against the clock, so congestion stays detectable.
