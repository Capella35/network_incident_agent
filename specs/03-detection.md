# 03 — Detection

Turns `telemetry.raw` into per-cell **alarms**. Built in Milestone 2 (`milestones/milestone-2.md`). Drawn from `PROJECT.md` → What is being built (items 4–5) and `README.md` → Models; details still open are marked **(M2)** and are decided in the Milestone 2 plan.

## Behavior
- **Spark Structured Streaming job** (`src/streaming/`) reads `telemetry.raw`, computes windowed per-cell features, scores them with the **promoted** detector, and writes alarms to Kafka `alarms` and Postgres. Checkpointing gives exactly-once output.
- **Features:** rolling means, deviation from a reference, rates of change. KPI names, units, and bad directions come from the KPI catalog (`01-domain-model.md` §3), never literals. Each detected KPI is judged against what explains its normal swings (Step 6b, `DECISIONS.md` 2026-11-26):
  - **against the clock**: real traffic KPIs (on a `log1p` scale, with a 7-day level adjustment) and MODELED `load_ratio`: deviation from the seasonal baseline for that cell, hour of day, and weekday (Europe/Rome);
  - **against the load**: MODELED `latency_ms`, as log latency vs what the training weeks show at the same `load_ratio` (100 equal-count load bins, one curve for all cells since load is capacity-normalized; a real network would need per-cell curves). A busy hour's queueing delay is normal; latency high for its load is not;
  - **not detected**: MODELED `drop_rate`. It is a function of load only (no injected fault moves it off its curve), so it adds nothing beyond load and false-alarms at busy hours.
- Data gaps (`data_gap = true`, `02-telemetry.md`) are not faults: the detector must not alarm on a gap alone.
- **An alarm** is raised on one cell for one interval, with the KPI(s), score, and detector version (`ALM-<uuid>`, `01-domain-model.md` §2). Alarm message fields **(M2)**.

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
- A **dead detector alerts** (no scores for N intervals while telemetry flows; N **(M2)**), shown on the system-health dashboard.

## Must never
- Read the ground-truth fault log. Tuning scores (detection rate, delay, false incidents) are computed by `src/evaluation/` on validation weeks and handed to Optuna as numbers (Rule 6).
- Tune on test weeks (Rule 7).

## Known issue to handle
About 0.22% of real training rows already have `load_ratio` > 0.9 at busy peaks (`BUILD_LOG.md` 2026-11-20). They will look like congestion and count against the false-alarm budget unless the detector or the signature accounts for them. Step 6b handled it for latency and drop rate (judged against load / not detected); `load_ratio` itself is still judged against the clock, so congestion stays detectable.
