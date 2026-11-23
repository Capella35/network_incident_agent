# 03 — Detection

Turns `telemetry.raw` into per-cell **alarms**. Built in Milestone 2 (`milestones/milestone-2.md`). Drawn from `PROJECT.md` → What is being built (items 4–5) and `README.md` → Models; details still open are marked **(M2)** and are decided in the Milestone 2 plan.

## Behavior
- **Spark Structured Streaming job** (`src/streaming/`) reads `telemetry.raw`, computes windowed per-cell features, scores them with the **promoted** detector, and writes alarms to Kafka `alarms` and Postgres. Checkpointing gives exactly-once output.
- **Features:** rolling means, deviation from the seasonal baseline for that hour of day and weekday (Europe/Rome), rates of change. KPI names, units, and bad directions come from the KPI catalog (`01-domain-model.md` §3), never literals.
- Data gaps (`data_gap = true`, `02-telemetry.md`) are not faults: the detector must not alarm on a gap alone.
- **An alarm** is raised on one cell for one interval, with the KPI(s), score, and detector version (`ALM-<uuid>`, `01-domain-model.md` §2). Alarm message fields **(M2)**.

## Candidates and selection
Trained offline on **training weeks** (`common.splits`, Rule 7):
1. **Seasonal baseline + z-score**: deviation from the same hour and weekday; the simplest baseline.
2. **Isolation Forest** on windowed multi-KPI features.
3. **Autoencoder**: reconstruction error on windowed features.

Optuna tunes window lengths, thresholds, contamination, latent size, and other hyperparameters on **validation weeks**. Objective: **maximize detection rate and minimize delay, subject to ≤ 1 false incident per day**. Every trial is logged to MLflow; the trial budget and wall-clock timeout are fixed per study (guardrail R4).

## Promotion
- The winner is registered in MLflow and promoted only through an **evaluation gate**: it must beat the z-score baseline on the validation objective (Definition of done, `00-overview.md`). Gate thresholds beyond that **(M2)**.
- Rollback to the previous registered version is always possible; retraining is a reviewed event, never silent.
- A **dead detector alerts** (no scores for N intervals while telemetry flows; N **(M2)**), shown on the system-health dashboard.

## Must never
- Read the ground-truth fault log. Tuning scores (detection rate, delay, false incidents) are computed by `src/evaluation/` on validation weeks and handed to Optuna as numbers (Rule 6).
- Tune on test weeks (Rule 7).

## Known issue to handle
About 0.22% of real training rows already have `load_ratio` > 0.9 at busy peaks (`BUILD_LOG.md` 2026-11-20). They will look like congestion and count against the false-alarm budget unless the detector or the signature accounts for them.
