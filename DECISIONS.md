# Decisions

Seeded from the decision log and "Still open" list in `PROJECT.md` (2026-11-14). Add new decisions here and in `PROJECT.md`'s decision log, with a date and a reason.

## Decided

| Date | Decision |
|---|---|
| 2026-11-13 | Original scope set: anomaly detection with Kafka over real data, plus incident classifier and triage agent |
| 2026-11-13 | Everything written from scratch; external projects are architecture reading only |
| 2026-11-13 | No public live demo — local only; demos by local run, screen-share, or recorded video |
| 2026-11-13 | Docker Compose first; Kubernetes as a later stretch |
| 2026-11-14 | The system must satisfy the AI infrastructure principles listed in `PROJECT.md` |
| 2026-11-14 | Data pulled programmatically from canonical sources, not via Kaggle |
| 2026-11-14 | **Re-scoped to telecom network operations** (target: FDE role at a US telecom carrier). Industrial SKAB/MetroPT-3 data dropped. Reason: the domain must match the target role. |
| 2026-11-14 | "Real data only" relaxed to real traffic + documented injected faults. Reason: public telecom data with labeled faults is essentially unavailable; injection gives known ground truth, and the trade-off is disclosed. |
| 2026-11-14 | Alarm correlation + topology root cause, and a tool-using ops agent, added as core features |
| 2026-11-14 | V1 bars re-pinned for the new scope (see Definition of done); earlier SKAB bars retired |
| 2026-11-14 | Delivery (video + engagement write-up) is a milestone, not an afterthought |
| 2026-11-14 | Project renamed to Network Incident Agent |
| 2026-11-14 | Tech stack chosen: Kafka, Spark Structured Streaming, PostgreSQL, XGBoost classifier, MLflow, Groq API for the agent (free tier) |
| 2026-11-14 | Detector chosen by optimization: candidates tuned with Optuna under a fixed false-alarm budget, best one promoted via MLflow |
| 2026-11-14 | Base data: Telecom Italia Milan dataset (real traffic) + injected faults |
| 2026-11-14 | Time budget: as long as needed for the best result (~35–50 h estimate), not a minimum-time build |
| 2026-11-14 | Code lives under `src/` (component folders from README + `src/common/` for shared schemas/config/DB); `infra/` and `tests/` stay top-level. Reason: the `src/` layout avoids accidental imports from the repo root and keeps the root clean. |
| 2026-11-14 | LLM settings are provider-neutral (`LLM_PROVIDER`, `LLM_MODEL`, `LLM_API_KEY`; Groq by default). Reason: matches the swappable provider interface. |
| 2026-11-14 | Data manifest is `data/manifest.json` (machine-checkable by the download script); `data/manifest-template.md` documents its fields. |
| 2026-11-14 | Evaluation stays split by time (train / validation / test weeks), and false alarms are counted per day. Reason: the Milan data is one continuous series with no experiment files; random row splits would leak neighbouring intervals. |
| 2026-11-14 | Milestone plans live in `docs/milestones/milestone-N.md` (no separate dated plan files). |
| 2026-11-14 | Lightweight domain model (`docs/domain-model.md`): YAML definitions + Postgres topology tables, no OWL/RDF or graph DB. The agent uses it for signature-based differential diagnosis with structured, code-validated answers; evals generate questions from ground truth + topology and grade deterministically. Reason: grounds the agent, keeps names consistent, and makes evals cheap and reproducible without extra infrastructure. |
| 2026-11-14 | Milan data license confirmed: ODbL 1.0. We attribute as required and publish code only, no derived data (so share-alike doesn't apply). Raw files (~20.8 GB) are processed one day at a time and deleted after filtering; only the subset and the hash manifest are kept. |
| 2026-11-14 | Milan files are downloaded manually in the browser; verification and processing are scripted. Reason: Dataverse requires a guestbook response with an email per download; keeping it manual means no script sends personal details. Definition of done updated to allow this one manual step. |
| 2026-11-14 | Milan processing rules: sum over country codes per square and interval; empty field = 0; missing row = 0 flagged as a data gap; timestamps stored in UTC, Europe/Rome used for seasonality. Reason: verified file format (tab-separated, no header, 10-min epoch-ms UTC intervals, 10,000 squares of ~235 m). |
| 2026-11-14 | Repo folder renamed to `network_incident_agent`; git initialized on `main` with a gitleaks pre-commit hook (guardrail S3). |
| 2026-11-19 | **Milestone 1 started.** Plan in `docs/milestones/milestone-1.md`; its default choices confirmed: Python 3.11, time split as proposed in `docs/data-sources.md`, 6 partitions on `telemetry.raw`, plain Python consumer as the M1 Postgres sink (Spark from M2), simple load-driven KPI model in M1. |
| 2026-11-19 | **GitHub repo stays public** (code and docs only). Reason: portfolio project. Rule 2 ("local only") applies to the running system and demo, not the source code. No data, secrets, or carrier material are ever committed (Rules 3, 8, 14), enforced by gitleaks pre-commit + CI. |
| 2026-11-19 | **Laptop has 8 GB RAM total** (Intel i5-10500, 12 threads, 54 GB disk free; ~47% RAM free at check). Adjustments: cell subset ~150 squares (was ~300); Spark behind a Compose profile, off by default in M1; hard memory limit per container; Docker Desktop capped at ~5 GB. Quality bars unchanged. Reason: the design assumed ~8 GB *free*. |
| 2026-11-19 | Tooling: `uv` with `uv.lock` as the exact dependency pin (guardrail S5); ruff (lint + format), mypy strict, pytest + coverage; pre-commit (gitleaks, ruff, large-file and private-key checks) replaces the unversioned hook; GitHub Actions CI (gitleaks + `make check`); `.claude/settings.json` denies Claude Code reads/edits of `.env` and edits under `data/`. Reason: a feedback loop and enforced guardrails before any application code. |
| 2026-11-20 | **Cell subset fixed:** 12 × 12 block (144 squares) centred on square 5061, nearest the Duomo. Reason: compact and contiguous, so synthetic sites and aggregation nodes are geographically sensible; ~150 fits the 8 GB laptop. |
| 2026-11-20 | Published Dataverse checksums (file ID, size, MD5 for all 63 files) committed in `src/producer/milan_files.json`. Reason: `make data` refuses any file not on the list or not matching it; metadata, not data, so committing it is allowed. |
| 2026-11-20 | Data processing uses Polars (streaming scan; ~400 MB peak for a 323 MB day file). Processed days are stored as `data/processed/traffic/<date>.parquet` (zstd, UTC ms timestamps), byte-for-byte reproducible. Reason: fits the 8 GB laptop. |
| 2026-11-20 | A source row with all activity fields empty counts as recorded zero activity, not a data gap; only a missing (square, interval) row is a gap. Reason: the empty fields are documented as "no recorded activity". |
| 2026-11-20 | Branch protection on `main`: CI checks required, no force-push or deletion, linear history; admins may bypass (solo project). |

## Open

| Item | Notes |
|---|---|
| Groq model | Chosen at Milestone 5, by tool-calling reliability on the agent eval set |
| Data versioning | DVC vs. plain versioned snapshots (plain snapshots recommended) |
