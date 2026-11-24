# Network Incident Agent — Project Overview

*What this project is, what it builds, and the plan.*
*Status: design stage (started 2026-11-13, re-scoped to telecom network operations 2026-11-14, tech stack chosen 2026-11-14). No code yet. This document is the project charter; `README.md` is the technical design. Specs and milestone plans live in `specs/`; the Milestone 1 plan is written just before it starts.*

---

## What is this project?

Network Incident Agent watches a cellular network through its telemetry, notices when cells start degrading, groups the resulting alarm storm into one incident, points to the most likely root cause in the network topology, labels the incident (fault category, severity, owning team), and lets an operations engineer investigate it with an agent that answers questions using cited evidence. A human approves every action.

In plain terms: when one upstream link fails, dozens of cell sites alarm at once. The NOC sees a wall of red, the real cause is buried, and the ticket goes to the wrong team. This project attacks that: **detect early, collapse the storm into one incident, find the root, label it correctly, explain with evidence, and keep a human in charge.**

Name: Network Incident Agent, set on 2026-11-14 (replacing "IoT Incident Agent"). Repo folder renamed to `network_incident_agent` on 2026-11-14.

## Purpose

1. **Portfolio project for a Forward Deployed Engineer role in telecom (target: a US telecom carrier).** It is framed as a customer engagement: a real operations problem, a scoped solution, measured results, and stated limits. The write-up and demo matter as much as the code.
2. **Solve a real NOC problem.** The expensive failures are late detection, alarm storms that hide the root cause, and mislabeled incidents routed to the wrong team. The system is built to measurably reduce all three.
3. **A complete, running streaming AI system.** Telemetry ingestion, detection, correlation, classification, and a tool-using agent integrated into one system with production-grade foundations: streaming, containerized serving, MLOps, observability, reliability, security, and cost control.
4. **Prove quality with numbers fixed in advance,** on data the system has never seen (see `specs/00-overview.md` → Definition of done).

## Tech stack

Chosen 2026-11-14.

| Layer | Choice |
|---|---|
| Streaming backbone | Apache Kafka |
| Stream processing | Spark Structured Streaming (PySpark) |
| Storage | PostgreSQL |
| Anomaly detection | Optimized model selection: several candidate detectors tuned with Optuna and compared under a fixed false-alarm budget; the best is promoted (see below) |
| Root cause / correlation | Topology rules + graph ranking over the alarm graph |
| Incident classifier | XGBoost |
| Experiment tracking + model registry | MLflow |
| Ops agent LLM | Groq API (free tier), open-weight model chosen at Milestone 5 |
| Console + dashboards | Streamlit (incident console, agent chat) + Grafana (KPIs, system health) |
| Deployment | Docker Compose |
| Data | Telecom Italia Milan dataset (real traffic) + documented injected faults |

## What is being built

A pipeline of small services, all running locally in containers:

1. **Replay producer** — real Milan traffic replayed into Kafka at a configurable speed-up (e.g., one 10-minute interval per second), with real-time speed available for demos. Every message is keyed by `(cell_id, interval_start)` so replays are deterministic and idempotent.
2. **Fault injector** — a separate, seeded component that injects documented faults (see The data) into the replay. It writes a ground-truth event log that the detectors never read.
3. **Kafka streaming backbone** — one defined telemetry schema; malformed messages go to a dead-letter topic, never silently dropped; raw data is stored in Postgres before processing so any incident can be re-derived.
4. **Spark Structured Streaming job** — reads from Kafka, computes windowed per-cell features (rolling means, deviations from the seasonal baseline for that hour and weekday, rates of change), scores them with the promoted detector, and writes alarms to Kafka and Postgres. Checkpointing gives exactly-once output.
5. **Detector (optimized)** — candidate models are trained offline on training weeks: seasonal baseline + z-score, Isolation Forest, and an autoencoder. Their hyperparameters (window lengths, thresholds, contamination, latent size) are tuned with Optuna. The objective is to maximize detection rate and minimize delay **subject to ≤ 1 false incident per day** on validation weeks. Every trial is logged to MLflow, and the winner is registered and promoted through an evaluation gate.
6. **Alarm correlator** — groups alarms in time and across the topology graph (cell → site → aggregation node → core) into one incident and ranks the most likely root node. Incidents open only from detector output, never from ground truth.
7. **Incident classifier (XGBoost)** — labels each incident with fault category, severity, and owning team. Features: blast radius (cells, sites, aggregation nodes affected), topology level of the root, KPI deltas, onset shape (sudden vs. gradual), time since the last change event, and TF-IDF features from the noisy human report. Tuned with Optuna, tracked and registered in MLflow.
8. **Ops agent (Groq)** — a tool-using agent that can query telemetry, topology, and past incidents to answer an engineer's questions ("why did throughput drop in sector X at 14:00?") and draft a root-cause summary that cites evidence. It can read and draft, never act: closing, escalating, or re-routing an incident requires a human decision, enforced in code, not in a prompt. The LLM sits behind a small provider interface so the model or provider can be swapped, and calls are rate-limited to stay inside Groq's free tier.
9. **Incident console + dashboards** — a Streamlit console where humans review incidents, see evidence charts, chat with the agent, and approve or correct labels and root cause (corrections become labeled data). Grafana shows network KPIs, alarm volume vs. incident volume, false alarms, drift, and system health.
10. **MLOps layer** — MLflow holds every model with its data snapshot, config, seed, and held-out metrics; promotion only through an evaluation gate; rollback always possible; retraining is a reviewed event, never silent.

Everything runs from one command (`docker compose up`).

## The data

Public telecom data with labeled faults is rare, so the data is **real traffic + injected, documented faults**. This is a deliberate trade-off and is stated openly in the README.

- **Base traffic (real): Telecom Italia "Big Data Challenge" Milan dataset** (Harvard Dataverse) — real call/SMS/internet activity per grid square in 10-minute intervals, Nov 2013 – Jan 2014 (~2 months). Pulled programmatically from its canonical source by a download script; never redistributed in the repo; file hashes recorded in a data manifest. License confirmed 2026-11-14: **ODbL 1.0** (attribution link "from BigDataChallenge contest" required; share-alike applies to any derived database we publish). 62 daily files, ~20.8 GB in total, processed one day at a time and filtered to the cell subset (see `specs/02-telemetry.md`). Column layout is confirmed from the first downloaded file in Milestone 1.
- **Cell subset.** A fixed, documented subset of grid squares (e.g., a few hundred) is used so the stack runs on a laptop.
- **Topology (synthetic, documented).** The dataset has no network topology, so a seeded generator maps grid squares to cells and sites, and sites to aggregation nodes and core, based on geographic proximity. The generator and its parameters are committed.
- **Derived KPIs (modeled, documented).** Latency, drop rate, and similar KPIs are derived from real load through a documented model, so they follow real daily and weekly patterns.
- **Fault taxonomy (injected).** Each type has a fixed category, severity rule, and owning team:
  - Cell outage — one cell's traffic drops to ~0 → RAN
  - Site power failure — all cells at a site go down → Field Ops
  - Backhaul/transport failure — every site under one aggregation node degrades at once → Transport
  - Congestion — load spike with rising latency and drops → RAN / Capacity
  - Bad config push — gradual degradation starting at a logged change event → Core / Config
- **Noisy reports.** Each incident also gets a human-style reported description that is vague and sometimes guesses wrong first. Report templates are written independently of the classifier, and the test set uses templates the classifier never saw, so the classifier can't just learn the generator.
- **Splits:** by time. Training weeks, validation weeks (used for Optuna tuning and the promotion gate), and test weeks touched only once for the final numbers. Test faults also use a separate injector seed and fault locations, timings, and magnitudes drawn from parameters disjoint from training.
- **Matching rule:** a detected incident counts as a hit if it overlaps a ground-truth fault window (± one interval). Incidents with no matching fault are false alarms, labeled "no fault". A fault is "detected" when at least one incident matches it.
- **Real-world events.** The Milan data covers the Christmas / New Year holidays. These unusual-but-normal days stay in the data as a realistic false-alarm test.

## Design principles

- **Built from scratch.** No third-party project is adopted, forked, or copied; existing projects are architecture reading only. Well-known maintained libraries are used as dependencies. Secrets (including the Groq API key) live in `.env` only, never committed.
- **Local only — no public live demo.** A public LLM-backed endpoint is a runaway-cost and abuse risk. Demonstration is by local run, screen-share, and a recorded video; the full system runs on any machine at $0 (the ops agent is off by default, so no API key is needed for the core demo).
- **Total build cost target: $0** — open-source software, free data, classical ML on a laptop, Groq free tier for the agent.
- **AI infrastructure is a requirement, not an add-on** — schema and dead-letter handling, training and serving as separate code paths, versioned models with evaluation gates, a per-incident audit trail, idempotent consumers, health checks that alert instead of failing silently, an LLM call/token log with a per-run budget cap, and one-command startup.
- **Honest about synthetic parts.** Every synthetic or modeled component (topology, derived KPIs, faults, reports) is labeled as such in the README, dashboards, and write-up.

## The plan — all milestones

Milestones 1–6 are the v1 build (estimated ~35–50 hours; Spark and model optimization add time over a plain-consumer design). The goal is the best result, not the fastest. Each milestone gets its own step-by-step plan just before it starts.

1. **Data + replay** — Milan download script + manifest (license and contents confirmed); cell subset + topology generator; `docker-compose.yml` (Kafka, Spark, Postgres, Grafana, MLflow); replay producer → `telemetry.raw`; raw rows in Postgres `telemetry_raw`; one Grafana panel showing live cell traffic. Done when re-running the replay reproduces the same rows (same keys, same order).
2. **Fault injection + detection** — injector with ground-truth log; Spark feature job; candidate detectors tuned with Optuna and tracked in MLflow; best detector promoted and scoring the stream; detection-rate / delay / false-alarm report.
3. **Correlation + root cause** — topology-aware alarm grouping into incidents; root-node ranking; alarm-compression report.
4. **Classifier + console** — noisy reports; XGBoost classifier tuned and registered in MLflow; Streamlit console with evidence view and approve/correct flow; corrections stored as labeled data.
5. **Ops agent** — Groq-backed agent with read-only tools over telemetry, topology, and incident history; cited root-cause drafts; human gate enforced and tested; agent eval set; rate limiting and call log.
6. **Hardening + delivery** — CI, drift and false-alarm dashboards, final test-week evaluation, README with results and known limits, demo video, and a write-up structured as a customer engagement (problem → scope → trade-offs → results → limits → next steps). This is the v1 finish line.

Stretch (outside the estimate):
7. **Kubernetes** — the Compose stack redeployed on a local cluster.
8. **Live feed** — one genuinely live public API as an extra, display-only Kafka topic (never used for training or scoring).
9. **IoT fleet view** — a customer-side view of connected devices that separates network-side causes from device-side causes.

## Definition of done — v1

Moved to `specs/00-overview.md` → Definition of done (2026-11-22). Changing a bar still requires a recorded reason next to it and in `DECISIONS.md`.

## Decision log

| Date | Decision |
|---|---|
| 2026-11-13 | Original scope set: anomaly detection with Kafka over real data, plus incident classifier and triage agent |
| 2026-11-13 | Everything written from scratch; external projects are architecture reading only |
| 2026-11-13 | No public live demo — local only; demos by local run, screen-share, or recorded video |
| 2026-11-13 | Docker Compose first; Kubernetes as a later stretch |
| 2026-11-14 | The system must satisfy the AI infrastructure principles listed above |
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
| 2026-11-14 | Lightweight domain model (`docs/domain-model.md`): YAML definitions + Postgres topology tables, no OWL/RDF or graph DB. The agent uses it for signature-based differential diagnosis with structured, code-validated answers; evals generate questions from ground truth + topology and grade deterministically. |
| 2026-11-14 | Milan data license confirmed: ODbL 1.0. We attribute as required and publish code only, no derived data (so share-alike doesn't apply). Raw files (~20.8 GB) are processed one day at a time and deleted after filtering; only the subset and the hash manifest are kept. |
| 2026-11-14 | Milan files are downloaded manually in the browser; verification and processing are scripted. Reason: Dataverse requires a guestbook response with an email per download; keeping it manual means no script sends personal details. Definition of done updated to allow this one manual step. |
| 2026-11-14 | Milan processing rules: sum over country codes per square and interval; empty field = 0; missing row = 0 flagged as a data gap; timestamps stored in UTC, Europe/Rome used for seasonality. Reason: verified file format (tab-separated, no header, 10-min epoch-ms UTC intervals, 10,000 squares of ~235 m). |
| 2026-11-14 | Repo folder renamed to `network_incident_agent`; git initialized on `main` with a gitleaks pre-commit hook (guardrail S3). |
| 2026-11-19 | Milestone 1 started; plan defaults confirmed (details in `DECISIONS.md`). |
| 2026-11-19 | GitHub repo stays public (code and docs only); "local only" applies to the running system and demo. |
| 2026-11-19 | Laptop has 8 GB RAM total: cell subset ~150 squares, Spark behind a Compose profile in M1, per-container memory limits. Quality bars unchanged. |
| 2026-11-19 | Tooling: uv + `uv.lock` pins, ruff, mypy strict, pytest, pre-commit (gitleaks), GitHub Actions CI, Claude Code deny rules. |
| 2026-11-22 | **Numbered `specs/` folder** (`00-overview` … `09-evals` + `milestones/`): one spec per product behavior, read in order. `docs/` folded in (architecture → 00, domain model → 01, data sources + telemetry message → 02, incident record → 04, guardrails → 08); Definition of done moved to `00-overview.md`; `evals/eval-plan.md` → `09-evals.md`; new 03, 05, 06, 07 assembled from this charter and the README. Supersedes the 2026-11-14 `docs/milestones/` entry. |
| 2026-11-22 | `HANDOFF.md` moved to `specs/HANDOFF.md` (user's choice), next to the milestone plans it tracks. Still read first in every session and updated last. |
| 2026-11-22 | **Milestone 1 done:** replay → Kafka → Postgres → Grafana, reproducible (`make replay-check` → MATCH on training week 1); all M1 guardrails pass; ODbL attribution links applied as the publisher words them. Details in `DECISIONS.md` and `BUILD_LOG.md`. |

## Still open

- **Groq model:** chosen at Milestone 5, by tool-calling reliability on the agent eval set.
- **Data versioning:** DVC vs. plain versioned snapshots (plain snapshots recommended).
- **Owning team (M4):** derive from the predicted fault category by lookup instead of predicting it; predict severity only for `congestion`.
- **SYNTHETIC dispatch matrix (M4):** `dispatch.yaml` mapping fault type + region/time/vendor to a team, deterministic, modeled on public frameworks.
- **SYNTHETIC runbooks + `get_runbook` tool (M5):** cited by the agent, passed as data.
- **Concurrent investigations (M5):** one agent run per incident, severity queue, shared global budget; possible `related_open_incidents` tool.
- **MCP wrapper for agent tools:** decide after Milestone 5's quality bar is met.
