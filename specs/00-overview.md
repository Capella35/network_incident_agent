# 00 — Overview (read first)

What the product does, how the parts fit, and what "done" means for v1. Each later file specifies one part. If a spec and an older note elsewhere disagree, the spec wins; changing a bar or a behavior needs a dated reason in `DECISIONS.md` (Rule 5).

Status: design, being built milestone by milestone (`milestones/`). **Synthetic or modeled parts are labeled as such everywhere** (Rule 9): topology, derived KPIs, faults, and reports.

## Reading order

| File | What it specifies |
|---|---|
| `00-overview.md` | This file: pipeline, components, Definition of done |
| `01-domain-model.md` | Shared vocabulary: entities, relations, KPI catalog, fault types and signatures |
| `02-telemetry.md` | Data in: Milan source and processing, synthetic topology, modeled KPIs, replay, the `telemetry.raw` message |
| `03-detection.md` | Windowed features, detector candidates, the optimization objective, promotion, alarms |
| `04-correlation.md` | Alarms → one incident with a ranked root node; the incident record |
| `05-classification.md` | Fault category, severity, owning team; noisy reports |
| `06-ops-agent.md` | The investigation agent: tools, reasoning procedure, cited and validated answers |
| `07-human-gate.md` | When a person takes over: lifecycle, console actions, corrections |
| `08-guardrails.md` | What it never does: cost, privacy, security, agent safety, reputation |
| `09-evals.md` | How we check it: splits, matching, agent eval design, results |
| `milestones/` | The build plan: one file per milestone, each step with its own **Done when** |
| `HANDOFF.md` | Current build state and the next step. Read first in every session, updated last |

## Pipeline

```
Milan traffic ─► replay producer ─► Kafka: telemetry.raw ──────────────┐
                 + fault injector ─► ground-truth log (eval only)       │
                                                                       ▼
                                                 Spark Structured Streaming
                                                 windowed features + promoted detector
                                                                       │
                                                         Kafka: alarms ▼
               topology graph ───────────────────────────► correlator ─► incident + root node
                                                                       │
                                                        XGBoost classifier ─► category / severity / team
                                                                       │
                                                                  PostgreSQL
                                                     ┌─────────────────┼──────────────────┐
                                                     ▼                 ▼                  ▼
                                                  Grafana      Streamlit console     Ops agent (Groq)
                                               KPIs, health   approve / correct /   read-only tools,
                                                              close (human only)    cited answers

MLflow: every detector/classifier run, the model registry, and the promotion gate
```

## Components

| Component | Folder | Input → Output | Spec |
|---|---|---|---|
| Replay producer | `src/producer/` | Milan files → Kafka `telemetry.raw`, keyed by `(cell_id, interval_start)`; configurable speed-up | 02 |
| Fault injector | `src/producer/` | Seeded faults applied to the replay → ground-truth log (never read by detectors) | 02, 09 |
| Kafka | `infra/` | Telemetry and alarm topics; malformed messages → dead-letter topic | 02 |
| Raw store | `infra/` | Raw telemetry in Postgres before processing, so any incident can be re-derived | 02 |
| Spark streaming job | `src/streaming/` | `telemetry.raw` → windowed features → promoted detector → Kafka `alarms` + Postgres; checkpointed | 03 |
| Detector | `src/models/` | Candidates tuned offline with Optuna under ≤ 1 false incident/day; winner promoted via MLflow | 03 |
| Correlator | `src/correlation/` | Alarms + topology graph → incidents with ranked root node | 04 |
| Classifier | `src/models/` | Incident features + noisy report → category, severity, owning team (XGBoost) | 05 |
| Ops agent | `src/agent/` | Engineer question → read-only tool calls → cited answer / root-cause draft | 06 |
| Console | `src/console/` | Streamlit: review, evidence, chat, approve/correct/close (human only) | 07 |
| Dashboards | `infra/` | Grafana: network KPIs, alarms vs. incidents, model health, system health | below |
| MLOps | `infra/`, `src/models/` | MLflow tracking, registry, promotion gate, rollback | 03, 05 |

### Grafana dashboards (planned)
1. Network KPIs with fault and incident markers
2. Alarm storm vs. incidents (compression)
3. Model health: false alarms/day, detection delay, drift
4. System health: Kafka lag, dead-letter counts, Spark batch time, LLM calls/tokens vs. budget, dead-detector alert

## Definition of done — v1

*Moved here from `PROJECT.md` on 2026-11-22; content unchanged.* Set on 2026-11-14 for the re-scoped project, before any build. Changing a bar later requires recording the reason next to it. How each bar is measured: `09-evals.md`.

- Detect **≥ 90%** of injected faults in the test weeks
- Median detection delay **≤ 2 intervals** (20 min of data time); **≤ 1 false incident per simulated day** across the whole network (all three numbers always reported together, with the days of data they were measured on)
  - *2026-11-25 (Rule 5 note; bar unchanged):* "median" is the **Kaplan–Meier** median of time-to-detection, with missed faults right-censored, reported with its 95% CI (`specs/09-evals.md`; `DECISIONS.md` 2026-11-25). Reason: a median over detected faults only is biased low.
- Root cause: correct root node ranked **#1 in ≥ 80%** of multi-cell incidents; alarm-to-incident compression ratio reported
- Incident mislabel rate **≤ 10%**, reported per category, on test weeks and unseen report templates
- The promoted detector beats the simplest baseline (seasonal z-score) on the validation objective; the comparison table is in the README
- Ops agent: **≥ 80%** correct on a fixed eval set of ≥ 20 investigation questions, every answer citing evidence; 100% of close/escalate/re-route actions blocked without human approval (tested)
- One command from a fresh clone reproduces the full demo, after the documented one-time manual data download *(changed 2026-11-14: Harvard Dataverse requires a guestbook form with an email for every download, so the download can't be automated without sending personal details)*; every reported number traces to a data snapshot, config, seed, and model version in MLflow; a dead detector alerts; known limits (including all synthetic components) are written in the README; demo video, screenshots, and the engagement write-up are in the repo
