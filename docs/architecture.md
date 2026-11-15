# Architecture

Drawn from `README.md`. Status: design, not built.

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

| Component | Folder | Input → Output |
|---|---|---|
| Replay producer | `src/producer/` | Milan files → Kafka `telemetry.raw`, keyed by `(cell_id, interval_start)`; configurable speed-up |
| Fault injector | `src/producer/` | Seeded faults applied to the replay → ground-truth log (never read by detectors) |
| Kafka | `infra/` | Telemetry and alarm topics; malformed messages → dead-letter topic |
| Raw store | `infra/` | Raw telemetry in Postgres before processing, so any incident can be re-derived |
| Spark streaming job | `src/streaming/` | `telemetry.raw` → windowed features → promoted detector → Kafka `alarms` + Postgres; checkpointed |
| Detector | `src/models/` | Candidates tuned offline with Optuna under ≤ 1 false incident/day; winner promoted via MLflow |
| Correlator | `src/correlation/` | Alarms + topology graph → incidents with ranked root node |
| Classifier | `src/models/` | Incident features + noisy report → category, severity, owning team (XGBoost) |
| Ops agent | `src/agent/` | Engineer question → read-only tool calls → cited answer / root-cause draft |
| Console | `src/console/` | Streamlit: review, evidence, chat, approve/correct/close (human only) |
| Dashboards | `infra/` | Grafana: network KPIs, alarms vs. incidents, model health, system health |
| MLOps | `infra/`, `src/models/` | MLflow tracking, registry, promotion gate, rollback |

## Grafana dashboards (planned)
1. Network KPIs with fault and incident markers
2. Alarm storm vs. incidents (compression)
3. Model health: false alarms/day, detection delay, drift
4. System health: Kafka lag, dead-letter counts, Spark batch time, LLM calls/tokens vs. budget, dead-detector alert
