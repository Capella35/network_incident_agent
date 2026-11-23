# infra/

Local stack for milestone 1, step 6. Start it with `make up` (needs `.env`; see `.env.example`).

| Path | What it holds |
|---|---|
| `docker-compose.yml` | Kafka (KRaft), Postgres, Grafana, MLflow; Spark under the `spark` profile (off by default). Ports on `127.0.0.1`, pinned tags, memory limits |
| `preflight.sh` | Startup check: refuses placeholder or short passwords (prints names only) |
| `db/000_roles.sh` | Creates the `pipeline_writer` and `readonly` roles on first start |
| `db/001_init.sql` | Schema: `telemetry_raw`, SYNTHETIC topology tables, least-privilege grants |
| `kafka/create-topics.sh` | Creates `telemetry.raw` and `telemetry.dlq` with bounded retention |

MLflow runs with server-side jobs off (`MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`): they need ~1.1 GB and were OOM-killed in a loop under the 768m limit. `make test-integration` includes `tests/integration/test_stack_health.py`, which fails on any restart or OOM kill, because a health check can pass between crashes.

Grafana dashboards come in a later step.
