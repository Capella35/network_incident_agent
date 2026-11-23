# common/

Shared code used by every component.

- `domain/`: the domain model (entities, KPIs) loaded from YAML (`specs/01-domain-model.md`).
- `splits.py`: train / validation / test days (Rule 7).
- `db.py`: Postgres settings for the application roles.
- `telemetry.py`: the `telemetry.raw` message schema, Kafka key, and topic names (`specs/02-telemetry.md`).
- `grafana.py`: generates the Grafana dashboards from `kpis.yaml` (`make dashboards`).
