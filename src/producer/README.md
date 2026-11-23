# producer/

Replay producer (Milan traffic → Kafka `telemetry.raw`) and, from milestone 2, the seeded fault injector with its ground-truth log.

- `milan.py`, `grid.py`, `integrity.py`, `manifest.py`, `data_prep.py`: verify and process the Milan files (`make data`).
- `topology.py`: SYNTHETIC topology (`make topology`); `topology_db.py` loads it into Postgres (`make db-topology`).
- `kpi_model.py`: MODELED KPIs and their calibration (`make calibrate`).
- `injector.py`: SYNTHETIC fault schedule per split + effects + change events; ground truth for `src/evaluation/` only (`make inject`).
- `replay.py`: processed days (optionally fault-injected) → `telemetry.raw` (`make replay`); `telemetry_for` is the shared assembly path for offline code.
- `sink.py`: `telemetry.raw` → Postgres `telemetry_raw`, effectively once (`make sink`).
