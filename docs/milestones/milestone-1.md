# Milestone 1 — Data + replay

**Goal:** Milan download script + manifest (license and contents confirmed); cell subset + topology generator; `docker-compose.yml` (Kafka, Spark, Postgres, Grafana, MLflow); replay producer → `telemetry.raw`; raw rows in Postgres `telemetry_raw`; one Grafana panel showing live cell traffic.

**Done when:** re-running the replay reproduces the same rows (same keys, same order).

**Domain model:** `entities.yaml`, `kpis.yaml`, loader + validation; topology tables (`docs/domain-model.md` §9).

**Guardrails due this milestone** (`docs/guardrails.md`): P3, P4, P5, S1, S2, S3, S5, S6, R1, R2, R5.

## Detailed plan

*Status: confirmed and started 2026-11-19. Estimate: ~8–12 h. Adjusted for an 8 GB laptop (see `DECISIONS.md` 2026-11-19).*

Each step ends with its own check. Commit after each step; one `BUILD_LOG.md` entry per session.

### Step 0 — Prerequisites (manual, ~30 min) — *done 2026-11-19*
- **Laptop resource check:** record free RAM, CPU cores, free disk, and Docker Desktop's memory limit in `BUILD_LOG.md`. This sets the cell subset size (Step 4) and container memory limits (Step 6).
- **Files on disk** in `data/raw/`: `milano-grid.geojson` and `sms-call-internet-mi-2013-11-01.txt` (downloaded and verified 2026-11-14; grid MD5 re-checked 2026-11-19).
- Download the **training + validation weeks** (2013-11-04 → 2013-12-15) in weekly batches only when Step 3 is ready to process them, so raw files never pile up (~2.3 GB per week).
- **Done when:** resources recorded; both files in `data/raw/`.

### Step 1 — Tooling and feedback loop (~1 h) — *done 2026-11-19*
Set this up before any application code so every later step has tests, lint, and secret scanning.
- `pyproject.toml`: Python 3.11 (default; PySpark-compatible), `src/` layout, **pinned** dependency versions (S5). Tools: `ruff` (lint + format), `mypy`, `pytest`, `pytest-cov`.
- `Makefile` targets: `lint`, `typecheck`, `test`, `check` (all three), `data`, `replay`, `clean` (R3, can start minimal).
- `.pre-commit-config.yaml`: gitleaks (pinned tag) + ruff. Replaces the unversioned `.git/hooks/pre-commit` (S3).
- `.github/workflows/ci.yml`: on push and PR → gitleaks, `make check`. No Docker or data in CI.
- `.claude/settings.json`: deny edits to `.env` and to `data/**` except the README/manifest files.
- **Done when:** `make check` passes on an empty test suite; a commit containing a fake key is blocked locally and in CI.

### Step 2 — Domain model: entities + KPIs (~1 h) — *done 2026-11-19*
- `src/common/domain/entities.yaml`: Core, AggregationNode, Site, Cell, Alarm, Incident, ChangeEvent, Team, with the ID formats and relations in `docs/domain-model.md` §2.
- `src/common/domain/kpis.yaml`: the §3 KPI catalog (name, unit, meaning, bad direction, source, real/modeled/derived).
- `src/common/domain/loader.py`: loads + validates both files into one typed, frozen object (e.g., pydantic); ID-format regexes exposed for reuse.
- **Tests:** loader rejects a duplicate KPI name, an unknown unit, and a bad ID pattern; the valid YAML loads.
- **Done when:** no other module hard-codes an entity or KPI name (Rule 15); later steps import from here.

### Step 3 — Verify + process script (`make data`) (~2 h) — *done 2026-11-20*
- `src/producer/data_prep.py`, run one day at a time:
  1. Check the file's **MD5 against the published value** (hard-coded list of expected MD5s per file ID, taken from Dataverse); also compute SHA-256. Mismatch → stop.
  2. Parse: tab-separated, no header, 8 columns; empty field = 0.
  3. Filter to the cell subset (Step 4); **sum over country codes** per (square, interval).
  4. Fill missing (square, interval) rows with 0 and `data_gap = true`.
  5. Write `data/processed/traffic/<date>.parquet` (UTC timestamps).
  6. Append to `data/manifest.json`: file name, source DOI + file ID, download date, MD5, SHA-256, license (ODbL 1.0), row counts in/out.
  7. Optionally delete the raw file (`--delete-raw`).
- Never execute anything from the downloaded data (S6); the script only reads text.
- **Tests:** small hand-written fixture files (not Milan data): country-code summing, empty → 0, gap filling, UTC handling, MD5 mismatch rejected, manifest entry written.
- **Done when:** 2013-11-01 processes with 144 intervals per subset square; manifest has its entry; re-running gives the same SHA-256 for the output file.

### Step 4 — Cell subset + topology generator (~1.5 h) — *done 2026-11-20*
- **Subset:** a compact, contiguous block of grid squares in central Milan (so sites and aggregation nodes are geographically sensible). Size from Step 0: **~150 squares** (8 GB laptop; R5). Selection rule + list written to `data/processed/subset.json` and documented here.
- **Topology (synthetic, seeded):** 1 square = 1 Cell; cells → Sites by proximity (~3 sectors per site); sites → AggregationNodes by k-means on location (~8–12 sites per node, enough for backhaul faults to span several sites); all AGG nodes → `CORE-1`. Seed and parameters committed in `src/producer/topology_config.yaml`.
- Output: `data/processed/topology_nodes.csv`, `topology_edges.csv`, loaded into Postgres in Step 6.
- **Tests:** same seed → identical topology; every cell has exactly one parent chain to `CORE-1`; all IDs match the `entities.yaml` patterns; no aggregation node with < 3 sites.
- **Done when:** topology generated and labeled **synthetic** in `topology_meta.json` (instead of a CSV header comment, which would break CSV readers) and in `docs/data-sources.md`.

### Step 5 — Derived KPIs (modeled) (~1 h)
The telemetry schema (`schemas/telemetry-message.md`) carries `latency_ms` and `drop_rate`, so a first, simple model is needed here; Milestone 2 may refine it.
- `src/producer/kpi_model.py`: `load_ratio` = cell traffic ÷ modeled capacity (e.g., the cell's 99th-percentile training-week load × headroom); `latency_ms` and `drop_rate` rise non-linearly with `load_ratio`, plus small seeded noise. Parameters in config, seeded.
- **Tests:** deterministic for a fixed seed; monotonic in load; values within catalog ranges.
- **Done when:** values follow the daily pattern of real load; labeled **modeled** in the code docstring and docs.

### Step 6 — Docker Compose stack (~2 h)
- `infra/docker-compose.yml`: Kafka (KRaft, no ZooKeeper), Postgres, Grafana, MLflow; Spark (single container) under the `spark` Compose profile, off by default — smoke check only this milestone. Docker Desktop capped at ~5 GB.
  - **All ports bound to `127.0.0.1`** (S1). **Pinned image tags**, no `latest` (S5). **Memory limit per container** sized from Step 0 (R1).
  - Passwords from `.env`; a startup check fails if any equals the `.env.example` placeholder; Grafana anonymous access off (S2).
- Kafka topics created by an init container: `telemetry.raw` (key `(cell_id, interval_start)`, 6 partitions by default) and `telemetry.dlq`, both with bounded `retention.ms` / `retention.bytes` (R2).
- `infra/db/001_init.sql`: `telemetry_raw` (PK `(replay_run_id, cell_id, interval_start)`), `topology_nodes`, `topology_edges`; DB users: `pipeline_writer` and `readonly` (S4 groundwork).
- **Done when:** `docker compose -f infra/docker-compose.yml up -d` starts all services healthy; `nc -z <laptop LAN IP> 5432` from another device fails (S1 check); `docker stats` within limits.

### Step 7 — Replay producer → Kafka (~1.5 h)
- `src/producer/replay.py`: reads processed parquet for a date range, joins topology + modeled KPIs, emits one message per (cell, interval) per `schemas/telemetry-message.md`, with `schema_version` and `replay_run_id`.
- **Deterministic order:** sort by `(interval_start, cell_id)` before sending; key = `cell_id|interval_start`; idempotent producer (`enable.idempotence=true`, `acks=all`). Since order is only guaranteed per partition, "same order" means: same per-partition sequence of keys.
- Speed control: `--speed` (data minutes per wall second); `--max-rate` cap.
- Malformed rows (schema validation fails) → `telemetry.dlq` with the reason.
- Resolve the two **TODO** fields in `schemas/telemetry-message.md` (`site_id` carried or joined; `schema_version`, `replay_run_id`) and update the schema doc.
- **Tests:** message builder against the schema; ordering function; a malformed row goes to the DLQ path (Kafka mocked).

### Step 8 — Postgres sink (~1 h)
- `src/producer/sink.py` (plain Python consumer; Spark takes over feature work in Milestone 2): consumes `telemetry.raw`, upserts into `telemetry_raw` in batches, commits Kafka offsets only after the DB commit (at-least-once + idempotent upsert ⇒ effectively once).
- **Integration test** (marked, runs against the local stack, not CI): replay one day → row count = subset size × 144.

### Step 9 — Grafana panel (~45 min)
- Provisioned (as code) Postgres data source using the `readonly` user, and one dashboard: live traffic for a selectable cell/site, time series of `traffic_internet` + `latency_ms`. Panel names and units taken from `kpis.yaml`.
- Footer text on the dashboard: "Traffic: real (from BigDataChallenge contest, ODbL). Topology and latency: synthetic/modeled."
- **Done when:** the panel updates while a replay runs.

### Step 10 — Reproducibility check (Definition of done) (~45 min)
- `make replay-check`: run the same replay twice with different `replay_run_id`s, then compare (a) the per-partition key sequences read back from Kafka and (b) the `telemetry_raw` rows (excluding `replay_run_id`) via an ordered hash. Must match exactly.
- Record the result in `BUILD_LOG.md`.

### Step 11 — Close-out (~30 min)
- Walk the guardrail checklist (P3, P4, P5, S1, S2, S3, S5, S6, R1, R2, R5); record each as pass/fail in `BUILD_LOG.md`.
- Update `README.md` quickstart (what now actually works), `docs/data-sources.md` (subset, time split confirmed), `DECISIONS.md` (subset size, Python version, partition count, KPI model), `CLAUDE.md` commands, and `HANDOFF.md`.
- ODbL attribution link in README (P3).

### Decisions (confirmed 2026-11-19)
| Item | Choice |
|---|---|
| Python version | 3.11 |
| Subset size | ~150 squares, central Milan block (8 GB RAM total) |
| Time split | as proposed in `docs/data-sources.md` (train 11-04→12-01, val 12-02→12-15, test 12-16→01-01) |
| `telemetry.raw` partitions | 6 |
| Raw → DB sink | Plain Python consumer in M1; Spark from M2 |
| Derived KPI model | Simple load-driven model in M1, refined in M2 if needed |

### Out of scope for this milestone
Fault injection, detectors, `faults.yaml`, alarms, correlation, classifier, console, agent (Milestones 2–5). Test-week files are not downloaded or processed until they are needed.

## Status

Started 2026-11-19. Steps 0–4 done: 2013-11-01 processed (4,842,625 rows → 20,736; reproducible); subset 144 squares; synthetic topology 1 core / 5 aggregation nodes / 48 sites / 144 cells. Next: Step 5 modeled KPIs.
