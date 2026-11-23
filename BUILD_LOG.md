# Build Log

One entry per working session, newest at the top. Copy the template below.

---

<!--
## YYYY-MM-DD — Milestone N, step N: <short title>

**What was built:**
-

**Tests run and results:**
-

**Decisions made:** (also add to DECISIONS.md)
-

**Open items:**
-
-->

## 2026-11-22 — Milestone 1, step 7: replay producer → Kafka

**What was built:**
- `src/common/telemetry.py`: the `telemetry.raw` message (pydantic, JSON, `schema_version` 1), key `cell_id|interval_start`, topic names; validation (finite non-negative traffic, domain ID patterns, UTC 10-minute grid, gap ⇔ null modeled values); `check_against_domain()` ties field names to `kpis.yaml` and the Milan fields.
- `src/producer/replay.py` + `make replay`: processed days + SYNTHETIC topology + MODELED KPIs → messages sorted by `(interval_start, cell_id)`; idempotent producer (`acks=all`, ≤ 5 in flight, murmur2); `--speed` / `--max-rate` pacing; invalid rows and unknown squares → `telemetry.dlq` with the reason; test-week days refused unless `--final-eval`.
- `topology.load_cell_sites()`; new dependency `confluent-kafka` 2.16.2.
- Spec: the three TODO message fields settled in `specs/02-telemetry.md`; Step 7 given a "Done when".

**Tests run and results:**
- Unit: 19 schema tests, 20 replay tests (order, site join, DLQ routes, pacing with a fake clock, buffer-full retry, delivery failures, determinism across run IDs, test-week refusal). `make check`: 222 passed, 94% coverage.
- Integration (`tests/integration/test_replay_kafka.py`, throwaway topics): 120 messages + 1 reject through the real broker; every key once, each partition in replay order, reject in the DLQ. `make test-integration`: 12 passed.
- Real run: `make replay ARGS="--start 2013-11-04 --speed 0 --run-id step7-check-2013-11-04"`: 20,736 sent in ~2 s, 0 dead-lettered, 0 failed; partitions 3,446 / 3,460 / 3,434 / 3,411 / 3,484 / 3,501.
- Read back and compared with keys recomputed from the parquet + topology (independent of the replay code): 20,736 read, 0 duplicates, 0 missing, 0 unexpected, every partition in source order; `telemetry.dlq` empty.

**Decisions made:** (also in DECISIONS.md)
- `confluent-kafka`; JSON + pydantic, not Avro; `site_id` carried; key format and partitioner; replay refuses test weeks without `--final-eval`.

**Open items:**
- `telemetry.raw` now holds the 2013-11-04 check run (`replay_run_id` `step7-check-2013-11-04`; kept 7 days by retention). The Step 8 sink will consume it.
- Pacing at the default `--speed 10` (one day ≈ 2.4 min) is unit-tested with a fake clock, not timed on the real broker.

## 2026-11-22 — Milestone 1, step 6: Docker Compose stack (+ repo tooling)

**What was built:**
- Repo tooling (commit `b52962a`, pushed, CI green): Claude Code guard hook `.claude/hooks/guard.py` with 101 tests; skills `milestone-step`, `new-package`; agent `eval-failure-reviewer`; docs reorganized into numbered `specs/00…09` + `specs/milestones/`; handoff moved to `specs/HANDOFF.md`; `evals/cases/` added.
- Step 6 run for the first time: `infra/docker-compose.yml` (Kafka 4.3.2 KRaft, Postgres 18.7, MLflow 3.17.2, Grafana 13.2.6; Spark 4.2.1 under the `spark` profile), preflight password check, DB roles + schema, Kafka topic init, `src/common/db.py`, `src/producer/topology_db.py`, make targets `up` / `down` / `ps` / `logs` / `db-topology` / `test-integration` / `spark-smoke`.
- New `tests/integration/test_stack_health.py`: fails on any restart or OOM kill, and on a missing memory limit.

**Tests run and results:**
- First `make up` failed: the user's `.env` (copied from an older `.env.example`) still had `POSTGRES_PORT=5432`, held by a local Postgres (PID 1373). User set 5433; then all services healthy; `preflight` and `kafka-init` exit 0 and `up --wait` accepts them.
- **MLflow crash loop found:** 8 restarts in ~5 min, exit 137 (cgroup OOM) every ~50 s, pinned at 767.9 / 768 MiB and ~350% CPU, while its health check passed. Cause: a job runner + 7 huey worker processes started by default. Fixed with `MLFLOW_SERVER_ENABLE_JOB_EXECUTION=false`: steady 408 MiB, idle CPU, 0 restarts over 3 min.
- `make db-topology`: 198 nodes (1 core + 5 agg + 48 sites + 144 cells), 197 edges, SYNTHETIC.
- `make test-integration`: 11 passed (DB roles: writer can't delete telemetry or create tables, readonly can't write; stack stability + memory limits).
- `docker stats`: postgres 91 / 512 MiB, mlflow 417 / 768, grafana 229 / 384, kafka 578 / 1024 (~1.3 GiB total of Docker's 5.3 GiB).
- Topics (R2): `telemetry.raw` 6 partitions, retention 7 d / 256 MB; `telemetry.dlq` 1 partition, 7 d / 64 MB.
- S1: 5433 / 3000 / 5001 / 9092 closed on the LAN address `192.168.254.159` from the Mac (open on 127.0.0.1); Grafana unreachable from a phone on the same Wi-Fi. S2: preflight passed; Grafana returns 401 to anonymous requests. S5: all tags pinned.
- `make spark-smoke`: Pi ≈ 3.133, exit 0.
- `make check`: 183 passed, 95% coverage.

**Decisions made:** (also in DECISIONS.md)
- Postgres host port 5433, MLflow 5001; MLflow server jobs off.

**Open items:**
- The guard hook flags heredoc text that mentions `.env` (it reads heredoc lines as commands), including commit messages. Workaround: write the text with the Edit/Write tools or `git commit -F <file>`. A fix needs the user, since Claude can't edit `.claude/hooks/`.
- `make up` reports "healthy" even when a service is crash-looping; run `make test-integration` about a minute after `make up` to catch that.
- `.env` drifted from `.env.example` once already; a preflight check for missing keys could catch that.

## 2026-11-20 — Milestone 1, step 5 (calibration): training week 1

**What was built:**
- Downloaded (manual): 2013-11-02 → 2013-11-10. `make data`: all 9 files passed size + MD5 against the published values; 4.7–5.9 M rows each → 20,736 rows, 0 gaps; 19 s total, ~630 MB peak RAM. Raw files kept for now.
- `make calibrate` on the 7 training days (2013-11-04 → 2013-11-10).

**Tests run and results (modeled values, all 7 training days, 145,152 rows):**
- Capacity per cell (scaled units): min 177, median 1,533, max 6,045.
- `load_ratio`: p1 0.05, p50 0.33, **p99 0.80** (as designed), max 1.87; 0.22% of rows > 0.9.
- `latency_ms`: p1 20.2, p50 29.9, p99 102, max 1,122.
- `drop_rate`: p50 0.0020, p99 0.0035, max 0.252.
- Daily pattern follows real traffic: median load 0.13 at 03h, 0.57 at 12h (Europe/Rome).

**Open items:**
- **For Milestone 2:** about 0.22% of real rows already exceed load_ratio 0.9 (busy peaks). The `congestion` signature (load > 0.9 with latency/drop up) will fire on these, so they count against the false-alarm budget unless the detector or signature accounts for them.
- `drop_rate` can exceed the configured `max` (0.2) by the noise factor (seen: 0.252). Still a valid ratio; note if a strict cap is wanted.
- 2013-11-02 / 11-03 are in no split (before training); processed but never used for fitting.

## 2026-11-20 — Milestone 1, step 5 (code): modeled KPIs

**What was built:**
- `src/common/splits.yaml` + `splits.py`: the train/validation/test days in one place; `require()` guard.
- `src/producer/kpi_model.py` + `kpi_model.yaml`: real KPIs from the `kpis.yaml` Milan-field mapping; per-cell capacity calibrated on training days only; `load_ratio`, `latency_ms`, `drop_rate` with seeded per-(cell, interval) noise; null modeled values on gap rows. CLI writes `data/processed/kpi_calibration.json` (`modeled: true`, params, train days, input hashes). `make calibrate`.
- `topology.load_cells()`; telemetry schema updated (`load_ratio` added; modeled KPIs nullable on gaps).

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 71 tests passed; coverage 96%.
- `make calibrate` on the real repo refuses to run: only 2013-11-01 is processed, and it isn't a training day (guard works).

**Decisions made:** (also in DECISIONS.md)
- Splits in code; KPI model curves and parameters.

**Open items:**
- Calibrate once training week 1 is downloaded and processed; check the real distributions (expect `load_ratio` p99 ≈ 0.8).

## 2026-11-20 — Milestone 1, step 4: synthetic topology

**What was built:**
- `entities.yaml`: `id_template` per entity; loader checks each template produces IDs that match the pattern; `EntityType.make_id()` and `Domain.parent_relation()`.
- `src/producer/topology.py` + `topology_config.yaml`: 1 square = 1 Cell, 3-square strips = Sites, seeded k-means (pure Python, deterministic tie-breaking) = AggregationNodes, one Core. Writes `topology_nodes.csv`, `topology_edges.csv`, `topology_meta.json` (`synthetic: true`, config, input and output hashes).
- `make data` now builds the topology after processing; `make topology` rebuilds it alone.

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 53 tests passed; coverage 96%.
- Real grid: 1 Core, 5 AggregationNodes (7, 8, 9, 12, 12 sites), 48 Sites, 144 Cells, 197 edges; rebuild is byte-identical.
- CI for the step 3 push (`e5be730`): both jobs green.

**Decisions made:** (also in DECISIONS.md)
- Topology layout and parameters; ID templates in the domain model.

**Open items:**
- Topology tables get loaded into Postgres in Step 6.

## 2026-11-20 — Milestone 1, step 3: verify + process (`make data`)

**What was built:**
- Branch protection on `main` (required CI checks, no force-push/deletion, linear history; admin bypass).
- `src/producer/milan_files.json`: published file IDs, sizes, MD5s for all 63 Dataverse files (from the public metadata API).
- `src/producer/integrity.py` (MD5 + SHA-256 in one pass, verify size + MD5), `grid.py` (centroids, 12 × 12 subset around the Duomo), `milan.py` (Polars streaming: sum over country codes, empty = 0, gap fill, UTC ms), `manifest.py` (atomic writes, stable keys), `data_prep.py` (CLI; skips up-to-date days; `--force`, `--delete-raw`, `--date`).
- `make data` target. Dependency added: polars 2.1.0.

**Tests run and results:**
- `make check`: ruff, mypy strict clean; 37 tests passed; coverage 95%.
- Real run on 2013-11-01: MD5 matches published (`658c49f8…`); 4,842,625 rows in → 20,736 out (144 squares × 144 intervals), 0 gaps, 0 out-of-day rows; 2.4 s, ~400 MB peak RAM.
- Cross-check: awk sums for square 5061 at the first interval match the parquet values to 6 decimals.
- Reproducibility: forced reprocess gives a byte-identical parquet (SHA-256 `a947728e…`); a plain rerun leaves `data/manifest.json` byte-identical.

**Decisions made:** (also in DECISIONS.md)
- Subset = 144 squares around square 5061. Published checksums committed. Polars + parquet. All-empty source row ≠ data gap. Branch protection settings.

**Open items:**
- Polars 2 gotchas hit: a list passed next to other expressions in `with_columns` is read as a literal (unpack with `*`); `pl.from_epoch(..., "ms")` returns µs (cast explicitly).
- CI warns that `actions/checkout@v4` and `setup-uv@v5` run on deprecated Node 20; bump later.

## 2026-11-19 — Milestone 1, steps 0–2: resource check, tooling, domain model

**What was built:**
- Step 0 (partial): laptop resource check — 8 GB RAM total (~47% free at check), Intel i5-10500, 12 threads, 54 GB disk free; Docker 24.0.2 installed (daemon not running at check). Milan files present in `data/raw/` (checked later the same session: grid MD5 `e64ed3858e97347c14cd049efde8c7d3` matches the published value; 2013-11-01 MD5 `658c49f8e3c7910bd3bb2bafbf62cdce`, 322,874,887 bytes). An earlier note that `data/raw/` was empty came from a truncated directory listing and was wrong.
- Step 1: `pyproject.toml` (Python 3.11, uv, pinned via `uv.lock`), `Makefile` (`setup`, `lint`, `format`, `typecheck`, `test`, `check`, `clean`), `.pre-commit-config.yaml` (gitleaks v8.30.1, ruff, large-file/private-key checks), `.github/workflows/ci.yml` (gitleaks + `make check`), `.claude/settings.json` (deny `.env` reads/edits and edits under `data/`).
- Step 2: `src/common/domain/entities.yaml`, `kpis.yaml`, `loader.py` (pydantic, frozen, cached `load_domain()`).

**Tests run and results:**
- `make check`: ruff clean, mypy strict clean, 17 tests passed, coverage 93%.
- `pre-commit run --all-files`: all hooks passed.
- gitleaks flags a fake Groq-style key (tested in a scratch dir, not the repo).

**Decisions made:** (also in DECISIONS.md)
- Milestone 1 started; plan defaults confirmed. Repo stays public. Subset ~150 squares and Spark behind a Compose profile (8 GB RAM). Tooling choices.

**Open items:**
- Set Docker Desktop memory to ~5 GB.
- Branch protection on `main` (GitHub settings).
- CI not yet seen running on GitHub (needs a push).
