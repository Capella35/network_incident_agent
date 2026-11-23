# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `specs/milestones/milestone-1.md`. Steps 0–7 done.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). Synthetic topology: 1 core, 5 aggregation nodes (7–12 sites each), 48 sites, 144 cells (`data/processed/topology_*.csv`), also loaded into Postgres (`make db-topology`).
- **Local stack works (Step 6):** `make up` starts Kafka, Postgres (`127.0.0.1:5433`), MLflow (`127.0.0.1:5001`), Grafana (`127.0.0.1:3000`); Spark only via `make spark-smoke`. ~1.3 GiB in total, local-only (checked from a phone). MLflow server jobs are off (they OOM-looped). Run `make test-integration` about a minute after `make up`: "healthy" alone missed a crash loop once.
- **Replay works (Step 7):** `make replay ARGS="--start 2013-11-04 --speed 0"` sends one day as 20,736 `telemetry.raw` messages (JSON, schema in `src/common/telemetry.py`, key `cell_id|interval_start`), exact and in order per partition; invalid rows go to `telemetry.dlq`. `telemetry.raw` currently holds that check run (`replay_run_id` `step7-check-2013-11-04`).
- `make check`: 222 tests, 94% coverage; `make test-integration`: 12 tests. CI green on GitHub; `main` is branch-protected (admin pushes bypass the required checks).
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** processed days 2013-11-01 → 2013-11-10 (training week 1 = 11-04 → 11-10). Raw daily files deleted 2026-11-20 after re-verification (hashes stay in `data/manifest.json`); only `data/raw/milano-grid.geojson` remains. Modeled-KPI calibration in `data/processed/kpi_calibration.json` (load_ratio p99 0.80).
- Claude Code's guard hook blocks heredoc text that mentions `.env` (false alarm; use the Edit/Write tools or `git commit -F`). Open item in `BUILD_LOG.md` 2026-11-22.

## Last session
- 2026-11-22 (latest): Step 7 built and verified: message schema, `make replay`, `confluent-kafka`, 39 unit + 1 integration test; one real day replayed and read back exactly. Not yet committed.
- 2026-11-22 (later): Step 6 run and verified (Postgres port in `.env`, MLflow OOM loop fixed, stability test, S1 from a phone); committed `ab42c9c` and pushed, CI green.
- 2026-11-22: Claude Code guard hook + skills + `eval-failure-reviewer` agent; docs reorganized into numbered `specs/`; handoff moved here; committed `b52962a` and pushed.
- 2026-11-20: branch protection on; Steps 3–5 built, tested, run on real data; raw daily files deleted.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Commit Step 7** (if not done): `Milestone 1 step 7: replay producer to Kafka`.
2. **Step 8 — Postgres sink** (`specs/milestones/milestone-1.md`): `src/producer/sink.py`, a plain Python consumer of `telemetry.raw` that validates with `common.telemetry`, upserts into `telemetry_raw` in batches as `pipeline_writer`, and commits Kafka offsets only after the DB commit. Integration test: replay one day → row count = 144 × 144. The 2013-11-04 check run already in `telemetry.raw` is its first input.
3. Then Step 9 (Grafana panel), Step 10 (`make replay-check`), Step 11 (close-out).
4. Later (before M2 tuning): download the remaining training weeks 2–4 (2013-11-11 → 2013-12-01) and re-run `make data` + `make calibrate`.

## Blocked on
- Nothing. Start Docker Desktop before `make up`.
