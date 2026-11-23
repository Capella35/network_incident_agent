# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `specs/milestones/milestone-1.md`. Steps 0–6 done.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). Synthetic topology: 1 core, 5 aggregation nodes (7–12 sites each), 48 sites, 144 cells (`data/processed/topology_*.csv`), also loaded into Postgres (`make db-topology`).
- **Local stack works (Step 6, 2026-11-22):** `make up` starts Kafka, Postgres (`127.0.0.1:5433`), MLflow (`127.0.0.1:5001`), Grafana (`127.0.0.1:3000`); Spark only via `make spark-smoke`. ~1.3 GiB in total, 0 restarts, local-only (checked from a phone). MLflow server jobs are off (they OOM-looped). Run `make test-integration` about a minute after `make up`: "healthy" alone missed a crash loop once.
- `make check`: 183 tests, 95% coverage; `make test-integration`: 11 tests. CI green on GitHub; `main` is branch-protected (admin pushes bypass the required checks).
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** processed days 2013-11-01 → 2013-11-10 (training week 1 = 11-04 → 11-10). Raw daily files deleted 2026-11-20 after re-verification (hashes stay in `data/manifest.json`); only `data/raw/milano-grid.geojson` remains. Modeled-KPI calibration in `data/processed/kpi_calibration.json` (load_ratio p99 0.80).
- The repo's specs are in `specs/00…09`; Claude Code's guard hook can block heredoc text that mentions `.env` (`BUILD_LOG.md` 2026-11-22, open items).

## Last session
- 2026-11-22 (later): Step 6 run and verified: fixed the `.env` Postgres port (5433, user), fixed the MLflow OOM crash loop (server jobs off), added `tests/integration/test_stack_health.py`, topology loaded, Spark smoke OK, S1 checked from a phone. Recorded in the milestone file, `BUILD_LOG.md`, `DECISIONS.md`, `CLAUDE.md`, `infra/README.md`.
- 2026-11-22: Claude Code guard hook + skills + `eval-failure-reviewer` agent; docs reorganized into numbered `specs/`; handoff moved here; committed (`b52962a`) and pushed, CI green.
- 2026-11-20 (later): reviewed the uncommitted Step 6 work; found Docker Compose broken on this Mac; fixed stale `infra/README.md`.
- 2026-11-20: branch protection on; Steps 3–5 built, tested, run on real data; raw daily files deleted.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Step 7 — replay producer → Kafka** (`specs/milestones/milestone-1.md`): `src/producer/replay.py` reads processed parquet for a date range, joins topology + modeled KPIs, emits one `telemetry.raw` message per (cell, interval) per `specs/02-telemetry.md`; deterministic order and key; idempotent producer; `--speed` / `--max-rate`; malformed rows → `telemetry.dlq`. First resolve the TODO fields in the telemetry message (`site_id`, `schema_version`, `replay_run_id`). Needs a Kafka client dependency (`uv add`).
2. Then Step 8 (Postgres sink), Step 9 (Grafana panel), Step 10 (`make replay-check`), Step 11 (close-out).
3. Later (before M2 tuning): download the remaining training weeks 2–4 (2013-11-11 → 2013-12-01) and re-run `make data` + `make calibrate`.

## Blocked on
- Nothing. Start Docker Desktop before `make up` (it was off at the start of the 2026-11-22 session).
