# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `specs/milestones/milestone-1.md`. Steps 0–8 done.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). Synthetic topology: 1 core, 5 aggregation nodes (7–12 sites each), 48 sites, 144 cells (`data/processed/topology_*.csv`), also loaded into Postgres (`make db-topology`).
- **Local stack works (Step 6):** `make up` starts Kafka, Postgres (`127.0.0.1:5433`), MLflow (`127.0.0.1:5001`), Grafana (`127.0.0.1:3000`); Spark only via `make spark-smoke`. ~1.3 GiB in total, local-only (checked from a phone). MLflow server jobs are off (they OOM-looped). Run `make test-integration` about a minute after `make up`: "healthy" alone missed a crash loop once.
- **Data flows end to end (Steps 7–8):** `make replay ARGS="--start 2013-11-04 --speed 0"` → `telemetry.raw` (20,736 messages per day, exact and in order per partition) → `make sink ARGS="--idle-exit 10"` → Postgres `telemetry_raw`, effectively once (redelivery overwrites). `telemetry_raw` now holds 2013-11-04 as run `step7-check-2013-11-04` (20,736 rows, totals match the parquet); sink lag 0; DLQ empty.
- `make check`: 243 tests, 92% coverage; `make test-integration`: 14 tests. CI green on GitHub; `main` is branch-protected (admin pushes bypass the required checks).
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** processed days 2013-11-01 → 2013-11-10 (training week 1 = 11-04 → 11-10). Raw daily files deleted 2026-11-20 after re-verification (hashes stay in `data/manifest.json`); only `data/raw/milano-grid.geojson` remains. Modeled-KPI calibration in `data/processed/kpi_calibration.json` (load_ratio p99 0.80).
- Claude Code's guard hook blocks heredoc text that mentions `.env` (false alarm; use the Edit/Write tools or `git commit -F`). Open item in `BUILD_LOG.md` 2026-11-22.

## Last session
- 2026-11-22 (latest): Step 8 built and verified: `make sink`, 20 unit + 2 integration tests; 2013-11-04 landed in Postgres exactly and effectively once. Not yet committed. Step 7 committed as `4fce442` (not pushed).
- 2026-11-22: Step 7 (replay producer → Kafka) built and verified; Step 6 committed `ab42c9c` and pushed, CI green; repo tooling + numbered `specs/` committed `b52962a`.
- 2026-11-20: branch protection on; Steps 3–5 built, tested, run on real data; raw daily files deleted.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Commit Step 8** (if not done): `Milestone 1 step 8: Postgres sink`. Steps 7 and 8 are not pushed yet.
2. **Step 9 — Grafana panel** (`specs/milestones/milestone-1.md`): provision (as code) a Postgres data source using the `readonly` role and one dashboard: live traffic for a selectable cell/site, time series of `traffic_internet` + `latency_ms`, names and units from `kpis.yaml`, footer "Traffic: real (from BigDataChallenge contest, ODbL). Topology and latency: synthetic/modeled." Done when the panel updates while a replay runs (run `make replay` with `--speed 10` and `make sink` side by side).
3. Then Step 10 (`make replay-check`), Step 11 (close-out).
4. Later (before M2 tuning): download the remaining training weeks 2–4 (2013-11-11 → 2013-12-01) and re-run `make data` + `make calibrate`.

## Blocked on
- Nothing. Start Docker Desktop before `make up`.
