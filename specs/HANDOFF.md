# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `specs/milestones/milestone-1.md`. Steps 0–5 done.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). Synthetic topology: 1 core, 5 aggregation nodes (7–12 sites each), 48 sites, 144 cells (`data/processed/topology_*.csv`). `make check` passes (2026-11-22, working tree incl. uncommitted Step 6): 183 tests, 101 of them for the Claude Code guard hook; 95% coverage. CI green on GitHub; `main` is branch-protected.
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** processed days 2013-11-01 → 2013-11-10 (training week 1 = 11-04 → 11-10). Raw daily files deleted 2026-11-20 after re-verification (hashes stay in `data/manifest.json`); only `data/raw/milano-grid.geojson` remains. Modeled-KPI calibration in `data/processed/kpi_calibration.json` (load_ratio p99 0.80).

- **Step 6 written, not yet run, not committed:** `infra/docker-compose.yml`, `infra/preflight.sh`, `infra/db/` (roles + schema), `infra/kafka/create-topics.sh`, `src/common/db.py`, `src/producer/topology_db.py`, tests (incl. `tests/integration/test_db_stack.py`), Makefile targets (`up`, `down`, `ps`, `logs`, `spark-smoke`, `db-topology`, `test-integration`), `psycopg` dependency. Reviewed against Step 6 + guardrails S1/S2/S4/S5/R1/R2: no issues found. Its unit tests pass in `make check` (3 integration tests deselected until the stack runs).

## Last session
- 2026-11-22: Claude Code guard hook (`.claude/hooks/guard.py`) + skills (`milestone-step`, `new-package`) + `eval-failure-reviewer` agent; docs reorganized into numbered `specs/00…09` + `specs/milestones/`; this handoff moved to `specs/HANDOFF.md`; `docs/` and `schemas/` removed; `evals/cases/` added. Committed on its own, before Step 6.
- 2026-11-20 (later): reviewed the uncommitted Step 6 work; `make check` green; found Docker Compose broken on this Mac (see Blocked on); fixed stale `infra/README.md`.
- 2026-11-20: branch protection on; Step 3 (`make data`), Step 4 (synthetic topology, ID templates in the domain model), and Step 5 (time split in code, modeled KPIs calibrated on training week 1) built, tested, run on real data; raw daily files deleted; all pushed except this handoff commit.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Finish Step 6** (Docker Desktop was not running on 2026-11-22; start it first): `cp .env.example .env` and set real passwords (12+ chars) if not done; `make up`; `make db-topology`; `make test-integration`. Check the done-when items: all services healthy, `nc -z <LAN IP> 5433` from another device fails, `docker stats` within limits. Also confirm `up --wait` accepts the one-shot `preflight` / `kafka-init` containers exiting 0.
2. Then update `CLAUDE.md` (commands table: Compose no longer "planned"), `BUILD_LOG.md`, and commit Step 6.
3. Later (before M2 tuning): download the remaining training weeks 2–4 (2013-11-11 → 2013-12-01) and re-run `make data` + `make calibrate`.

## Blocked on
- Nothing. (Docker Desktop 4.20.1 had a broken Compose plugin; updated 2026-11-20 to Docker Desktop 4.99.0: Engine 29.9.3, Compose v5.7.0, ~5.3 GB memory.)
