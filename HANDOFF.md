# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `docs/milestones/milestone-1.md`. Steps 0–4 done; Step 5 code done, calibration waiting for data.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). Synthetic topology: 1 core, 5 aggregation nodes (7–12 sites each), 48 sites, 144 cells (`data/processed/topology_*.csv`). `make check` passes: 71 tests, 96% coverage. CI green on GitHub; `main` is branch-protected.
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** `data/raw/` holds the grid and 2013-11-01 only. Training/validation weeks (2013-11-04 → 2013-12-15) still to download, in weekly batches.

## Last session
- 2026-11-20: branch protection on; Step 3 (`make data`) and Step 4 (synthetic topology, ID templates in the domain model) built, tested, run on real data; docs and decisions updated.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Manual (in progress 2026-11-20):** download training week 1 (2013-11-04 → 2013-11-10, 7 files, 2.5 GB) from https://doi.org/10.7910/DVN/EGZHFV into `data/raw/`, then `make data ARGS="--delete-raw"`.
2. `make calibrate`, then check the logged distributions (`load_ratio` p99 should be near 0.8) and finish Step 5.
3. **Step 6:** Docker Compose stack.
4. **Manual, before Step 6:** start Docker Desktop with ~5 GB memory.

## Blocked on
- Nothing.
