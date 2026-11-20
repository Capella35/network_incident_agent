# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `docs/milestones/milestone-1.md`. Steps 0–3 done; subset part of Step 4 done.
- `make data` works: verifies each file against the committed published checksums, writes `data/processed/traffic/<date>.parquet` and `data/manifest.json`. 2013-11-01 processed (20,736 rows, 0 gaps), reproducible byte-for-byte.
- Subset: 144 squares (12 × 12 around square 5061, the Duomo). `make check` passes: 37 tests, 95% coverage. CI green on GitHub; `main` is branch-protected.
- **Laptop: 8 GB RAM total.** Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** `data/raw/` holds the grid and 2013-11-01 only. Training/validation weeks (2013-11-04 → 2013-12-15) still to download, in weekly batches.

## Last session
- 2026-11-20: branch protection on; Step 3 (`make data`) built, tested, and run on 2013-11-01; subset fixed; docs and decisions updated.
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2 (tooling, domain model); first CI run green.

## Next step
1. **Step 4:** topology generator (cells → sites → aggregation nodes → `CORE-1`, seeded, labeled synthetic) from `data/processed/subset.json`.
2. **Step 5:** modeled KPIs (`load_ratio`, `latency_ms`, `drop_rate`).
3. **Manual, any time before Step 5 needs real weeks:** download training week 1 (2013-11-04 → 2013-11-10) into `data/raw/`, then `make data ARGS="--delete-raw"`.
4. **Manual, before Step 6:** start Docker Desktop with ~5 GB memory.

## Blocked on
- Nothing.
