# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) in progress, started 2026-11-19.** Plan: `docs/milestones/milestone-1.md`. Steps 1 (tooling) and 2 (domain model: entities + KPIs) done; Step 0 done (resources checked; grid + 2013-11-01 files present in `data/raw/`).
- Feedback loop works: `make check` (ruff, mypy strict, pytest) passes — 17 tests, 93% coverage. Pre-commit runs gitleaks + ruff. CI workflow written but not yet run on GitHub.
- **Laptop: 8 GB RAM total.** Subset cut to ~150 squares; Spark behind a Compose profile; containers memory-capped (`DECISIONS.md` 2026-11-19).
- **Data on disk:** `data/raw/milano-grid.geojson` (MD5 matches published) and `data/raw/sms-call-internet-mi-2013-11-01.txt` (323 MB). `data/manifest.json` doesn't exist yet (Step 3 writes it). Training/validation weeks are downloaded in weekly batches when Step 3 is ready.
- Repo is public on GitHub by decision (code and docs only). `main` has no branch protection yet.

## Last session
- 2026-11-19: audit (maturity 2: strong design docs, no build/test/CI). Milestone 1 plan written and started. Steps 1–2 built and passing. Decisions recorded (start date, public repo, 8 GB adjustments, tooling). Not yet committed.
- 2026-11-14: repo renamed, git + gitleaks hook, first push; folder moved out of iCloud.

## Next step
1. Commit Steps 0–2 and push; confirm CI goes green on GitHub.
2. Recommended: turn on branch protection for `main` (block force-push, require CI) and GitHub secret scanning + push protection.
3. **Step 3:** verify + process script (`make data`): MD5 check against the Dataverse value, country-code sum, gap fill, parquet out, `data/manifest.json`. Run it end-to-end on 2013-11-01.
4. **Manual, before Step 6:** start Docker Desktop and set its memory limit to ~5 GB (Settings → Resources).

## Blocked on
- Nothing.
