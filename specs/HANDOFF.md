# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) done 2026-11-22** (started 2026-11-19). Definition of done met: `make replay-check` on training week 1 → MATCH. All 11 guardrails due pass (`BUILD_LOG.md` 2026-11-22, step 11). **Milestone 2 (fault injection + detection):** detailed plan written 2026-11-23 in `specs/milestones/milestone-2.md` (11 steps, ~14–20 h); two decisions confirmed by the user (provisional incident grouping; scikit-learn MLP autoencoder); **the plan as a whole awaits the user's approval** and is not committed.
- **What works** (README quickstart): `make data` / `calibrate` / `up` / `db-topology` / `replay` / `sink` / `replay-check` / `dashboards` / `check` / `test-integration`. Real Milan traffic for 144 cells → Kafka `telemetry.raw` → Postgres `telemetry_raw` effectively once → Grafana *Network Incident Agent / Network KPIs (milestone 1)*.
- **Local stack:** Kafka (`127.0.0.1:9092`), Postgres (`5433`), MLflow (`5001`), Grafana (`3000`, user `admin`); Spark via `make spark-smoke`. Limits ~2.9 GiB (+1.5 GiB Spark) of Docker's ~5.3 GiB; actual ~1.4 GiB. MLflow server jobs off and Grafana trimmed to the PostgreSQL plugin (both OOM-looped before). Run `make test-integration` about a minute after `make up`: it checks restarts and the cgroup `oom_kill` counter.
- `make check`: 267 tests, 88.6% coverage (floor 85%); `make test-integration`: 24 tests (~12 s).
- **Code-quality rules (2026-11-23):** `CLAUDE.md` "Code quality"; ruff C90/ERA/PLR; coverage floor; the `milestone-step` skill reviews (`/simplify`, `/code-review`) at the end of Build, before verification. Reuse `common/kafka.py`, `tests/factories.py`, `tests/fakes.py`, `tests/integration/conftest.py`.
- **GitHub:** pushed through `77219ca` (M2 data prep), CI green.
- **Data on disk (2026-11-22):** processed days 2013-11-01 → 2013-12-15 (all 4 training weeks + both validation weeks; 45 days, 31 MB), all MD5-verified (manifest: 46 entries). Raw files re-verified and deleted 2026-11-22 (user's OK); only `data/raw/milano-grid.geojson` remains; disk ~46 GiB free. Calibration now from all 28 training days (load_ratio p99 0.800). Test weeks (12-16 → 01-01) not downloaded (Rule 7). `telemetry_raw` holds ~370k check/demo rows.
- **Laptop: 8 GB RAM total.**
- Claude Code's guard hook blocks heredoc text that mentions `.env` (false alarm; use the Edit/Write tools or `git commit -F`).

## Last session
- 2026-11-23 (latest): code-quality guardrails + duplication cleanup (reviewed with `/simplify`, 4 reviewers); Milestone 2 plan written. Neither committed yet.
- 2026-11-22: Milestone 2 data prep: 35 days (training weeks 2–4 + validation weeks) downloaded by the user, MD5-verified, processed (20,736 rows/day, 0 gaps), calibration redone on all 28 training days (load_ratio p99 0.800), raw files deleted with the user's OK.
- 2026-11-22: Milestone 1 done and pushed (`13037d7`, CI green): Steps 6–11 (stack, replay, sink, Grafana, reproducibility check, close-out); Grafana and MLflow OOM loops found and fixed.
- 2026-11-22: repo tooling (guard hook, skills, reviewer agent) and numbered `specs/` (pushed `b52962a`).
- 2026-11-20: Steps 3–5 (data processing, synthetic topology, modeled KPIs).
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2.

## Next step
1. **Commit** the code-quality change (if not done): `Code-quality guardrails and duplication cleanup`. The Milestone 2 plan file is a separate commit.
2. **Get the user's approval of the Milestone 2 plan** (`specs/milestones/milestone-2.md`), then commit it and start Step 1 (fault types in the domain model) with the `milestone-step` skill.
3. Carried open items: optional rule tightening (ruff ARG/T20, coverage excluding live-stack entry points with a higher floor, trimming `CLAUDE.md`, max-args 5); cleanup path for check/demo rows in `telemetry_raw`; pin GitHub Actions to SHAs and move off Node 20; Grafana memory headroom (~120 MiB); the guard hook's heredoc false alarm (user-only fix).

## Blocked on
- Nothing. All data Milestone 2 needs (training + validation weeks) is processed.
