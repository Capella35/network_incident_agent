# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) done 2026-11-22** (started 2026-11-19). Definition of done met: `make replay-check` on training week 1 → MATCH. All 11 guardrails due pass (`BUILD_LOG.md` 2026-11-22, step 11). **Milestone 2 (fault injection + detection) is next and has no detailed plan yet** (`specs/milestones/milestone-2.md`).
- **What works** (README quickstart): `make data` / `calibrate` / `up` / `db-topology` / `replay` / `sink` / `replay-check` / `dashboards` / `check` / `test-integration`. Real Milan traffic for 144 cells → Kafka `telemetry.raw` → Postgres `telemetry_raw` effectively once → Grafana *Network Incident Agent / Network KPIs (milestone 1)*.
- **Local stack:** Kafka (`127.0.0.1:9092`), Postgres (`5433`), MLflow (`5001`), Grafana (`3000`, user `admin`); Spark via `make spark-smoke`. Limits ~2.9 GiB (+1.5 GiB Spark) of Docker's ~5.3 GiB; actual ~1.4 GiB. MLflow server jobs off and Grafana trimmed to the PostgreSQL plugin (both OOM-looped before). Run `make test-integration` about a minute after `make up`: it checks restarts and the cgroup `oom_kill` counter.
- `make check`: 267 tests, 89% coverage; `make test-integration`: 24 tests.
- **Not pushed:** Steps 7–10 committed locally (`4fce442`, `c851250`, `fbba24a`, `5be3599`); Step 11 not committed yet. CI last ran on Step 6.
- **Data on disk:** processed days 2013-11-01 → 2013-11-10 (training week 1 = 11-04 → 11-10). Raw daily files deleted after verification; only `data/raw/milano-grid.geojson` remains. Calibration from training week 1 (load_ratio p99 0.80). `telemetry_raw` holds ~370k check/demo rows.
- **Laptop: 8 GB RAM total.**
- Claude Code's guard hook blocks heredoc text that mentions `.env` (false alarm; use the Edit/Write tools or `git commit -F`).

## Last session
- 2026-11-22 (latest): Step 11 close-out: guardrail walk (11/11 pass), P3 attribution links from the publisher's terms (README + Grafana footer + spec), README status and working quickstart; Milestone 1 marked done. Not yet committed.
- 2026-11-22: Steps 6–10 built and verified (stack, replay, sink, Grafana, reproducibility check); Grafana and MLflow OOM loops found and fixed; 6 pushed, 7–10 committed locally.
- 2026-11-22: repo tooling (guard hook, skills, reviewer agent) and numbered `specs/` (pushed `b52962a`).
- 2026-11-20: Steps 3–5 (data processing, synthetic topology, modeled KPIs).
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2.

## Next step
1. **Commit Step 11** (if not done): `Milestone 1 step 11: close-out`. Then **push Steps 7–11** when the user says so, and confirm CI is green.
2. **Data for Milestone 2 (manual, user):** download training weeks 2–4 (2013-11-11 → 12-01) and the validation weeks (12-02 → 12-15) in the browser, a week at a time, into `data/raw/`; then `make data ARGS="--delete-raw"` per batch and `make calibrate` once training is complete. Not the test weeks (12-16 → 01-01) until the final evaluation (Rule 7).
3. **Write the Milestone 2 detailed plan** in `specs/milestones/milestone-2.md` (as M1 was): fault injector with a ground-truth log (no fault signatures, Rule 15), `faults.yaml` in the domain model, Spark feature job, detector candidates + Optuna under ≤ 1 false incident/day on validation weeks, MLflow promotion gate, detection report. Specs to read first: `specs/03-detection.md`, `specs/09-evals.md`, `specs/01-domain-model.md` §4. Confirm the plan with the user before building.
4. Carried open items: cleanup path for check/demo rows in `telemetry_raw`; pin GitHub Actions to SHAs and move off Node 20; Grafana memory headroom (~120 MiB); the guard hook's heredoc false alarm (user-only fix).

## Blocked on
- Nothing for planning. Milestone 2 tuning needs the validation weeks downloaded (manual, user).
