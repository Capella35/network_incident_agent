# Handoff

*Read this first at the start of every session; update it last.*

## Where things stand
- **Milestone 1 (Data + replay) done 2026-11-22** (started 2026-11-19). Definition of done met: `make replay-check` on training week 1 → MATCH. All 11 guardrails due pass (`BUILD_LOG.md` 2026-11-22, step 11). **Milestone 2 (fault injection + detection):** detailed plan written 2026-11-23 in `specs/milestones/milestone-2.md` (11 steps, ~14–20 h); two decisions confirmed by the user (provisional incident grouping; scikit-learn MLP autoencoder); **plan confirmed by the user 2026-11-23**; Steps 1 (fault types) and 2 (fault injector) done and on `main`; Step 3 (features) merged (PR #2); **Step 4 (detector candidates + option b) done 2026-11-24** on branch `m2/step-4-detectors`, not committed yet.
- **What works** (README quickstart): `make data` / `calibrate` / `up` / `db-topology` / `replay` / `sink` / `replay-check` / `dashboards` / `check` / `test-integration` / `inject` / `baseline` / `features` / `candidates`. Real Milan traffic for 144 cells → Kafka `telemetry.raw` → Postgres `telemetry_raw` effectively once → Grafana *Network Incident Agent / Network KPIs (milestone 1)*.
- **Local stack:** Kafka (`127.0.0.1:9092`), Postgres (`5433`), MLflow (`5001`), Grafana (`3000`, user `admin`); Spark via `make spark-smoke`. Limits ~2.9 GiB (+1.5 GiB Spark) of Docker's ~5.3 GiB; actual ~1.4 GiB. MLflow server jobs off and Grafana trimmed to the PostgreSQL plugin (both OOM-looped before). Run `make test-integration` about a minute after `make up`: it checks restarts and the cgroup `oom_kill` counter.
- `make check`: 389 tests, 91.4% coverage (floor 85%); `make test-integration`: 25 tests (~12 s). Schema changes after `001_init.sql`: `make db-migrate`.
- **Code-quality rules (2026-11-23):** `CLAUDE.md` "Code quality"; ruff C90/ERA/PLR; coverage floor; the `milestone-step` skill reviews (`/simplify`, `/code-review`) at the end of Build, before verification. Reuse `common/kafka.py`, `tests/factories.py`, `tests/fakes.py`, `tests/integration/conftest.py`.
- **GitHub:** `main` at `fd7941e` (Step 3, PR #2), CI green. Every change now goes on a branch and reaches `main` by pull request; the user merges (`DECISIONS.md` 2026-11-23). Branch protection still allows the owner to bypass CI (user setting).
- **Data on disk (2026-11-22):** processed days 2013-11-01 → 2013-12-15 (all 4 training weeks + both validation weeks; 45 days, 31 MB), all MD5-verified (manifest: 46 entries). Raw files re-verified and deleted 2026-11-22 (user's OK); only `data/raw/milano-grid.geojson` remains; disk ~46 GiB free. Calibration now from all 28 training days (load_ratio p99 0.800). Test weeks (12-16 → 01-01) not downloaded (Rule 7). `telemetry_raw` holds ~370k check/demo rows.
- **Laptop: 8 GB RAM total.**
- Claude Code's guard hook blocks heredoc text that mentions `.env` (false alarm; use the Edit/Write tools or `git commit -F`).

## Last session
- 2026-11-24 (latest): M2 Step 4: option (b) for the December internet shift (real KPIs' z level-adjusted; week-2 internet false highs 12.2% → 1.6%); `src/models/` with z-score, Isolation Forest, autoencoder; `make candidates` (881 / 895 / 926 MiB of 1,536, MLflow pyfunc models); reviewed (`/simplify` + `/code-review`, 4 important findings fixed). Class-imbalance open decision recorded.
- 2026-11-23: M2 Step 3: `src/streaming/` (seasonal baseline `make baseline`, causal features, `make features`); validation features in 0.3 s / ≤ 415 MiB; stale-baseline check; shared helpers moved to `common.telemetry` / `data_prep.processed_days`; reviewed (`/simplify` + `/code-review`, 1 important finding fixed). Found a real December internet level shift (18/144 cells). Earlier the same day: architecture discussion → 5 open decisions; step-2 CI failure fixed (`234b411`); branch + PR workflow adopted (PR #1).
- 2026-11-23: M2 Step 2: seeded fault injector + ground truth with fingerprint (`make inject`), `replay … --inject`, shared `replay.telemetry_for`, `topology.Index`; verified on all 14 validation days (57 faults, 0 mismatches); reviewed (`/simplify` + `/code-review`, 7 findings fixed, incl. a Rule 6 leak).
- 2026-11-23: M2 Step 1: fault types + separate signatures, Rule 6/15 import and file-name boundaries, `change_events` + `make db-migrate`; reviewed with `/simplify` and `/code-review` (9 findings fixed, incl. Severity sorting alphabetically).
- 2026-11-23: code-quality guardrails + duplication cleanup pushed (`2e1d8a2`, CI green); Milestone 2 plan written and confirmed by the user.
- 2026-11-22: Milestone 2 data prep: 35 days (training weeks 2–4 + validation weeks) downloaded by the user, MD5-verified, processed (20,736 rows/day, 0 gaps), calibration redone on all 28 training days (load_ratio p99 0.800), raw files deleted with the user's OK.
- 2026-11-22: Milestone 1 done and pushed (`13037d7`, CI green): Steps 6–11 (stack, replay, sink, Grafana, reproducibility check, close-out); Grafana and MLflow OOM loops found and fixed.
- 2026-11-22: repo tooling (guard hook, skills, reviewer agent) and numbered `specs/` (pushed `b52962a`).
- 2026-11-20: Steps 3–5 (data processing, synthetic topology, modeled KPIs).
- 2026-11-19: audit; Milestone 1 plan; Steps 0–2.

## Next step
1. **Commit Step 4** on `m2/step-4-detectors` (`Milestone 2 step 4: detector candidates`), push, open a pull request; the user merges.
2. **Milestone 2, Step 5 — evaluation** (`specs/milestones/milestone-2.md`, `specs/09-evals.md`), with the `milestone-step` skill: `src/evaluation/` (new package; the only reader of the ground truth, Rule 6): matching (overlap ± 1 interval), detection rate per fault type, median delay, false incidents/day with the provisional grouping (± 1 interval, same site or aggregation node); report as Markdown + MLflow artifact; hand-checked 1-day fixture. Score with `detectors.alarms` on `offline.features_for(validation days, inject=True)`.
3. Carried open items: disjoint test-split fault parameters (M6); autoencoder `max_iter` (Step 6); per-trial feature config and feature caching (Step 6); threshold + alarm settings inside the logged model (Steps 6–7); Spark level-factor state and 7-day history in the parity replay (Steps 8–9); busy-peak false alarms (Steps 5–6); cleanup path for check/demo rows in `telemetry_raw`; pin GitHub Actions to SHAs and move off Node 20; Grafana memory headroom (~120 MiB); the guard hook's false alarm on text that mentions the env file (user-only fix); optional rule tightening.

## Blocked on
- Nothing. All data Milestone 2 needs (training + validation weeks) is processed.
