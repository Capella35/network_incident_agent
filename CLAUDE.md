# CLAUDE.md — context for any LLM working in this repo

## Mission
Network Incident Agent watches a cellular network's telemetry, detects degrading cells, groups the alarm storm into **one incident**, ranks the most likely **root node** in the topology, labels the incident (fault category, severity, owning team), and lets an ops engineer investigate it with an agent that answers using **cited evidence**. A human approves every action.
Purpose: portfolio project for a Forward Deployed Engineer role in telecom (target: a US telecom carrier), framed as a customer engagement. Source: `PROJECT.md`.

## Location
The project lives in `~/Projects/network_incident_agent` (moved 2026-11-14). Keep it out of `~/Desktop` and `~/Documents`: iCloud sync there hides files inside `.venv` and breaks Docker volumes. Start Claude Code from this folder.

## Architecture in brief
Milan traffic replay + fault injector → **Kafka** (`telemetry.raw`) → **Spark Structured Streaming** (windowed features + promoted detector) → Kafka `alarms` → **correlator** (topology rules + graph ranking) → incident + root node → **XGBoost** classifier → **PostgreSQL** → **Grafana** (KPIs, health), **Streamlit** console (human approve/correct/close), **ops agent** (Groq API, read-only tools). **MLflow** tracks every model run, holds the registry, and runs the promotion gate. Everything starts with `docker compose up`. Full diagram and component table: `specs/00-overview.md`.

## Status and commands
Milestone 1 done (2026-11-19 → 2026-11-22); Milestone 2 next, not planned yet. Python 3.11, managed with `uv`; exact versions pinned in `uv.lock` (never edit it by hand; use `uv add`).

| Task | Command |
|---|---|
| First-time setup (venv + git hooks) | `make setup` |
| Lint + format check | `make lint` (`make format` to fix) |
| Type check (mypy strict) | `make typecheck` |
| Unit tests + coverage | `make test` |
| Everything CI runs | `make check` — run before every commit |
| Verify + process Milan files in `data/raw/`, then build the topology | `make data` (`ARGS="--date 2013-11-01 --delete-raw"`, `--force`) |
| Rebuild only the synthetic topology | `make topology` |
| Fit modeled cell capacity on training days | `make calibrate` |
| Write a split's SYNTHETIC fault schedule (ground truth, read only by `src/evaluation/`) | `make inject ARGS="--split validation"` |
| Fit the seasonal KPI baseline (cell, weekday, hour) on training weeks; refit after `make calibrate` | `make baseline` |
| Compute a split's detection features and report time + memory | `make features ARGS="--split validation --inject"` |
| Train each detector candidate on training weeks, score a validation day, log to MLflow (stack running) | `make candidates` (`ARGS="--score-day 2013-12-02"`) |
| Start / stop the local stack (Kafka, Postgres, MLflow, Grafana); needs `.env` | `make up` / `make down` (`make ps`, `make logs`) |
| Load the synthetic topology into Postgres | `make db-topology` |
| Apply schema changes after `001_init.sql` to the running database (idempotent) | `make db-migrate` |
| Integration tests against the running stack (not in CI) | `make test-integration` |
| One-off Spark smoke job (Spark profile) | `make spark-smoke` |
| Replay processed days into Kafka `telemetry.raw` (stack running); `--inject` adds the days' split's SYNTHETIC faults and logs their change events | `make replay ARGS="--start 2013-11-04 [--end …] [--speed 0] [--run-id …] [--inject]"` |
| Kafka `telemetry.raw` → Postgres `telemetry_raw` (runs until Ctrl-C) | `make sink` (`ARGS="--idle-exit 10"` to stop when drained) |
| Regenerate Grafana dashboards from `kpis.yaml` (never edit the JSON by hand) | `make dashboards` |
| Milestone 1 Definition of done: replay twice, compare Kafka key order + Postgres rows | `make replay-check ARGS="--start 2013-11-04 --end 2013-11-10"` |

Host ports, all on `127.0.0.1`: Postgres 5433 (5432 is often taken by a local Postgres), MLflow 5001, Grafana 3000 (user `admin`), Kafka 9092.

Planned, not built yet: Milestone 2 Steps 5–11 (evaluation, `make train`, promotion, the Spark detector job, `make detect-check`) and everything after. Don't claim it works until it exists.

**Laptop limit:** 8 GB RAM total. Keep containers memory-capped; Spark runs only under its Compose profile.

## Code conventions
- `src/` layout, one package per component (`common`, `producer`, …); add each new package to `[tool.hatch.build.targets.wheel]` and `[tool.mypy] packages` in `pyproject.toml`.
- Entity, KPI, team, fault, and state names come from `common.domain.load_domain()`; never string literals elsewhere. Build entity IDs with `domain.entity(<type>).make_id(...)`, never by string formatting.
- Every new behavior ships with a test in `tests/<package>/`. Tests that need Docker are marked `@pytest.mark.integration`.
- Tests use small hand-written fixtures, never Milan data files.
- Anything that fits or tunes on data must go through `common.splits` (`splits.require(day, Split.TRAIN)` or `VALIDATION`); never read test days outside the final evaluation.

## Code quality (no AI slop)
- **Search before you write.** Reuse what exists: `common.kafka` (Kafka settings), `common.db` (Postgres roles), `common.telemetry` (message schema, `INTERVAL`, `LOCAL_TZ`, `add_real_kpis`), `producer.data_prep.processed_days` (a split's processed days), `producer.replay.telemetry_for` (telemetry for a day range, optionally fault-injected: the one path for replay and offline code), `producer.topology.Index` (topology lookups), `tests/factories.py` (test messages; `telemetry_frame` for `telemetry_for`-shaped rows), `streaming.features` (detection features; imports only `common`), `streaming.offline.features_for` (features for consecutive days with their history: the one offline path), `models.detectors` (candidates, `alarms`), `common.domain.loader.read_config` (versioned YAML config → model), `tests/fakes.py` (fake Kafka message, fake clock), `tests/integration/conftest.py` (throwaway topics, producer, writer connection, read-back). Never copy a block into a second file; move it to `common/` or a shared test helper instead.
- **Smallest change that meets the step's "Done when".** No speculative options, layers, or abstractions for later milestones; add them when a step needs them.
- **No dead code:** no commented-out code, unused parameters, `if False`, or leftover debug output.
- **Comments say why, not what.** Docstrings state the contract (inputs, outputs, what is SYNTHETIC/MODELED); the code says the rest.
- **Many options = keyword-only.** At most 5 positional and 7 total parameters (ruff enforces); group related settings into a small frozen dataclass (e.g. `sink.LoopSettings`).
- **Verify library APIs against the installed version** (type stubs, `mypy`), not from memory.
- Enforced by ruff (complexity ≤ 10, no commented-out code, refactor rules), mypy strict, and a **coverage floor of 85%** (`make test` fails below it). Review the diff before verifying it (the `milestone-step` skill runs `/simplify`, then `/code-review`, at the end of Build).

## Common mistakes to avoid
- Using ground-truth fault labels outside `src/evaluation/` (Rule 6), or reading test-week data while tuning (Rule 7).
- Letting the fault injector import fault signatures (Rule 15).
- Adding an agent tool that writes, or that accepts SQL or free-form queries (Rules 4, 13).
- Binding a port to `0.0.0.0`, using `latest` image tags, or a default password (Rule 12, S5).
- Writing anything under `data/` except via the data scripts; reading or editing `.env`.
- Putting env values, file paths, or raw config into an LLM prompt (Rule 11).

## Non-negotiable rules
1. **Built from scratch.** No third-party project is adopted, forked, or copied; others are architecture reading only. Maintained libraries as dependencies are fine.
2. **Local only.** No public hosting or public live demo. Demos are local runs, screen-shares, and a recorded video. (The source repo itself is public on GitHub: code and docs only, never data or secrets.)
3. **Data comes only from its canonical source and is never committed.** Milan files are downloaded manually in the browser (guestbook needs an email; no script sends it), then verified and processed by script. File hashes go in the manifest. Never run code from a dataset repository; download data files only.
4. **Human gate.** The ops agent can read and draft, but it can **never** close, escalate, or re-route an incident. This is enforced in code (not exposed as tools; console action endpoints require a human user) and covered by a test, not just a prompt.
5. **Quality bars are fixed in advance** (`specs/00-overview.md` → Definition of done; measured per `specs/09-evals.md`). Changing one requires a recorded reason next to it and in `DECISIONS.md`.
6. **Incidents open only from detector output, never from ground truth.** The ground-truth fault log is for evaluation only.
7. **Test weeks are touched once**, for the final numbers. Tuning uses validation weeks only.
8. **Secrets live in `.env` only**, never committed. `.env.example` holds fake placeholders.
9. **Be honest about synthetic parts.** Topology, derived KPIs, faults, and reports are synthetic or modeled, and are labeled that way everywhere.
10. **Cost caps.** No payment method on the LLM account; agent off by default; hard per-run call/token cap and max tool calls per question; every LLM call logged. No cloud resources in v1.
11. **Only aggregated, synthetic, or modeled data goes to the LLM.** Never secrets, env values, file paths, or raw config.
12. **Local services stay local.** All ports bound to `127.0.0.1`; no default passwords; secret scanning before every commit.
13. **Agent tools are read-only and parameterized.** The model never writes SQL; free text (e.g., incident reports) is passed as data, not instructions; citations are verified in code.
14. **No carrier branding or affiliation claims.** No carrier name or logo in the product, demo, or repo; no confidential employer material.
15. **One domain model.** Entity names, KPI names/units, fault types, and incident states come from `src/common/domain/` (`specs/01-domain-model.md`); never hard-code them elsewhere. The fault injector must not use the fault signatures.

Full list with enforcement points and per-milestone checklist: `specs/08-guardrails.md`.

## Where things are
| File | What it holds |
|---|---|
| `PROJECT.md` | Charter: goals, stack, milestones overview, decision log |
| `README.md` | Technical design |
| `DECISIONS.md` | Decided and open decisions |
| `specs/HANDOFF.md` | Current state and next step. **Read first in every session, update last.** |
| `BUILD_LOG.md` | Session-by-session build journal |
| `specs/` | **What done means, read in order.** `00-overview` (read first: pipeline, components, Definition of done), `01-domain-model`, `02-telemetry` (data sources, licenses, `telemetry.raw` message), `03-detection`, `04-correlation` (+ incident record), `05-classification`, `06-ops-agent`, `07-human-gate`, `08-guardrails`, `09-evals`; `milestones/` (one file per milestone, each step with its "Done when"). Read the spec for the part you are building. |
| `evals/` | `cases/` (recorded cases with known answers) and `results-log.md` (measured numbers); the plan is `specs/09-evals.md` |
| `data/` | Downloaded data (git-ignored) + manifest |
| `prompts/` | Pointer to `.claude/skills/` (reusable procedures live there) |
| `.claude/` | Claude Code config for *building* the repo (not the ops agent): `settings.json` (deny rules + hook wiring), `hooks/guard.py` (blocks `.env` access, writes under `data/`/`uv.lock`/`.git`, force-push, `--no-verify`, email over network, non-local `docker -p`), `skills/` (`milestone-step`, `new-package`), `agents/` (`eval-failure-reviewer`: diagnoses failed eval cases, read-only). Only the user edits `settings.json` and `hooks/`. |
| `src/` | Application code, one folder per component (+ `common/`) |
| `infra/`, `tests/` | Compose/Grafana/DB schema; tests incl. the human-gate test and `tests/claude_hooks/` |
