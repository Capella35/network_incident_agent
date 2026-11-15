# CLAUDE.md — context for any LLM working in this repo

## Mission
Network Incident Agent watches a cellular network's telemetry, detects degrading cells, groups the alarm storm into **one incident**, ranks the most likely **root node** in the topology, labels the incident (fault category, severity, owning team), and lets an ops engineer investigate it with an agent that answers using **cited evidence**. A human approves every action.
Purpose: portfolio project for a Forward Deployed Engineer role in telecom (target: a US telecom carrier), framed as a customer engagement. Source: `PROJECT.md`.

## Architecture in brief
Milan traffic replay + fault injector → **Kafka** (`telemetry.raw`) → **Spark Structured Streaming** (windowed features + promoted detector) → Kafka `alarms` → **correlator** (topology rules + graph ranking) → incident + root node → **XGBoost** classifier → **PostgreSQL** → **Grafana** (KPIs, health), **Streamlit** console (human approve/correct/close), **ops agent** (Groq API, read-only tools). **MLflow** tracks every model run, holds the registry, and runs the promotion gate. Everything starts with `docker compose up`. Full diagram: `docs/architecture.md`.

## Non-negotiable rules
1. **Built from scratch.** No third-party project is adopted, forked, or copied; others are architecture reading only. Maintained libraries as dependencies are fine.
2. **Local only.** No public hosting or public live demo. Demos are local runs, screen-shares, and a recorded video.
3. **Data comes only from its canonical source and is never committed.** Milan files are downloaded manually in the browser (guestbook needs an email; no script sends it), then verified and processed by script. File hashes go in the manifest. Never run code from a dataset repository; download data files only.
4. **Human gate.** The ops agent can read and draft, but it can **never** close, escalate, or re-route an incident. This is enforced in code (not exposed as tools; console action endpoints require a human user) and covered by a test, not just a prompt.
5. **Quality bars are fixed in advance** (`evals/eval-plan.md`). Changing one requires a recorded reason next to it and in `DECISIONS.md`.
6. **Incidents open only from detector output, never from ground truth.** The ground-truth fault log is for evaluation only.
7. **Test weeks are touched once**, for the final numbers. Tuning uses validation weeks only.
8. **Secrets live in `.env` only**, never committed. `.env.example` holds fake placeholders.
9. **Be honest about synthetic parts.** Topology, derived KPIs, faults, and reports are synthetic or modeled, and are labeled that way everywhere.
10. **Cost caps.** No payment method on the LLM account; agent off by default; hard per-run call/token cap and max tool calls per question; every LLM call logged. No cloud resources in v1.
11. **Only aggregated, synthetic, or modeled data goes to the LLM.** Never secrets, env values, file paths, or raw config.
12. **Local services stay local.** All ports bound to `127.0.0.1`; no default passwords; secret scanning before every commit.
13. **Agent tools are read-only and parameterized.** The model never writes SQL; free text (e.g., incident reports) is passed as data, not instructions; citations are verified in code.
14. **No carrier branding or affiliation claims.** No carrier name or logo in the product, demo, or repo; no confidential employer material.
15. **One domain model.** Entity names, KPI names/units, fault types, and incident states come from `src/common/domain/` (`docs/domain-model.md`); never hard-code them elsewhere. The fault injector must not use the fault signatures.

Full list with enforcement points and per-milestone checklist: `docs/guardrails.md`.

## Where things are
| File | What it holds |
|---|---|
| `PROJECT.md` | Charter: goals, stack, milestones, definition of done, decision log |
| `README.md` | Technical design |
| `DECISIONS.md` | Decided and open decisions |
| `HANDOFF.md` | Current state and next step. **Read first in every session, update last.** |
| `BUILD_LOG.md` | Session-by-session build journal |
| `docs/architecture.md` | Component and data-flow diagram |
| `docs/data-sources.md` | Datasets, sources, licenses, download rules |
| `docs/domain-model.md` | Entities, relations, KPI catalog, fault signatures, lifecycle; how the agent and evals use them |
| `docs/guardrails.md` | Cost, privacy, security, agent-safety, and reputation guardrails + per-milestone checklist |
| `docs/milestones/` | One file per milestone |
| `schemas/` | Telemetry message and incident record field tables |
| `evals/` | Evaluation plan (quality bars) and measured results log |
| `data/` | Downloaded data (git-ignored) + manifest |
| `prompts/` | Reusable prompts for building or reviewing the project |
| `src/` | Application code, one folder per component (+ `common/`) |
| `infra/`, `tests/` | Compose/Grafana/DB schema; tests incl. the human-gate test |
