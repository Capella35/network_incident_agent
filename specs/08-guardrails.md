# 08 — Guardrails (what it never does)

Rules that prevent extra cost, privacy problems, security holes, unsafe agent behavior, and reputational risk. Set 2026-11-14. Status: design. Each guardrail names where it is enforced and how it is checked. Values marked **(default)** are starting points; change them with a recorded reason in `DECISIONS.md`.

The key rules are repeated in `CLAUDE.md`. These guard the **product**; Claude Code, which builds the repo, is guarded separately by `.claude/settings.json` and `.claude/hooks/guard.py`.

---

## 1. Cost

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| C1 | **No payment method on the LLM account.** Stay on Groq's free tier, so an overrun is rate-limited, not billed. Re-check Groq's current free-tier terms at Milestone 5. | Account setup | Manual check, recorded in `BUILD_LOG.md` |
| C2 | **Agent off by default.** It runs only when `LLM_API_KEY` is set *and* the agent is enabled in config. | `src/agent/` config | Test: no key → no LLM calls |
| C3 | **Hard per-run budget:** max LLM calls and max tokens per run, e.g., 200 calls / 200k tokens **(default)**. On reaching the cap the agent stops and says so; it never silently continues. | `src/agent/` call wrapper | Test: cap reached → calls refused |
| C4 | **Max tool calls per question**, e.g., 8 **(default)**, so the agent can't loop. | `src/agent/` loop | Test: loop stops at the limit |
| C5 | **Client-side rate limiter** set below Groq's free-tier limits; backoff on 429, no tight retry loops (max 3 retries). | `src/agent/` call wrapper | Test with a mocked 429 |
| C6 | **Every LLM call logged:** time, model, tokens in/out, question ID. Shown on Grafana's system-health dashboard. | `src/agent/` + Postgres | Dashboard panel exists |
| C7 | **Eval runs are bounded:** the agent eval set (≥ 20 questions) runs once per change, not in CI on every commit. | `evals/`, CI config | CI config review |
| C8 | **No cloud resources in v1.** Nothing billable is created outside the laptop. | Project scope | Review at each milestone |
| C9 | **No public endpoint.** Already a rule; it also removes runaway-traffic cost. | `infra/docker-compose.yml` | See S1 |

## 2. Laptop resources

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| R1 | **Memory limit per container** in Docker Compose (Kafka, Spark, Postgres, MLflow, Grafana, Streamlit), sized to fit the laptop. | `infra/docker-compose.yml` | `docker stats` during a full replay |
| R2 | **Kafka retention bounded** (`retention.ms` / `retention.bytes` per topic). | `infra/` topic config | Disk use stable across repeated replays |
| R3 | **Spark checkpoints, MLflow artifacts, and logs cleaned up** by a `make clean` target; old runs pruned. | `Makefile`, `infra/` | Disk use checked at each milestone |
| R4 | **Optuna budget:** a fixed number of trials and a wall-clock timeout per study. | `src/models/` | Config review; study stops on time |
| R5 | **Cell subset size** chosen so the full stack runs within the laptop's free RAM (see open item in `DECISIONS.md`). | `src/producer/` config | Milestone 1 resource check |

## 3. Privacy and data

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| P1 | **Only aggregated, synthetic, or modeled data goes to the LLM.** Never secrets, environment values, file paths, or raw config. Tool outputs are built from whitelisted fields only. | `src/agent/` tools | Test: tool outputs contain only whitelisted fields |
| P2 | **Check Groq's data-use and retention policy** before the first real call; record the result. | Milestone 5 | `BUILD_LOG.md` entry |
| P3 | **Milan license (ODbL 1.0) respected:** attribution link "from BigDataChallenge contest" in README, write-up, and video; never redistribute raw data; publish no derived data unless under ODbL (recorded in `DECISIONS.md`). | `specs/02-telemetry.md`, README | Milestone 1 + 6 checklists |
| P4 | **Data never committed.** `data/` is git-ignored except README and manifest files. | `.gitignore` | Pre-commit check (S3) |
| P5 | **No personal data.** The Milan data is aggregated per grid square; nothing per-person is added, generated, or joined. | Project scope | Review at Milestone 1 |
| P6 | **Logs hold no secrets or full LLM prompts with sensitive values**; API keys are masked if printed. | `src/common/` logging | Grep logs for key patterns in CI |

## 4. Security (local services)

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| S1 | **All service ports bound to `127.0.0.1`**, never `0.0.0.0` (Kafka, Postgres, MLflow, Grafana, Streamlit, Spark UI). | `infra/docker-compose.yml` | Check that ports aren't reachable from another device on the network |
| S2 | **No default passwords:** Postgres and Grafana admin passwords come from `.env`; Grafana anonymous access off. | `infra/`, `.env` | Startup fails if a password is the `.env.example` placeholder |
| S3 | **Secret scanning** (e.g., gitleaks) as a pre-commit hook and in CI. | `.pre-commit-config.yaml`, CI | Hook installed; CI step passes |
| S4 | **Separate database users:** a read-only user for the agent and Grafana; a writer user for the pipeline; the console's write user can change incident status only. | `infra/` DB schema | Test: agent DB user cannot write |
| S5 | **Pinned dependency versions** and container image tags (no `latest`). | `requirements*.txt`, Compose | Config review |
| S6 | **Dataset safety:** download data files only; never run code from a dataset repository or archive. | Download script | Code review |

## 5. Agent safety

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| A1 | **Human gate:** no tool can close, escalate, or re-route an incident; console action endpoints require a human user. | `src/agent/`, `src/console/` | Human-gate test (100% of attempts blocked) |
| A2 | **Read-only tools with fixed, parameterized queries.** The model never writes SQL; it only fills validated parameters (IDs, time ranges with max span). | `src/agent/` tools | Test: invalid or oversized parameters rejected |
| A3 | **Prompt-injection defense:** incident report text and any free text are passed as clearly delimited data, never as instructions. With no action tools, a successful injection can at most read data. | `src/agent/` prompt building | Eval questions include injected reports; agent must not follow them |
| A4 | **Citation check:** every cited row, alarm, or incident ID is verified to exist and to support the claim's time range; failures are flagged in the console as "unverified". | `src/agent/` output check | Test with a fabricated citation |
| A5 | **Agent output is a draft,** shown as such in the console; it never overwrites labels or root cause directly. | `src/console/` | Test: agent draft doesn't change incident fields |

## 6. Reputation and legal

| # | Guardrail | Enforced in | Checked by |
|---|---|---|---|
| L1 | **No carrier name, logo, or branding** in the product, UI, demo, or repo. Describe it as "built for telecom NOC use cases". | All docs and UI | Review before publishing the repo or video |
| L2 | **No claim of real carrier data or affiliation.** The README states the data sources and the synthetic parts. | README | Review before publishing |
| L3 | **No confidential material** from any current or past employer, in code, data, or examples. | All work | Self-check at each milestone |
| L4 | **Dependency and dataset licenses respected;** a short license list is kept in the README. | README | Review at Milestone 6 |

---

## Checklist by milestone

- **Milestone 1:** P3, P4, P5, S1, S2, S3, S5, S6, R1, R2, R5
- **Milestone 2:** R3, R4
- **Milestone 3:** —
- **Milestone 4:** S4 (console and Grafana users), A5
- **Milestone 5:** C1–C7, P1, P2, P6, S4 (agent user), A1–A4
- **Milestone 6:** C8, C9, L1–L4; re-check all items, record the result in `BUILD_LOG.md`
