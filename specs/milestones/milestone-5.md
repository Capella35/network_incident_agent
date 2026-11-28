# Milestone 5 — Ops agent

**Goal:** Groq-backed agent with read-only tools over telemetry, topology, and incident history; cited root-cause drafts; human gate enforced and tested; agent eval set; rate limiting and call log.

**Domain model:** ontology card, typed tools incl. `check_signature`, structured validated answers, generated eval set.

## Detailed plan

TODO: written just before this milestone starts (per `PROJECT.md`).

### Test plan notes (fixed before the plan, 2026-11-28)
- One test runs the agent with a deliberately failing tool: the error comes back as a structured observation, counts against the C4 cap, is in the run trace, and the run still ends with a §5 answer (`06-ops-agent.md` §1).
- One test reaches the C4 cap with no conclusion: the answer is `fault_type: unknown`, unverified, citing the completed checks.
- One test exhausts C5's retries: no fallback provider is called, the incident shows "no draft yet", and the reason is logged.
- One test checks the run trace (C6) holds every step of a run.

## Status

Not started.
