# 06 — Ops agent

Lets an ops engineer investigate an incident by asking questions; answers cite evidence. **It reads and drafts; it never acts.** Built in Milestone 5 (`milestones/milestone-5.md`). Drawn from `PROJECT.md` → What is being built (item 8), `README.md` → Ops agent, and the former domain-model section 7.

## 1. Behavior and limits
- Answers questions like *"Why did sector 12-B degrade at 14:05?"* by calling read-only tools over telemetry, topology, alarms, incidents, and change events, and drafts a root-cause summary stored as `agent_draft` on the incident.
- **No action tools.** Closing, escalating, or re-routing an incident is not exposed as a tool; the console's action endpoints require a human user (`07-human-gate.md`). A test proves 100% of attempts are blocked (Rule 4).
- **Tools are read-only and parameterized** (Rule 13): fixed queries, validated parameters (IDs, time ranges with a max span). The model never writes SQL.
- **Free text is data, not instructions:** incident report text is passed delimited; an injected instruction must not be followed (guardrail A3).
- **Only aggregated, synthetic, or modeled data goes to the LLM** (Rule 11). Never secrets, env values, file paths, or raw config.
- **Provider interface:** Groq by default (`LLM_PROVIDER`, `LLM_MODEL`, `LLM_API_KEY`), swappable. Off by default; hard per-run call/token caps, max tool calls per question, rate limiter, every call logged (guardrails C1–C6).
- **Code layout (M5):** instructions (system prompt + ontology card generator) and tools kept apart inside `src/agent/`, so each tool has its own test and the prompt has no hand-copied facts.


## 2. Ontology card in the system prompt
A compact, auto-generated summary of `entities.yaml`, `kpis.yaml`, and `faults.yaml` (entity types, relations, KPI meanings, fault signatures). It is generated from the YAML, never hand-copied, so it can't drift. It's small enough to cache.

## 3. Typed tools
| Tool | Returns |
|---|---|
| `get_topology(node_id, depth)` | Typed ancestors and descendants |
| `query_kpis(node_ids, kpis, time_range)` | KPI values with units and baseline; KPI names validated against the catalog; time span capped |
| `list_alarms(scope, time_range)` | Alarms with cell, interval, KPI, score |
| `get_incident(incident_id)` | Incident record |
| `check_signature(fault_type, incident_id)` | Each predicate of that fault's signature: true/false, measured value vs. threshold, and the rows used |
| `list_change_events(node_ids, time_range)` | ChangeEvents |
| `describe_kpi(name)`, `describe_fault_type(name)` | Catalog entries |
| `similar_past_incidents(incident_id)` | Past incidents with the same root level and fault type |

All are read-only, with fixed parameterized queries.

## 4. Differential diagnosis procedure
The agent is instructed to reason like a NOC engineer:
1. Find the incident's affected entities and their common ancestor.
2. List candidate fault types whose root level fits.
3. Call `check_signature` for each candidate.
4. Mark each hypothesis **supported**, **ruled out**, or **unknown**, citing the predicate results.
5. Answer with the best-supported hypothesis, or say "unknown" if none is supported.

This keeps reasoning tied to checkable facts instead of free association, and makes wrong answers easy to diagnose.

## 5. Structured answer, validated in code
```json
{
  "answer": "short text for the engineer",
  "root_node": "AGG-3",
  "fault_type": "backhaul",
  "hypotheses": [
    {"fault_type": "backhaul", "status": "supported",
     "checks": [{"predicate": "sites_alarming_under_agg >= 0.7", "value": 0.92, "citation": "ALM-…"}]},
    {"fault_type": "site_power", "status": "ruled_out",
     "checks": [{"predicate": "single_site_only", "value": false, "citation": "INC-…"}]}
  ],
  "citations": ["ALM-…", "INC-…"]
}
```
Code checks every answer before it's shown:
- `root_node` and every cited ID exist.
- `fault_type` is in the taxonomy.
- The consistency constraints in `01-domain-model.md` §4 hold.
- Every "supported" claim matches the actual `check_signature` result.

If a check fails, the agent gets one retry with the error. If it still fails, the answer is shown marked **unverified**.

## 6. Quality bar
**≥ 80%** correct on a fixed eval set of ≥ 20 investigation questions, every answer citing evidence (`00-overview.md`; eval design in `09-evals.md`).
