# Domain Model

A lightweight semantic layer: one shared definition of the network's entities, relations, KPIs, fault types, and incident lifecycle. Every component (Spark job, correlator, classifier, agent, console, evals) reads from it, so names, units, and rules exist in exactly one place. Status: design. Set 2026-11-14.

**What it is not:** no OWL/RDF, no graph database, no semantic-layer product. Those add infrastructure without moving any v1 quality bar. Topology lives in two Postgres tables; definitions live in YAML.

Values marked **(default)** are starting points, tuned on validation weeks only.

---

## 1. Where it lives

```
src/common/domain/
  entities.yaml    # entity types, ID formats, relations
  kpis.yaml        # KPI catalog
  faults.yaml      # fault taxonomy with machine-checkable signatures
  lifecycle.yaml   # incident states and allowed transitions
  loader.py        # loads + validates the YAML; one typed object used everywhere
```

Postgres: `topology_nodes(node_id, node_type, parent_id, lat, lon)` and `topology_edges(src, dst, relation)`, queried with recursive SQL or networkx.

## 2. Entities and relations

```
Core ──parent_of──► AggregationNode ──parent_of──► Site ──parent_of──► Cell
                                                                        │
                    ChangeEvent ──applied_to──► Site                    │ emits
                                                                        ▼
                          Incident ◄──grouped_into── Alarm ◄──raised_on── Cell
                             │
                             ├──has_root──► (Cell | Site | AggregationNode | Core)
                             └──owned_by──► Team
```

| Entity | ID format | Key attributes |
|---|---|---|
| Core | `CORE-1` | — |
| AggregationNode | `AGG-<n>` | parent core |
| Site | `SITE-<n>` | parent aggregation node, location |
| Cell | `CELL-<site>-<sector>` | parent site, Milan grid square |
| Alarm | `ALM-<uuid>` | cell, interval, KPI(s), score, detector version |
| Incident | `INC-<uuid>` | alarms, root node, labels, status (see `schemas/incident-record.md`) |
| ChangeEvent | `CHG-<n>` | site(s), time, description |
| Team | enum | RAN, Field Ops, Transport, RAN Capacity, Core/Config |

**Naming:** where it fits, names follow telecom conventions (3GPP network resource models, TM Forum information framework) so the model reads naturally to a carrier. These standards are borrowed for naming only, not implemented.

## 3. KPI catalog

| KPI | Unit | Meaning | Bad direction | Source |
|---|---|---|---|---|
| `traffic_internet` | scaled activity | Internet activity | sudden drop | Milan `internet` (real) |
| `traffic_voice` | scaled activity | Call in + out | sudden drop | Milan `call_in` + `call_out` (real) |
| `traffic_sms` | scaled activity | SMS in + out | sudden drop | Milan `sms_in` + `sms_out` (real) |
| `load_ratio` | 0–1 | Traffic ÷ modeled cell capacity | high | modeled |
| `latency_ms` | ms | Modeled user-plane latency | high | modeled |
| `drop_rate` | 0–1 | Share of dropped connections | high | modeled |
| `baseline_deviation` | z-score | Deviation from same hour + weekday | either | derived |

Milan field mapping verified 2026-11-14 (see `schemas/telemetry-message.md`). Values are scaled, so KPIs are compared against each cell's own baseline, never as absolute numbers.

## 4. Fault taxonomy with signatures

Each fault type has a **signature**: a set of predicates over the topology and KPIs that code can check. The same signatures are used by the correlator, the classifier's consistency check, the agent's reasoning, and the eval graders. The fault injector deliberately does **not** use them (see section 9), so the evals measure whether the signatures hold on injected faults rather than restating them.

| Fault | Root level | Signature predicates **(default thresholds)** | Severity | Team |
|---|---|---|---|---|
| `cell_outage` | Cell | traffic < 5% of baseline on one cell; sibling cells at the same site within normal range | minor | RAN |
| `site_power` | Site | ≥ 90% of a site's cells drop to < 5% of baseline within 1 interval of each other | major | Field Ops |
| `backhaul` | AggregationNode | ≥ 70% of sites under one aggregation node alarm within 2 intervals; sites under other aggregation nodes normal | critical | Transport |
| `congestion` | Cell / Site | `load_ratio` > 0.9 and `latency_ms` or `drop_rate` above baseline; traffic *not* dropping | minor–major | RAN Capacity |
| `config_push` | Site(s) | gradual degradation (slope over ≥ 3 intervals) starting within 2 intervals after a ChangeEvent on the same site(s) | major | Core/Config |
| `no_fault` | — | none of the above hold (e.g., holiday traffic patterns) | — | — |

**Consistency constraints** (checked in code on every incident and every agent answer):
- The fault type's root level must match the root node's level (e.g., `backhaul` ⇒ root is an AggregationNode).
- Severity and team must match the fault type's rule.
- Every affected cell must be a descendant of the root node.
- `config_push` requires a ChangeEvent on an affected site before onset.

## 5. Incident lifecycle

```
open ──► approved ──► closed
  │         ▲
  ├──► corrected ──┘
  └──► escalated
```

Every transition requires `actor = human`. The agent has no transition rights (see `docs/guardrails.md` A1). Allowed transitions are listed in `lifecycle.yaml`; the console refuses anything else.

---

## 6. How the components use it

| Component | Uses |
|---|---|
| Spark job | KPI catalog: names, units, bad direction, for feature engineering and validation |
| Correlator | Topology relations to group alarms; fault signatures to rank root candidates |
| Classifier (XGBoost) | Features from the model: blast radius per topology level, root level, signature predicate results; constraint check flags predictions that conflict with the root level |
| Console | Shows entity names, KPI meanings, and the signature checks behind each label |
| Grafana | KPI catalog drives panel names and units |
| Agent | See section 7 |
| Evals | See section 8 |

## 7. How the agent uses it for better reasoning

### 7.1 Ontology card in the system prompt
A compact, auto-generated summary of `entities.yaml`, `kpis.yaml`, and `faults.yaml` (entity types, relations, KPI meanings, fault signatures). It is generated from the YAML, never hand-copied, so it can't drift. It's small enough to cache.

### 7.2 Typed tools
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

### 7.3 Differential diagnosis procedure
The agent is instructed to reason like a NOC engineer:
1. Find the incident's affected entities and their common ancestor.
2. List candidate fault types whose root level fits.
3. Call `check_signature` for each candidate.
4. Mark each hypothesis **supported**, **ruled out**, or **unknown**, citing the predicate results.
5. Answer with the best-supported hypothesis, or say "unknown" if none is supported.

This keeps reasoning tied to checkable facts instead of free association, and makes wrong answers easy to diagnose.

### 7.4 Structured answer, validated in code
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
- The consistency constraints in section 4 hold.
- Every "supported" claim matches the actual `check_signature` result.

If a check fails, the agent gets one retry with the error. If it still fails, the answer is shown marked **unverified**.

## 8. How evals use it

### 8.1 Questions generated from ground truth + topology
From validation-week ground truth and the topology, templates generate questions with **known answers**:
- "What is the root node of INC-x?" → node ID
- "Which team should own INC-x?" → team
- "Which sites are affected by INC-x?" → set of IDs
- "Was there a change event before INC-x?" → yes/no + ChangeEvent ID
- **Negative:** "Is INC-x caused by a backhaul failure?" when it isn't → must answer no and cite the failing predicate
- **No-fault:** false-alarm incidents → must answer `no_fault` or "unknown", not invent a cause
- **Injection:** incidents whose report text contains instructions → must not follow them

A fixed, frozen set of **≥ 20** questions meets the Definition of done bar. A larger generated set (e.g., 100) is run for extra signal **(default)**. Test-week questions are generated once, for the final numbers.

### 8.2 Grading is deterministic
Answers are structured, so grading is exact match on IDs and enums plus set overlap for affected entities. No LLM judge is needed for the headline score, which keeps it cheap (see `docs/guardrails.md` C7) and reproducible.

### 8.3 Extra metrics the model makes possible (reported, not bars)
| Metric | Meaning |
|---|---|
| Hallucinated-entity rate | Share of answers citing an ID that doesn't exist in the graph |
| Constraint-violation rate | Share of answers (or classifier predictions) breaking a consistency constraint |
| Ruled-out accuracy | Share of "ruled out" hypotheses that really don't match ground truth |
| Signature agreement | How often correlator, classifier, and agent agree on fault type |
| Unverified rate | Share of answers that failed code validation after the retry |

These are reported next to the Definition of done bars in `evals/results-log.md`; they don't change the bars.

---

## 9. Build order
- **Milestone 1:** `entities.yaml`, `kpis.yaml`, loader + validation; topology tables.
- **Milestone 2:** `faults.yaml` fault types, severities, and teams shared with the fault injector. The injector does **not** use the signature thresholds: it injects physical effects (traffic drop, load spike, gradual drift) from its own parameters, so the signatures are tested, not copied.
- **Milestone 3:** correlator uses signatures for root ranking; consistency constraints.
- **Milestone 4:** classifier features + constraint check; `lifecycle.yaml` in the console.
- **Milestone 5:** ontology card, typed tools, `check_signature`, structured answers + validation, generated eval set.
