# 01 — Domain model

A lightweight semantic layer: one shared definition of the network's entities, relations, KPIs, fault types, and incident lifecycle. Every component (Spark job, correlator, classifier, agent, console, evals) reads from it, so names, units, and rules exist in exactly one place. Status: design. Set 2026-11-14.

**What it is not:** no OWL/RDF, no graph database, no semantic-layer product. Those add infrastructure without moving any v1 quality bar. Topology lives in two Postgres tables; definitions live in YAML.

Values marked **(default)** are starting points, tuned on validation weeks only.

---

## 1. Where it lives

```
src/common/domain/
  entities.yaml    # entity types, ID formats, relations
  kpis.yaml        # KPI catalog
  faults.yaml            # fault types: root levels, severities, owning team (shared with the injector)
  fault_signatures.yaml  # how each fault shows in the data, with default thresholds
  signatures.py          # loads the signatures; never imported by the injector (Rule 15)
  lifecycle.yaml         # incident states and allowed transitions (spec: 07-human-gate.md; M4)
  loader.py              # loads + validates the YAML; one typed object used everywhere
```

Fault types and signatures are separate files on purpose: the injector shares the types but must not see the signatures, and `tests/test_import_boundaries.py` enforces that (by import, transitively, and by file name). Severity is an ordered enum (`minor < major < critical`).

Postgres: `topology_nodes(node_id, node_type, parent_id, lat, lon, square_id)` (`square_id` set only for Cells) and `topology_edges(src, dst, relation)`, queried with recursive SQL or networkx. Each entity in `entities.yaml` has an `id_pattern` and an `id_template`; code builds IDs only through `make_id()`, which checks the result against the pattern.

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
| Incident | `INC-<uuid>` | alarms, root node, labels, status (see `04-correlation.md`) |
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

Milan field mapping verified 2026-11-14 (see `02-telemetry.md`). Values are scaled, so KPIs are compared against each cell's own baseline, never as absolute numbers.

## 4. Fault taxonomy with signatures

Each fault type has a **signature**: a set of predicates over the topology and KPIs that code can check. The same signatures are used by the correlator, the classifier's consistency check, the agent's reasoning, and the eval graders. The fault injector deliberately does **not** use them (see section 6), so the evals measure whether the signatures hold on injected faults rather than restating them.

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

## 5. How the components use it

| Component | Uses |
|---|---|
| Spark job | KPI catalog: names, units, bad direction, for feature engineering and validation |
| Correlator | Topology relations to group alarms; fault signatures to rank root candidates |
| Classifier (XGBoost) | Features from the model: blast radius per topology level, root level, signature predicate results; constraint check flags predictions that conflict with the root level |
| Console | Shows entity names, KPI meanings, and the signature checks behind each label; enforces the incident lifecycle (`07-human-gate.md`) |
| Grafana | KPI catalog drives panel names and units |
| Agent | Ontology card, typed tools, differential diagnosis, validated answers: `06-ops-agent.md` |
| Evals | Generated questions with known answers, deterministic grading: `09-evals.md` |

## 6. Build order
- **Milestone 1:** `entities.yaml`, `kpis.yaml`, loader + validation; topology tables.
- **Milestone 2:** `faults.yaml` fault types, severities, and teams shared with the fault injector. The injector does **not** use the signature thresholds: it injects physical effects (traffic drop, load spike, gradual drift) from its own parameters, so the signatures are tested, not copied.
- **Milestone 3:** correlator uses signatures for root ranking; consistency constraints.
- **Milestone 4:** classifier features + constraint check; `lifecycle.yaml` in the console.
- **Milestone 5:** ontology card, typed tools, `check_signature`, structured answers + validation, generated eval set.
