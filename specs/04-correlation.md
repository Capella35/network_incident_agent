# 04 — Correlation and root cause

Groups the alarm storm into **one incident** and ranks the most likely **root node**. Built in Milestone 3 (`milestones/milestone-3.md`). Drawn from `PROJECT.md` → What is being built (item 6) and `01-domain-model.md`; details still open are marked **(M3)**.

## Behavior
- The correlator (`src/correlation/`) groups alarms **in time** and **across the topology graph** (cell → site → aggregation node → core) into one incident.
- **Incidents open only from detector output, never from ground truth** (Rule 6).
- **Root-node ranking:** candidates are the affected cells' common ancestors; each is scored with the fault signatures (`01-domain-model.md` §4), and the ranked list is stored (`root_candidates`). Scoring function and grouping windows **(M3)**.
- **Consistency constraints** (`01-domain-model.md` §4) are checked on every incident: every affected cell is a descendant of the root node, and the fault type's root level matches.
- **Alarm-to-incident compression ratio** is reported (Definition of done) and shown on the "alarm storm vs. incidents" dashboard.

## Quality bar
Correct root node ranked **#1 in ≥ 80%** of multi-cell incidents (`00-overview.md`, measured as in `09-evals.md`).

## Incident record

The record every later stage reads and adds to (classifier: labels; agent: draft; console: human decisions). Fields marked **TODO** were inferred.

| Field | Type | Meaning |
|---|---|---|
| `incident_id` | string | Unique incident ID |
| `opened_at` | timestamp | When the correlator opened the incident (from detector output only) |
| `alarm_ids` | list[string] | Cell alarms grouped into this incident |
| `affected_cells` | list[string] | Cells in the incident (blast radius) |
| `root_node_id` | string | Top-ranked root node (cell, site, aggregation node, or core) |
| `root_node_level` | enum | `cell` / `site` / `aggregation` / `core` |
| `root_candidates` | list[{node_id, score}] | TODO (inferred): ranked root-node candidates |
| `fault_category` | enum | `cell_outage` / `site_power` / `backhaul` / `congestion` / `config_push` / `no_fault` |
| `severity` | enum | `minor` / `major` / `critical` |
| `owning_team` | enum | `RAN` / `Field Ops` / `Transport` / `RAN Capacity` / `Core/Config` |
| `classifier_confidence` | float | TODO (inferred): XGBoost probability of the predicted category |
| `reported_description` | string | Noisy human-style report |
| `evidence` | list[ref] | Telemetry windows / charts attached as evidence |
| `agent_draft` | string | Ops agent's root-cause draft with citations (optional) |
| `status` | enum | `open` / `approved` / `corrected` / `closed` / `escalated` — changed by a human only |
| `human_decision_by` | string | Who approved, corrected, closed, or escalated |
| `human_correction` | object | Corrected labels/root node; stored as labeled data |
| `detector_model_version` | string | MLflow version of the detector that raised the alarms |
| `classifier_model_version` | string | MLflow version of the classifier |
| `audit_trail` | list[event] | Every change with timestamp and actor |
