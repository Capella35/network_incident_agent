# Incident record

Derived from `README.md` / `PROJECT.md`. Fields marked **TODO** were inferred.

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
