# Network Incident Agent

**Turns a cellular network's alarm storm into one incident with a root cause, a label, and an AI ops assistant that explains it with evidence, while a human stays in charge.**

> Status: design. Nothing is built yet. Commands and results below describe the target, not working code. The project charter (goals, decisions, milestones) is in [`PROJECT.md`](PROJECT.md).

---

## The problem

When one upstream link in a mobile network fails, every cell site behind it raises alarms at the same moment. The NOC sees dozens of red alerts, the real cause is buried, and the ticket often goes to the wrong team (RAN instead of Transport, for example). Each extra hop adds time to resolution.

## What this does

1. **Replays real network traffic** (Telecom Italia Milan dataset) into Kafka, with documented faults injected on top.
2. **Detects** degraded cells in a Spark Structured Streaming job, using a detector picked by optimization.
3. **Correlates** alarms across the network topology into **one incident** and names the **root node**.
4. **Classifies** the incident (fault category, severity, owning team) with XGBoost.
5. **Ops agent (Groq)** answers questions like *"Why did sector 12-B degrade at 14:05?"* by calling tools over the telemetry, topology, and incidents, and cites the data it used.
6. **Human gate**: the agent can read and draft. Only a human, through the console, can close, escalate, or re-route an incident. This is enforced in code.

## Architecture

```
Milan traffic ─► replay producer ─► Kafka: telemetry.raw ──────────────┐
                 + fault injector ─► ground-truth log (eval only)       │
                                                                       ▼
                                                 Spark Structured Streaming
                                                 windowed features + promoted detector
                                                                       │
                                                         Kafka: alarms ▼
               topology graph ───────────────────────────► correlator ─► incident + root node
                                                                       │
                                                        XGBoost classifier ─► category / severity / team
                                                                       │
                                                                  PostgreSQL
                                                     ┌─────────────────┼──────────────────┐
                                                     ▼                 ▼                  ▼
                                                  Grafana      Streamlit console     Ops agent (Groq)
                                               KPIs, health   approve / correct /   read-only tools,
                                                              close (human only)    cited answers

MLflow: every detector/classifier run, the model registry, and the promotion gate
```

## Tech stack

| Layer | Choice | Why |
|---|---|---|
| Streaming | Apache Kafka | Industry standard for telemetry; dead-letter topic, keyed and replayable |
| Stream processing | Spark Structured Streaming (PySpark) | Windowed aggregations at scale, checkpointing, exactly-once output to sinks |
| Storage | PostgreSQL | Raw telemetry, alarms, incidents, audit trail, human corrections |
| Detection | Optimized selection (Optuna) over seasonal z-score, Isolation Forest, autoencoder | Pick the best model by measured results, not by assumption |
| Correlation | Topology rules + graph ranking | Explainable root cause, which a NOC needs to trust it |
| Classifier | XGBoost | Strong on tabular features, fast, explainable via feature importance |
| MLOps | MLflow | Tracking, model registry, promotion gate, rollback |
| Agent LLM | Groq API (free tier) | $0 to run; open-weight model chosen at Milestone 5 |
| UI | Streamlit + Grafana | Console and agent chat in Streamlit; KPI and health dashboards in Grafana |
| Deployment | Docker Compose | One command to start everything |

## The data

- **Real traffic:** [Telecom Italia Big Data Challenge — Milan](https://dataverse.harvard.edu/) (Harvard Dataverse). Call, SMS, and internet activity per grid square in 10-minute intervals, Nov 2013 – Jan 2014. Licensed under ODbL 1.0; data is used "from BigDataChallenge contest" (attribution links in `docs/data-sources.md`). The 62 daily files (~20.8 GB) are downloaded manually in the browser (Dataverse asks for an email on a guestbook form), then checked against their published MD5, filtered to the cell subset, and optionally deleted by script. Hashes are recorded in `data/manifest.json`. Nothing is committed.
- **Topology (synthetic):** grid squares are mapped to cells and sites, and sites to aggregation nodes and core, by geographic proximity (seeded, committed).
- **Derived KPIs (modeled):** latency and drop rate are modeled from real load, so they follow real daily and weekly patterns.
- **Injected faults:**

| Fault | What the telemetry shows | Root node | Owning team | Severity |
|---|---|---|---|---|
| Cell outage | One cell's traffic drops to ~0 | Cell | RAN | Minor |
| Site power failure | All cells of one site drop together | Site | Field Ops | Major |
| Backhaul failure | Every site under one aggregation node degrades at once | Aggregation node | Transport | Critical |
| Congestion | Load spike with rising latency and drop rate | Cell / site | RAN Capacity | Minor–Major |
| Bad config push | Gradual degradation starting at a logged change event | Site(s) | Core / Config | Major |

- **Noisy reports:** each incident gets a human-style description that is vague and sometimes wrong at first. The test set uses report templates the classifier never saw.

## Models

### Detector: chosen by optimization

Candidates are trained offline on training weeks:

1. **Seasonal baseline + z-score:** deviation from the same hour and weekday, the simplest baseline
2. **Isolation Forest:** on windowed multi-KPI features
3. **Autoencoder:** reconstruction error on windowed features

For each candidate, Optuna tunes the window lengths, thresholds, and model hyperparameters on validation weeks. The objective is to **maximize detection rate and minimize delay, subject to ≤ 1 false incident per day**. Every trial is logged to MLflow. The winner is registered, promoted through the evaluation gate, and loaded by the Spark job. The comparison table goes in the Results section.

### Incident classifier: XGBoost

Features: blast radius (cells, sites, and aggregation nodes affected), topology level of the root node, KPI deltas, onset shape (sudden vs. gradual), time since the last change event, and TF-IDF features from the noisy report. It predicts fault category, severity, and owning team. It is tuned with Optuna and registered in MLflow, and human corrections from the console are stored for the next reviewed retrain.

### Ops agent: Groq

It uses read-only, typed tools built on the domain model (`docs/domain-model.md`): `query_kpis`, `get_topology`, `list_alarms`, `get_incident`, `check_signature`, `list_change_events`, `describe_kpi`, `describe_fault_type`, `similar_past_incidents`. It reasons by differential diagnosis: it checks each candidate fault type's signature, marks it supported or ruled out with citations, and returns a structured answer that code validates (IDs exist, the fault type fits the root level). Every answer must cite the rows or charts it used. Actions (close, escalate, re-route) are not exposed as tools, and the console's action endpoints check for a human user. A test verifies this. The LLM sits behind a small provider interface, so it can be swapped. Calls are rate-limited for the free tier, and every call is logged with its token count.

## Evaluation

- **Splits by time:** training weeks, then validation weeks (tuning and promotion gate), then test weeks (used once, for the final numbers). Test faults use a separate seed and unseen locations.
- **Matching:** an incident is a hit if it overlaps a ground-truth fault window (± one interval). Unmatched incidents are false alarms.
- **Targets (set before the build):**

| Metric | Target |
|---|---|
| Faults detected | ≥ 90% |
| Median detection delay | ≤ 2 intervals (20 min) |
| False incidents | ≤ 1 per day |
| Root node ranked #1 (multi-cell faults) | ≥ 80% |
| Mislabel rate (per category) | ≤ 10% |
| Promoted detector vs. z-score baseline | must beat it |
| Agent answers correct with cited evidence (≥ 20 questions) | ≥ 80% |
| Agent close/escalate/re-route attempts blocked | 100% |
| Alarm-to-incident compression | reported |

## Results

*To be filled in after Milestone 6. Every number will link to its MLflow run.*

## Quickstart (target)

```bash
cp .env.example .env              # add LLM_API_KEY only if you want the agent
# first: download the Milan files in your browser into data/raw/ (see docs/data-sources.md)
make data                         # verify MD5s, filter to the cell subset, build topology
docker compose up                 # Kafka, Spark, Postgres, MLflow, Grafana, Streamlit
make train                        # tune detectors + classifier, register best in MLflow
make replay                       # stream the test weeks with injected faults
make evaluate                     # score against ground truth, write results
```

Planned URLs: Streamlit console `localhost:8501`, Grafana `localhost:3000`, MLflow `localhost:5000`.

## Repo layout (target)

```
src/
  producer/      # replay producer + fault injector
  streaming/     # Spark Structured Streaming job
  models/        # detector candidates, Optuna search, XGBoost classifier
  correlation/   # topology graph, alarm grouping, root-node ranking
  agent/         # provider interface (Groq by default), read-only tools, call log
  console/       # Streamlit app
  evaluation/    # matching, metrics, report
  common/        # shared schemas, config, DB access, logging
infra/           # docker-compose.yml, Grafana dashboards, DB schema
data/            # downloaded + generated, git-ignored (manifest committed)
tests/           # incl. human-gate test
docs/, schemas/, evals/, prompts/   # project docs, field specs, eval plan + results
```

## Honest limits

- **Traffic is real, but faults, topology, and KPIs like latency are synthetic or modeled.** Public telecom data with labeled faults is essentially unavailable. Every synthetic part is labeled as such.
- **The same project injects the faults and detects them.** Time-based splits, a separate test seed, unseen fault locations, and unseen report templates limit this circularity, but don't remove it.
- **The data is from 2013–2014 and covers voice/SMS/internet activity**, not modern 5G RAN counters.
- **No real alarm feed, OSS, or ticketing integration.** In a real engagement, the agent's tools would wrap the customer's APIs.
- **Groq's free tier has rate limits** and its model list changes over time. The provider interface keeps this swappable.

## Later

- Kubernetes deployment of the same containers
- A live public data feed as a display-only Kafka topic
- IoT fleet view: separate network-side causes from device-side causes
