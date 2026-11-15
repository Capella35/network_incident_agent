# Evaluation Plan

Source of truth: `PROJECT.md` → The data, and Definition of done.

## Rules
- **Splits are by time**, never random row splits: training weeks, then validation weeks (Optuna tuning and the promotion gate), then **test weeks, touched only once** for the final numbers.
- Test faults use a **separate injector seed** and fault locations, timings, and magnitudes drawn from parameters disjoint from training.
- The classifier is tested on **report templates it never saw**.
- **Matching rule:** a detected incident counts as a hit if it overlaps a ground-truth fault window (± one interval). Incidents with no matching fault are false alarms, labeled "no fault". A fault is "detected" when at least one incident matches it.
- Incidents open only from detector output, never from ground truth.
- Every reported number traces to a data snapshot, config, seed, and model version in MLflow. Record each run in `results-log.md`.

## Agent eval design
Details in `docs/domain-model.md` section 8.
- Questions are generated from validation-week ground truth + topology with known answers (root node, team, affected sites, change events), plus negative, no-fault, and prompt-injection cases.
- A frozen set of ≥ 20 questions is the Definition-of-done set; a larger generated set gives extra signal. Test-week questions are generated once.
- Grading is deterministic: exact match on IDs and enums, set overlap for affected entities. No LLM judge for the headline score.
- Reported alongside the bars (not bars themselves): hallucinated-entity rate, constraint-violation rate, ruled-out accuracy, signature agreement, unverified rate.

## V1 quality bars (copied exactly from `PROJECT.md`)

Set on 2026-11-14 for the re-scoped project, before any build. Changing a bar later requires recording the reason next to it.

- Detect **≥ 90%** of injected faults in the test weeks
- Median detection delay **≤ 2 intervals** (20 min of data time); **≤ 1 false incident per simulated day** across the whole network (all three numbers always reported together, with the days of data they were measured on)
- Root cause: correct root node ranked **#1 in ≥ 80%** of multi-cell incidents; alarm-to-incident compression ratio reported
- Incident mislabel rate **≤ 10%**, reported per category, on test weeks and unseen report templates
- The promoted detector beats the simplest baseline (seasonal z-score) on the validation objective; the comparison table is in the README
- Ops agent: **≥ 80%** correct on a fixed eval set of ≥ 20 investigation questions, every answer citing evidence; 100% of close/escalate/re-route actions blocked without human approval (tested)
- One command from a fresh clone reproduces the full demo, after the documented one-time manual data download *(changed 2026-11-14: Harvard Dataverse requires a guestbook form with an email for every download, so the download can't be automated without sending personal details)*; every reported number traces to a data snapshot, config, seed, and model version in MLflow; a dead detector alerts; known limits (including all synthetic components) are written in the README; demo video, screenshots, and the engagement write-up are in the repo
