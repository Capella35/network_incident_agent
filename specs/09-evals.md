# 09 — Evals (how we check it)

How each bar in the Definition of done (`00-overview.md`) is measured. Recorded cases with known answers live in `evals/cases/`; measured numbers in `evals/results-log.md`.

## Rules
- **Splits are by time**, never random row splits: training weeks, then validation weeks (Optuna tuning and the promotion gate), then **test weeks, touched only once** for the final numbers.
- Test faults use a **separate injector seed** and fault locations, timings, and magnitudes drawn from parameters disjoint from training.
- The classifier is tested on **report templates it never saw**.
- **Matching rule:** a detected incident counts as a hit if it overlaps a ground-truth fault window (± one interval). Incidents with no matching fault are false alarms, labeled "no fault". A fault is "detected" when at least one incident matches it. Matching also requires the incident to share a cell with the fault (2026-11-25). **Detection delay:** intervals from onset to the first matching alarm; median by **Kaplan–Meier** with missed faults right-censored (at end + 1 interval, or the window end), reported with its 95% CI; faults whose every cell-interval is a data gap are unobservable and reported separately (`src/evaluation/metrics.py`).
- Incidents open only from detector output, never from ground truth.
- Every reported number traces to a data snapshot, config, seed, and model version in MLflow. Record each run in `results-log.md`.

## Quality bars
The bars are kept once, in `00-overview.md` → Definition of done (moved there 2026-11-22; this file used to hold a copy). Changing one requires a recorded reason next to it and in `DECISIONS.md` (Rule 5).

## Agent eval design (uses the domain model)

### Questions generated from ground truth + topology
From validation-week ground truth and the topology, templates generate questions with **known answers**:
- "What is the root node of INC-x?" → node ID
- "Which team should own INC-x?" → team
- "Which sites are affected by INC-x?" → set of IDs
- "Was there a change event before INC-x?" → yes/no + ChangeEvent ID
- **Negative:** "Is INC-x caused by a backhaul failure?" when it isn't → must answer no and cite the failing predicate
- **No-fault:** false-alarm incidents → must answer `no_fault` or "unknown", not invent a cause
- **Injection:** incidents whose report text contains instructions → must not follow them

A fixed, frozen set of **≥ 20** questions meets the Definition of done bar (`00-overview.md`). A larger generated set (e.g., 100) is run for extra signal **(default)**. Test-week questions are generated once, for the final numbers.

### Grading is deterministic
Answers are structured, so grading is exact match on IDs and enums plus set overlap for affected entities. No LLM judge is needed for the headline score, which keeps it cheap (`08-guardrails.md` C7) and reproducible.

### Extra metrics the model makes possible (reported, not bars)
| Metric | Meaning |
|||
| Hallucinated-entity rate | Share of answers citing an ID that doesn't exist in the graph |
| Constraint-violation rate | Share of answers (or classifier predictions) breaking a consistency constraint |
| Ruled-out accuracy | Share of "ruled out" hypotheses that really don't match ground truth |
| Signature agreement | How often correlator, classifier, and agent agree on fault type |
| Unverified rate | Share of answers that failed code validation after the retry |

These are reported next to the Definition of done bars in `evals/results-log.md`; they don't change the bars.
