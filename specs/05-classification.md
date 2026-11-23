# 05 — Classification

Labels each incident with **fault category, severity, and owning team**. Built in Milestone 4 (`milestones/milestone-4.md`). Drawn from `PROJECT.md` → What is being built (item 7) and `README.md` → Models; details still open are marked **(M4)**.

## Behavior
- **XGBoost** classifier (`src/models/`) predicts `fault_category`, `severity`, and `owning_team` for each incident (fields in `04-correlation.md`). Category, severity, and team values come from the domain model (`01-domain-model.md` §4), never literals.
- **Features:** blast radius (cells, sites, aggregation nodes affected), topology level of the root node, KPI deltas, onset shape (sudden vs. gradual), time since the last change event, signature predicate results, and TF-IDF features from the noisy report.
- **Constraint check:** a prediction that conflicts with the root level or the severity/team rule is flagged, not silently shown (`01-domain-model.md` §4).
- Tuned with Optuna on validation weeks, tracked and registered in MLflow, with `classifier_model_version` stored on the incident.
- **Human corrections** from the console (`07-human-gate.md`) are stored as labeled data for the next **reviewed** retrain; retraining is never automatic.

## Noisy reports (synthetic)
Each incident gets a human-style description that is vague and sometimes wrong at first. Templates are synthetic and labeled so. **The test set uses report templates the classifier never saw.** Template design **(M4)**.

## Quality bar
Incident mislabel rate **≤ 10%**, reported per category, on test weeks and unseen report templates (`00-overview.md`, `09-evals.md`).
