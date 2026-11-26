# evaluation/

Matching incidents to ground truth, metrics, and results report. The only package that reads the
ground-truth fault log (Rule 6, `tests/test_import_boundaries.py`).

- `ground_truth.py`: reads the SYNTHETIC fault schedule `make inject` wrote, with its fingerprint.
- `metrics.py`: provisional M2 incident grouping, matching (± 1 interval, shared cell), detection
  rate, Kaplan-Meier median delay with CI, false incidents per day.
- `report.py`: `make evaluate`: scores validation days with the z-score baseline at a threshold,
  renders the Markdown report, logs it with metrics and per-fault outcomes to MLflow.
