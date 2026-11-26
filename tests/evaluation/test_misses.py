from __future__ import annotations

from pathlib import Path

import mlflow
import polars as pl
import pytest

from evaluation import ground_truth, metrics, misses
from models import tune
from tests.evaluation.conftest import DAY, FAULTS, INDEX, alarms, gaps, t
from tests.models.conftest import logged_study, normal_rows, to_features, use_validation


@pytest.fixture(scope="module")
def result() -> metrics.Result:
    return metrics.evaluate(alarms(), FAULTS, days=[DAY], index=INDEX, gaps=gaps())


def _rows() -> pl.DataFrame:
    """Scores and bad z during F1 (detected) and F4 (missed); nothing on F2, F3, F6, F7's
    other cells is needed except what is listed."""
    pts = [
        ("CELL-1-1", 11, 9.0, 20.0, 1.0),  # F1: alarmed
        ("CELL-3-1", 81, 1.0, 0.5, 7.0),  # F4: load high, score below threshold
        ("CELL-3-1", 82, 2.0, 0.2, 3.0),
        ("CELL-3-1", 86, 8.0, 0.0, 9.0),  # after F4's match window [79, 85): ignored
    ]
    return pl.DataFrame(
        {
            "cell_id": [p[0] for p in pts],
            "interval_start": [t(p[1]) for p in pts],
            "score": [p[2] for p in pts],
            "traffic_internet": [p[3] for p in pts],
            "load_ratio": [p[4] for p in pts],
        }
    )


def test_per_fault_joins_outcome_duration_deviation_and_margin(result: metrics.Result) -> None:
    table = misses.per_fault(result, FAULTS, _rows(), threshold=4.0)
    assert "F5" not in table["fault_id"].to_list()  # all gaps: unobservable
    f1 = table.row(by_predicate=pl.col("fault_id") == "F1", named=True)
    assert (f1["detected"], f1["delay"], f1["duration"], f1["cells"]) == (True, 1, 3, 1)
    assert (f1["worst_kpi"], f1["worst_bad"], f1["peak_ratio"]) == ("traffic_internet", 20.0, 2.25)
    f4 = table.row(by_predicate=pl.col("fault_id") == "F4", named=True)
    assert (f4["detected"], f4["delay"], f4["duration"]) == (False, None, 4)
    assert (f4["worst_kpi"], f4["worst_bad"], f4["peak_ratio"]) == ("load_ratio", 7.0, 0.5)
    f6 = table.row(by_predicate=pl.col("fault_id") == "F6", named=True)
    assert (f6["worst_kpi"], f6["worst_bad"], f6["peak_ratio"]) == (None, None, None)  # no rows


def test_render_breaks_misses_down_and_lists_them(result: metrics.Result) -> None:
    table = misses.per_fault(result, FAULTS, _rows(), threshold=4.0)
    md = misses.render(table, result, title="T")
    assert "Faults detected: 5 of 6 observable; 3.00 false incidents/day." in md
    assert "median 1, worst 6" in md
    assert "| congestion | 1 / 2 |" in md  # F4 missed, F6 caught
    assert "| < 6 int | 1 / 3 |" in md  # durations F1 3, F2 10, F3 6, F4 4, F6 6, F7 3
    assert "| 6-12 | 1 / 1 |" in md  # F4's worst bad z 7
    buckets = [
        line.split("|")[1].strip() for line in md.split("| worst bad z |")[1].splitlines()[2:4]
    ]
    assert buckets == ["6-12", ">= 12"]  # numeric order (F4 7, F1 20), not string order
    assert "| F4 | congestion | 1 | 4 | load_ratio | 7.00 | 0.50 |" in md


def test_caught_delays(result: metrics.Result) -> None:
    assert metrics.caught_delays(result.outcomes) == (1, 6)  # F1 1, F2 0, F3 6, F6 1, F7 0
    missed = [o for o in result.outcomes if not o.detected]
    assert metrics.caught_delays(missed) == (None, None)


def test_per_fault_refuses_a_threshold_at_or_below_zero(result: metrics.Result) -> None:
    with pytest.raises(ValueError, match="> 0"):
        misses.per_fault(result, FAULTS, _rows(), threshold=0.0)


def test_diagnose_scores_the_study_model_and_logs(
    local_mlflow: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    study = logged_study(5.0)
    use_validation(monkeypatch, to_features(normal_rows()))
    res = metrics.evaluate(alarms(), FAULTS, days=[DAY], index=INDEX, gaps=gaps())
    monkeypatch.setattr(tune.report, "score", lambda *_a, **_k: (res, "fp"))
    monkeypatch.setattr(
        misses.ground_truth, "read", lambda *_a: ground_truth.GroundTruth(FAULTS, "fp")
    )
    md = misses.diagnose(Path(), "zscore", study, tune.load_validation(Path()))
    run_id = study.info.run_id
    assert md.startswith(f"## Missed faults: zscore (study {run_id[:8]})")
    (logged,) = mlflow.search_runs(
        experiment_names=["detector-evaluation"],
        filter_string="tags.role = 'misses'",
        output_format="list",
    )
    assert logged.data.tags["study"] == run_id
    assert logged.data.tags["fault_schedule_fingerprint"] == "fp"


def test_main_without_a_feasible_study(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(tune, "feasible_studies", dict)
    assert misses.main(["--data-dir", "unused"]) == 1
    assert "no feasible study" in caplog.text
