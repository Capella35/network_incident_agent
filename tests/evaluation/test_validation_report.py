"""Milestone 2 validation report (step 11): champion vs z-score baseline, on the one-cell
fixture with a throwaway MLflow store (no Milan files)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import mlflow
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run

from evaluation import report, validation_report
from models import promote, tune
from streaming import offline
from tests.models.conftest import (
    REFERENCE,
    FakeScore,
    logged_study,
    outage_features,
    use_validation,
)


@pytest.fixture
def promoted(local_mlflow: None, monkeypatch: pytest.MonkeyPatch) -> Run:
    """A z-score study (threshold 5) promoted to champion and still the latest feasible study,
    returned; validation features = the outage fixture, scored by `FakeScore`."""
    frame = outage_features()
    use_validation(monkeypatch, frame)
    study = logged_study(5.0)
    monkeypatch.setattr(tune, "feasible_studies", lambda: {"zscore": study})
    monkeypatch.setattr(report, "score", FakeScore(frame))
    assert promote.promote(Path(), "zscore").promoted
    return study


@pytest.mark.usefixtures("promoted")
def test_the_report_compares_champion_and_baseline_and_logs_one_run() -> None:
    md = validation_report.run(Path())
    assert "| detector v1 (champion) | 1/1 |" in md
    assert "| zscore (baseline, study " in md
    assert md.count("# Per fault type:") == 2
    assert "SYNTHETIC" in md
    (run,) = mlflow.search_runs(
        experiment_names=[report.EXPERIMENT],
        filter_string="tags.role = 'validation-report'",
        output_format="list",
    )
    assert run.data.tags["champion_version"] == "1"
    assert run.data.metrics["detected"] == run.data.metrics["baseline.detected"] == 1
    logged = {a.path for a in MlflowClient().list_artifacts(run.info.run_id)}
    assert logged == {"report.md", "champion_fault_outcomes.csv", "baseline_fault_outcomes.csv"}


def test_a_champion_packaged_with_other_features_is_refused(
    promoted: Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    refit = replace(REFERENCE, load_curve=REFERENCE.load_curve.head(0))  # a reference refitted
    monkeypatch.setattr(
        tune.offline,
        "features_for",
        lambda *_a, **_k: offline.OfflineFeatures(outage_features(), (), 1, refit),
    )
    with pytest.raises(ValueError, match=r"packaged with another features\.yaml"):
        validation_report.score(Path(), promoted)


def test_no_feasible_baseline_is_refused(
    local_mlflow: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tune, "feasible_studies", dict)
    with pytest.raises(ValueError, match="no feasible tuning study for the baseline"):
        validation_report.run(Path())


def test_a_champion_promoted_on_other_validation_data_is_refused(
    promoted: Run, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(tune.data_prep, "snapshot", lambda _d, _days: "reprocessed")
    with pytest.raises(ValueError, match=r"v1 was promoted on .*'snap'.*promote again"):
        validation_report.score(Path(), promoted)
