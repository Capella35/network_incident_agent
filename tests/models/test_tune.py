from __future__ import annotations

from datetime import date
from pathlib import Path

import mlflow
import polars as pl
import pytest
from mlflow.entities import Run
from pydantic import ValidationError

from common import process
from evaluation import report
from models import detectors as det
from models import tune
from streaming import offline
from tests.factories import flat_baseline
from tests.models.conftest import DAY, FakeScore, result_of


def test_objective_is_lexicographic_detected_then_delay() -> None:
    v = tune.objective_value
    assert v(result_of(2, None, 0)) > v(result_of(1, 0, 0))  # one more fault beats any delay
    assert (
        v(result_of(1, 0, 0)) > v(result_of(1, 3, 0)) > v(result_of(1, None, 0))
    )  # not reached: worst
    assert v(result_of(1, 3, 0)) == v(result_of(1, 3, 50))  # false incidents: the constraint's job


def _cfg(n_trials: int = 3, **search: dict[str, tune.IntRange]) -> tune.Config:
    return tune.Config(
        budget=tune.Budget(n_trials=n_trials, timeout_s=60),
        max_false_incidents_per_day=1.0,
        alarm_share=(1e-4, 5e-2),
        search={"zscore": {}, "iforest": {}, "autoencoder": {}} | search,
    )


def test_config_checks_candidates_and_parameter_names() -> None:
    assert tune.load_config().budget.n_trials > 0  # the shipped tune.yaml is valid
    with pytest.raises(
        ValidationError, match=r"search.iforest: not tunable parameters: \['hidden'\]"
    ):
        _cfg(iforest={"hidden": tune.IntRange(low=1, high=2)})
    with pytest.raises(ValidationError, match="max_train_rows"):  # sampled once per study
        _cfg(iforest={"max_train_rows": tune.IntRange(low=1, high=2)})
    with pytest.raises(ValidationError, match="one entry per candidate"):
        tune.Config(
            budget=tune.Budget(n_trials=1, timeout_s=1),
            max_false_incidents_per_day=1.0,
            alarm_share=(1e-4, 1e-2),
            search={"zscore": {}},
        )
    with pytest.raises(ValidationError, match="low 5 > high 2"):
        tune.IntRange(low=5, high=2)


def test_build_applies_validated_overrides() -> None:
    cfg = det.load_config()
    assert det.build("iforest", cfg, {"n_estimators": 7}).params()["n_estimators"] == 7
    with pytest.raises(ValidationError):
        det.build("iforest", cfg, {"n_estimators": 0})


def test_load_study_data_keeps_only_the_training_sample(
    monkeypatch: pytest.MonkeyPatch, train: pl.DataFrame, normal_with_outage: pl.DataFrame
) -> None:
    def fake(_d: Path, days: list[date], *, inject: bool = False) -> offline.OfflineFeatures:
        return offline.OfflineFeatures(
            normal_with_outage if inject else train, (), 0, flat_baseline(["CELL-1-1"])
        )

    monkeypatch.setattr(tune.data_prep, "require_processed_days", lambda _d, s: {DAY: Path()})
    monkeypatch.setattr(tune.data_prep, "snapshot", lambda _d, _days: "snap")
    monkeypatch.setattr(tune.offline, "features_for", fake)
    cfg = det.load_config()
    iforest = tune.load_study_data(Path(), "iforest")
    rows = cfg.candidates.iforest.max_train_rows
    assert iforest.train.height == min(rows, train.filter(~pl.col("data_gap")).height)
    assert iforest.gaps.columns == ["cell_id", "interval_start"]
    assert tune.load_study_data(Path(), "zscore").train.height == 0  # nothing to fit


@pytest.fixture
def study(
    local_mlflow: None,
    monkeypatch: pytest.MonkeyPatch,
    train: pl.DataFrame,
    normal_with_outage: pl.DataFrame,
) -> pl.DataFrame:
    """Fixture data instead of Milan files; returns the validation frame."""
    data = tune.StudyData(
        train=train,
        validation=normal_with_outage,
        gaps=report.gaps(normal_with_outage),
        days=[DAY],
        snapshot="snap",
    )
    monkeypatch.setattr(tune, "load_study_data", lambda _d, _name: data)
    return normal_with_outage


def _use(monkeypatch: pytest.MonkeyPatch, cfg: tune.Config, score: FakeScore) -> None:
    monkeypatch.setattr(tune, "load_config", lambda: cfg)
    monkeypatch.setattr(tune.report, "score", score)


def _runs(filter_string: str) -> list[Run]:
    return mlflow.search_runs(
        experiment_names=[tune.EXPERIMENT], filter_string=filter_string, output_format="list"
    )


def test_study_runs_its_budget_and_logs_the_best_feasible_model(
    study: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _cfg().model_copy(update={"alarm_share": (0.005, 0.01)})  # 2 of 288 rows: the outage
    score = FakeScore(study)
    _use(monkeypatch, cfg, score)
    builds: list[str] = []
    real_build = det.build
    monkeypatch.setattr(det, "build", lambda *a: builds.append(a[0]) or real_build(*a))
    best = tune.run_study(Path("unused"), "zscore")
    assert score.calls == 3 and builds == ["zscore"]  # same (no) parameters: fitted once
    assert best is not None and best.result.overall.detected == 1
    assert best.result.false_per_day <= 1.0
    (parent,) = _runs("tags.role = 'study'")
    assert parent.data.tags["feasible"] == "yes" and parent.data.tags["data_snapshot"] == "snap"
    assert parent.data.metrics["trials"] == 3.0
    assert parent.data.params["best_trial"] == str(best.trial)
    trials = _runs(f"tags.mlflow.parentRunId = '{parent.info.run_id}'")
    assert len(trials) == 3
    assert {t.data.tags["data_snapshot"] for t in trials} == {"snap"}  # provenance per trial
    model = mlflow.pyfunc.load_model(f"runs:/{parent.info.run_id}/model")
    assert model.unwrap_python_model().threshold == best.threshold
    assert model.predict(study.to_pandas())["alarm"].sum() >= 1


def test_infeasible_study_logs_no_model(
    study: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _cfg(n_trials=2, iforest={"n_estimators": tune.IntRange(low=10, high=30)})
    _use(monkeypatch, cfg, FakeScore(study, false_incidents=5))
    assert tune.run_study(Path("unused"), "iforest") is None
    (parent,) = _runs("tags.role = 'study'")
    assert parent.data.tags["feasible"] == "no" and "objective" not in parent.data.metrics
    trials = _runs(f"tags.mlflow.parentRunId = '{parent.info.run_id}'")
    assert {int(t.data.params["n_estimators"]) for t in trials} <= set(range(10, 31))
    with pytest.raises(mlflow.exceptions.MlflowException):
        mlflow.pyfunc.load_model(f"runs:/{parent.info.run_id}/model")


def test_over_memory_budget_fails_before_the_model_is_logged(
    study: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use(monkeypatch, _cfg(n_trials=1), FakeScore(study))
    monkeypatch.setattr(process, "peak_rss_mib", lambda: 1e9)
    with pytest.raises(process.OverBudgetError):
        tune.run_study(Path("unused"), "zscore")
    (parent,) = _runs("tags.role = 'study'")
    assert parent.info.status == "FAILED"
    assert tune.latest_studies() == {}  # a failed study is never compared or promoted


def test_compare_tables_the_latest_finished_study_per_candidate(
    study: pl.DataFrame, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use(monkeypatch, _cfg(n_trials=2), FakeScore(study, false_incidents=5))
    tune.run_study(Path("unused"), "zscore")
    table = tune.compare()
    assert "| zscore | no | n/a | n/a [n/a, n/a] | n/a | 2 |" in table
    assert "iforest" not in table
    (logged,) = _runs("tags.role = 'comparison'")
    assert mlflow.artifacts.load_text(f"runs:/{logged.info.run_id}/comparison.md") == table


def test_main_reports_errors(local_mlflow: None, tmp_path: Path) -> None:
    assert tune.main(["--compare"]) == 1  # nothing logged yet
    assert tune.main(["--data-dir", str(tmp_path), "--candidate", "zscore"]) == 1  # no data
