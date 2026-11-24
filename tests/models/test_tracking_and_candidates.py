from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import mlflow
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from common import splits
from models import candidates, tracking
from models import detectors as det
from streaming import offline

CFG = det.load_config()
VAL_DAY = splits.days(splits.Split.VALIDATION)[0]


@pytest.fixture
def local_mlflow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A throwaway MLflow file store (mlflow-skinny has no SQL store; the file store is opt-in)."""
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setenv("MLFLOW_DISABLE_AGENT_HINT", "1")
    mlflow.set_tracking_uri((tmp_path / "mlruns").as_uri())


def test_logged_model_scores_like_the_detector(
    local_mlflow: None, train: pl.DataFrame, normal_with_outage: pl.DataFrame
) -> None:
    detector = det.build("iforest", CFG).fit(train)
    with mlflow.start_run() as run:
        tracking.log_detector(detector, params={"x": 1}, metrics={"m": 2.0})
    logged = mlflow.get_run(run.info.run_id)
    assert logged.data.params["candidate"] == "iforest" and logged.data.params["x"] == "1"
    assert logged.data.params["n_estimators"] == str(CFG.candidates.iforest.n_estimators)
    assert logged.data.metrics["m"] == 2.0
    model = mlflow.pyfunc.load_model(f"runs:/{run.info.run_id}/model")
    loaded = pl.from_pandas(model.predict(normal_with_outage.to_pandas()))
    assert_frame_equal(loaded, detector.score(normal_with_outage), check_dtypes=False)


TRAIN_DAYS = {date(2013, 11, 4): Path("stub"), date(2013, 11, 5): Path("stub")}


def _fake_run_inputs(
    monkeypatch: pytest.MonkeyPatch, train: pl.DataFrame, day: pl.DataFrame
) -> list[tuple[list[date], bool]]:
    """Processed training days and offline features without data; returns the recorded calls."""
    seen: list[tuple[list[date], bool]] = []

    def fake(_d: Path, days: list[date], *, inject: bool = False) -> offline.OfflineFeatures:
        seen.append((days, inject))
        return offline.OfflineFeatures(day if inject else train, (), n_faults=int(inject))

    monkeypatch.setattr(candidates.data_prep, "require_processed_days", lambda *_a: TRAIN_DAYS)
    monkeypatch.setattr(candidates.offline, "features_for", fake)
    return seen


def test_run_trains_clean_scores_injected_and_logs(
    local_mlflow: None,
    monkeypatch: pytest.MonkeyPatch,
    train: pl.DataFrame,
    normal_with_outage: pl.DataFrame,
) -> None:
    seen = _fake_run_inputs(monkeypatch, train, normal_with_outage)
    m = candidates.run(Path("data"), "zscore", VAL_DAY)
    assert seen == [(list(TRAIN_DAYS), False), ([VAL_DAY], True)]  # train clean, score injected
    assert m["train_rows"] == train.height and m["scored_rows"] == normal_with_outage.height
    assert m["score_max"] >= m["score_p99"] >= m["score_p50"]
    runs = mlflow.search_runs(experiment_names=[tracking.EXPERIMENT], output_format="list")
    assert len(runs) == 1 and runs[0].data.metrics["train_rows"] == train.height
    assert runs[0].data.tags["synthetic_faults_on_score_day"] == "yes"


def test_run_over_budget_logs_nothing(
    local_mlflow: None,
    monkeypatch: pytest.MonkeyPatch,
    train: pl.DataFrame,
    normal_with_outage: pl.DataFrame,
) -> None:
    _fake_run_inputs(monkeypatch, train, normal_with_outage)
    monkeypatch.setattr(candidates, "peak_rss_mib", lambda: CFG.memory_budget_mib + 1.0)
    with pytest.raises(candidates.OverBudgetError, match="not logged"):
        candidates.run(Path("data"), "zscore", VAL_DAY)
    assert mlflow.search_experiments(filter_string=f"name = '{tracking.EXPERIMENT}'") == []


def test_run_refuses_a_non_validation_score_day() -> None:
    with pytest.raises(ValueError, match="only validation days"):
        candidates.run(Path("data"), "zscore", splits.days(splits.Split.TRAIN)[0])


def test_main_reports_metrics_or_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO):
        assert candidates.main(["--candidate", "zscore", "--data-dir", str(tmp_path)]) == 1
        assert "no processed train days" in caplog.text
        monkeypatch.setattr(candidates, "run", lambda *_a: {"peak_rss_mib": 1.0})
        assert candidates.main(["--candidate", "zscore"]) == 0
    assert "'peak_rss_mib': 1.0" in caplog.text
