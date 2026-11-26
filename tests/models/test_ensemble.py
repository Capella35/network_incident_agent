from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import mlflow
import polars as pl
import pytest

from models import ensemble, tune
from tests.models.conftest import FakeScore, logged_study, result_of, use_validation

T = datetime(2013, 12, 2, tzinfo=UTC)


def _alarms(rows: list[tuple[str, int, float, str]]) -> pl.DataFrame:
    """`detectors.alarms`-shaped rows: (cell, interval index, score, top KPI)."""
    return pl.DataFrame(
        {
            "cell_id": [c for c, *_ in rows],
            "site_id": ["SITE-1"] * len(rows),
            "interval_start": [T + timedelta(minutes=10 * i) for _, i, *_ in rows],
            "score": [s for *_, s, _ in rows],
            "kpis": [[k] for *_, k in rows],
        }
    )


def test_union_keeps_each_alarm_once_from_the_member_furthest_above_its_threshold() -> None:
    a = _alarms([("C1", 0, 0.8, "latency_ms"), ("C1", 1, 0.9, "latency_ms")])  # threshold 0.7
    b = _alarms([("C1", 1, 60.0, "traffic_sms"), ("C2", 1, 40.0, "load_ratio")])  # threshold 30
    out = ensemble.union([(a, 0.7), (b, 30.0)])
    assert out.columns == a.columns
    rows = [(r["cell_id"], r["score"], r["kpis"]) for r in out.iter_rows(named=True)]
    assert rows == [
        ("C1", pytest.approx(0.8 / 0.7), ["latency_ms"]),  # only a
        ("C1", pytest.approx(2.0), ["traffic_sms"]),  # b: 60/30 = 2.0 beats a: 0.9/0.7
        ("C2", pytest.approx(40 / 30), ["load_ratio"]),  # only b
    ]


def test_union_tie_goes_to_the_earlier_member() -> None:
    a = _alarms([("C1", 0, 2.0, "latency_ms")])
    b = _alarms([("C1", 0, 4.0, "traffic_sms")])
    assert ensemble.union([(a, 1.0), (b, 2.0)])["kpis"].to_list() == [["latency_ms"]]


def test_run_measures_each_member_and_the_union(
    local_mlflow: None, monkeypatch: pytest.MonkeyPatch, normal_with_outage: pl.DataFrame
) -> None:
    studies = {"strict": logged_study(20.0), "loose": logged_study(5.0)}
    monkeypatch.setattr(tune, "feasible_studies", lambda: studies)
    use_validation(monkeypatch, normal_with_outage)
    score = FakeScore(normal_with_outage)
    monkeypatch.setattr(tune.report, "score", score)
    md = ensemble.run(Path())
    assert score.calls == 3  # each member alone, then the union
    names = [line.split("|")[1].strip() for line in md.splitlines()[2:]]
    assert names == ["loose", "strict", "union(loose+strict)"]
    (logged,) = mlflow.search_runs(
        experiment_names=[tune.EXPERIMENT],
        filter_string="tags.role = 'ensemble'",
        output_format="list",
    )
    assert logged.data.tags["data_snapshot"] == "snap"
    assert logged.data.tags["members"].startswith("loose:")
    assert logged.data.metrics["peak_rss_mib"] > 0


def test_union_refuses_a_threshold_at_or_below_zero() -> None:
    a = _alarms([("C1", 0, 2.0, "latency_ms")])
    with pytest.raises(ValueError, match="> 0"):
        ensemble.union([(a, 1.0), (a, 0.0)])


@pytest.mark.parametrize(
    "stale",
    [
        {"features_json": '{"other": 1}', "fingerprint": "fp", "match": "features.yaml"},
        {"features_json": None, "fingerprint": "another", "match": "fault schedule"},
    ],
)
def test_score_study_refuses_a_stale_study(
    local_mlflow: None,
    monkeypatch: pytest.MonkeyPatch,
    normal_with_outage: pl.DataFrame,
    stale: dict[str, str | None],
) -> None:
    study = logged_study(5.0, features_json=stale["features_json"])
    use_validation(monkeypatch, normal_with_outage)
    monkeypatch.setattr(
        tune.report, "score", lambda *_a, **_k: (result_of(0, None, 0), stale["fingerprint"])
    )
    with pytest.raises(ValueError, match=str(stale["match"])):
        tune.score_study(Path(), study, tune.load_validation(Path()))


def test_run_needs_two_feasible_members(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(tune, "feasible_studies", dict)
    assert ensemble.main(["--data-dir", "unused"]) == 1
    assert "need >= 2 feasible studies" in caplog.text
