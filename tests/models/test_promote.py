"""Promotion gate, packaging, and rollback (milestone 2, step 7), on the one-cell fixture."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import mlflow
import polars as pl
import pytest
from mlflow import MlflowClient
from mlflow.entities import Run
from mlflow.exceptions import MlflowException
from mlflow.protos.databricks_pb2 import TEMPORARILY_UNAVAILABLE

from models import promote, tracking, tune
from streaming import features
from tests.models.conftest import REFERENCE, FakeScore, logged_study, use_validation

CAP = 1.0
Promote = Callable[..., promote.Decision]


def _n(detected: int, false_per_day: float = 0.5, *, schedule: str = "fp") -> promote.Numbers:
    return promote.Numbers(
        detected=detected,
        observable=57,
        false_per_day=false_per_day,
        km_median=3,
        days="2013-12-02..2013-12-15",
        fault_schedule=schedule,
        snapshot="snap",
    )


@pytest.mark.parametrize(
    ("candidate", "incumbent", "promoted", "why"),
    [
        (_n(41, 0.29), None, True, "first champion"),
        (_n(41, 1.01), None, False, "cap"),
        (_n(45, 1.5), _n(41), False, "cap"),  # more faults never buys back the cap
        (_n(41, 0.29), _n(41), False, "not more"),  # a tie keeps the incumbent
        (_n(40, 0.0), _n(41), False, "not more"),
        (_n(42, 1.0), _n(41), True, "42 > incumbent's 41"),  # at the cap is within it
        (_n(50), _n(41, schedule="other"), False, "not comparable"),
    ],
)
def test_gate_needs_the_cap_and_more_faults_than_the_incumbent(
    candidate: promote.Numbers, incumbent: promote.Numbers | None, promoted: bool, why: str
) -> None:
    verdict = promote.gate(candidate, incumbent, CAP)
    assert verdict.promote is promoted
    assert why in verdict.reason


def test_numbers_round_trip_through_version_tags() -> None:
    not_reached = promote.Numbers(3, 5, 0.0, None, "d..d", "fp", "snap")
    for n in (_n(41), not_reached):
        assert promote.Numbers.from_tags({**n.tags(), "other": "x"}) == n
    with pytest.raises(ValueError, match="validation tag"):
        promote.Numbers.from_tags({"validation.detected": "1"})


@pytest.fixture
def promote_with(
    local_mlflow: None, monkeypatch: pytest.MonkeyPatch, normal_with_outage: pl.DataFrame
) -> Promote:
    """`promote_with(threshold, false=0, study=None)`: log a tuned z-score study (or reuse
    `study`) as the only feasible one, score it on the outage fixture with `false` false
    incidents, and run the gate. The validation features claim `REFERENCE` as their baseline."""
    use_validation(monkeypatch, normal_with_outage)
    studies: dict[str, Run] = {}
    monkeypatch.setattr(tune, "feasible_studies", lambda: studies)

    def run(
        threshold: float = 5.0, *, false: int = 0, study: Run | None = None
    ) -> promote.Decision:
        studies["zscore"] = study or logged_study(threshold)
        monkeypatch.setattr(
            tune.report, "score", FakeScore(normal_with_outage, false_incidents=false)
        )
        return promote.promote(Path(), "zscore")

    return run


def _runs(role: str) -> list[Run]:
    return mlflow.search_runs(
        experiment_names=[promote.EXPERIMENT],
        filter_string=f"tags.role = '{role}'",
        order_by=["attributes.start_time ASC"],
        output_format="list",
    )


def _champion() -> str:
    version = promote.champion(MlflowClient())
    assert version is not None
    return str(version.version)


def test_promote_packages_registers_and_rolls_back(promote_with: Promote) -> None:
    client = MlflowClient()
    # 1: catches nothing but is within the cap: the first champion
    decision = promote_with(1e9)
    assert decision.promoted and "**Promoted:** first champion" in decision.report
    first = promote.champion(client)
    assert first is not None and first.tags["validation.detected"] == "0"
    assert first.tags["validation.snapshot"] == "snap"
    package = tracking.load_package(f"models:/{promote.MODEL}@{promote.ALIAS}")
    assert package.tuned.threshold == 1e9
    assert package.spec.config == features.load_config()  # the feature config travels with it
    assert package.spec.reference.seasonal.equals(REFERENCE.seasonal)  # the one scored with
    # 2: catches the outage: more than the incumbent's 0, so it replaces it
    promote_with(5.0)
    assert _champion() == "2"
    assert client.get_model_version(promote.MODEL, "2").tags[promote.PREVIOUS] == "1"
    # 3: the same numbers from another study are a tie: refused, nothing registered
    decision = promote_with(5.0)
    assert not decision.promoted
    assert "**Not promoted:** catches 1, not more than the incumbent's 1" in decision.report
    assert len(client.search_model_versions(f"name='{promote.MODEL}'")) == 2
    runs = _runs("promotion")
    assert [r.data.tags["verdict"] for r in runs] == ["promoted", "promoted", "refused"]
    assert [r.data.tags.get("registered_version") for r in runs] == ["1", "2", None]
    assert _runs("promotion")[0].data.metrics["peak_rss_mib"] > 0
    # rollback restores the version the champion replaced, is logged, then refuses to go further
    assert "from version 2 to 1" in promote.rollback()
    assert _champion() == "1"
    assert _runs("rollback")[0].data.tags["to_version"] == "1"
    assert client.get_model_version(promote.MODEL, "2").tags[promote.ROLLED_BACK] == "yes"
    with pytest.raises(ValueError, match="replaced no earlier"):
        promote.rollback()
    # the rolled-back study cannot come straight back
    rolled_back = mlflow.get_run(client.get_model_version(promote.MODEL, "2").tags["study"])
    with pytest.raises(ValueError, match="was rolled back"):
        promote_with(study=rolled_back)
    # a newly tuned study replaces the restored champion, not the rolled-back one
    promote_with(5.0)
    assert _champion() == "3"
    assert client.get_model_version(promote.MODEL, "3").tags[promote.PREVIOUS] == "1"


def test_promote_refuses_a_candidate_over_the_cap(promote_with: Promote) -> None:
    decision = promote_with(5.0, false=2)  # 2 per day
    assert "**Not promoted:** 2.00 false incidents/day > cap 1" in decision.report
    assert promote.champion(MlflowClient()) is None
    assert [r.data.tags["verdict"] for r in _runs("promotion")] == ["refused"]


def test_promote_refuses_the_champion_study_before_reading_data(
    promote_with: Promote, monkeypatch: pytest.MonkeyPatch
) -> None:
    promote_with(1e9)
    (run,) = _runs("promotion")
    study = mlflow.get_run(run.data.tags["study"])

    def no_data(_d: Path) -> None:
        raise AssertionError("validation data read")

    monkeypatch.setattr(tune, "load_validation", no_data)
    with pytest.raises(ValueError, match="already champion"):
        promote_with(study=study)


def test_champion_raises_registry_errors_other_than_a_missing_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(_self: MlflowClient, _name: str) -> None:
        raise MlflowException("server busy", error_code=TEMPORARILY_UNAVAILABLE)

    monkeypatch.setattr(MlflowClient, "get_registered_model", unavailable)
    with pytest.raises(MlflowException, match="server busy"):
        promote.champion(MlflowClient())


def test_promote_refuses_a_stale_or_missing_study(promote_with: Promote) -> None:
    stale = logged_study(5.0, features_json='{"other": 1}')
    with pytest.raises(ValueError, match=r"another features\.yaml"):
        promote_with(study=stale)
    with pytest.raises(ValueError, match="no feasible tuning study"):
        promote.promote(Path(), "autoencoder")


def test_load_champion_downloads_each_version_once(
    promote_with: Promote, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    promote_with(1e9)
    cache = tmp_path / "cache"
    first = tracking.load_champion(MlflowClient(), cache=cache)
    (copy,) = cache.iterdir()  # one copy, no download folder left behind
    assert (copy / "MLmodel").exists()

    def no_download(**_kw: object) -> str:
        raise AssertionError("downloaded again")

    monkeypatch.setattr(mlflow.artifacts, "download_artifacts", no_download)
    again = tracking.load_champion(MlflowClient(), cache=cache)
    assert again.version == first.version
    assert again.package.tuned.threshold == first.package.tuned.threshold


def test_local_copy_leaves_nothing_when_the_download_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*, dst_path: str, **_kw: object) -> str:
        Path(dst_path).mkdir(parents=True)
        (Path(dst_path) / "MLmodel").write_text("half")
        raise OSError("connection reset")

    monkeypatch.setattr(mlflow.artifacts, "download_artifacts", broken)
    with pytest.raises(OSError, match="connection reset"):
        tracking.local_copy("models:/detector/1", tmp_path / "1-run")
    assert list(tmp_path.iterdir()) == []


def test_local_copy_keeps_a_copy_another_process_finished_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "1-run"

    def racing(*, dst_path: str, **_kw: object) -> str:
        target.mkdir()
        (target / "MLmodel").write_text("theirs")
        Path(dst_path).mkdir()
        (Path(dst_path) / "MLmodel").write_text("ours")
        return dst_path

    monkeypatch.setattr(mlflow.artifacts, "download_artifacts", racing)
    assert tracking.local_copy("models:/detector/1", target) == target
    assert (target / "MLmodel").read_text() == "theirs"
    assert [p.name for p in tmp_path.iterdir()] == [target.name]


def test_load_package_refuses_a_tuning_model_without_features(local_mlflow: None) -> None:
    run = logged_study(5.0)
    with pytest.raises(ValueError, match="no feature spec"):
        tracking.load_package(f"runs:/{run.info.run_id}/model")


def test_main_exits_2_when_the_gate_refuses(
    promote_with: Promote, monkeypatch: pytest.MonkeyPatch
) -> None:
    decisions = iter(
        [promote.Decision("ok", promoted=True), promote.Decision("no", promoted=False)]
    )
    monkeypatch.setattr(promote, "promote", lambda *_a: next(decisions))
    assert promote.main(["--candidate", "zscore"]) == 0
    assert promote.main(["--candidate", "zscore"]) == promote.REFUSED


def test_main_reports_a_rollback_without_champion(
    local_mlflow: None, caplog: pytest.LogCaptureFixture
) -> None:
    assert promote.main(["--rollback"]) == 1
    assert "no champion" in caplog.text
