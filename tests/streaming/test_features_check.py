from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pytest

from producer import replay
from streaming import features
from streaming import features_check as fc
from tests.factories import telemetry_frame
from tests.streaming.conftest import flat_baseline


def test_main_reports(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    frame = telemetry_frame(n_intervals=4)
    monkeypatch.setattr(fc.baseline, "load", lambda _d: flat_baseline(["CELL-1-1"]))
    days = {date(2013, 12, 2): Path("stub"), date(2013, 12, 3): Path("stub")}
    monkeypatch.setattr(fc.data_prep, "processed_days", lambda *_a: days)
    monkeypatch.setattr(
        fc.replay, "telemetry_for", lambda *_a, **_k: replay.Telemetry(frame, [], n_faults=2)
    )
    with caplog.at_level(logging.INFO):
        assert fc.main(["--split", "validation", "--inject"]) == 0
    assert "validation (2 days)" in caplog.text
    assert (
        f"4 rows x {len(features.feature_columns(features.load_config()))} features" in caplog.text
    )
    assert "2 SYNTHETIC faults" in caplog.text


def test_main_without_baseline_fails(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert fc.main(["--split", "train", "--data-dir", str(tmp_path)]) == 1
    assert "make baseline" in caplog.text


@pytest.mark.parametrize("argv", [["--split", "test"], ["--split", "train", "--inject"]])
def test_main_refuses_test_weeks_and_faulty_training(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        fc.main(argv)
