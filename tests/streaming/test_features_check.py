from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pytest

from streaming import features, offline
from streaming import features_check as fc
from tests.factories import flat_baseline, telemetry_frame

CFG = features.load_config()


def test_main_reports(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    days = {date(2013, 12, 2): Path("stub"), date(2013, 12, 3): Path("stub")}
    monkeypatch.setattr(fc.data_prep, "processed_days", lambda *_a: days)
    frame = features.compute(telemetry_frame(n_intervals=4), flat_baseline(["CELL-1-1"]), CFG)
    seen: list[object] = []

    def fake(_d: Path, ds: list[date], *, inject: bool) -> offline.OfflineFeatures:
        seen.append((ds, inject))
        return offline.OfflineFeatures(
            frame, (date(2013, 12, 1),), n_faults=2, reference=flat_baseline(["CELL-1-1"])
        )

    monkeypatch.setattr(fc.offline, "features_for", fake)
    with caplog.at_level(logging.INFO):
        assert fc.main(["--split", "validation", "--inject"]) == 0
    assert seen == [(list(days), True)]
    assert "validation (2 days + 1 history) + 2 SYNTHETIC faults" in caplog.text
    assert f"4 rows x {len(features.feature_columns(CFG))} features" in caplog.text


def test_main_without_processed_days(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert fc.main(["--split", "train", "--data-dir", str(tmp_path)]) == 1
    assert "no processed train days" in caplog.text


@pytest.mark.parametrize("argv", [["--split", "test"], ["--split", "train", "--inject"]])
def test_main_refuses_test_weeks_and_faulty_training(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        fc.main(argv)
