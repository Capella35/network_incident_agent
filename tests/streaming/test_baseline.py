from __future__ import annotations

import json
import statistics
from collections.abc import Callable
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from producer import replay
from producer.kpi_model import CALIBRATION_FILE
from streaming import baseline
from streaming import features as ft
from tests.factories import telemetry_frame

CFG = ft.load_config()


def test_fit_one_row_per_bucket_with_mean_std_and_floor() -> None:
    # 12 intervals from T0 (2013-11-05 00:00 UTC = Tue 01:00 Europe/Rome) -> hours 1 and 2
    base = baseline.fit(telemetry_frame(n_cells=2, n_intervals=12), CFG)
    assert base.select(ft.BUCKET).rows() == [
        ("CELL-1-1", 2, 1),
        ("CELL-1-1", 2, 2),
        ("CELL-1-2", 2, 1),
        ("CELL-1-2", 2, 2),
    ]
    first = base.row(0, named=True)
    internet = [1 + i / 7 for i in range(6)]  # cell 1, hour 1
    assert first["traffic_internet_mean"] == pytest.approx(sum(internet) / 6)
    assert first["traffic_internet_std"] == pytest.approx(pl.Series(internet).std())
    # drop_rate is constant (std 0): floored at std_floor_rel x |mean|
    assert first["drop_rate_std"] == pytest.approx(0.002 * CFG.baseline.std_floor_rel)


def test_fit_leaves_out_gap_rows() -> None:
    base = baseline.fit(telemetry_frame(n_intervals=6, gaps=[0, 1]), CFG)
    internet = [1 + i / 7 for i in range(2, 6)]  # intervals 0 and 1 are gaps
    assert base["traffic_internet_mean"].to_list() == pytest.approx([statistics.mean(internet)])


def test_fit_refuses_non_training_days() -> None:
    frame = telemetry_frame(n_intervals=2).with_columns(
        pl.col("interval_start").dt.offset_by("27d")  # 2013-12-02: validation
    )
    with pytest.raises(ValueError, match="only train days"):
        baseline.fit(frame, CFG)


def _data_dir(tmp_path: Path, days: list[str]) -> Path:
    processed = tmp_path / "processed"
    (processed / "traffic").mkdir(parents=True)
    for d in days:
        (processed / "traffic" / f"{d}.parquet").write_bytes(b"stub")
    (processed / CALIBRATION_FILE).write_text("{}")
    (processed / "topology_nodes.csv").write_text("stub")
    return tmp_path


def test_run_writes_baseline_and_sidecar(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = _data_dir(tmp_path, ["2013-11-05"])
    seen: list[object] = []

    def fake_telemetry(_d: Path, days: list[object], **kw: object) -> replay.Telemetry:
        seen.append((days, kw))
        return replay.Telemetry(telemetry_frame(n_intervals=12), [])

    monkeypatch.setattr(baseline.replay, "telemetry_for", fake_telemetry)
    out = baseline.run(data_dir)
    assert seen == [([date(2013, 11, 5)], {})]  # clean training days: no injection
    assert baseline.load(data_dir).height == 2
    meta = json.loads(out.with_suffix(".meta.json").read_text())
    assert meta["modeled"] is True and meta["train_days"] == ["2013-11-05"]
    assert set(meta["traffic_sha256"]) == {"2013-11-05.parquet"}
    assert meta["config"] == CFG.model_dump(mode="json")


@pytest.mark.parametrize(
    ("change", "stale"),
    [
        (lambda d: (d / "processed" / CALIBRATION_FILE).write_text('{"new": 1}'), "calibration"),
        (lambda d: (d / "processed" / "topology_nodes.csv").write_text("new"), "topology_nodes"),
    ],
)
def test_load_refuses_a_stale_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: Callable[[Path], object], stale: str
) -> None:
    data_dir = _data_dir(tmp_path, ["2013-11-05"])
    monkeypatch.setattr(
        baseline.replay, "telemetry_for", lambda *_a, **_k: replay.Telemetry(telemetry_frame(), [])
    )
    baseline.run(data_dir)
    change(data_dir)
    with pytest.raises(ValueError, match=f"stale.*{stale}.*make baseline"):
        baseline.load(data_dir)


def test_load_refuses_another_feature_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = _data_dir(tmp_path, ["2013-11-05"])
    monkeypatch.setattr(
        baseline.replay, "telemetry_for", lambda *_a, **_k: replay.Telemetry(telemetry_frame(), [])
    )
    baseline.run(data_dir)
    monkeypatch.setattr(baseline, "load_config", lambda: CFG.model_copy(update={"z_clip": 5.0}))
    with pytest.raises(ValueError, match="config"):
        baseline.load(data_dir)


def test_load_missing(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="make baseline"):
        baseline.load(tmp_path)


def test_main_without_training_days(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert baseline.main(["--data-dir", str(_data_dir(tmp_path, ["2013-12-02"]))]) == 1
    assert "no processed training days" in caplog.text
