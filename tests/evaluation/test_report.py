from __future__ import annotations

from datetime import date
from pathlib import Path

import mlflow
import polars as pl
import pytest

from common import splits
from evaluation import ground_truth, metrics, report
from producer import injector
from streaming import features, offline
from tests.evaluation.conftest import DAY, FAULTS, INDEX, SITE, SLOTS, alarms, gaps, t
from tests.factories import flat_baseline

SPLIT = splits.Split.VALIDATION


def _injected(f: ground_truth.Fault) -> injector.InjectedFault:
    return injector.InjectedFault(
        fault_id=f.fault_id,
        fault_type=f.fault_type,
        root_node=f.cells[0],
        root_level="Cell",
        cells=f.cells,
        onset=f.onset,
        duration=(f.end - f.onset) // metrics.INTERVAL,
        severity="minor",
        traffic_factor=0.0,
        latency_factor=1.0,
        ramp=1,
    )


def _write_truth(data_dir: Path, faults: tuple[ground_truth.Fault, ...]) -> None:
    schedule = [_injected(f) for f in faults]
    injector.write_ground_truth(schedule, SPLIT, injector.load_config(), data_dir / "processed")


def test_ground_truth_round_trips_with_its_fingerprint(tmp_path: Path) -> None:
    _write_truth(tmp_path, FAULTS[::-1])
    truth = ground_truth.read(tmp_path, SPLIT)
    assert truth.faults == FAULTS  # sorted by onset
    assert truth.fingerprint == injector.fingerprint([_injected(f) for f in FAULTS[::-1]])


def test_missing_ground_truth_names_the_command(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="make inject"):
        ground_truth.read(tmp_path, SPLIT)


def test_render_shows_every_hand_checked_number() -> None:
    result = metrics.evaluate(alarms(), FAULTS, days=[DAY], index=INDEX, gaps=gaps())
    md = report.render(result, title="T")
    assert "Days measured: 2013-12-02 .. 2013-12-02 (1 days)" in md
    assert "| backhaul | 1 | 0 | 1 | 100.0% | 6 [n/a, n/a] |" in md
    assert "| config_push | 1 | 1 | 0 | n/a | not reached [n/a, n/a] |" in md
    assert "| congestion | 2 | 0 | 1 | 50.0% | 1 [1, n/a] |" in md
    assert "| **overall** | 7 | 1 | 5 | 83.3% | 1 [0, n/a] |" in md
    assert "Incidents (provisional M2 grouping): 7; false incidents: 3 (3.00 per day)." in md
    assert "SYNTHETIC" in md


def _feature_day(hot: set[tuple[str, int]]) -> pl.DataFrame:
    """A feature frame for DAY: every feature 0 except `traffic_internet_bad` = 10 at `hot`."""
    rows = [
        (c, SITE[c], t(i), False, 10.0 if (c, i) in hot else 0.0)
        for c in sorted(SITE)
        for i in range(len(SLOTS))
    ]
    frame = pl.DataFrame(
        rows, schema=[*features.KEYS, "traffic_internet_bad"], orient="row"
    ).with_columns(pl.col("interval_start").cast(pl.Datetime("ms", "UTC")))
    others = [c for c in features.feature_columns(features.load_config()) if c not in frame.columns]
    return frame.with_columns(pl.lit(0.0).alias(c) for c in others)


def test_run_scores_evaluates_and_logs_to_mlflow(
    local_mlflow: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_truth(tmp_path, FAULTS)
    seen: list[tuple[list[date], bool]] = []

    def fake(_d: Path, days: list[date], *, inject: bool = False) -> offline.OfflineFeatures:
        seen.append((days, inject))
        return offline.OfflineFeatures(
            _feature_day({("CELL-1-1", 11)}), (), n_faults=7, reference=flat_baseline(["CELL-1-1"])
        )

    monkeypatch.setattr(report.data_prep, "require_processed_days", lambda *_a: {DAY: Path()})
    monkeypatch.setattr(report.offline, "features_for", fake)
    monkeypatch.setattr(report.topology, "load_index", lambda _p: INDEX)
    md = report.run(tmp_path, [DAY], threshold=4.0)
    assert seen == [([DAY], True)]
    assert "| cell_outage | 2 | 0 | 1 | 50.0% |" in md  # F1 hit at 11, F7 missed
    run = mlflow.search_runs(experiment_names=[report.EXPERIMENT], output_format="list")[0]
    assert run.data.params["threshold"] == "4.0" and run.data.params["candidate"] == "zscore"
    assert run.data.metrics["detected"] == 1.0 and run.data.metrics["false_incidents"] == 0.0
    assert "median_delay_intervals" not in run.data.metrics  # 1 of 6 detected: not reached
    assert run.data.metrics["detection_rate.cell_outage"] == 0.5
    assert run.data.metrics["median_delay_intervals.cell_outage"] == 1.0
    artifacts = {a.path for a in mlflow.artifacts.list_artifacts(run_id=run.info.run_id)}
    assert artifacts == {"report.md", "fault_outcomes.csv"}


@pytest.mark.parametrize(
    "args",
    [
        ["--threshold", "4", "--start", "2013-11-30", "--end", "2013-12-02"],  # training day
        ["--threshold", "4", "--start", "2013-12-16", "--end", "2013-12-16"],  # test week
        ["--threshold", "4", "--start", "2013-12-03", "--end", "2013-12-02"],  # reversed
    ],
)
def test_main_refuses_non_validation_days(
    args: list[str], tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    assert report.main(["--data-dir", str(tmp_path), *args]) == 1
    assert caplog.records


def test_main_reports_missing_data(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    assert report.main(["--data-dir", str(tmp_path), "--threshold", "4"]) == 1
    assert "make data" in caplog.text or "missing" in caplog.text
