"""Hand-written feature frames for the detector tests (never Milan data)."""

from __future__ import annotations

from datetime import date
from functools import cache
from pathlib import Path

import mlflow
import numpy as np
import polars as pl
import pytest
from mlflow.entities import Run

from common import splits, telemetry
from evaluation import metrics
from streaming import baseline, features, offline
from tests.factories import telemetry_frame

OUTAGE = range(
    200, 203
)  # rows (time order) of the planted 3-interval outage, the injector's shortest


def normal_rows(
    n_intervals: int = 288, seed: int = 1, *, gaps: tuple[int, ...] = ()
) -> pl.DataFrame:
    """One cell of stationary telemetry: every traffic field 1 + N(0, 0.1), seeded."""
    frame = telemetry_frame(n_intervals=n_intervals, gaps=gaps)
    noise = np.random.default_rng(seed).normal(0.0, 0.1, frame.height)
    ok = ~pl.col("data_gap")
    return frame.with_columns(
        pl.when(ok).then(1.0 + pl.lit(noise)).otherwise(0.0).alias(f)
        for f in telemetry.TRAFFIC_FIELDS
    )


DAY = splits.days(splits.Split.VALIDATION)[0]
TRAIN_INTERVALS = 144 * 21  # three weeks from T0, all training days: 18 samples per hour bucket


@cache
def _baseline() -> features.Reference:
    return baseline.fit(normal_rows(TRAIN_INTERVALS, seed=1), features.load_config())


def to_features(rows: pl.DataFrame) -> pl.DataFrame:
    """Rows -> feature frame against a baseline fitted on the training fixture's rows."""
    return features.compute(rows, _baseline(), features.load_config())


@pytest.fixture
def train() -> pl.DataFrame:
    return to_features(normal_rows(TRAIN_INTERVALS, seed=1))


@pytest.fixture
def normal_with_outage() -> pl.DataFrame:
    """Normal rows (another seed) where all traffic stops for `OUTAGE` (a cell outage)."""
    rows = normal_rows(seed=2).with_row_index("_i")
    outage = pl.col("_i").is_in(list(OUTAGE))
    rows = rows.with_columns(
        pl.when(outage).then(0.0).otherwise(pl.col(f)).alias(f) for f in telemetry.TRAFFIC_FIELDS
    )
    return to_features(rows.drop("_i"))


def result_of(detected: int, delay: int | None, false: int, days: int = 1) -> metrics.Result:
    s = metrics.Summary(
        faults=1, unobservable=0, detected=detected, delay=metrics.Median(delay, None, None)
    )
    return metrics.Result(
        days=tuple(DAY for _ in range(days)),
        overall=s,
        by_type={},
        incidents=detected + false,
        false_incidents=false,
        outcomes=(),
    )


class FakeScore:
    """Stand-in for `evaluation.report.score` on the one-cell fixture: the planted outage is the
    one fault (detected at delay 0 if any alarm falls in it); every other alarm is a false
    incident, or `false_incidents` for each trial when set."""

    def __init__(self, validation: pl.DataFrame, false_incidents: int | None = None) -> None:
        times = validation.sort("interval_start")["interval_start"].to_list()
        self.outage = {times[i] for i in OUTAGE}
        self.false_incidents = false_incidents
        self.calls = 0

    def __call__(
        self, _d: Path, alarms: pl.DataFrame, *, days: list[date], gaps: pl.DataFrame
    ) -> tuple[metrics.Result, str]:
        self.calls += 1
        inside = alarms.filter(pl.col("interval_start").is_in(list(self.outage))).height
        false = alarms.height - inside if self.false_incidents is None else self.false_incidents
        return result_of(int(inside > 0), 0 if inside else None, false, len(days)), "fp"


def logged_study(threshold: float, *, features_json: str | None = None) -> Run:
    """A finished `detector-tuning` study run holding a tuned z-score model (needs
    `local_mlflow`), tagged and parameterized like `tune.run_study` logs them; `features_json`:
    the features config it claims to be tuned under (default: the current one)."""
    from models import detectors, tracking, tune

    with tracking.start_run(tune.EXPERIMENT, run_name="zscore") as run:
        mlflow.set_tags({"role": "study", "feasible": "yes", "fault_schedule_fingerprint": "fp"})
        mlflow.log_param("features", features_json or features.load_config().model_dump_json())
        tracking.log_detector(
            detectors.build("zscore", detectors.load_config()),
            params={},
            metrics={},
            threshold=threshold,
        )
    return mlflow.get_run(run.info.run_id)


def use_validation(monkeypatch: pytest.MonkeyPatch, frame: pl.DataFrame) -> None:
    """`tune.load_validation` reads `frame` as the validation features (no Milan files)."""
    from models import tune

    monkeypatch.setattr(tune.data_prep, "require_processed_days", lambda _d, _s: {DAY: Path()})
    monkeypatch.setattr(tune.data_prep, "snapshot", lambda _d, _days: "snap")
    monkeypatch.setattr(
        tune.offline, "features_for", lambda *_a, **_k: offline.OfflineFeatures(frame, (), 1)
    )
