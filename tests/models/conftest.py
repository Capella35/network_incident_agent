"""Hand-written feature frames for the detector tests (never Milan data)."""

from __future__ import annotations

from functools import cache

import numpy as np
import polars as pl
import pytest

from common import telemetry
from streaming import baseline, features
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


TRAIN_INTERVALS = 144 * 21  # three weeks from T0, all training days: 18 samples per hour bucket


@cache
def _baseline() -> pl.DataFrame:
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
