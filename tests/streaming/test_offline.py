from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import pytest

from common import splits
from producer import replay
from streaming import features, offline
from tests.factories import T0, flat_baseline, telemetry_frame

VAL_DAY = splits.days(splits.Split.VALIDATION)[0]  # its history is the last training week


def _day_rows(day: date) -> pl.DataFrame:
    """One cell, one local day (Europe/Rome) of rows, built from the shared factory."""
    shift = (day - T0.date()) - timedelta(hours=1)  # local midnight = 23:00 UTC (CET)
    return telemetry_frame(n_intervals=144).with_columns(pl.col("interval_start") + shift)


def test_history_from_an_earlier_split_comes_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = offline.data_prep.Paths(tmp_path)
    paths.traffic.mkdir(parents=True)
    history = [VAL_DAY - timedelta(days=i) for i in range(7, 0, -1)]
    for d in [*history[2:], VAL_DAY]:  # the two oldest history days are not processed
        paths.traffic_day(d).write_bytes(b"stub")
    calls: list[tuple[list[date], bool]] = []

    def fake(_d: Path, days: list[date], *, inject: bool = False) -> replay.Telemetry:
        calls.append((days, inject))
        return replay.Telemetry(pl.concat([_day_rows(d) for d in days]), [], n_faults=3 * inject)

    monkeypatch.setattr(offline.replay, "telemetry_for", fake)
    monkeypatch.setattr(offline.baseline, "load", lambda _d: flat_baseline(["CELL-1-1"]))
    out = offline.features_for(tmp_path, [VAL_DAY], inject=True)
    assert calls == [([VAL_DAY], True), (history[2:], False)]  # training history: never injected
    assert out.history_days == tuple(history[2:]) and out.n_faults == 3
    days = out.frame.select(features.LOCAL_TIME.dt.date().unique()).to_series().to_list()
    assert days == [VAL_DAY]
    assert out.frame["traffic_internet_z"].null_count() == 0


def test_days_must_be_consecutive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="consecutive"):
        offline.features_for(tmp_path, [VAL_DAY, VAL_DAY + timedelta(days=2)])
