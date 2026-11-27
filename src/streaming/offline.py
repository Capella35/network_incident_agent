"""Features for a run of days, offline (milestone 2): the one path for detector training,
scoring, and tuning, so they all see the same rows and the same history.

The level-adjusted features need `level.days` of history before the first day. History days in
the same split as `days` come with the same injection as `days`; history from an earlier split
comes clean (e.g. the last training week before validation week 1). Faults are never returned:
only their count (Rule 6).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from common import splits
from producer import data_prep, replay
from streaming import baseline, features


@dataclass(frozen=True)
class OfflineFeatures:
    frame: pl.DataFrame  # features.KEYS + feature columns, only the requested days
    history_days: tuple[date, ...]  # days loaded only as history (not in `frame`)
    n_faults: int  # SYNTHETIC faults injected into the loaded days, history days included


def _require_consecutive(days: list[date]) -> None:
    if any((b - a).days != 1 for a, b in itertools.pairwise(days)):
        raise ValueError(f"days must be consecutive ({days[0]}..{days[-1]} has a gap)")


@dataclass(frozen=True)
class WithHistory:
    rows: pl.DataFrame  # telemetry rows of the history days, then of the requested days
    history_days: tuple[date, ...]
    n_faults: int


def telemetry_with_history(
    data_dir: Path, days: list[date], *, inject: bool = False
) -> WithHistory:
    """Telemetry for `days` plus up to `level.days` processed days before them, injected as
    described in the module docstring. Same contract and errors as `features_for`."""
    _require_consecutive(days)
    cfg = features.load_config()
    split = splits.split_of(days[0])
    paths = data_prep.Paths(data_dir)
    history = [
        d
        for i in range(cfg.level.days, 0, -1)
        if paths.traffic_day(d := days[0] - timedelta(days=i)).exists()
    ]
    earlier = [d for d in history if splits.split_of(d) is not split]  # a prefix: date order
    loaded = replay.telemetry_for(data_dir, [*history[len(earlier) :], *days], inject=inject)
    parts = [replay.telemetry_for(data_dir, earlier).rows] if earlier else []
    return WithHistory(pl.concat([*parts, loaded.rows]), tuple(history), loaded.n_faults)


def features_for(data_dir: Path, days: list[date], *, inject: bool = False) -> OfflineFeatures:
    """`days` must be consecutive and in one split (`replay.telemetry_for` checks the split
    when injecting). Raises FileNotFoundError / ValueError / `replay.ReplayError`."""
    _require_consecutive(days)
    ref = baseline.load(data_dir)  # a missing or stale baseline fails before any telemetry
    loaded = telemetry_with_history(data_dir, days, inject=inject)
    return OfflineFeatures(
        features_of(loaded.rows, days, ref), loaded.history_days, loaded.n_faults
    )


def features_of(rows: pl.DataFrame, days: list[date], ref: features.Reference) -> pl.DataFrame:
    """Features of `rows` (`telemetry_with_history(...).rows`), only `days` kept."""
    frame = features.compute(rows, ref, features.load_config())
    return frame.filter(features.LOCAL_TIME.dt.date().is_in(days))
