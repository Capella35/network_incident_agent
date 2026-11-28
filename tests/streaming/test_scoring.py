"""Streaming scoring (milestone 2, step 8): a stream fed in any batching and arrival order gives
the offline features and alarms. Hand-written telemetry, never Milan data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from common import telemetry
from common.domain import load_domain
from streaming import features, scoring
from tests.factories import PARTITIONS, flat_baseline, noisy_rows, zscore_scorer

CFG = features.load_config()
DAY = timedelta(days=1)
local_date = features.LOCAL_TIME.dt.date()
PER_DAY = 144  # 10-minute intervals in a local day without a clock change


@dataclass(frozen=True)
class Streamed:
    released: pl.DataFrame  # every row the stream scored, in release order
    features: pl.DataFrame  # their features, sorted by interval
    steps: list[scoring.Step]


def stream(batches: list[pl.DataFrame], sc: scoring.Scorer | None = None) -> Streamed:
    """Feed one cell's batches through `step`, as the job does."""
    sc = sc or zscore_scorer()
    state, released, frames, steps = scoring.CellState.empty(), [], [], []
    for batch in batches:
        result = scoring.step(state, batch, sc, run="run-1", partitions=PARTITIONS)
        fresh = result.state.scored.join(state.scored, on="interval_start", how="anti")
        if not fresh.is_empty():
            released.append(fresh)
            frames.append(scoring.new_features(state.scored, fresh, sc.spec))
        steps.append(result)
        state = result.state
    return Streamed(pl.concat(released), pl.concat(frames).sort("interval_start"), steps)


def in_batches(rows: pl.DataFrame, sizes: list[int]) -> list[pl.DataFrame]:
    out, i = [], 0
    for k in sizes:
        out.append(rows.slice(i, k))
        i += k
    return [*out, rows.slice(i)] if i < rows.height else out


def per_day_offline(rows: pl.DataFrame) -> pl.DataFrame:
    """Each local day computed on itself + the `history_days` before it, concatenated."""
    reach = scoring.history_days(CFG) * DAY
    days = rows.select(local_date.unique().sort()).to_series().to_list()
    return pl.concat(
        features.compute(
            rows.filter(local_date.is_between(d - reach, d)), flat_baseline(["CELL-1-1"]), CFG
        ).filter(local_date == d)
        for d in days
    ).sort("interval_start")


def test_history_days_covers_the_level_and_the_longest_window() -> None:
    assert scoring.history_days(CFG) == CFG.level.days + 1  # windows reach back < 1 day


def test_in_order_stream_matches_offline_features_bitwise() -> None:
    rows = noisy_rows(PER_DAY * 10, gaps=(50, 51, 900))
    batches = in_batches(rows, [1, 7, 150, 399, 144, 1000, 2])
    got = stream(batches).features
    assert got.height == rows.height
    assert_frame_equal(got, per_day_offline(rows), check_exact=True)
    # One frame holding all history (how the detector was tuned): the same up to rounding of the
    # rolling level sums, which depends on where a frame starts.
    whole = features.compute(rows, flat_baseline(["CELL-1-1"]), CFG).sort("interval_start")
    assert_frame_equal(got, whole, check_exact=False, rel_tol=0, abs_tol=1e-12)


def test_with_only_level_days_of_history_it_matches_the_offline_single_day_path() -> None:
    rows = noisy_rows(PER_DAY * (CFG.level.days + 1))  # a stream's first days
    got = stream(in_batches(rows, [500, 3, 300])).features
    last = rows.select(local_date.max()).item()
    single_day = features.compute(rows, flat_baseline(["CELL-1-1"]), CFG).filter(local_date == last)
    assert_frame_equal(got.filter(local_date == last), single_day, check_exact=True)


def test_out_of_order_and_repeated_arrival_gives_the_in_order_result() -> None:
    rows = noisy_rows(PER_DAY * 3)
    sc = zscore_scorer(threshold=1.5)
    in_order = stream(in_batches(rows, [200, 200]), sc)
    # Like a fast replay: one cell's intervals come from different Kafka partitions, so a batch
    # holds a shuffled window of them, some again in the next batch.
    order = np.random.default_rng(5).permutation(rows.height)
    shuffled = rows[[int(i) for i in order]]
    by_window = [
        shuffled.filter(
            pl.col("interval_start").is_between(
                rows["interval_start"][s], rows["interval_start"][min(s + 59, rows.height - 1)]
            )
        )
        for s in range(0, rows.height, 40)
    ]
    jumbled = stream(by_window, sc)
    assert_frame_equal(jumbled.released.sort("interval_start"), in_order.released)
    assert_frame_equal(jumbled.features, in_order.features, check_exact=True)
    alarms = pl.concat(s.alarms for s in in_order.steps).sort("interval_start")
    jumbled_alarms = pl.concat(s.alarms for s in jumbled.steps).sort("interval_start")
    assert alarms.height > 0
    assert_frame_equal(jumbled_alarms, alarms, check_exact=True)
    assert sum(s.n_scored for s in jumbled.steps) == rows.height
    assert sum(s.n_late for s in jumbled.steps) > 0  # the repeats came after their interval


def test_a_hole_is_given_up_once_every_partition_has_passed_it() -> None:
    rows = noisy_rows(40)
    hole_at = rows["interval_start"][10]
    hole = pl.col("interval_start") == hole_at
    sc = zscore_scorer()

    def go(state: scoring.CellState, new: pl.DataFrame) -> scoring.Step:
        return scoring.step(state, new, sc, run="r", partitions=PARTITIONS)

    # Rows 0-15 without row 10: only partitions 0-3 (rows 12-15) have passed the hole.
    first = go(scoring.CellState.empty(), rows.head(16).filter(~hole))
    assert (first.n_scored, first.state.pending.height) == (10, 5)  # waits behind the hole
    filled = go(first.state, rows.filter(hole))
    assert (filled.n_scored, filled.state.pending.height) == (6, 0)
    # The rest arrives without row 10: partitions 4 and 5 deliver rows 16-17 too, so every
    # partition has a row after the hole and it can never come (each partition is in order).
    gave_up = go(first.state, rows.slice(16))
    assert (gave_up.n_scored, gave_up.state.pending.height) == (29, 0)
    late = go(gave_up.state, rows.filter(hole))
    assert (late.n_scored, late.n_late) == (0, 1)
    assert late.alarms.is_empty()


def test_a_partition_a_day_behind_loses_nothing() -> None:
    """Step 9's case: while a fast replay is being written, one partition can sit a local day of
    data behind the others; its holes are not given up, however long they last."""
    rows = noisy_rows(PER_DAY * 3)
    behind = pl.col("partition") == 2  # one lagging partition
    ahead, lagging = rows.filter(~behind), rows.filter(behind)
    batches = [ahead.head(300), ahead.slice(300), lagging.head(30), lagging.slice(30)]
    jumbled = stream(batches)
    assert sum(s.n_late for s in jumbled.steps) == 0
    assert_frame_equal(jumbled.features, stream([rows]).features, check_exact=True)


@pytest.mark.parametrize("pending", [0, 3])
def test_state_round_trips_and_keeps_only_history_days(pending: int) -> None:
    rows = noisy_rows(PER_DAY * 12)
    head = rows.head(rows.height - pending)
    result = scoring.step(
        scoring.CellState.empty(), head, zscore_scorer(), run="r", partitions=PARTITIONS
    )
    tail = rows.tail(pending)
    if pending:  # rows after a hole stay pending
        result = scoring.step(
            result.state, tail.slice(1), zscore_scorer(), run="r", partitions=PARTITIONS
        )
    back = scoring.CellState.decode(*result.state.encode())
    assert back.seen == result.state.seen
    assert_frame_equal(back.scored, result.state.scored)
    assert_frame_equal(back.pending, result.state.pending)
    days = back.scored.select(local_date.n_unique()).item()
    assert days == scoring.history_days(CFG) + 1  # the latest day and the history it needs
    assert back.pending.height == max(pending - 1, 0)


def test_alarm_rows_carry_valid_deterministic_ids() -> None:
    rows = noisy_rows(PER_DAY)
    sc = zscore_scorer(threshold=1.0)
    a = scoring.step(scoring.CellState.empty(), rows, sc, run="run-9", partitions=PARTITIONS).alarms
    b = scoring.step(scoring.CellState.empty(), rows, sc, run="run-9", partitions=PARTITIONS).alarms
    assert a.height > 0
    assert a.schema == scoring.NO_ALARMS.schema
    assert a["alarm_id"].to_list() == b["alarm_id"].to_list()
    assert a["alarm_id"].n_unique() == a.height
    assert all(load_domain().entity("Alarm").matches(i) for i in a["alarm_id"])
    assert set(a["detector_version"]) == {"detector/7"}
    assert set(a["replay_run_id"]) == {"run-9"}
    other = scoring.step(
        scoring.CellState.empty(), rows, sc, run="run-10", partitions=PARTITIONS
    ).alarms
    assert set(other["alarm_id"]).isdisjoint(a["alarm_id"])


def test_a_batch_with_nothing_new_scores_nothing() -> None:
    rows = noisy_rows(20)
    sc = zscore_scorer()
    first = scoring.step(scoring.CellState.empty(), rows, sc, run="r", partitions=PARTITIONS)
    again = scoring.step(first.state, rows, sc, run="r", partitions=PARTITIONS)
    assert (again.n_scored, again.n_late) == (0, 20)
    assert again.alarms.is_empty()
    assert_frame_equal(again.state.scored, first.state.scored)
    empty = scoring.step(
        scoring.CellState.empty(), rows.clear(), sc, run="r", partitions=PARTITIONS
    )
    assert (empty.n_scored, empty.n_late, empty.alarms.height) == (0, 0, 0)


def test_releasable_counts_the_gap_free_run() -> None:
    times = noisy_rows(6).select("interval_start")
    step = times["interval_start"][1] - times["interval_start"][0]
    first = times["interval_start"][0]
    assert scoring.releasable(times, first - step, None) == 6
    assert scoring.releasable(times, first - 2 * step, None) == 0  # one interval still missing
    assert scoring.releasable(times, first - 2 * step, first) == 6  # lost: given up
    gappy = times.filter(pl.col("interval_start") != times["interval_start"][3])
    third = times["interval_start"][2]
    assert scoring.releasable(gappy, first - step, third) == 3  # the later hole is not lost yet
    assert scoring.releasable(times.clear(), first, None) == 0


def test_a_new_cell_waits_for_the_start_of_its_local_day() -> None:
    rows = noisy_rows(30)
    assert telemetry.local_day_start(rows["interval_start"][5]) == rows["interval_start"][0]
    sc = zscore_scorer()
    # Partitions 0-2, with the first three intervals, lag: they deliver nothing in batch one.
    lagging = pl.col("partition") < 3
    early = scoring.step(
        scoring.CellState.empty(), rows.filter(~lagging), sc, run="r", partitions=PARTITIONS
    )
    assert (early.n_scored, early.state.pending.height) == (0, 15)
    rest = scoring.step(early.state, rows.filter(lagging), sc, run="r", partitions=PARTITIONS)
    assert (rest.n_scored, rest.n_late) == (30, 0)
