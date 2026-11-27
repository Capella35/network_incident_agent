"""Separation of a fault type from normal busy rows (milestone 2, step 6d), on hand fixtures.

Local hour h of DAY starts at `t(6 * h)` (`conftest.t`); BUSY_HOURS 11-15 = t(66) .. t(95)."""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest

from evaluation import ground_truth, separation
from streaming import features, offline
from tests.evaluation.conftest import DAY, SITE, fault, t

CFG = features.load_config()
LOAD = CFG.load_curve.load
LOAD_Z, NET_Z, SURPRISE = separation.signals(CFG)


def _load_rows(xs: list[float | None], at: int = 72) -> pl.DataFrame:
    """One cell, one row per day ending on DAY, at interval `at` (local 12:00), log1p load `xs`."""
    n = len(xs)
    return pl.DataFrame(
        {
            "cell_id": ["CELL-1-1"] * n,
            "interval_start": [t(at) - timedelta(days=n - 1 - i) for i in range(n)],
            "data_gap": [False] * n,
            LOAD: [None if x is None else math.expm1(x) for x in xs],
        }
    )


def test_load_surprise_is_z_against_the_same_hour_of_previous_days() -> None:
    out = separation.with_load_surprise(_load_rows([0.1, 0.2, 0.3, 0.5]), CFG)
    s = out[SURPRISE].to_list()
    assert s[:3] == [None, None, None]  # fewer than level.min_days (3) days of history
    assert s[3] == pytest.approx((0.5 - 0.2) / 0.1)  # mean 0.2, sample std 0.1 (> floor 0.05)


def test_load_surprise_counts_only_days_with_data_and_floors_the_std() -> None:
    gap = separation.with_load_surprise(_load_rows([0.1, None, 0.3, 0.5]), CFG)
    assert gap[SURPRISE][3] is None  # the null day is not a day of history
    flat = separation.with_load_surprise(_load_rows([0.2, 0.2, 0.2, 0.25]), CFG)
    assert flat[SURPRISE][3] == pytest.approx(0.05 / CFG.baseline.std_floor_log)
    big = separation.with_load_surprise(_load_rows([0.1, 0.2, 0.3, 50.0]), CFG)
    assert big[SURPRISE][3] == CFG.z_clip


def _frame(
    values: dict[tuple[str, int], float], gaps: frozenset[tuple[str, int]] = frozenset()
) -> pl.DataFrame:
    """Every interval of DAY on two cells; each signal = `values` at (cell, i), else 0."""
    keys = [(c, i) for c in ("CELL-1-1", "CELL-2-1") for i in range(144)]
    v = [values.get(k, 0.0) for k in keys]
    return pl.DataFrame(
        {
            "cell_id": [c for c, _ in keys],
            "site_id": [SITE[c] for c, _ in keys],
            "interval_start": [t(i) for _, i in keys],
            "data_gap": [k in gaps for k in keys],
            LOAD_Z: v,
            NET_Z: v,
            SURPRISE: v,
        }
    )


F_A = fault("A", "congestion", ("CELL-1-1",), 70, 74)  # widened: [69, 75)
F_B = fault("B", "cell_outage", ("CELL-2-1",), 80, 82)  # another type: still not "normal"


def test_fault_peaks_take_the_max_over_the_widened_window_on_its_cells() -> None:
    frame = _frame({("CELL-1-1", 69): 5.0, ("CELL-1-1", 75): 99.0, ("CELL-2-1", 72): 99.0})
    peaks = separation.fault_peaks(frame, [F_A], [LOAD_Z])
    assert peaks.rows() == [("A", 5.0)]  # 75 is past the window; CELL-2-1 is not its cell


def test_normal_busy_drops_fault_windows_gaps_and_quiet_hours() -> None:
    normal = separation.normal_busy(_frame({}, frozenset({("CELL-2-1", 66)})), [F_A, F_B])
    got = set(zip(normal["cell_id"], normal["interval_start"], strict=True))
    busy = {(c, t(i)) for c in ("CELL-1-1", "CELL-2-1") for i in range(66, 96)}
    excluded = {("CELL-1-1", t(i)) for i in range(69, 75)} | {
        ("CELL-2-1", t(i)) for i in range(79, 83)
    }
    assert got == busy - excluded - {("CELL-2-1", t(66))}


def test_separates_compares_the_median_peak_with_the_normal_tail() -> None:
    normal = pl.Series(list(range(1001)), dtype=pl.Float64)  # p99.9 = 999
    assert separation.separates(pl.Series([1.0, 1000.0, 1000.0]), normal) == (1000.0, 999.0, True)
    assert separation.separates(pl.Series([1.0, 2.0, 1000.0]), normal)[2] is False
    assert separation.separates(pl.Series([None], dtype=pl.Float64), normal) == (None, 999.0, False)


def test_control_windows_shift_by_days_inside_the_span_and_avoid_faults() -> None:
    span = (t(0) - timedelta(days=1), t(0) + timedelta(days=2))  # DAY - 1 .. DAY + 1
    day = timedelta(days=1)
    later = ground_truth.Fault("H", "backhaul", ("CELL-1-1",), t(70) + day, t(71) + day)  # A +1d
    got = separation.control_windows([F_A], [F_A, later], span)
    assert [g.fault_id for g in got] == ["A@-1d"]
    assert (got[0].onset, got[0].end, got[0].cells) == (
        F_A.onset - timedelta(days=1),
        F_A.end - timedelta(days=1),
        F_A.cells,
    )


@pytest.mark.parametrize(("peak", "beat"), [(5.0, 0), (7.0, 1)])
def test_above_controls_needs_a_peak_above_every_own_control(peak: float, beat: int) -> None:
    peaks = pl.DataFrame({"fault_id": ["A", "B"], LOAD_Z: [peak, None]})
    control = pl.DataFrame({"fault_id": ["A@-1d", "A@+1d", "B@+1d"], LOAD_Z: [3.0, 6.0, 1.0]})
    odd = pl.DataFrame({"fault_id": ["x@y"], LOAD_Z: [peak]})  # an id with the separator in it
    odd_control = pl.DataFrame({"fault_id": ["x@y@+2d"], LOAD_Z: [6.0]})
    assert separation.above_controls(odd, odd_control, LOAD_Z) == (beat, 1)
    assert separation.above_controls(peaks, control, LOAD_Z) == (beat, 1)  # B has no peak


def test_run_reports_each_signal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    frame = _frame({("CELL-1-1", 72): 9.0}).drop(SURPRISE)
    rows = _load_rows([0.1, 0.2, 0.3, 0.5])  # load surprise 3.0 at t(72), inside A's window
    monkeypatch.setattr(separation.data_prep, "require_processed_days", lambda *_a: {DAY: Path()})
    monkeypatch.setattr(
        separation.offline,
        "telemetry_with_history",
        lambda *_a, **_k: offline.WithHistory(rows, (), 2),
    )
    monkeypatch.setattr(separation.offline, "features_of", lambda *_a: frame)
    monkeypatch.setattr(separation.baseline, "load", lambda _d: None)
    monkeypatch.setattr(
        separation.ground_truth, "read", lambda *_a: ground_truth.GroundTruth((F_A, F_B), "fp")
    )
    md = separation.run(tmp_path, "congestion")
    assert "1 congestion faults" in md
    assert "0 control windows" in md  # one day: no shifted window fits
    row = f"| {LOAD_Z} | 9.00 / 9.00 / 9.00 | 0.00 / 0.00 / 0.00 | 1 / 1 | n/a | 0 / 0 | yes |"
    assert row in md
    assert f"| A | 9.00 | 9.00 | {(0.5 - 0.2) / 0.1:.2f} |" in md


def test_main_rejects_an_unknown_fault_type(caplog: pytest.LogCaptureFixture) -> None:
    assert separation.main(["--fault-type", "no_such_fault"]) == 1
    assert "unknown fault type" in caplog.text
