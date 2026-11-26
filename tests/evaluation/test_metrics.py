from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import polars as pl
import pytest

from evaluation import metrics
from evaluation.metrics import FaultOutcome, Median
from tests.evaluation.conftest import DAY, FAULTS, INDEX, SLOTS, alarms, fault, gaps, t

# --- The hand-checked 1-day fixture (worked out in conftest.py) -------------------------------


@pytest.fixture(scope="module")
def result() -> metrics.Result:
    return metrics.evaluate(alarms(), FAULTS, days=[DAY], index=INDEX, gaps=gaps())


def test_every_fault_outcome_matches_the_hand_check(result: metrics.Result) -> None:
    assert result.outcomes == (
        FaultOutcome("F1", "cell_outage", observable=True, detected=True, time=1),
        FaultOutcome("F2", "site_power", observable=True, detected=True, time=0),
        FaultOutcome("F3", "backhaul", observable=True, detected=True, time=6),
        FaultOutcome("F4", "congestion", observable=True, detected=False, time=4),
        FaultOutcome("F5", "config_push", observable=False, detected=False, time=3),
        FaultOutcome("F6", "congestion", observable=True, detected=True, time=1),
        FaultOutcome("F7", "cell_outage", observable=True, detected=True, time=0),
    )


def test_incidents_and_false_incidents_match_the_hand_check(result: metrics.Result) -> None:
    assert (result.incidents, result.false_incidents, result.false_per_day) == (7, 3, 3.0)
    inc = metrics.incidents(alarms(), INDEX)
    assert inc.select("group", "start", "end").rows() == [
        ("AGG-1", t(11), t(12)),
        ("AGG-1", t(29), t(31)),
        ("AGG-2", t(50), t(51)),
        ("AGG-1", t(66), t(67)),
        ("AGG-1", t(70), t(72)),
        ("AGG-1", t(73), t(74)),
        ("AGG-1", t(121), t(123)),
    ]
    assert inc["cells"].to_list()[-1] == ["CELL-1-1", "CELL-2-1"]  # F6 and F7 share it


def test_overall_numbers_match_the_hand_check(result: metrics.Result) -> None:
    # Observable times (detected): 1, 0, 6, 4 (censored), 1, 0.
    # t=0: 6 at risk, 2 detected -> S = 2/3; Greenwood 2/(6*4) = 1/12;
    #      lower band 2/3 / exp(1.96 * sqrt(1/12)) = 0.379 <= 0.5 -> CI low = 0.
    # t=1: 4 at risk (1, 6, 4, 1), 2 detected -> S = 1/3 <= 0.5 -> median = 1;
    #      upper band 1/3 * exp(1.96 * sqrt(1/12 + 2/8)) = 1.03 -> capped at 1, not <= 0.5.
    # t=6: 1 at risk, 1 detected -> S = 0: infinite variance, the upper band never reaches 0.5.
    assert result.overall == metrics.Summary(
        faults=7, unobservable=1, detected=5, delay=Median(1, 0, None)
    )
    assert result.overall.detection_rate == pytest.approx(5 / 6)


def test_per_type_numbers_match_the_hand_check(result: metrics.Result) -> None:
    s = metrics.Summary
    assert result.by_type == {
        # F3 alone: detected at 6 -> S = 0 at once: median 6, no CI.
        "backhaul": s(1, 0, 1, Median(6, None, None)),
        # F7 at 0 (2 at risk -> S = 1/2, lower band 0.125), F1 at 1 -> S = 0: no upper bound.
        "cell_outage": s(2, 0, 2, Median(0, 0, None)),
        # F5 only, unobservable: nothing to estimate.
        "config_push": s(1, 1, 0, Median(None, None, None)),
        # F6 at 1 (2 at risk -> S = 1/2), F4 censored at 4: S never reaches 0, upper band stays 1.
        "congestion": s(2, 0, 1, Median(1, 1, None)),
        "site_power": s(1, 0, 1, Median(0, None, None)),
    }
    assert result.by_type["config_push"].detection_rate is None
    assert result.by_type["congestion"].detection_rate == 0.5


# --- Single cases ----------------------------------------------------------------------------


def test_alarm_two_intervals_early_or_late_misses() -> None:
    f = fault("F", "cell_outage", ("CELL-1-1",), 10, 13)
    for at in (8, 14):
        out = metrics.outcome(f, alarms((("CELL-1-1", at),)), gaps(()), t(143) + timedelta(1))
        assert not out.detected and out.time == 3  # censored at 14 - 10 - 1


def test_alarm_on_another_cell_does_not_detect_and_is_false() -> None:
    f = fault("F", "cell_outage", ("CELL-1-1",), 10, 13)
    r = metrics.evaluate(alarms((("CELL-1-2", 11),)), [f], days=[DAY], index=INDEX, gaps=gaps(()))
    assert (r.overall.detected, r.false_incidents) == (0, 1)


def test_partly_gapped_fault_is_observable() -> None:
    f = fault("F", "config_push", ("CELL-3-1",), 100, 104)  # 4 rows, 3 of them gaps
    r = metrics.evaluate(alarms(()), [f], days=[DAY], index=INDEX, gaps=gaps())
    assert (r.overall.unobservable, r.overall.detection_rate) == (0, 0.0)


def test_fault_cut_by_the_window_end_is_censored_there() -> None:
    f = fault("F", "backhaul", ("CELL-2-1",), 140, 143)
    f = replace(f, end=f.end + 10 * metrics.INTERVAL)
    r = metrics.evaluate(alarms(()), [f], days=[DAY], index=INDEX, gaps=gaps(()))
    assert r.outcomes[0].time == len(SLOTS) - 140 - 1


def test_fault_from_before_the_window_matches_but_is_not_scored() -> None:
    f = fault("F", "cell_outage", ("CELL-1-1",), 0, 3)
    early = replace(f, onset=f.onset - metrics.INTERVAL)
    r = metrics.evaluate(
        alarms((("CELL-1-1", 1),)), [early], days=[DAY], index=INDEX, gaps=gaps(())
    )
    assert (r.overall.faults, r.incidents, r.false_incidents) == (0, 1, 0)


def test_non_consecutive_days_and_unknown_sites_are_refused() -> None:
    with pytest.raises(ValueError, match="consecutive"):
        metrics.evaluate(alarms(()), [], days=[DAY, DAY + timedelta(2)], index=INDEX, gaps=gaps(()))
    stray = alarms((("CELL-1-1", 11),)).with_columns(site_id=pl.lit("SITE-99"))
    with pytest.raises(ValueError, match="SITE-99"):
        metrics.incidents(stray, INDEX)


# --- Kaplan-Meier ----------------------------------------------------------------------------


def test_km_median_not_reached_when_most_faults_are_missed() -> None:
    # t=2: 3 at risk, 1 detected -> S = 2/3 > 0.5, but its lower band (0.30) reaches 0.5.
    assert metrics.km_median([2, 5, 5], [True, False, False]) == Median(None, 2, None)


def test_km_counts_detections_before_censorings_at_the_same_time() -> None:
    # t=3: both at risk, 1 detected -> S = 1/2 -> median 3 (censoring first: 1 at risk, S = 0).
    assert metrics.km_median([3, 3], [True, False]).value == 3


def test_km_median_of_all_detected_is_the_lower_sample_median() -> None:
    assert metrics.km_median([4, 1, 2, 3], [True] * 4).value == 2  # S = 1/2 at 2: R says 2.5
    assert metrics.km_median([], []) == Median(None, None, None)
