"""The parity check's pure parts (milestone 2, step 9): the replay plan, the alarm comparison,
the report, the wait for the job. The check itself runs against the stack (`make detect-check`)."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pytest

from common import telemetry
from evaluation import detect_check as dc
from tests.factories import ALARM_AT, CHECK_DAY, alarm_frame, alarm_message, check_report


def test_history_from_an_earlier_split_is_replayed_clean() -> None:
    day = date(2013, 12, 3)  # the second validation day
    history = [day - timedelta(days=i) for i in range(7, 0, -1)]
    p = dc.plan(day, history)
    assert p.clean == [date(2013, 11, d) for d in range(26, 31)] + [date(2013, 12, 1)]
    assert p.injected == [date(2013, 12, 2), day]
    first_day = date(2013, 12, 2)  # the first validation day: all its history is training
    first = dc.plan(first_day, [first_day - timedelta(days=i) for i in range(7, 0, -1)])
    assert (len(first.clean), first.injected) == (7, [first_day])


def test_identical_alarm_sets_have_no_differences() -> None:
    a = alarm_frame(alarm_message(), alarm_message(at=ALARM_AT + timedelta(minutes=10)))
    assert dc.compare(a, a.reverse()) == []


def test_every_kind_of_difference_is_listed() -> None:
    same = alarm_message()
    later = ALARM_AT + timedelta(minutes=10)
    streamed = alarm_frame(same, alarm_message(at=later, score=3.5), alarm_message(run="only-s"))
    offline = alarm_frame(
        same.model_copy(update={"kpis": ("traffic_sms",)}),
        alarm_message(at=later, score=3.5 + 1e-15),
        alarm_message(run="only-o"),
    )
    diffs = dc.compare(streamed, offline)
    assert len(diffs) == 4
    assert sum(d.startswith("only streamed") for d in diffs) == 1
    assert sum(d.startswith("only offline") for d in diffs) == 1
    assert any(f"score 3.5 vs {3.5 + 1e-15!r}" in d for d in diffs)  # exact, no tolerance
    assert any("kpis ['traffic_internet', 'load_ratio'] vs ['traffic_sms']" in d for d in diffs)


def test_render_states_the_verdict() -> None:
    r = check_report([])
    assert "MATCH" in dc.render(r)
    assert "SYNTHETIC" in dc.render(r)
    assert dc.render(r).endswith(r.validation_report)
    assert r.validation_report.startswith(f"# Validation report, {CHECK_DAY}")
    many = [f"d{i}" for i in range(dc.SHOWN + 3)]
    text = dc.render(check_report(many))
    assert f"MISMATCH ({dc.SHOWN + 3} differences)" in text
    assert "… 3 more" in text


def test_stored_row_holds_totals_and_the_verdict() -> None:
    assert dc.report_row(check_report([])) == {
        "replay_run_id": "detect-check-x",
        "day": CHECK_DAY,
        "detector_version": "detector/1",
        "faults": 4,
        "unobservable": 1,
        "detected": 2,
        "delay_median": 1,
        "delay_low": 0,
        "delay_high": None,
        "incidents": 3,
        "false_incidents": 1,
        "parity": True,
    }
    assert dc.report_row(check_report(["only streamed: x"]))["parity"] is False


def write_batch(checkpoint: Path, batch: int, offsets: dict[str, int], *, done: bool) -> None:
    """A Spark offset-log entry (version line, metadata line, source offsets) and its commit."""
    (checkpoint / "offsets").mkdir(parents=True, exist_ok=True)
    (checkpoint / "commits").mkdir(exist_ok=True)
    source = json.dumps({telemetry.TOPIC_RAW: offsets})
    (checkpoint / "offsets" / str(batch)).write_text(f'v1\n{{"batchWatermarkMs":0}}\n{source}\n')
    if done:
        (checkpoint / "commits" / str(batch)).write_text('v1\n{"nextBatchWatermarkMs":0}\n')


def test_committed_offsets_are_those_of_the_last_committed_batch(tmp_path: Path) -> None:
    assert dc.committed_offsets(tmp_path) == {}
    write_batch(tmp_path, 9, {"0": 90, "1": 91}, done=True)
    write_batch(tmp_path, 10, {"0": 100, "1": 101}, done=True)
    write_batch(tmp_path, 11, {"0": 110, "1": 111}, done=False)  # still running
    assert dc.committed_offsets(tmp_path) == {0: 100, 1: 101}


def test_the_check_waits_until_the_job_committed_past_the_replay(tmp_path: Path) -> None:
    write_batch(tmp_path, 0, {"0": 50, "1": 60}, done=True)
    dc.wait_for_stream(tmp_path, {0: 50, 1: 60}, timeout_s=0)
    with pytest.raises(dc.CheckError, match="15 messages behind"):
        dc.wait_for_stream(tmp_path, {0: 55, 1: 70}, timeout_s=0)


def test_a_history_with_an_unprocessed_day_is_refused() -> None:
    day = date(2013, 12, 3)
    history = [day - timedelta(days=i) for i in (7, 6, 5, 3, 2, 1)]  # day - 4 not processed
    with pytest.raises(dc.CheckError, match="unprocessed day"):
        dc.plan(day, history)
