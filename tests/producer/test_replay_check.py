from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pytest

from producer import replay_check as rc
from producer import sink
from tests.fakes import FakeKafkaMessage

DAY = date(2013, 11, 4)


def payload(run: str, key: str, internet: float = 1.0) -> bytes:
    cell, ts = key.split("|")
    return json.dumps(
        {"replay_run_id": run, "cell_id": cell, "interval_start": ts, "internet": internet}
    ).encode()


# --- fake Kafka --------------------------------------------------------------------------------


@dataclass
class FakeReader:
    """A topic as {partition: [(key, value), ...]}; offsets are list positions."""

    log: dict[int, list[tuple[str, bytes]]]
    batch: int = 3
    _queue: list[FakeKafkaMessage] = field(default_factory=list)

    def end_offset(self, topic: str, partition: int) -> int:
        return len(self.log[partition])

    def assign_from(self, topic: str, starts: Mapping[int, int]) -> None:
        self._queue = [
            FakeKafkaMessage(partition_=p, offset_=o, key_=k.encode(), value_=v)
            for p, start in sorted(starts.items())
            for o, (k, v) in enumerate(self.log[p])
            if o >= start
        ]

    def consume(self, num_messages: int, timeout: float) -> Sequence[FakeKafkaMessage]:
        out, self._queue = self._queue[: self.batch], self._queue[self.batch :]
        return out


def append(reader: FakeReader, run: str, keys_by_partition: dict[int, list[str]]) -> None:
    for p, keys in keys_by_partition.items():
        reader.log.setdefault(p, []).extend((k, payload(run, k)) for k in keys)


KEYS = {
    0: ["CELL-1-1|2013-11-03T23:00:00Z", "CELL-1-1|2013-11-03T23:10:00Z"],
    1: ["CELL-1-2|2013-11-03T23:00:00Z"],
    2: [],
}


# --- read-back ---------------------------------------------------------------------------------


def test_read_range_returns_only_the_written_window() -> None:
    reader = FakeReader({0: [], 1: [], 2: []})
    append(reader, "old", {0: ["CELL-9-9|2013-11-01T00:00:00Z"]})  # before the window
    before = rc.end_offsets(reader, "t", [0, 1, 2])
    append(reader, "run-a", KEYS)
    after = rc.end_offsets(reader, "t", [0, 1, 2])
    append(reader, "later", {1: ["CELL-9-9|2013-11-02T00:00:00Z"]})  # after the window
    got = rc.read_range(reader, "t", before, after)
    assert {p: [k for k, _ in v] for p, v in got.items()} == KEYS


def test_read_range_with_nothing_written_is_empty() -> None:
    reader = FakeReader({0: [], 1: []})
    offsets = rc.end_offsets(reader, "t", [0, 1])
    assert rc.read_range(reader, "t", offsets, offsets) == {0: [], 1: []}


def test_read_range_times_out() -> None:
    reader = FakeReader({0: [("k", b"{}")]})
    reader.consume = lambda n, t: []  # type: ignore[method-assign]
    ticks = iter([0.0, 0.0, 10.0])
    with pytest.raises(rc.CheckError, match="timed out"):
        rc.read_range(reader, "t", {0: 0}, {0: 1}, timeout_s=5, clock=lambda: next(ticks))


def test_read_range_surfaces_kafka_errors() -> None:
    reader = FakeReader({0: [("k", b"{}")]})
    reader.consume = lambda n, t: [
        FakeKafkaMessage(partition_=0, offset_=0, key_=b"k", value_=b"{}", error_="broker gone")
    ]  # type: ignore[method-assign]
    with pytest.raises(rc.CheckError, match="broker gone"):
        rc.read_range(reader, "t", {0: 0}, {0: 1})


# --- per-run evidence --------------------------------------------------------------------------


def test_payload_normalization_ignores_only_the_run_id() -> None:
    k = KEYS[0][0]
    assert rc.normalized_payload(payload("a", k)) == rc.normalized_payload(payload("b", k))
    assert rc.normalized_payload(payload("a", k, 1.0)) != rc.normalized_payload(
        payload("a", k, 2.0)
    )


def test_kafka_run_keeps_only_its_own_messages() -> None:
    msgs = {0: [(KEYS[0][0], payload("a", KEYS[0][0])), ("x|y", payload("other", "x|y"))]}
    run = rc.kafka_run("a", msgs)
    assert run.keys == {0: [KEYS[0][0]]} and run.count == 1


def _report(a: dict[int, list[str]], b: dict[int, list[str]], **kw: Any) -> rc.Report:
    def evidence(run: str, keys: dict[int, list[str]]) -> rc.KafkaRun:
        return rc.kafka_run(run, {p: [(k, payload(run, k)) for k in ks] for p, ks in keys.items()})

    n_a, n_b = sum(map(len, a.values())), sum(map(len, b.values()))
    report = rc.Report(days=[DAY], runs=("a", "b"), sent=(n_a, n_b))
    report.kafka = (evidence("a", a), evidence("b", b))
    report.db = kw.get("db", ((n_a, "h"), (n_b, "h")))
    return rc.compare(report)


def test_identical_runs_match() -> None:
    report = _report(KEYS, KEYS)
    assert report.ok, report.problems
    assert "RESULT: MATCH" in rc.render(report)


def test_reordered_partition_is_a_mismatch() -> None:
    swapped = {**KEYS, 0: list(reversed(KEYS[0]))}
    report = _report(KEYS, swapped)
    assert report.problems == ["partition 0: key sequences differ"]
    assert "MISMATCH" in rc.render(report)


def test_changed_payload_is_a_mismatch() -> None:
    report = rc.Report(days=[DAY], runs=("a", "b"), sent=(1, 1))
    k = KEYS[1][0]
    report.kafka = (
        rc.kafka_run("a", {1: [(k, payload("a", k, 1.0))]}),
        rc.kafka_run("b", {1: [(k, payload("b", k, 9.0))]}),
    )
    assert rc.compare(report).problems == ["partition 1: payloads differ (beyond replay_run_id)"]


def test_db_digest_or_count_mismatch_reported() -> None:
    n = sum(map(len, KEYS.values()))
    report = _report(KEYS, KEYS, db=((n, "h1"), (n - 1, "h2")))
    assert "telemetry_raw row digests differ" in report.problems
    assert any("rows, sent" in p for p in report.problems)


def test_missing_read_back_reported() -> None:
    short = {**KEYS, 0: KEYS[0][:1]}
    report = _report(KEYS, short)
    report2 = rc.Report(days=[DAY], runs=("a", "b"), sent=(3, 3), kafka=report.kafka)
    assert any("read back" in p for p in rc.compare(report2).problems)


# --- SQL ---------------------------------------------------------------------------------------


def test_digest_covers_every_column_but_the_run_id() -> None:
    cols = rc.digest_columns()
    assert "replay_run_id" not in cols
    assert set(cols) | {"replay_run_id"} == set(sink.COLUMNS)
    text = repr(rc.digest_statement())
    for c in cols:
        assert f"Identifier('{c}')" in text
    assert "'NULL'" in text and "ORDER BY" in text and "md5" in text


def test_main_refuses_test_week() -> None:
    assert rc.main(["--start", "2013-12-20"]) == 1


def test_main_needs_db_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_WRITER_PASSWORD", raising=False)
    assert rc.main(["--start", str(DAY)]) == 1
