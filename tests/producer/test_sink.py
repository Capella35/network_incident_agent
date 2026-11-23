from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import pytest

from common import kafka, telemetry
from common.telemetry import TelemetryMessage
from producer import sink
from tests.factories import telemetry_message as msg
from tests.fakes import FakeClock, FakeKafkaMessage

# --- fakes -----------------------------------------------------------------------------------


def kmsg(m: TelemetryMessage, offset: int = 0) -> FakeKafkaMessage:
    return FakeKafkaMessage(value_=m.to_json(), key_=m.key().encode(), offset_=offset)


@dataclass
class Events:
    log: list[str] = field(default_factory=list)


@dataclass
class FakeConsumer:
    events: Events
    batches: list[list[FakeKafkaMessage]] = field(default_factory=list)
    commits: int = 0
    closed: bool = False

    def consume(self, num_messages: int, timeout: float) -> Sequence[FakeKafkaMessage]:
        return self.batches.pop(0) if self.batches else []

    def commit(self, *, asynchronous: Literal[False]) -> object:
        self.commits += 1
        self.events.log.append("commit")
        return None

    def close(self) -> None:
        self.closed = True


@dataclass
class FakeWriter:
    events: Events
    fail: set[str] = field(default_factory=set)  # keys that the "database" rejects
    rows: dict[tuple[str, str, datetime], TelemetryMessage] = field(default_factory=dict)

    def upsert(self, rows: Sequence[TelemetryMessage]) -> list[tuple[TelemetryMessage, str]]:
        self.events.log.append(f"upsert:{len(rows)}")
        failed = []
        for m in rows:
            if m.key() in self.fail:
                failed.append((m, "postgres 23514: check violation"))
            else:
                self.rows[(m.replay_run_id, m.cell_id, m.interval_start)] = m
        return failed


@dataclass
class FakeDlq:
    events: Events
    unacked: int = 0
    sent: list[tuple[str, bytes, bytes]] = field(default_factory=list)

    def produce(self, topic: str, *, value: bytes, key: bytes) -> None:
        self.sent.append((topic, key, value))

    def flush(self, timeout: float) -> int:
        self.events.log.append("dlq-flush")
        return self.unacked


def wiring(
    batches: list[list[FakeKafkaMessage]], **writer: Any
) -> tuple[Events, FakeConsumer, FakeWriter, FakeDlq]:
    ev = Events()
    return ev, FakeConsumer(ev, batches), FakeWriter(ev, **writer), FakeDlq(ev)


def quick(idle_exit: float | None = None) -> sink.LoopSettings:
    """Loop settings for tests: no real waiting on an empty poll."""
    return sink.LoopSettings(idle_exit=idle_exit, poll_timeout=0)


# --- parsing ---------------------------------------------------------------------------------


def test_valid_message_parses() -> None:
    m = msg()
    assert sink.parse(kmsg(m)) == m


@pytest.mark.parametrize(
    ("value", "fragment"),
    [
        (b"not json", "json"),
        (
            json.dumps({**json.loads(msg().to_json()), "schema_version": 2}).encode(),
            "schema_version",
        ),
        (json.dumps({**json.loads(msg().to_json()), "internet": -1}).encode(), "internet"),
        (None, "message"),
    ],
)
def test_invalid_messages_become_dead_letters(value: bytes | None, fragment: str) -> None:
    out = sink.parse(FakeKafkaMessage(value_=value, key_=b"k", offset_=7))
    assert isinstance(out, sink.DeadLetter)
    assert fragment in out.reason.lower()
    assert out.source == {"topic": telemetry.TOPIC_RAW, "partition": 0, "offset": 7}


def test_key_must_match_message() -> None:
    m = msg()
    out = sink.parse(FakeKafkaMessage(value_=m.to_json(), key_=b"CELL-9-9|2013-11-05T00:00:00Z"))
    assert isinstance(out, sink.DeadLetter) and "does not match" in out.reason


def test_dead_letter_json() -> None:
    d = sink.DeadLetter("bad", "k", "raw text", {"topic": "t", "partition": 1, "offset": 2})
    doc = json.loads(d.to_json())
    assert doc["stage"] == "sink" and doc["reason"] == "bad" and doc["value"] == "raw text"


# --- SQL -------------------------------------------------------------------------------------


def test_upsert_statement_covers_every_field_and_keys_on_pk() -> None:
    assert set(sink.COLUMNS) == set(TelemetryMessage.model_fields)
    assert set(sink.PRIMARY_KEY) <= set(sink.COLUMNS)
    text = repr(sink.upsert_statement())
    assert "ON CONFLICT" in text and "EXCLUDED" in text and "ingested_at = now()" in text
    for c in sink.COLUMNS:
        assert f"Identifier('{c}')" in text


# --- batch processing ------------------------------------------------------------------------


def test_db_commit_before_offset_commit() -> None:
    ev, consumer, writer, dlq = wiring([])
    sink.process_batch(
        [kmsg(msg(cell=1)), FakeKafkaMessage(value_=b"junk", key_=b"x")],
        writer,
        dlq,
        consumer,
        sink.Summary(),
    )
    assert ev.log == ["upsert:1", "dlq-flush", "commit"]


def test_batch_counts_and_dlq_contents() -> None:
    _ev, consumer, writer, dlq = wiring([])
    s = sink.Summary()
    sink.process_batch(
        [
            kmsg(msg(cell=1)),
            kmsg(msg(cell=2)),
            FakeKafkaMessage(value_=b"junk", key_=b"x", offset_=3),
        ],
        writer,
        dlq,
        consumer,
        s,
    )
    assert (s.consumed, s.written, s.dead_lettered, s.batches) == (3, 2, 1, 1)
    topic, key, value = dlq.sent[0]
    assert topic == telemetry.TOPIC_DLQ and key == b"x"
    assert json.loads(value)["source"]["offset"] == 3


def test_rows_the_database_rejects_are_dead_lettered() -> None:
    bad = msg(cell=2)
    _ev, consumer, writer, dlq = wiring([], fail={bad.key()})
    s = sink.Summary()
    sink.process_batch([kmsg(msg(cell=1)), kmsg(bad)], writer, dlq, consumer, s)
    assert (s.written, s.dead_lettered) == (1, 1)
    doc = json.loads(dlq.sent[0][2])
    assert doc["source"] == {"stage": "postgres"} and "23514" in doc["reason"]
    assert consumer.commits == 1  # the stream moves on


def test_unacknowledged_dlq_blocks_the_offset_commit() -> None:
    _ev, consumer, writer, dlq = wiring([])
    dlq.unacked = 1
    with pytest.raises(sink.SinkError):
        sink.process_batch(
            [FakeKafkaMessage(value_=b"junk", key_=b"x")], writer, dlq, consumer, sink.Summary()
        )
    assert consumer.commits == 0  # batch will be re-delivered


def test_transport_errors_are_reported_not_stored() -> None:
    _ev, consumer, writer, dlq = wiring([])
    s = sink.Summary()
    sink.process_batch(
        [FakeKafkaMessage(error_="broker gone"), kmsg(msg())], writer, dlq, consumer, s
    )
    assert s.kafka_errors == ["broker gone"] and (s.consumed, s.written) == (1, 1)
    assert dlq.sent == []


def test_redelivery_is_idempotent() -> None:
    m = kmsg(msg(cell=1))
    _ev, consumer, writer, dlq = wiring([[m], [m]])
    s = sink.run(consumer, writer, dlq, quick(idle_exit=0.5), clock=FakeClock(start=0.0, tick=1.0))
    assert s.written == 2 and len(writer.rows) == 1


# --- loop ------------------------------------------------------------------------------------


def test_run_stops_when_idle_and_closes_consumer() -> None:
    batches = [[kmsg(msg(cell=c, minutes=10 * i)) for c in (1, 2, 3)] for i in range(4)]
    _ev, consumer, writer, dlq = wiring(batches)
    s = sink.run(consumer, writer, dlq, quick(idle_exit=3.0), clock=FakeClock(start=0.0, tick=1.0))
    assert (s.written, s.batches, consumer.commits) == (12, 4, 4)
    assert consumer.closed


def test_run_honours_stop_request() -> None:
    _ev, consumer, writer, dlq = wiring([[kmsg(msg())]] * 5)
    calls = iter([False, False, True])
    s = sink.run(consumer, writer, dlq, quick(), stop=lambda: next(calls))
    assert s.batches == 2 and consumer.closed


def test_consumer_closed_even_on_failure() -> None:
    _ev, consumer, writer, dlq = wiring([[FakeKafkaMessage(value_=b"junk", key_=b"x")]])
    dlq.unacked = 1
    with pytest.raises(sink.SinkError):
        sink.run(consumer, writer, dlq, quick())
    assert consumer.closed


def test_consumer_config_commits_manually() -> None:
    cfg = sink.consumer_config(kafka.DEFAULT_BOOTSTRAP)
    assert cfg["enable.auto.commit"] is False and cfg["auto.offset.reset"] == "earliest"


def test_main_needs_db_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_WRITER_PASSWORD", raising=False)
    assert sink.main(["--idle-exit", "1"]) == 1


def test_main_rejects_bad_batch_size() -> None:
    assert sink.main(["--batch-size", "0"]) == 1
