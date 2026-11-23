"""Sink against the running stack: throwaway Kafka topics, real Postgres, nothing left behind.

`pipeline_writer` may not delete telemetry, so every write here happens inside an outer
transaction that is rolled back; the sink's own transactions become savepoints within it.
"""

from __future__ import annotations

import json
from typing import Any

import psycopg
import pytest

from common.telemetry import TelemetryMessage
from producer import sink
from tests.factories import message_grid
from tests.integration.conftest import BOOTSTRAP, MakeTopics, ck, consume, unique

pytestmark = pytest.mark.integration


def _unvalidated(msg: TelemetryMessage, **over: Any) -> TelemetryMessage:
    """A message that skips validation (model_construct): the sink or Postgres must catch it."""
    return TelemetryMessage.model_construct(**{**msg.model_dump(), **over})


def test_sink_writes_once_dead_letters_and_rolls_back(
    make_topics: MakeTopics, producer: Any, writer_conn: psycopg.Connection[Any]
) -> None:
    raw, dlq = make_topics(("sink", 3), ("sink-dlq", 1))
    run = unique("it-sink")
    msgs = message_grid(run, n_cells=6, n_intervals=4)
    zero_latency = _unvalidated(msgs[0], latency_ms=0.0, cell_id="CELL-9-9", square_id=99)
    for m in [*msgs, *msgs, zero_latency]:  # every message twice: re-delivery must not duplicate
        producer.produce(raw, value=m.to_json(), key=m.key().encode())
    producer.produce(raw, value=b"not json", key=b"junk")
    assert producer.flush(15) == 0

    consumer = ck.Consumer(sink.consumer_config(BOOTSTRAP, unique("it")))
    consumer.subscribe([raw])
    settings = sink.LoopSettings(batch_size=10, idle_exit=3.0, dlq_topic=dlq)
    with writer_conn.transaction() as outer:
        summary = sink.run(
            consumer,
            sink.PgWriter(writer_conn),
            producer,
            settings,
        )
        rows = writer_conn.execute(
            "SELECT count(*), count(DISTINCT cell_id), min(schema_version) "
            "FROM telemetry_raw WHERE replay_run_id = %s",
            (run,),
        ).fetchone()
        raise psycopg.Rollback(outer)  # undo every write made above

    assert summary.consumed == 2 * len(msgs) + 2
    assert summary.written == 2 * len(msgs)
    assert summary.dead_lettered == 2
    assert rows == (len(msgs), 6, 1)  # 24 unique rows despite 48 deliveries
    assert sink.count_rows(writer_conn, run) == 0  # nothing left behind

    reasons = [json.loads(v)["reason"] for _, _, v in consume(dlq, 2, max_wait_s=20)]
    assert any("latency_ms" in r for r in reasons)  # caught by the schema before Postgres
    assert any("json" in r.lower() for r in reasons)


def test_writer_falls_back_row_by_row(writer_conn: psycopg.Connection[Any]) -> None:
    """A row Postgres refuses is reported alone; the rest of the batch is still written."""
    run = unique("it-writer")
    good = message_grid(run, n_cells=3, n_intervals=1)
    bad = _unvalidated(good[0], latency_ms=0.0, cell_id="CELL-9-9")
    with writer_conn.transaction() as outer:
        failed = sink.PgWriter(writer_conn).upsert([*good, bad])
        n = sink.count_rows(writer_conn, run)
        raise psycopg.Rollback(outer)
    assert n == 3
    assert [m.cell_id for m, _ in failed] == ["CELL-9-9"]
    assert "23514" in failed[0][1]  # check_violation
