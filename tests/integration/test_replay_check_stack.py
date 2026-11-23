"""replay_check pieces against the running stack: throwaway topic, rolled-back writes."""

from __future__ import annotations

from datetime import date
from typing import Any

import psycopg
import pytest

from common import kafka
from common.telemetry import TelemetryMessage
from producer import replay, sink
from producer import replay_check as rc
from tests.factories import message_grid
from tests.integration.conftest import BOOTSTRAP, MakeTopics, ck, unique

pytestmark = pytest.mark.integration

PARTITIONS = 4
N_CELLS, N_INTERVALS = 8, 6
N = N_CELLS * N_INTERVALS  # messages (and rows) per run


def _grid(run: str) -> list[TelemetryMessage]:
    return message_grid(run, N_CELLS, N_INTERVALS)


def test_two_runs_read_back_identically(make_topics: MakeTopics, producer: Any) -> None:
    (topic,) = make_topics(("replaycheck", PARTITIONS))
    consumer = ck.Consumer(kafka.consumer_config(BOOTSTRAP, unique("it")))
    reader = rc._KafkaRangeReader(consumer)
    parts = list(range(PARTITIONS))
    runs = []
    try:
        for run in ("it-a", "it-b"):
            before = rc.end_offsets(reader, topic, parts)
            replay.send(
                _grid(run), producer, replay.Summary(run), replay.Pacer(speed=0), topic=topic
            )
            after = rc.end_offsets(reader, topic, parts)
            runs.append(rc.kafka_run(run, rc.read_range(reader, topic, before, after)))
    finally:
        consumer.close()
    report = rc.Report(days=[date(2013, 11, 5)], runs=("it-a", "it-b"), sent=(N, N))
    report.kafka = (runs[0], runs[1])
    assert rc.compare(report).ok, report.problems
    assert runs[0].count == N and len(runs[0].keys) == PARTITIONS


def test_db_digest_equal_for_equal_rows_and_sensitive_to_one_value(
    writer_conn: psycopg.Connection[Any],
) -> None:
    tag = unique("it-dig")
    a, b, c = f"{tag}-a", f"{tag}-b", f"{tag}-c"
    changed = _grid(c)
    changed[7] = changed[7].model_copy(update={"internet": changed[7].internet + 1e-9})
    with writer_conn.transaction() as outer:
        writer = sink.PgWriter(writer_conn)
        for rows in (_grid(a), list(reversed(_grid(b))), changed):  # b inserted in reverse
            assert writer.upsert(rows) == []
        da, db, dc = (rc.db_digest(writer_conn, r) for r in (a, b, c))
        raise psycopg.Rollback(outer)
    assert da[0] == db[0] == dc[0] == N
    assert da == db  # insertion order does not matter, only content
    assert da[1] != dc[1]  # a 1e-9 change in one value is visible
