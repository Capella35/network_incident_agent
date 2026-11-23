"""replay_check pieces against the running stack: throwaway topic, rolled-back writes."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest

from common import telemetry
from common.db import DbConfigError, DbSettings, Role
from common.telemetry import TelemetryMessage
from producer import replay, sink
from producer import replay_check as rc

pytestmark = pytest.mark.integration

ck = pytest.importorskip("confluent_kafka")
admin_mod = pytest.importorskip("confluent_kafka.admin")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", replay.DEFAULT_BOOTSTRAP)
PARTITIONS = 4


def _messages(run: str) -> list[TelemetryMessage]:
    t0 = datetime(2013, 11, 5, tzinfo=UTC)
    msgs = [
        TelemetryMessage(
            replay_run_id=run,
            cell_id=f"CELL-1-{c}",
            site_id="SITE-1",
            square_id=c,
            interval_start=t0 + timedelta(minutes=10 * i),
            **dict.fromkeys(telemetry.TRAFFIC_FIELDS, c + i / 7),
            data_gap=False,
            load_ratio=0.3 + i / 100,
            latency_ms=25.0 + c,
            drop_rate=0.002,
        )
        for i in range(6)
        for c in range(1, 9)
    ]
    return sorted(msgs, key=lambda m: (m.interval_start, m.cell_id))


@pytest.fixture
def topic() -> Iterator[str]:
    admin = admin_mod.AdminClient({"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4"})
    try:
        admin.list_topics(timeout=5)
    except ck.KafkaException:
        pytest.skip("Kafka not reachable; run `make up` first")
    name = f"it.replaycheck.{uuid.uuid4().hex[:8]}"
    for f in admin.create_topics([admin_mod.NewTopic(name, PARTITIONS, 1)]).values():
        f.result(timeout=15)
    yield name
    for f in admin.delete_topics([name]).values():
        f.result(timeout=15)


def test_two_runs_read_back_identically(topic: str) -> None:
    producer = ck.Producer(replay.kafka_config(BOOTSTRAP))
    consumer = ck.Consumer(
        {"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4", "group.id": "it-rc"}
    )
    reader = rc._KafkaRangeReader(consumer)
    parts = list(range(PARTITIONS))
    runs = []
    try:
        for run in ("it-a", "it-b"):
            before = rc.end_offsets(reader, topic, parts)
            replay.send(
                _messages(run), producer, replay.Summary(run), replay.Pacer(speed=0), topic=topic
            )
            after = rc.end_offsets(reader, topic, parts)
            runs.append(rc.kafka_run(run, rc.read_range(reader, topic, before, after)))
    finally:
        consumer.close()
    report = rc.Report(days=[datetime(2013, 11, 5).date()], runs=("it-a", "it-b"))
    report.sent = (len(_messages("x")), len(_messages("x")))
    report.kafka = (runs[0], runs[1])
    assert rc.compare(report).ok, report.problems
    assert runs[0].count == 48 and len(runs[0].keys) == PARTITIONS


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    try:
        settings = DbSettings.from_env(Role.PIPELINE_WRITER)
    except DbConfigError as exc:
        pytest.skip(f"stack credentials not in the environment: {exc}")
    with settings.connect() as c:
        yield c


def test_db_digest_equal_for_equal_rows_and_sensitive_to_one_value(
    conn: psycopg.Connection[Any],
) -> None:
    tag = uuid.uuid4().hex[:8]
    a, b, c = f"it-dig-{tag}-a", f"it-dig-{tag}-b", f"it-dig-{tag}-c"
    changed = _messages(c)
    changed[7] = changed[7].model_copy(update={"internet": changed[7].internet + 1e-9})
    with conn.transaction() as outer:
        writer = sink.PgWriter(conn)
        for rows in (_messages(a), list(reversed(_messages(b))), changed):  # b inserted in reverse
            assert writer.upsert(rows) == []
        da, db, dc = (rc.db_digest(conn, r) for r in (a, b, c))
        raise psycopg.Rollback(outer)
    assert da[0] == db[0] == dc[0] == 48
    assert da == db  # insertion order does not matter, only content
    assert da[1] != dc[1]  # a 1e-9 change in one value is visible
