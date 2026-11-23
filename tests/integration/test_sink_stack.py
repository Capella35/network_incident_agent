"""Sink against the running stack: throwaway Kafka topics, real Postgres, nothing left behind.

`pipeline_writer` may not delete telemetry, so every write here happens inside an outer
transaction that is rolled back; the sink's own transactions become savepoints within it.
"""

from __future__ import annotations

import json
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

pytestmark = pytest.mark.integration

ck = pytest.importorskip("confluent_kafka")
admin_mod = pytest.importorskip("confluent_kafka.admin")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", replay.DEFAULT_BOOTSTRAP)


@pytest.fixture
def topics() -> Iterator[tuple[str, str]]:
    admin = admin_mod.AdminClient({"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4"})
    try:
        admin.list_topics(timeout=5)
    except ck.KafkaException:
        pytest.skip("Kafka not reachable; run `make up` first")
    suffix = uuid.uuid4().hex[:8]
    raw, dlq = f"it.sink.{suffix}", f"it.sink.dlq.{suffix}"
    for f in admin.create_topics(
        [admin_mod.NewTopic(raw, 3, 1), admin_mod.NewTopic(dlq, 1, 1)]
    ).values():
        f.result(timeout=15)
    yield raw, dlq
    for f in admin.delete_topics([raw, dlq]).values():
        f.result(timeout=15)


@pytest.fixture
def conn() -> Iterator[psycopg.Connection[Any]]:
    try:
        settings = DbSettings.from_env(Role.PIPELINE_WRITER)
    except DbConfigError as exc:
        pytest.skip(f"stack credentials not in the environment: {exc}")
    with settings.connect() as c:
        yield c


def _messages(run: str, n_cells: int = 6, n_intervals: int = 4) -> list[TelemetryMessage]:
    t0 = datetime(2013, 11, 5, tzinfo=UTC)
    return [
        TelemetryMessage(
            replay_run_id=run,
            cell_id=f"CELL-1-{c}",
            site_id="SITE-1",
            square_id=c,
            interval_start=t0 + timedelta(minutes=10 * i),
            **dict.fromkeys(telemetry.TRAFFIC_FIELDS, float(c)),
            data_gap=False,
            load_ratio=0.3,
            latency_ms=25.0,
            drop_rate=0.002,
        )
        for i in range(n_intervals)
        for c in range(1, n_cells + 1)
    ]


def test_sink_writes_once_dead_letters_and_rolls_back(
    topics: tuple[str, str], conn: psycopg.Connection[Any]
) -> None:
    raw, dlq = topics
    run = f"it-sink-{uuid.uuid4().hex[:8]}"
    msgs = _messages(run)
    # Built with model_construct to skip validation on the way in; the sink must catch it.
    bad = {**msgs[0].model_dump(), "latency_ms": 0.0, "cell_id": "CELL-9-9", "square_id": 99}
    db_reject = TelemetryMessage.model_construct(**bad)
    producer = ck.Producer(replay.kafka_config(BOOTSTRAP))
    for m in [*msgs, *msgs, db_reject]:  # every message twice: re-delivery must not duplicate
        producer.produce(raw, value=m.to_json(), key=m.key().encode())
    producer.produce(raw, value=b"not json", key=b"junk")
    assert producer.flush(15) == 0

    consumer = ck.Consumer(sink.consumer_config(BOOTSTRAP, f"it-{uuid.uuid4().hex[:8]}"))
    consumer.subscribe([raw])
    dlq_producer = ck.Producer(replay.kafka_config(BOOTSTRAP))

    with conn.transaction() as outer:
        summary = sink.run(
            consumer,
            sink.PgWriter(conn),
            dlq_producer,
            batch_size=10,
            idle_exit=5.0,
            dlq_topic=dlq,
        )
        rows = conn.execute(
            "SELECT count(*), count(DISTINCT cell_id), min(schema_version) "
            "FROM telemetry_raw WHERE replay_run_id = %s",
            (run,),
        ).fetchone()
        raise psycopg.Rollback(outer)  # undo every write made above

    assert summary.consumed == 2 * len(msgs) + 2
    assert summary.written == 2 * len(msgs)
    assert summary.dead_lettered == 2
    assert rows == (len(msgs), 6, 1)  # 24 unique rows despite 48 deliveries

    left = conn.execute(
        "SELECT count(*) FROM telemetry_raw WHERE replay_run_id = %s", (run,)
    ).fetchone()
    assert left == (0,)  # rolled back: nothing left behind

    seen = ck.Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "broker.address.family": "v4",
            "group.id": f"it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    seen.subscribe([dlq])
    reasons: list[str] = []
    for _ in range(20):
        for m in seen.consume(10, 1.0):
            if m.error() is None:
                reasons.append(json.loads(m.value())["reason"])
        if len(reasons) >= 2:
            break
    seen.close()
    assert any("latency_ms" in r for r in reasons)  # caught by the schema before Postgres
    assert any("json" in r.lower() for r in reasons)


def test_writer_falls_back_row_by_row(conn: psycopg.Connection[Any]) -> None:
    """A row Postgres refuses is reported alone; the rest of the batch is still written."""
    run = f"it-writer-{uuid.uuid4().hex[:8]}"
    good = _messages(run, n_cells=3, n_intervals=1)
    bad = TelemetryMessage.model_construct(
        **{**good[0].model_dump(), "latency_ms": 0.0, "cell_id": "CELL-9-9"}
    )
    with conn.transaction() as outer:
        failed = sink.PgWriter(conn).upsert([*good, bad])
        n = conn.execute(
            "SELECT count(*) FROM telemetry_raw WHERE replay_run_id = %s", (run,)
        ).fetchone()
        raise psycopg.Rollback(outer)
    assert n == (3,)
    assert [m.cell_id for m, _ in failed] == ["CELL-9-9"]
    assert "23514" in failed[0][1]  # check_violation
