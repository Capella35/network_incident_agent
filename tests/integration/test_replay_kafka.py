"""Replay against the running Kafka (`make up`), into throwaway topics that are deleted after.

Checks what a mock can't: the idempotent producer really delivers every message once, and each
partition holds its keys in replay order.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest

from common import telemetry
from common.telemetry import TelemetryMessage
from producer import replay

pytestmark = pytest.mark.integration

ck = pytest.importorskip("confluent_kafka")
admin_mod = pytest.importorskip("confluent_kafka.admin")

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", replay.DEFAULT_BOOTSTRAP)
PARTITIONS = 6


@pytest.fixture
def topics() -> Iterator[tuple[str, str]]:
    admin = admin_mod.AdminClient({"bootstrap.servers": BOOTSTRAP, "broker.address.family": "v4"})
    try:
        admin.list_topics(timeout=5)
    except ck.KafkaException:
        pytest.skip("Kafka not reachable; run `make up` first")
    suffix = uuid.uuid4().hex[:8]
    raw, dlq = f"it.replay.{suffix}", f"it.replay.dlq.{suffix}"
    new = [admin_mod.NewTopic(raw, PARTITIONS, 1), admin_mod.NewTopic(dlq, 1, 1)]
    for f in admin.create_topics(new).values():
        f.result(timeout=15)
    yield raw, dlq
    for f in admin.delete_topics([raw, dlq]).values():
        f.result(timeout=15)


def _messages(n_cells: int, n_intervals: int) -> list[TelemetryMessage]:
    t0 = datetime(2013, 11, 5, tzinfo=UTC)
    out = [
        TelemetryMessage(
            replay_run_id="it-run",
            cell_id=f"CELL-1-{c}",
            site_id="SITE-1",
            square_id=c,
            interval_start=t0 + timedelta(minutes=10 * i),
            **dict.fromkeys(telemetry.TRAFFIC_FIELDS, float(i)),
            data_gap=False,
            load_ratio=0.5,
            latency_ms=40.0,
            drop_rate=0.002,
        )
        for i in range(n_intervals)
        for c in range(1, n_cells + 1)
    ]
    return sorted(out, key=lambda m: (m.interval_start, m.cell_id))


def _consume(topic: str, expected: int) -> list[tuple[int, str]]:
    consumer = ck.Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "broker.address.family": "v4",
            "group.id": f"it-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    got: list[tuple[int, str]] = []
    try:
        for _ in range(60):
            for m in consumer.consume(num_messages=500, timeout=1.0):
                assert m.error() is None, m.error()
                got.append((m.partition(), m.key().decode()))
            if len(got) >= expected:
                break
    finally:
        consumer.close()
    return got


def test_every_message_once_and_in_order_per_partition(topics: tuple[str, str]) -> None:
    raw, dlq = topics
    msgs = _messages(n_cells=12, n_intervals=10)
    reject = replay.Reject(key="square-99|x", reason="square not in the topology", row={})
    producer = ck.Producer(replay.kafka_config(BOOTSTRAP))
    summary = replay.send(
        [reject, *msgs],
        producer,
        replay.Summary("it-run"),
        replay.Pacer(speed=0),
        topic=raw,
        dlq_topic=dlq,
    )
    assert (summary.sent, summary.dead_lettered, summary.failed) == (len(msgs), 1, 0)
    assert summary.delivered == len(msgs) + 1

    got = _consume(raw, len(msgs))
    keys = [k for _, k in got]
    assert len(keys) == len(msgs) and len(set(keys)) == len(keys)  # no loss, no duplicates
    sent_order = [m.key() for m in msgs]
    for p in range(PARTITIONS):
        in_p = [k for part, k in got if part == p]
        assert in_p == [k for k in sent_order if k in set(in_p)]  # replay order kept
    assert {p for p, _ in got} == set(summary.per_partition)
    assert len(_consume(dlq, 1)) == 1
