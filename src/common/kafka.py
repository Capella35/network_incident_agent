"""Kafka client settings shared by every client in this repo (producer, sink, checks, tests)."""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any, Protocol

DEFAULT_BOOTSTRAP = "127.0.0.1:9092"  # the broker's host listener (infra/docker-compose.yml)

Config = dict[str, str | int | bool]


class Producer(Protocol):
    """What this repo's producers use of `confluent_kafka.Producer` (fakes in unit tests)."""

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> None:
        """`on_delivery(err, msg)` runs from `flush`/`poll` once the broker answers; `err` is
        None on success. A message that failed for good is no longer queued, so `flush`
        returning 0 alone does not mean it was delivered."""
        ...

    def flush(self, timeout: float) -> int: ...


def bootstrap_servers() -> str:
    """`KAFKA_BOOTSTRAP_SERVERS` from the environment (filled from .env by make), else local."""
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS") or DEFAULT_BOOTSTRAP


def base_config(bootstrap: str) -> Config:
    """Settings every client needs. IPv4 only: `localhost` can resolve to ::1, and the
    broker is published on 127.0.0.1 only."""
    return {"bootstrap.servers": bootstrap, "broker.address.family": "v4"}


def producer_config(bootstrap: str, client_id: str) -> Config:
    """Idempotent, ordered producer: same key -> same partition as the Java clients, and
    at most 5 requests in flight so idempotence keeps per-partition order."""
    return {
        **base_config(bootstrap),
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "partitioner": "murmur2_random",
        "linger.ms": 20,
        "compression.type": "lz4",
    }


def consumer_config(bootstrap: str, group_id: str, **extra: str | int | bool) -> Config:
    """A consumer that never auto-commits; callers commit (or never do) explicitly."""
    return {**base_config(bootstrap), "group.id": group_id, "enable.auto.commit": False, **extra}


def end_offset(consumer: Any, topic: str, partition: int, attempts: int = 20) -> int:
    """The partition's end offset (high watermark) via a `confluent_kafka.Consumer`, retried:
    right after a topic is created a broker can answer NOT_LEADER for a moment."""
    from confluent_kafka import KafkaException, TopicPartition

    for i in range(attempts):
        try:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            return int(high)
        except KafkaException:
            if i == attempts - 1:  # read-only call: retrying is always safe
                raise
            time.sleep(0.5)
    raise AssertionError("unreachable")  # pragma: no cover


def end_offsets(consumer: Any, topic: str) -> dict[int, int]:
    """End offset of every partition of `topic`."""
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    return {p: end_offset(consumer, topic, p) for p in sorted(partitions)}
