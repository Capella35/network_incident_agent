"""Kafka client settings shared by every client in this repo (producer, sink, checks, tests)."""

from __future__ import annotations

import os

DEFAULT_BOOTSTRAP = "127.0.0.1:9092"  # the broker's host listener (infra/docker-compose.yml)

Config = dict[str, str | int | bool]


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
