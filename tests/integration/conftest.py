"""Shared fixtures for tests against the running stack (`make up`)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any, Protocol

import psycopg
import pytest

from common import kafka
from common.db import DbConfigError, DbSettings, Role
from producer import replay

ck = pytest.importorskip("confluent_kafka")
ck_admin = pytest.importorskip("confluent_kafka.admin")

BOOTSTRAP = kafka.bootstrap_servers()


def unique(prefix: str) -> str:
    """A name no other test run uses (topics, consumer groups, replay run IDs)."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class MakeTopics(Protocol):
    def __call__(self, *specs: tuple[str, int]) -> list[str]: ...


@pytest.fixture
def make_topics() -> Iterator[MakeTopics]:
    """Factory: `make_topics(("raw", 6), ("dlq", 1))` creates throwaway topics (deleted after
    the test) and returns their names in the same order."""
    admin = ck_admin.AdminClient(kafka.base_config(BOOTSTRAP))
    try:
        admin.list_topics(timeout=5)
    except ck.KafkaException:
        pytest.skip("Kafka not reachable; run `make up` first")
    created: list[str] = []

    def make(*specs: tuple[str, int]) -> list[str]:
        names = [unique(f"it.{label}") for label, _ in specs]
        new = [ck_admin.NewTopic(n, p, 1) for n, (_, p) in zip(names, specs, strict=True)]
        for f in admin.create_topics(new).values():
            f.result(timeout=15)
        created.extend(names)
        return names

    yield make
    if created:  # the admin client rejects an empty delete list
        for f in admin.delete_topics(created).values():
            f.result(timeout=15)


@pytest.fixture
def producer() -> Any:
    """The replay's idempotent producer, pointed at the local broker."""
    return ck.Producer(replay.kafka_config(BOOTSTRAP))


def consume(topic: str, expected: int, max_wait_s: int = 60) -> list[tuple[int, str, bytes]]:
    """(partition, key, value) of up to `expected` messages from the start of `topic`, in a
    throwaway consumer group that never commits."""
    consumer = ck.Consumer(
        kafka.consumer_config(BOOTSTRAP, unique("it"), **{"auto.offset.reset": "earliest"})
    )
    consumer.subscribe([topic])
    got: list[tuple[int, str, bytes]] = []
    try:
        for _ in range(max_wait_s):
            # Ask only for what is missing: a batch consume blocks until it has that many.
            for m in consumer.consume(num_messages=max(1, expected - len(got)), timeout=1.0):
                assert m.error() is None, m.error()
                got.append((m.partition(), (m.key() or b"").decode(), m.value() or b""))
            if len(got) >= expected:
                break
    finally:
        consumer.close()
    return got


@pytest.fixture
def writer_conn() -> Iterator[psycopg.Connection[Any]]:
    """A `pipeline_writer` connection; tests wrap writes in a transaction they roll back."""
    try:
        settings = DbSettings.from_env(Role.PIPELINE_WRITER)
    except DbConfigError as exc:
        pytest.skip(f"stack credentials not in the environment: {exc}")
    with settings.connect() as conn:
        yield conn
