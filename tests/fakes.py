"""Test doubles shared by unit tests: a fake Kafka message and producer, a manual clock."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from common import telemetry


@dataclass
class FakeKafkaMessage:
    """Stands in for `confluent_kafka.Message` (the read-only accessors the code uses)."""

    value_: bytes | None = None
    key_: bytes | None = None
    topic_: str = telemetry.TOPIC_RAW
    partition_: int = 0
    offset_: int = 0
    error_: Any = None

    def error(self) -> Any:
        return self.error_

    def value(self) -> bytes | None:
        return self.value_

    def key(self) -> bytes | None:
        return self.key_

    def topic(self) -> str:
        return self.topic_

    def partition(self) -> int:
        return self.partition_

    def offset(self) -> int:
        return self.offset_


@dataclass
class FakeProducer:
    """Stands in for `confluent_kafka.Producer` (`common.kafka.Producer`): keeps what was sent;
    each flush appends `<name>-flush` to `log` (to check ordering against other fakes), reports
    `unacked` messages as still queued, and calls each delivery callback with `error` (None:
    delivered)."""

    log: list[str]
    name: str
    unacked: int = 0
    error: Any = None
    sent: list[tuple[str, bytes, bytes]] = field(default_factory=list)
    callbacks: list[Callable[[Any, Any], None]] = field(default_factory=list)

    def produce(
        self,
        topic: str,
        *,
        value: bytes,
        key: bytes,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> None:
        self.sent.append((topic, key, value))
        if on_delivery is not None:
            self.callbacks.append(on_delivery)

    def flush(self, timeout: float) -> int:
        self.log.append(f"{self.name}-flush")
        for callback in self.callbacks:
            callback(self.error, None)
        self.callbacks.clear()
        return self.unacked


class FakeClock:
    """A manual monotonic clock: each read advances it by `tick`; `sleep` advances it too."""

    def __init__(self, start: float = 100.0, tick: float = 0.0) -> None:
        self.now = start
        self.tick = tick
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        self.now += self.tick
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds
