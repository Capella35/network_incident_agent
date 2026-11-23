"""Test doubles shared by unit tests: one fake Kafka message, one manual clock."""

from __future__ import annotations

from dataclasses import dataclass
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
