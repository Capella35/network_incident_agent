"""Milestone 1 Definition of done: re-running the replay reproduces the same rows (step 10).

    python -m producer.replay_check --start 2013-11-04 [--end 2013-11-10]
    make replay-check ARGS="--start 2013-11-04 --end 2013-11-10"

Replays the same days twice under two new `replay_run_id`s, then compares the two runs:

  (a) Kafka: each run's messages are read back from exactly the offset range it wrote; per
      partition, the key sequences must be identical (and so must the payloads once
      `replay_run_id` is blanked out).
  (b) Postgres: after the sink has written both runs, an ordered MD5 over every
      `telemetry_raw` column except `replay_run_id` and `ingested_at` must be identical, as
      must the row counts. Read as the `readonly` role.

Exit 0 only if everything matches. Needs the stack (`make up`); the sink is run in-process
with the normal consumer group, so it also drains anything else waiting in `telemetry.raw`.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Protocol

import psycopg
from psycopg import sql

from common import splits, telemetry
from common.db import DbConfigError, DbSettings, Role
from producer import replay, sink

log = logging.getLogger("producer.replay_check")

_EXCLUDED = ("replay_run_id",)  # differs by design; ingested_at is not a message field
_ORDER = ("cell_id", "interval_start")


class CheckError(RuntimeError):
    pass


# --- Kafka read-back ---------------------------------------------------------------------------


class Message(Protocol):
    def error(self) -> Any: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...


class RangeReader(Protocol):
    """What `read_range` needs from a Kafka consumer (a fake in unit tests)."""

    def end_offset(self, topic: str, partition: int) -> int: ...
    def assign_from(self, topic: str, starts: Mapping[int, int]) -> None: ...
    def consume(self, num_messages: int, timeout: float) -> Sequence[Message]: ...


def end_offsets(reader: RangeReader, topic: str, partitions: Sequence[int]) -> dict[int, int]:
    return {p: reader.end_offset(topic, p) for p in partitions}


def read_range(
    reader: RangeReader,
    topic: str,
    start: Mapping[int, int],
    end: Mapping[int, int],
    timeout_s: float = 120.0,
    clock: Callable[[], float] = time.monotonic,
) -> dict[int, list[tuple[str, bytes]]]:
    """(key, value) per partition for offsets in [start, end), in offset order."""
    out: dict[int, list[tuple[str, bytes]]] = {p: [] for p in start}
    todo = {p for p in start if end[p] > start[p]}
    if not todo:
        return out
    reader.assign_from(topic, {p: start[p] for p in todo})
    deadline = clock() + timeout_s
    while todo:
        if clock() > deadline:
            raise CheckError(f"timed out reading back partitions {sorted(todo)} of {topic}")
        for m in reader.consume(1000, 1.0):
            if m.error() is not None:
                raise CheckError(f"kafka error while reading back: {m.error()}")
            p, off = m.partition(), m.offset()
            if p is None or off is None or p not in todo or off >= end[p]:
                continue
            out[p].append(((m.key() or b"").decode("utf-8"), m.value() or b""))
            if off >= end[p] - 1:
                todo.discard(p)
    return out


def normalized_payload(value: bytes) -> bytes:
    """The message with `replay_run_id` blanked, as canonical JSON."""
    doc = json.loads(value)
    doc["replay_run_id"] = ""
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass
class KafkaRun:
    replay_run_id: str
    keys: dict[int, list[str]] = field(default_factory=dict)
    payload_sha256: dict[int, str] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return sum(len(k) for k in self.keys.values())


def kafka_run(replay_run_id: str, messages: Mapping[int, list[tuple[str, bytes]]]) -> KafkaRun:
    """Keep only this run's messages (anything else in the range is ignored, and counted)."""
    run = KafkaRun(replay_run_id)
    for p, msgs in sorted(messages.items()):
        digest = hashlib.sha256()
        keys = []
        for key, value in msgs:
            if json.loads(value).get("replay_run_id") != replay_run_id:
                continue
            keys.append(key)
            digest.update(normalized_payload(value) + b"\n")
        run.keys[p] = keys
        run.payload_sha256[p] = digest.hexdigest()
    return run


# --- Postgres digest ---------------------------------------------------------------------------


def digest_columns() -> tuple[str, ...]:
    return tuple(c for c in sink.COLUMNS if c not in _EXCLUDED)


def digest_statement() -> sql.Composed:
    """count(*) and an ordered MD5 over the run's rows; NULL is spelled out, not skipped."""
    fields = sql.SQL(", ").join(
        sql.SQL("coalesce({c}::text, 'NULL')").format(c=sql.Identifier(c)) for c in digest_columns()
    )
    order = sql.SQL(", ").join(map(sql.Identifier, _ORDER))
    return sql.SQL(
        "SELECT count(*), md5(coalesce(string_agg(concat_ws('|', {fields}), E'\\n' "
        "ORDER BY {order}), '')) FROM {table} WHERE replay_run_id = %s"
    ).format(fields=fields, order=order, table=sql.Identifier(sink.TABLE))


def db_digest(conn: psycopg.Connection[Any], replay_run_id: str) -> tuple[int, str]:
    conn.execute("SET TIME ZONE 'UTC'")  # timestamptz::text must not depend on the session
    row = conn.execute(digest_statement(), (replay_run_id,)).fetchone()
    if row is None:
        raise CheckError("digest query returned nothing")
    return int(row[0]), str(row[1])


# --- Comparison --------------------------------------------------------------------------------


@dataclass
class Report:
    days: list[date]
    runs: tuple[str, str]
    sent: tuple[int, int] = (0, 0)
    kafka: tuple[KafkaRun, KafkaRun] | None = None
    db: tuple[tuple[int, str], tuple[int, str]] | None = None
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.problems


def compare(report: Report) -> Report:
    a_sent, b_sent = report.sent
    if a_sent != b_sent:
        report.problems.append(f"sent {a_sent} vs {b_sent} messages")
    if report.kafka is not None:
        ka, kb = report.kafka
        if ka.count != a_sent or kb.count != b_sent:
            report.problems.append(
                f"read back {ka.count}/{kb.count} messages, sent {a_sent}/{b_sent}"
            )
        for p in sorted(set(ka.keys) | set(kb.keys)):
            if ka.keys.get(p) != kb.keys.get(p):
                report.problems.append(f"partition {p}: key sequences differ")
            elif ka.payload_sha256.get(p) != kb.payload_sha256.get(p):
                report.problems.append(f"partition {p}: payloads differ (beyond replay_run_id)")
    if report.db is not None:
        (na, ha), (nb, hb) = report.db
        if na != a_sent or nb != b_sent:
            report.problems.append(f"telemetry_raw has {na}/{nb} rows, sent {a_sent}/{b_sent}")
        if ha != hb:
            report.problems.append("telemetry_raw row digests differ")
    return report


def render(report: Report) -> str:
    a, b = report.runs
    lines = [
        f"replay-check {report.days[0]} -> {report.days[-1]} ({len(report.days)} day(s))",
        f"  runs: {a} | {b}",
        f"  sent: {report.sent[0]} | {report.sent[1]}",
    ]
    if report.kafka is not None:
        ka, kb = report.kafka
        lines.append(f"  kafka read back: {ka.count} | {kb.count}")
        for p in sorted(ka.keys):
            same = ka.keys[p] == kb.keys.get(p) and ka.payload_sha256[p] == kb.payload_sha256.get(p)
            lines.append(
                f"    partition {p}: {len(ka.keys[p])} keys, "
                f"payload sha256 {ka.payload_sha256[p][:12]}… {'==' if same else '!='}"
            )
    if report.db is not None:
        (na, ha), (nb, hb) = report.db
        lines.append(f"  telemetry_raw rows: {na} | {nb}")
        lines.append(f"  telemetry_raw md5:  {ha} | {hb}")
    lines.append(
        "  RESULT: " + ("MATCH" if report.ok else "MISMATCH: " + "; ".join(report.problems))
    )
    return "\n".join(lines)


# --- Orchestration (real Kafka + Postgres) -----------------------------------------------------


class _KafkaRangeReader:
    """`RangeReader` over a confluent_kafka.Consumer in assign (no-group) mode."""

    def __init__(self, consumer: Any) -> None:
        self.c = consumer

    def end_offset(self, topic: str, partition: int, attempts: int = 20) -> int:
        from confluent_kafka import KafkaException, TopicPartition

        for i in range(attempts):
            try:
                _, high = self.c.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
                return int(high)
            except KafkaException:  # e.g. NOT_LEADER right after a topic is created
                if i == attempts - 1:  # read-only call: retrying is always safe
                    raise
                time.sleep(0.5)
        raise CheckError("unreachable")  # pragma: no cover

    def assign_from(self, topic: str, starts: Mapping[int, int]) -> None:
        from confluent_kafka import TopicPartition

        self.c.assign([TopicPartition(topic, p, o) for p, o in starts.items()])

    def consume(self, num_messages: int, timeout: float) -> Sequence[Message]:
        msgs: Sequence[Message] = self.c.consume(num_messages=num_messages, timeout=timeout)
        return msgs


def _wait_for_rows(
    conn: psycopg.Connection[Any], runs: Sequence[str], expected: int, timeout_s: float = 300
) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        counts = [
            conn.execute(
                "SELECT count(*) FROM telemetry_raw WHERE replay_run_id = %s", (r,)
            ).fetchone()
            for r in runs
        ]
        conn.rollback()
        if all(c is not None and c[0] >= expected for c in counts):
            return
        if time.monotonic() > deadline:
            raise CheckError(f"sink did not write {expected} rows per run in {timeout_s}s")
        time.sleep(2)


def run_check(data_dir: Path, start: date, end: date, bootstrap: str) -> Report:
    from confluent_kafka import Consumer as KafkaConsumer
    from confluent_kafka import Producer as KafkaProducer

    days = replay.days_in_range(start, end)  # refuses test-week days (Rule 7)
    tag = uuid.uuid4().hex[:8]
    runs = (f"replay-check-{start}-{tag}-a", f"replay-check-{start}-{tag}-b")
    report = Report(days=days, runs=runs)
    topic = telemetry.TOPIC_RAW

    producer = KafkaProducer(replay.kafka_config(bootstrap))
    raw_consumer = KafkaConsumer(
        {
            "bootstrap.servers": bootstrap,
            "broker.address.family": "v4",
            "group.id": f"replay-check-{tag}",  # assign mode: never commits
            "enable.auto.commit": False,
        }
    )
    reader = _KafkaRangeReader(raw_consumer)
    try:
        partitions = sorted(raw_consumer.list_topics(topic, timeout=10).topics[topic].partitions)
        kafka_runs: list[KafkaRun] = []
        sent: list[int] = []
        for run in runs:
            before = end_offsets(reader, topic, partitions)
            summary = replay.run(data_dir, start, end, producer, replay.Pacer(speed=0), run)
            if summary.failed or summary.dead_lettered:
                raise CheckError(
                    f"replay {run}: {summary.failed} failed, {summary.dead_lettered} dead-lettered"
                )
            after = end_offsets(reader, topic, partitions)
            kafka_runs.append(kafka_run(run, read_range(reader, topic, before, after)))
            sent.append(summary.sent)
            log.info("run %s: sent %d, read back %d", run, summary.sent, kafka_runs[-1].count)
        report.sent = (sent[0], sent[1])
        report.kafka = (kafka_runs[0], kafka_runs[1])
    finally:
        raw_consumer.close()

    writer_settings = DbSettings.from_env(Role.PIPELINE_WRITER)
    sink_consumer = KafkaConsumer(sink.consumer_config(bootstrap))
    sink_consumer.subscribe([topic])
    with writer_settings.connect() as wconn:
        s = sink.run(sink_consumer, sink.PgWriter(wconn), producer, idle_exit=5.0)
    log.info(
        "sink: consumed %d, written %d, dead-lettered %d", s.consumed, s.written, s.dead_lettered
    )

    with DbSettings.from_env(Role.READONLY).connect() as rconn:
        _wait_for_rows(rconn, runs, max(report.sent))
        report.db = (db_digest(rconn, runs[0]), db_digest(rconn, runs[1]))
    return compare(report)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument(
        "--start",
        type=date.fromisoformat,
        default=splits.bounds()[splits.Split.TRAIN][0],
        help="first day (default: first training day)",
    )
    ap.add_argument("--end", type=date.fromisoformat, help="last day, inclusive (default: start)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    end = args.end or args.start
    try:
        replay.days_in_range(args.start, end)
        DbSettings.from_env(Role.PIPELINE_WRITER)
        DbSettings.from_env(Role.READONLY)
    except (replay.ReplayError, DbConfigError) as exc:
        log.error("%s", exc)
        return 1
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", replay.DEFAULT_BOOTSTRAP)
    try:
        report = run_check(args.data_dir, args.start, end, bootstrap)
    except (CheckError, replay.ReplayError, FileNotFoundError, psycopg.OperationalError) as exc:
        log.error("replay-check could not finish: %s", exc)
        return 2
    print(render(report))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
