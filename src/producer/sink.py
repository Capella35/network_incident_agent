"""Kafka `telemetry.raw` -> Postgres `telemetry_raw` (milestone 1, step 8).

    python -m producer.sink [--idle-exit 10] [--batch-size 500]
    make sink ARGS="--idle-exit 10"

A plain Python consumer (Spark takes over feature work in milestone 2). Per batch: validate each
message with `common.telemetry`, upsert the valid rows in one transaction as `pipeline_writer`,
send invalid messages to `telemetry.dlq` with the reason, and only then commit the batch's Kafka
offsets. A crash at any point re-delivers the batch; the upsert on
`(replay_run_id, cell_id, interval_start)` makes that harmless (at-least-once + idempotent
upsert = effectively once). If the batch insert fails on a data error, rows are retried one by
one and only the failing rows are dead-lettered, so one bad row never blocks the stream.

Runs until Ctrl-C / SIGTERM (finishes the current batch first), or `--idle-exit` seconds without
messages.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import psycopg
from psycopg import sql
from pydantic import ValidationError

from common import kafka, telemetry
from common.db import DbConfigError, DbSettings, Role, insert_into
from common.telemetry import TelemetryMessage

log = logging.getLogger("producer.sink")

PROGRESS_EVERY = 20  # batches between progress log lines
GROUP_ID = "telemetry-sink"
TABLE = "telemetry_raw"
PRIMARY_KEY = ("replay_run_id", "cell_id", "interval_start")
COLUMNS = tuple(TelemetryMessage.model_fields)  # every message field is a column
_MAX_DLQ_VALUE = 10_000  # characters of the raw value kept in a DLQ record


class SinkError(RuntimeError):
    pass


# --- Kafka-side protocols (fakes in unit tests) --------------------------------------------


class Message(Protocol):
    def error(self) -> Any: ...
    def value(self) -> bytes | None: ...
    def key(self) -> bytes | None: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...


class Consumer(Protocol):
    def consume(self, num_messages: int, timeout: float) -> Sequence[Message]: ...
    def commit(self, *, asynchronous: Literal[False]) -> object: ...
    def close(self) -> None: ...


class Writer(Protocol):
    def upsert(self, rows: Sequence[TelemetryMessage]) -> list[tuple[TelemetryMessage, str]]:
        """Write rows durably; return the rows that could not be written, with the reason."""
        ...


# --- Parsing ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class DeadLetter:
    reason: str
    key: str
    value: str
    source: dict[str, Any]

    def to_json(self) -> bytes:
        doc = {
            "schema_version": telemetry.SCHEMA_VERSION,
            "stage": "sink",
            "reason": self.reason,
            "source": self.source,
            "value": self.value,
        }
        return json.dumps(doc, sort_keys=True).encode("utf-8")


def _source(msg: Message) -> dict[str, Any]:
    return {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()}


def parse(msg: Message) -> TelemetryMessage | DeadLetter:
    raw = msg.value() or b""
    key = (msg.key() or b"").decode("utf-8", errors="replace")
    text = raw.decode("utf-8", errors="replace")
    try:
        parsed = TelemetryMessage.model_validate_json(raw)
    except ValidationError as exc:
        reason = "; ".join(
            f"{'.'.join(str(p) for p in e['loc']) or 'message'}: {e['msg']}" for e in exc.errors()
        )
        return DeadLetter(reason, key, text[:_MAX_DLQ_VALUE], _source(msg))
    if key and key != parsed.key():
        return DeadLetter(
            f"key {key!r} does not match the message ({parsed.key()!r})",
            key,
            text[:_MAX_DLQ_VALUE],
            _source(msg),
        )
    return parsed


# --- Postgres --------------------------------------------------------------------------------


def upsert_statement() -> sql.Composed:
    """INSERT ... ON CONFLICT (pk) DO UPDATE: re-delivered messages overwrite, never duplicate."""
    updates = sql.SQL(", ").join(
        sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c))
        for c in COLUMNS
        if c not in PRIMARY_KEY
    )
    return insert_into(TABLE, COLUMNS) + sql.SQL(
        " ON CONFLICT ({pk}) DO UPDATE SET {updates}, ingested_at = now()"
    ).format(pk=sql.SQL(", ").join(map(sql.Identifier, PRIMARY_KEY)), updates=updates)


def _params(m: TelemetryMessage) -> tuple[Any, ...]:
    return tuple(getattr(m, c) for c in COLUMNS)


class PgWriter:
    """Upserts with one transaction per batch; row-by-row fallback on data errors."""

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn
        self.stmt = upsert_statement()

    def upsert(self, rows: Sequence[TelemetryMessage]) -> list[tuple[TelemetryMessage, str]]:
        if not rows:
            return []
        try:
            with self.conn.transaction(), self.conn.cursor() as cur:
                cur.executemany(self.stmt, [_params(m) for m in rows])
            return []
        except (psycopg.errors.IntegrityError, psycopg.errors.DataError) as exc:
            log.warning("batch of %d rejected (%s); retrying row by row", len(rows), exc.sqlstate)
        failed: list[tuple[TelemetryMessage, str]] = []
        for m in rows:
            try:
                with self.conn.transaction():
                    self.conn.execute(self.stmt, _params(m))
            except (psycopg.errors.IntegrityError, psycopg.errors.DataError) as exc:
                failed.append((m, f"postgres {exc.sqlstate}: {exc.diag.message_primary}"))
        return failed


# --- Loop ------------------------------------------------------------------------------------


@dataclass
class Summary:
    consumed: int = 0
    written: int = 0
    dead_lettered: int = 0
    batches: int = 0
    kafka_errors: list[str] = field(default_factory=list)


def process_batch(
    batch: Sequence[Message],
    writer: Writer,
    dlq: kafka.Producer,
    consumer: Consumer,
    summary: Summary,
    *,
    dlq_topic: str = telemetry.TOPIC_DLQ,
) -> None:
    good: list[TelemetryMessage] = []
    dead: list[DeadLetter] = []
    for msg in batch:
        err = msg.error()
        if err is not None:  # transport-level error (not a bad payload): nothing to store
            summary.kafka_errors.append(str(err))
            continue
        summary.consumed += 1
        item = parse(msg)
        if isinstance(item, DeadLetter):
            dead.append(item)
        else:
            good.append(item)

    failed = writer.upsert(good)  # 1. Postgres commit
    for m, reason in failed:
        dead.append(DeadLetter(reason, m.key(), m.model_dump_json(), {"stage": "postgres"}))
    summary.written += len(good) - len(failed)

    for d in dead:  # 2. dead letters durable
        dlq.produce(dlq_topic, value=d.to_json(), key=d.key.encode("utf-8"))
    if dead and dlq.flush(30.0):
        raise SinkError("dead-letter topic did not acknowledge; offsets not committed")
    summary.dead_lettered += len(dead)

    consumer.commit(asynchronous=False)  # 3. only now: Kafka offsets
    summary.batches += 1


@dataclass(frozen=True)
class LoopSettings:
    batch_size: int = 500
    idle_exit: float | None = None  # stop after this many quiet seconds; None = run until stopped
    poll_timeout: float = 1.0
    dlq_topic: str = telemetry.TOPIC_DLQ


DEFAULT_LOOP = LoopSettings()


def count_rows(conn: psycopg.Connection[Any], replay_run_id: str) -> int:
    """Rows stored for one replay run."""
    row = conn.execute(
        sql.SQL("SELECT count(*) FROM {} WHERE replay_run_id = %s").format(sql.Identifier(TABLE)),
        (replay_run_id,),
    ).fetchone()
    return int(row[0]) if row else 0


def run(
    consumer: Consumer,
    writer: Writer,
    dlq: kafka.Producer,
    settings: LoopSettings = DEFAULT_LOOP,
    *,
    stop: Callable[[], bool] = lambda: False,
    clock: Callable[[], float] = time.monotonic,
) -> Summary:
    summary = Summary()
    last_message = clock()
    try:
        while not stop():
            batch = consumer.consume(
                num_messages=settings.batch_size, timeout=settings.poll_timeout
            )
            if not batch:
                if settings.idle_exit is not None and clock() - last_message >= settings.idle_exit:
                    break
                continue
            last_message = clock()
            process_batch(batch, writer, dlq, consumer, summary, dlq_topic=settings.dlq_topic)
            if summary.batches % PROGRESS_EVERY == 0:
                log.info(
                    "progress: %d written, %d dead-lettered", summary.written, summary.dead_lettered
                )
    finally:
        consumer.close()  # leaves the group cleanly; uncommitted batches are re-delivered
    return summary


def consumer_config(bootstrap: str, group_id: str = GROUP_ID) -> kafka.Config:
    # No auto-commit (common.kafka): offsets are committed only after the DB commit.
    return kafka.consumer_config(
        bootstrap,
        group_id,
        **{
            "client.id": "telemetry-sink",
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        },
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--batch-size", type=int, default=LoopSettings.batch_size)
    ap.add_argument("--idle-exit", type=float, default=None, help="stop after N quiet seconds")
    ap.add_argument("--group-id", default=GROUP_ID)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.batch_size < 1:
        log.error("--batch-size must be >= 1")
        return 1
    try:
        settings = DbSettings.from_env(Role.PIPELINE_WRITER)
    except DbConfigError as exc:
        log.error("%s", exc)
        return 1

    from confluent_kafka import Consumer as KafkaConsumer
    from confluent_kafka import Producer as KafkaProducer

    stopping = False

    def request_stop(signum: int, _frame: object) -> None:
        nonlocal stopping
        log.info("signal %d: finishing the current batch, then stopping", signum)
        stopping = True

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    bootstrap = kafka.bootstrap_servers()
    consumer = KafkaConsumer(consumer_config(bootstrap, args.group_id))
    consumer.subscribe([telemetry.TOPIC_RAW])
    try:
        with settings.connect() as conn:
            summary = run(
                consumer,
                PgWriter(conn),
                KafkaProducer(kafka.producer_config(bootstrap, client_id="telemetry-sink-dlq")),
                LoopSettings(batch_size=args.batch_size, idle_exit=args.idle_exit),
                stop=lambda: stopping,
            )
    except (psycopg.OperationalError, SinkError) as exc:
        log.error("%s (uncommitted messages will be re-delivered on the next start)", exc)
        return 2
    log.info(
        "sink: consumed %d, written %d, dead-lettered %d, %d batch(es)",
        summary.consumed,
        summary.written,
        summary.dead_lettered,
        summary.batches,
    )
    for e in summary.kafka_errors[:5]:
        log.warning("kafka error: %s", e)
    return 0


if __name__ == "__main__":
    sys.exit(main())
