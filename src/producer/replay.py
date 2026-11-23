"""Replay processed Milan traffic into Kafka `telemetry.raw` (milestone 1, step 7).

    python -m producer.replay --start 2013-11-04 [--end 2013-11-04] [--speed 10] [--max-rate 2000]
    make replay ARGS="--start 2013-11-04 --speed 0"

One message per (cell, 10-minute interval), built from `data/processed/traffic/<day>.parquet`,
the SYNTHETIC topology, and the MODELED KPIs (`kpi_calibration.json`); schema in
`common.telemetry`. Messages are sent in `(interval_start, cell_id)` order with key
`cell_id|interval_start` through an idempotent producer (acks=all), so a replay of the same days
gives every partition the same key sequence. Rows that fail the schema go to `telemetry.dlq` with
the reason, never silently dropped.

`--speed` is data minutes per wall-clock second (10 = one interval per second; 0 = as fast as
possible); `--max-rate` caps messages per second. Test-week days are refused unless
`--final-eval` is given (Rule 7).
"""

from __future__ import annotations

import argparse
import functools
import itertools
import json
import logging
import sys
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import polars as pl
from pydantic import ValidationError

from common import kafka, splits, telemetry
from common.telemetry import TelemetryMessage
from producer import kpi_model, topology

log = logging.getLogger("producer.replay")

MAX_ERRORS_KEPT = 5  # delivery errors kept verbatim in the summary


class ReplayError(RuntimeError):
    pass


# --- Input -----------------------------------------------------------------------------------


def days_in_range(start: date, end: date, final_eval: bool = False) -> list[date]:
    """Inclusive day range; test-split days only with `final_eval` (Rule 7)."""
    if end < start:
        raise ReplayError(f"--end {end} is before --start {start}")
    out = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    test_days = [d for d in out if splits.split_of(d) is splits.Split.TEST]
    if test_days and not final_eval:
        raise ReplayError(
            f"{test_days[0]} is a test-week day; test weeks are replayed once, for the final "
            "numbers (pass --final-eval only then)"
        )
    return out


def load_traffic(data_dir: Path, days: Iterable[date]) -> pl.DataFrame:
    traffic_dir = data_dir / "processed" / "traffic"
    frames = []
    for d in days:
        path = traffic_dir / f"{d.isoformat()}.parquet"
        if not path.exists():
            raise ReplayError(f"{d} not processed: download it and run `make data`")
        frames.append(pl.read_parquet(path))
    return pl.concat(frames)


@dataclass(frozen=True)
class Reject:
    """A row that cannot become a valid message; goes to the dead-letter topic."""

    key: str
    reason: str
    row: dict[str, Any]

    def to_json(self, replay_run_id: str) -> bytes:
        doc = {
            "schema_version": telemetry.SCHEMA_VERSION,
            "replay_run_id": replay_run_id,
            "reason": self.reason,
            "row": {k: telemetry.finite_or_none(v) for k, v in self.row.items()},
        }
        return json.dumps(doc, sort_keys=True, allow_nan=False).encode("utf-8")


def build_rows(
    traffic: pl.DataFrame,
    cells: dict[int, str],
    cell_sites: dict[str, str],
    calibration: dict[str, Any],
    params: kpi_model.Params,
) -> tuple[pl.DataFrame, list[Reject]]:
    """Join topology + modeled KPIs, sort by (interval_start, cell_id); reject unknown squares."""
    known = traffic.filter(pl.col("square_id").is_in(list(cells)))
    rejects = [
        Reject(
            key=f"square-{r['square_id']}|{r['interval_start']}",
            reason="square not in the topology",
            row=r,
        )
        for r in traffic.filter(~pl.col("square_id").is_in(list(cells))).iter_rows(named=True)
    ]
    rows = kpi_model.add_kpis(known, cells, calibration, params).with_columns(
        pl.col("cell_id").replace_strict(cell_sites, default=None).alias("site_id")
    )
    columns = [
        "cell_id",
        "site_id",
        "square_id",
        "interval_start",
        *telemetry.TRAFFIC_FIELDS,
        "data_gap",
        *telemetry.MODELED_FIELDS,
    ]
    return rows.select(columns).sort("interval_start", "cell_id"), rejects


def to_messages(rows: pl.DataFrame, replay_run_id: str) -> Iterator[TelemetryMessage | Reject]:
    """Validate each row against the schema, in the frame's order."""
    for r in rows.iter_rows(named=True):
        try:
            yield TelemetryMessage(replay_run_id=replay_run_id, **r)
        except ValidationError as exc:
            reasons = "; ".join(
                f"{'.'.join(str(p) for p in e['loc']) or 'row'}: {e['msg']}" for e in exc.errors()
            )
            yield Reject(
                key=telemetry.message_key(str(r["cell_id"]), r["interval_start"]),
                reason=reasons,
                row=r,
            )


# --- Pacing ----------------------------------------------------------------------------------


@dataclass
class Pacer:
    """Holds messages back to `speed` (data minutes per wall second) and `max_rate` (msg/s)."""

    speed: float
    max_rate: float | None = None
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    _t0: float | None = None
    _first: datetime | None = None
    _sent: int = 0

    def __post_init__(self) -> None:
        if self.speed < 0:
            raise ReplayError("--speed must be >= 0 (0 = as fast as possible)")
        if self.max_rate is not None and self.max_rate <= 0:
            raise ReplayError("--max-rate must be > 0")

    def wait(self, interval_start: datetime) -> None:
        now = self.clock()
        if self._t0 is None or self._first is None:
            self._t0, self._first = now, interval_start
        due = self._t0
        if self.speed > 0:
            data_minutes = (interval_start - self._first).total_seconds() / 60.0
            due = max(due, self._t0 + data_minutes / self.speed)
        if self.max_rate is not None:
            due = max(due, self._t0 + self._sent / self.max_rate)
        if due > now:
            self.sleep(due - now)
        self._sent += 1


# --- Kafka -----------------------------------------------------------------------------------


class Producer(Protocol):
    """The part of `confluent_kafka.Producer` the replay uses (a fake in unit tests)."""

    def produce(
        self, topic: str, *, value: bytes, key: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


def kafka_config(bootstrap: str) -> kafka.Config:
    return kafka.producer_config(bootstrap, client_id="replay-producer")


@dataclass
class Summary:
    replay_run_id: str
    sent: int = 0
    dead_lettered: int = 0
    delivered: int = 0
    failed: int = 0
    per_partition: Counter[int] = field(default_factory=Counter)
    errors: list[str] = field(default_factory=list)

    def record_delivery(self, err: Any, msg: Any, telemetry_topic: str) -> None:
        """Delivery callback: count successes per telemetry partition, keep a few errors."""
        if err is not None:
            self.failed += 1
            if len(self.errors) < MAX_ERRORS_KEPT:
                self.errors.append(str(err))
            return
        self.delivered += 1
        if msg.topic() == telemetry_topic:
            self.per_partition[msg.partition()] += 1


def _produce(
    producer: Producer, topic: str, key: str, value: bytes, on_delivery: Callable[[Any, Any], None]
) -> None:
    """Produce, waiting for deliveries to drain whenever the local queue is full."""
    while True:
        try:
            producer.produce(topic, value=value, key=key.encode("utf-8"), on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.5)
    producer.poll(0)


def send(
    items: Iterable[TelemetryMessage | Reject],
    producer: Producer,
    summary: Summary,
    pacer: Pacer,
    *,
    topic: str = telemetry.TOPIC_RAW,
    dlq_topic: str = telemetry.TOPIC_DLQ,
    flush_timeout: float = 60.0,
) -> Summary:
    on_delivery = functools.partial(summary.record_delivery, telemetry_topic=topic)
    for item in items:
        if isinstance(item, Reject):
            _produce(
                producer, dlq_topic, item.key, item.to_json(summary.replay_run_id), on_delivery
            )
            summary.dead_lettered += 1
            continue
        pacer.wait(item.interval_start)
        _produce(producer, topic, item.key(), item.to_json(), on_delivery)
        summary.sent += 1
    remaining = producer.flush(flush_timeout)
    if remaining:
        summary.failed += remaining
        summary.errors.append(f"{remaining} message(s) still queued after {flush_timeout}s")
    return summary


# --- Entry point -----------------------------------------------------------------------------


def run(
    data_dir: Path,
    start: date,
    end: date,
    producer: Producer,
    pacer: Pacer,
    *,
    replay_run_id: str | None = None,
    final_eval: bool = False,
) -> Summary:
    telemetry.check_against_domain()
    days = days_in_range(start, end, final_eval)
    processed = data_dir / "processed"
    nodes = processed / "topology_nodes.csv"
    if not nodes.exists():
        raise ReplayError(f"{nodes} missing: run `make data` first")
    rows, rejects = build_rows(
        load_traffic(data_dir, days),
        topology.load_cells(nodes),
        topology.load_cell_sites(nodes),
        kpi_model.load_calibration(data_dir),
        kpi_model.load_params(),
    )
    summary = Summary(replay_run_id=replay_run_id or str(uuid.uuid4()))
    log.info(
        "replay %s: %s -> %s, %d rows, %d unknown-square rows",
        summary.replay_run_id,
        days[0],
        days[-1],
        rows.height,
        len(rejects),
    )
    items = itertools.chain(
        rejects, to_messages(rows, summary.replay_run_id)
    )  # lazy: one row at a time
    return send(items, producer, summary, pacer)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--start", type=date.fromisoformat, required=True, help="first day (Milan)")
    ap.add_argument("--end", type=date.fromisoformat, help="last day, inclusive (default: start)")
    ap.add_argument("--speed", type=float, default=10.0, help="data minutes per second; 0 = max")
    ap.add_argument("--max-rate", type=float, default=None, help="messages per second cap")
    ap.add_argument("--run-id", default=None, help="replay_run_id (default: a new UUID)")
    ap.add_argument("--final-eval", action="store_true", help="allow test-week days (Rule 7)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    end = args.end or args.start
    try:  # check the cheap things before a Kafka client starts its threads
        days_in_range(args.start, end, args.final_eval)
        pacer = Pacer(speed=args.speed, max_rate=args.max_rate)
    except ReplayError as exc:
        log.error("%s", exc)
        return 1

    from confluent_kafka import Producer as KafkaProducer

    bootstrap = kafka.bootstrap_servers()
    try:
        summary = run(
            args.data_dir,
            args.start,
            end,
            KafkaProducer(kafka_config(bootstrap)),
            pacer,
            replay_run_id=args.run_id,
            final_eval=args.final_eval,
        )
    except (ReplayError, FileNotFoundError, ValueError) as exc:
        log.error("%s", exc)
        return 1
    log.info(
        "replay %s: sent %d, dead-lettered %d, delivered %d, failed %d; per partition %s",
        summary.replay_run_id,
        summary.sent,
        summary.dead_lettered,
        summary.delivered,
        summary.failed,
        dict(sorted(summary.per_partition.items())),
    )
    for e in summary.errors:
        log.error("delivery error: %s", e)
    return 0 if summary.failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
