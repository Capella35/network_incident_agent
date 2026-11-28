"""Spark Structured Streaming detector job (milestone 2, step 8; specs/03-detection.md).

    python -m streaming.job [--checkpoint /checkpoints/detector]   # in the detector image
    make detect / make detect-stop

Reads Kafka `telemetry.raw`; per (replay run, cell), `applyInPandasWithState` keeps the cell's
recent raw rows as state and scores the new ones with the champion (`streaming.scoring`). Per
micro-batch the alarms go to Kafka `alarms` (key = alarm ID), then to Postgres `alarms` together
with one `detector_heartbeats` row, in one transaction.

Restarts: Spark re-runs an unfinished micro-batch from its checkpoint with the same state, so it
raises the same alarms with the same deterministic IDs (`common.alarms.alarm_id`); alarms already
in Postgres are left out before anything is sent, and the insert skips any that remain. Postgres
therefore gets every alarm once. Kafka is at-least-once: a crash between the Kafka send and the
Postgres commit sends that batch's alarms again, under the same keys (consumers dedupe by ID).

The champion (`models:/detector@champion`) is loaded once at start, by version, from a local copy
(`DEFAULT_MODEL_CACHE`; downloaded once per version); a new promotion takes effect on restart. The
replay validates every message before sending it; a message that does not parse here is skipped.
Settings: `job.yaml`. Connections from the environment: KAFKA_BOOTSTRAP_SERVERS,
MLFLOW_TRACKING_URI, POSTGRES_* and PIPELINE_WRITER_PASSWORD.
"""

from __future__ import annotations

import argparse
import functools
import logging
import operator
import signal
import sys
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, astuple, dataclass, fields
from datetime import datetime
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import polars as pl
import psycopg
from polars.datatypes import DataTypeClass
from psycopg import sql
from pydantic import Field, PositiveInt
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common import alarms, kafka, telemetry
from common.alarms import AlarmMessage
from common.db import DbConfigError, DbSettings, Role, insert_into
from common.domain import load_domain
from common.domain.loader import FrozenModel, read_config
from streaming import features, scoring

log = logging.getLogger("streaming.job")

DEFAULT_CHECKPOINT = "/checkpoints/detector"  # the detector image's checkpoint volume
DEFAULT_MODEL_CACHE = "/models-cache"  # the detector image's volume of champion copies
ALARMS = "alarms"  # Postgres tables (infra/db/003_alarms.sql)
HEARTBEATS = "detector_heartbeats"
FLUSH_TIMEOUT_S = 30.0


class Config(FrozenModel):
    master: str
    shuffle_partitions: PositiveInt
    driver_memory: str = Field(pattern=r"^[0-9]+[mg]$")
    max_offsets_per_trigger: PositiveInt
    trigger_seconds: PositiveInt
    state_timeout_minutes: PositiveInt
    min_batches_to_retain: PositiveInt


@cache
def load_config() -> Config:
    return read_config(Config, files("streaming").joinpath("job.yaml"))


# --- Spark schemas ---------------------------------------------------------------------------

_SPARK_TYPES: dict[object, DataType] = {  # by polars base type
    pl.String: StringType(),
    pl.Int64: LongType(),
    pl.Float64: DoubleType(),
    pl.Boolean: BooleanType(),
    pl.Datetime: TimestampType(),
}


def _spark_type(dtype: pl.DataType | DataTypeClass) -> DataType:
    if isinstance(dtype, pl.List):
        return ArrayType(_spark_type(dtype.inner))
    return _SPARK_TYPES[dtype.base_type()]


def _struct(schema: dict[str, pl.DataType]) -> StructType:
    return StructType([StructField(name, _spark_type(t)) for name, t in schema.items()])


RUN = "replay_run_id"
# What the job reads of a `telemetry.raw` message (other fields are ignored).
TELEMETRY_SCHEMA = _struct({RUN: pl.String(), **scoring.SCHEMA})
# The stateful step's output: one row per alarm, then one progress row per (run, cell) call
# (alarm columns null) carrying its row counts and the latest interval it has scored. One
# schema because `applyInPandasWithState` allows only one.
PROGRESS: dict[str, pl.DataType] = {
    "n_scored": pl.Int64(),
    "n_late": pl.Int64(),
    "latest": scoring.SCHEMA["interval_start"],
}
OUTPUT = {**dict(scoring.NO_ALARMS.schema), **PROGRESS}
OUTPUT_SCHEMA = _struct(OUTPUT)
STATE_SCHEMA = StructType(  # `scoring.CellState.encode`: the rows, the partitions' progress
    [StructField("rows", BinaryType()), StructField("seen", StringType())]
)


# --- The stateful step (runs in the Python workers) -------------------------------------------


_TIMES = pl.col(pl.Datetime)


def from_spark(pdf: pd.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """Spark's pandas frame as polars. Spark hands timestamps over as naive datetimes in the
    session time zone (UTC); an empty frame (pandas keeps no types) gets `schema`."""
    if pdf.empty:
        return pl.DataFrame(schema=schema)
    return pl.from_pandas(pdf).with_columns(_TIMES.dt.replace_time_zone("UTC"))


def to_spark(frame: pl.DataFrame) -> pd.DataFrame:
    return frame.with_columns(_TIMES.dt.replace_time_zone(None)).to_pandas()


def step_output(result: scoring.Step) -> pl.DataFrame:
    progress = pl.DataFrame(
        {
            "n_scored": [result.n_scored],
            "n_late": [result.n_late],
            "latest": [result.state.scored["interval_start"].max()],
        },
        schema=PROGRESS,
    )
    return pl.concat([result.alarms, progress], how="diagonal_relaxed").select(OUTPUT_SCHEMA.names)


Update = Callable[[tuple[Any, ...], Iterator[pd.DataFrame], GroupState], Iterator[pd.DataFrame]]


def make_update(scorer: scoring.Scorer, cfg: Config, partitions: int) -> Update:
    """The `applyInPandasWithState` function: key = (replay run, cell); `partitions`: how many
    `telemetry.raw` has."""
    timeout_ms = cfg.state_timeout_minutes * 60_000

    def update(
        key: tuple[Any, ...], batches: Iterator[pd.DataFrame], state: GroupState
    ) -> Iterator[pd.DataFrame]:
        if state.hasTimedOut:  # the run went quiet: score what still waits, drop the state
            held = scoring.CellState.decode(*state.get)
            none = pl.DataFrame(schema=scoring.SCHEMA)
            result = scoring.step(
                held, none, scorer, run=str(key[0]), partitions=partitions, quiet=True
            )
            state.remove()
            yield to_spark(step_output(result))
            return
        held = scoring.CellState.decode(*state.get) if state.exists else scoring.CellState.empty()
        new = from_spark(pd.concat(list(batches), ignore_index=True), scoring.SCHEMA)
        result = scoring.step(held, new, scorer, run=str(key[0]), partitions=partitions)
        state.update(result.state.encode())
        state.setTimeoutDuration(timeout_ms)
        yield to_spark(step_output(result))

    return update


# --- Output (runs on the driver, once per micro-batch) ----------------------------------------


@dataclass(frozen=True)
class Heartbeat:
    job_id: str  # one per job process: a restart starts a new one
    batch_id: int
    detector_version: str
    rows_scored: int
    rows_late: int  # never scored: came after a later interval, or waiting at a state timeout
    alarms: int  # alarms the batch raised (also those already stored by an earlier attempt)
    latest_interval: datetime | None


class Store(Protocol):
    def known(self, alarm_ids: Sequence[str]) -> set[str]:
        """The IDs among `alarm_ids` that are already stored."""
        ...

    def commit(self, new: Sequence[AlarmMessage], heartbeat: Heartbeat) -> None:
        """Store `new` (skipping any stored meanwhile) and `heartbeat` in one transaction."""
        ...


@dataclass(frozen=True)
class Progress:
    rows_scored: int
    rows_late: int
    latest_interval: datetime | None


def parse_output(out: pl.DataFrame) -> tuple[list[AlarmMessage], Progress]:
    """(validated alarms, row counts and latest interval) of one micro-batch's output rows."""
    hits = out.filter(pl.col("alarm_id").is_not_null()).select(scoring.NO_ALARMS.columns)
    messages = [AlarmMessage.model_validate(row) for row in hits.iter_rows(named=True)]
    progress = out.filter(pl.col("alarm_id").is_null())
    latest = progress["latest"].max()
    return messages, Progress(
        int(progress["n_scored"].sum()),
        int(progress["n_late"].sum()),
        latest if isinstance(latest, datetime) else None,
    )


@dataclass(frozen=True)
class Sink:
    """`foreachBatch` target: Kafka first (an alarm is never stored but unsent), then Postgres."""

    producer: kafka.Producer
    store: Store
    job_id: str
    detector_version: str

    def write(self, batch_id: int, out: pl.DataFrame) -> Heartbeat:
        messages, progress = parse_output(out)
        known = self.store.known([m.alarm_id for m in messages])
        new = [m for m in messages if m.alarm_id not in known]
        failed: list[str] = []

        def delivered(err: Any, _msg: Any) -> None:
            if err is not None:
                failed.append(str(err))

        for m in new:
            self.producer.produce(
                alarms.TOPIC, value=m.to_json(), key=m.alarm_id.encode(), on_delivery=delivered
            )
        if new and (self.producer.flush(FLUSH_TIMEOUT_S) or failed):
            detail = f": {failed[0]}" if failed else ""
            raise RuntimeError(f"Kafka did not take the alarms{detail}; the batch will be re-run")
        beat = Heartbeat(
            job_id=self.job_id,
            batch_id=batch_id,
            detector_version=self.detector_version,
            alarms=len(messages),
            **asdict(progress),
        )
        self.store.commit(new, beat)
        log.info(
            "batch %d: %d rows scored, %d late, %d alarms (%d new)",
            *(beat.batch_id, beat.rows_scored, beat.rows_late, beat.alarms, len(new)),
        )
        return beat

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        self.write(batch_id, from_spark(batch.toPandas(), OUTPUT))


class PgStore:
    """`Store` on Postgres `alarms` + `detector_heartbeats` as `pipeline_writer`. Each call is
    its own transaction (a savepoint inside a caller's), so none stays open between batches."""

    def __init__(self, conn: psycopg.Connection[Any]) -> None:
        self.conn = conn
        self.columns = scoring.NO_ALARMS.columns
        self.insert_alarm = insert_into(ALARMS, self.columns) + sql.SQL(
            " ON CONFLICT (alarm_id) DO NOTHING"
        )
        self.insert_heartbeat = insert_into(HEARTBEATS, [f.name for f in fields(Heartbeat)])

    def known(self, alarm_ids: Sequence[str]) -> set[str]:
        if not alarm_ids:
            return set()
        query = sql.SQL("SELECT alarm_id FROM {} WHERE alarm_id = ANY(%s)").format(
            sql.Identifier(ALARMS)
        )
        with self.conn.transaction():
            rows = self.conn.execute(query, (list(alarm_ids),)).fetchall()
        return {str(r[0]) for r in rows}

    def commit(self, new: Sequence[AlarmMessage], heartbeat: Heartbeat) -> None:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(
                self.insert_alarm, [tuple(_value(m, c) for c in self.columns) for m in new]
            )
            cur.execute(self.insert_heartbeat, astuple(heartbeat))


def _value(m: AlarmMessage, column: str) -> object:
    v = getattr(m, column)
    return list(v) if isinstance(v, tuple) else v  # kpis -> text[]


# --- Wiring ----------------------------------------------------------------------------------


def session(cfg: Config) -> SparkSession:
    return (
        SparkSession.builder.master(cfg.master)
        .appName("detector")
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.minBatchesToRetain", str(cfg.min_batches_to_retain))
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def telemetry_stream(spark: SparkSession, cfg: Config) -> DataFrame:
    """Parsed `telemetry.raw` rows. Messages that do not parse, or whose IDs or interval break
    the message schema (`TelemetryMessage`; the sink dead-letters them), are dropped here, so
    one bad message cannot fail every re-run of its micro-batch. Offsets that aged out of the
    topic's retention while the job was stopped are skipped (Spark logs them), not fatal."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka.bootstrap_servers())
        .option("subscribe", telemetry.TOPIC_RAW)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", str(cfg.max_offsets_per_trigger))
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("m"), "partition"
    )
    return parsed.select("m.*", "partition").where(valid_rows())


def valid_rows() -> Column:
    """Rows a `TelemetryMessage` would accept, as far as scoring depends on it: keys present,
    cell and site IDs matching the domain's patterns, interval on the grid."""
    site, cell = load_domain().topology_types()[-2:]
    present = [F.col(c).isNotNull() for c in (RUN, *features.KEYS)]
    t = F.col("interval_start")
    on_grid = (F.second(t) == 0) & (F.minute(t) % telemetry.INTERVAL_MINUTES == 0)
    ids = F.col("cell_id").rlike(cell.id_pattern) & F.col("site_id").rlike(site.id_pattern)
    return functools.reduce(operator.and_, present) & ids & on_grid


def pin_version(marker: Path, version: str) -> None:
    """Tie a checkpoint to the detector version that scores it (`marker` next to it): alarm
    IDs include the version, so a re-run micro-batch under another one would raise its alarms
    again under new IDs. Raises ValueError if the checkpoint belongs to another version."""
    if marker.exists() and (held := marker.read_text().strip()) != version:
        raise ValueError(
            f"the checkpoint was scored by {held}, the champion is now {version}: "
            "start from a new checkpoint (make detect-reset)"
        )
    marker.write_text(version)


def topic_end_offsets() -> dict[int, int]:
    """`telemetry.raw`'s partitions and their end offsets."""
    from confluent_kafka import Consumer

    consumer = Consumer(kafka.consumer_config(kafka.bootstrap_servers(), "detector-metadata"))
    try:
        return kafka.end_offsets(consumer, telemetry.TOPIC_RAW)
    finally:
        consumer.close()


def run(checkpoint: str) -> int:
    from confluent_kafka import Producer as KafkaProducer
    from mlflow import MlflowClient
    from mlflow.exceptions import MlflowException

    from models import tracking

    cfg = load_config()
    try:
        db = DbSettings.from_env(Role.PIPELINE_WRITER)
        champion = tracking.load_champion(MlflowClient(), cache=Path(DEFAULT_MODEL_CACHE))
    except (DbConfigError, ValueError, OSError, MlflowException) as exc:
        log.error("%s", exc)
        return 1
    scorer = scoring.Scorer.of(champion, tracking.MODEL)
    try:
        # The state layout is pinned too: Step 9 changed it (rows + per-partition progress).
        pin_version(Path(f"{checkpoint}.detector"), f"{scorer.detector_version} state-2")
    except ValueError as exc:
        log.error("%s", exc)
        return 1
    log.info("scoring with %s", scorer.detector_version)
    spark = session(cfg)
    scored = (
        telemetry_stream(spark, cfg)
        .groupBy(RUN, "cell_id")
        .applyInPandasWithState(
            make_update(scorer, cfg, len(topic_end_offsets())),
            OUTPUT_SCHEMA,
            STATE_SCHEMA,
            "append",
            GroupStateTimeout.ProcessingTimeTimeout,
        )
    )
    producer = KafkaProducer(kafka.producer_config(kafka.bootstrap_servers(), "detector-alarms"))
    with db.connect() as conn:
        sink = Sink(producer, PgStore(conn), str(uuid.uuid4()), scorer.detector_version)
        query = (
            scored.writeStream.queryName("detector")
            .foreachBatch(sink)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=f"{cfg.trigger_seconds} seconds")
            .start()
        )

        def stop(signum: int, _frame: object) -> None:
            log.info("signal %d: stopping (an unfinished micro-batch re-runs on restart)", signum)
            query.stop()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        query.awaitTermination()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    return run(args.checkpoint)


if __name__ == "__main__":
    sys.exit(main())
