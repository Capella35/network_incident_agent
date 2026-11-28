"""The Spark job's pieces that run without a JVM (milestone 2, step 8): schemas, the stateful
function with PySpark's own `GroupState`, and the micro-batch sink with fakes. The job itself
runs only in the detector image (`make detect`)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, fields
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
from pyspark.sql import Row
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from common import alarms
from common.alarms import AlarmMessage
from common.telemetry import TelemetryMessage
from streaming import job, scoring
from tests.factories import DB_DIR, PARTITIONS, noisy_rows, zscore_scorer
from tests.fakes import FakeProducer

CFG = job.load_config()


def spark_state(value: tuple[bytes, str] | None = None, *, timed_out: bool = False) -> GroupState:
    """A `GroupState` as Spark hands it to the function (no JVM needed)."""
    return GroupState(
        optionalValue=Row(*value) if value is not None else Row(),
        batchProcessingTimeMs=1_000,
        eventTimeWatermarkMs=GroupState.NO_TIMESTAMP,
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        hasTimedOut=timed_out,
        watermarkPresent=False,
        defined=value is not None,
        updated=False,
        removed=False,
        timeoutTimestamp=GroupState.NO_TIMESTAMP,
        keyAsUnsafe=b"",
        valueSchema=job.STATE_SCHEMA,
    )


def as_spark_input(rows: pl.DataFrame) -> pd.DataFrame:
    """Rows the way Spark passes them: pandas, naive UTC timestamps, the run column."""
    return job.to_spark(rows.with_columns(pl.lit("run-1").alias(job.RUN)))


def test_schemas_follow_the_message_and_the_scoring_frames() -> None:
    assert set(job.TELEMETRY_SCHEMA.names) == {job.RUN, *scoring.SCHEMA}
    assert set(job.TELEMETRY_SCHEMA.names) <= set(TelemetryMessage.model_fields)
    assert job.OUTPUT_SCHEMA.names == [*scoring.NO_ALARMS.columns, *job.PROGRESS]
    assert job.OUTPUT_SCHEMA["kpis"].dataType.simpleString() == "array<string>"
    assert job.OUTPUT_SCHEMA["n_late"].dataType.simpleString() == "bigint"


def test_update_keeps_state_across_batches_and_drops_it_on_timeout() -> None:
    rows = noisy_rows(300)
    update = job.make_update(zscore_scorer(threshold=1.0), CFG, PARTITIONS)
    state = spark_state()
    out = pd.concat(update(("run-1", "CELL-1-1"), iter([as_spark_input(rows.head(200))]), state))
    assert state.exists
    assert state.getCurrentProcessingTimeMs() == 1_000
    progress = out[out["alarm_id"].isna()]
    assert progress[["n_scored", "n_late"]].to_numpy().tolist() == [[200, 0]]
    assert (out["alarm_id"].notna()).sum() > 0
    later = spark_state(state.get)
    more = pd.concat(update(("run-1", "CELL-1-1"), iter([as_spark_input(rows.slice(150))]), later))
    assert more[more["alarm_id"].isna()][["n_scored", "n_late"]].to_numpy().tolist() == [[100, 50]]
    held = scoring.CellState.decode(*later.get)
    assert held.scored.height == 300
    waiting = spark_state()  # partition 0 never delivers (row 0 lost): rows wait at the timeout
    quiet = rows.slice(1, 9).filter(pl.col("partition") != 0)
    list(update(("run-1", "CELL-1-1"), iter([as_spark_input(quiet)]), waiting))
    gone = spark_state(waiting.get, timed_out=True)
    [out] = list(update(("run-1", "CELL-1-1"), iter([]), gone))
    progress = out[out["alarm_id"].isna()]  # waiting rows are scored when the run goes quiet
    assert progress[["n_scored", "n_late"]].to_numpy().tolist() == [[8, 0]]
    assert not gone.exists


def batch_output() -> tuple[scoring.Step, pl.DataFrame]:
    """One cell-day scored, and the micro-batch output the driver gets for it."""
    rows = noisy_rows(144)
    result = scoring.step(
        scoring.CellState.empty(),
        rows,
        zscore_scorer(threshold=1.0),
        run="run-1",
        partitions=PARTITIONS,
    )
    return result, job.from_spark(job.to_spark(job.step_output(result)), job.OUTPUT)


def test_output_round_trips_into_validated_alarms_and_progress() -> None:
    result, out = batch_output()
    messages, progress = job.parse_output(out)
    assert [m.alarm_id for m in messages] == result.alarms["alarm_id"].to_list()
    assert [m.score for m in messages] == result.alarms["score"].to_list()  # exact
    assert messages[0].interval_start == result.alarms["interval_start"][0]
    assert progress == job.Progress(144, 0, result.state.scored["interval_start"].max())
    empty = job.from_spark(pd.DataFrame(), job.OUTPUT)
    assert job.parse_output(empty) == ([], job.Progress(0, 0, None))


@dataclass
class FakeStore:
    log: list[str]
    stored: dict[str, AlarmMessage] = field(default_factory=dict)
    heartbeats: list[job.Heartbeat] = field(default_factory=list)

    def known(self, alarm_ids: Sequence[str]) -> set[str]:
        return set(alarm_ids) & set(self.stored)

    def commit(self, new: Sequence[AlarmMessage], heartbeat: job.Heartbeat) -> None:
        self.log.append("commit")
        self.stored |= {m.alarm_id: m for m in new}
        self.heartbeats.append(heartbeat)


def sink(log: list[str]) -> tuple[job.Sink, FakeProducer, FakeStore]:
    producer, store = FakeProducer(log, name="kafka"), FakeStore(log)
    return job.Sink(producer, store, "job-1", "detector/7"), producer, store


def test_sink_sends_to_kafka_before_postgres_and_writes_a_heartbeat() -> None:
    log: list[str] = []
    s, producer, store = sink(log)
    _, out = batch_output()
    beat = s.write(3, out)
    n = out.filter(pl.col("alarm_id").is_not_null()).height
    assert log == ["kafka-flush", "commit"]
    assert len(producer.sent) == len(store.stored) == n > 0
    topic, key, value = producer.sent[0]
    assert topic == alarms.TOPIC
    assert AlarmMessage.model_validate_json(value).alarm_id == key.decode()
    assert beat == store.heartbeats[0]
    assert (beat.job_id, beat.batch_id, beat.detector_version, beat.alarms) == (
        "job-1",
        3,
        "detector/7",
        n,
    )
    assert (beat.rows_scored, beat.rows_late) == (144, 0)


def test_a_rerun_batch_sends_and_stores_nothing_twice() -> None:
    log: list[str] = []
    s, producer, store = sink(log)
    _, out = batch_output()
    s.write(3, out)
    sent = len(producer.sent)
    again = s.write(3, out)  # Spark re-runs the batch after a restart: same alarms, same IDs
    assert len(producer.sent) == sent
    assert len(store.stored) == sent
    assert again.alarms == sent  # still reported as raised
    assert log == ["kafka-flush", "commit", "commit"]  # nothing new: no flush


@pytest.mark.parametrize(("unacked", "error"), [(1, None), (0, "UNKNOWN_TOPIC_OR_PART")])
def test_alarms_kafka_did_not_take_stop_the_batch_before_postgres(
    unacked: int, error: str | None
) -> None:
    log: list[str] = []
    s, producer, store = sink(log)
    producer.unacked, producer.error = unacked, error  # still queued / failed for good
    with pytest.raises(RuntimeError, match="re-run"):
        s.write(0, batch_output()[1])
    assert store.stored == {}
    assert "commit" not in log


def test_a_checkpoint_stays_with_the_detector_version_that_scored_it(tmp_path: Path) -> None:
    marker = tmp_path / "detector.detector"
    job.pin_version(marker, "detector/1")
    job.pin_version(marker, "detector/1")  # a restart of the same version
    with pytest.raises(ValueError, match="make detect-reset"):
        job.pin_version(marker, "detector/2")
    assert marker.read_text() == "detector/1"


def table_columns(name: str) -> list[str]:
    """Column names of `CREATE TABLE IF NOT EXISTS name` in the step 8 migration."""
    sql = (DB_DIR / "003_alarms.sql").read_text()
    body = sql[sql.index(f"CREATE TABLE IF NOT EXISTS {name} (") :].split(");")[0]
    lines = [line.split()[0] for line in body.splitlines()[1:] if line.strip()]
    return [c for c in lines if c.islower()]  # constraint lines start in upper case


def test_alarm_and_heartbeat_fields_are_the_table_columns() -> None:
    message = [f for f in AlarmMessage.model_fields if f != "schema_version"]
    assert list(scoring.NO_ALARMS.columns) == message
    assert table_columns("alarms") == [*message, "stored_at"]
    assert table_columns("detector_heartbeats") == [f.name for f in fields(job.Heartbeat)] + ["at"]
