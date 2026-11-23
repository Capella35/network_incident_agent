from __future__ import annotations

import csv
import json
import zlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from common import kafka, telemetry
from common.telemetry import TelemetryMessage
from producer import kpi_model, replay, topology
from tests.factories import T0, telemetry_message
from tests.fakes import FakeClock, FakeKafkaMessage
from tests.producer.test_kpi_model import CELLS, traffic

DAY = date(2013, 11, 5)  # a training day
SITES = dict.fromkeys(CELLS.values(), "SITE-1")
PARTITIONS = 6


# --- fakes -----------------------------------------------------------------------------------


@dataclass
class FakeProducer:
    """Records what was produced; delivers on flush. Partition = crc32(key) % 6 (stable)."""

    full_once: bool = False
    undeliverable: int = 0
    sent: list[tuple[str, str, bytes]] = field(default_factory=list)
    _pending: list[tuple[Callable[[Any, Any], None], FakeKafkaMessage]] = field(
        default_factory=list
    )

    def produce(
        self, topic: str, *, value: bytes, key: bytes, on_delivery: Callable[[Any, Any], None]
    ) -> None:
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        self.sent.append((topic, key.decode(), value))
        self._pending.append(
            (on_delivery, FakeKafkaMessage(topic_=topic, partition_=zlib.crc32(key) % PARTITIONS))
        )

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for cb, msg in self._pending:
            cb(None, msg)
        self._pending.clear()
        return self.undeliverable


def fast() -> replay.Pacer:
    return replay.Pacer(speed=0)


# --- fixtures --------------------------------------------------------------------------------


@pytest.fixture
def params() -> kpi_model.Params:
    return kpi_model.load_params()


@pytest.fixture
def calib(params: kpi_model.Params) -> dict[str, Any]:
    ramp = [float(v) for v in range(1, 101)]
    days = {date(2013, 11, 4): traffic(date(2013, 11, 4), {1: ramp, 2: ramp, 3: ramp})}
    return kpi_model.calibrate(days, CELLS, params)


@pytest.fixture
def data_dir(tmp_path: Path, calib: dict[str, Any]) -> Path:
    processed = tmp_path / "processed"
    (processed / "traffic").mkdir(parents=True)
    with (processed / "topology_nodes.csv").open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(topology.NODE_COLUMNS)
        w.writerow(["SITE-1", "Site", "AGG-1", 45.46, 9.19, ""])
        for sq, cell in CELLS.items():
            w.writerow([cell, "Cell", "SITE-1", 45.46, 9.19, sq])
    (processed / kpi_model.CALIBRATION_FILE).write_text(json.dumps(calib), encoding="utf-8")
    # squares inserted in reverse so the replay has to sort them; one gap row
    frame = traffic(DAY, {3: [5.0] * 4, 2: [6.0] * 4, 1: [7.0] * 4}, gaps={(2, 1)})
    frame.write_parquet(processed / "traffic" / f"{DAY.isoformat()}.parquet")
    return tmp_path


# --- day range -------------------------------------------------------------------------------


def test_test_week_days_need_final_eval() -> None:
    with pytest.raises(replay.ReplayError, match="test-week"):
        replay.days_in_range(date(2013, 12, 15), date(2013, 12, 16))
    assert len(replay.days_in_range(date(2013, 12, 15), date(2013, 12, 16), final_eval=True)) == 2


def test_end_before_start_rejected() -> None:
    with pytest.raises(replay.ReplayError, match="before"):
        replay.days_in_range(DAY, DAY - timedelta(days=1))


def test_missing_day_named(data_dir: Path) -> None:
    with pytest.raises(replay.ReplayError, match="2013-11-06 not processed"):
        replay.load_traffic(data_dir, [date(2013, 11, 6)])


# --- rows and messages -----------------------------------------------------------------------


def test_rows_sorted_with_site_and_modeled_kpis(
    calib: dict[str, Any], params: kpi_model.Params
) -> None:
    frame = traffic(DAY, {3: [5.0] * 2, 1: [7.0] * 2, 2: [6.0] * 2})
    rows, rejects = replay.build_rows(frame, CELLS, SITES, calib, params)
    assert rejects == []
    keys = list(zip(rows["interval_start"], rows["cell_id"], strict=True))
    assert keys == sorted(keys)
    assert rows["cell_id"].to_list()[:3] == ["CELL-1-1", "CELL-1-2", "CELL-1-3"]
    assert set(rows["site_id"]) == {"SITE-1"}
    assert rows["load_ratio"].null_count() == 0


def test_unknown_square_goes_to_dlq(calib: dict[str, Any], params: kpi_model.Params) -> None:
    frame = traffic(DAY, {1: [7.0], 99: [1.0]})
    rows, rejects = replay.build_rows(frame, CELLS, SITES, calib, params)
    assert rows.height == 1
    assert len(rejects) == 1 and rejects[0].reason == "square not in the topology"
    doc = json.loads(rejects[0].to_json("run-x"))
    assert doc["replay_run_id"] == "run-x" and doc["row"]["square_id"] == 99


def test_invalid_row_becomes_reject_with_reason(
    calib: dict[str, Any], params: kpi_model.Params
) -> None:
    frame = traffic(DAY, {1: [7.0], 2: [float("nan")]})
    rows, _ = replay.build_rows(frame, CELLS, {"CELL-1-1": "SITE-1"}, calib, params)
    items = list(replay.to_messages(rows, "run-1"))
    good = [i for i in items if isinstance(i, TelemetryMessage)]
    bad = [i for i in items if isinstance(i, replay.Reject)]
    assert [m.cell_id for m in good] == ["CELL-1-1"]
    assert len(bad) == 1 and bad[0].key.startswith("CELL-1-2|")
    assert "internet" in bad[0].reason and "site_id" in bad[0].reason
    assert json.loads(bad[0].to_json("run-1"))["row"]["internet"] == "nan"  # JSON-safe


# --- pacing ----------------------------------------------------------------------------------


def _at(minutes: int) -> datetime:
    return T0 + timedelta(minutes=minutes)


def test_pacer_speed_spaces_intervals() -> None:
    clock = FakeClock()
    pacer = replay.Pacer(speed=10.0, clock=clock, sleep=clock.sleep)
    for m in (0, 0, 10, 10, 20):  # two cells per interval
        pacer.wait(_at(m))
    assert clock.sleeps == pytest.approx([1.0, 1.0])  # 10 data minutes = 1 s at speed 10


def test_pacer_max_rate_caps_messages() -> None:
    clock = FakeClock()
    pacer = replay.Pacer(speed=0, max_rate=4.0, clock=clock, sleep=clock.sleep)
    for _ in range(5):
        pacer.wait(_at(0))
    assert clock.now - 100.0 == pytest.approx(1.0)  # 5th message at 4/4 s


def test_pacer_speed_zero_never_sleeps() -> None:
    clock = FakeClock()
    pacer = replay.Pacer(speed=0, clock=clock, sleep=clock.sleep)
    for m in range(0, 600, 10):
        pacer.wait(_at(m))
    assert clock.sleeps == []


@pytest.mark.parametrize(("speed", "rate"), [(-1.0, None), (1.0, 0.0)])
def test_pacer_rejects_bad_settings(speed: float, rate: float | None) -> None:
    with pytest.raises(replay.ReplayError):
        replay.Pacer(speed=speed, max_rate=rate)


# --- sending ---------------------------------------------------------------------------------


def test_run_sends_every_row_in_order(data_dir: Path) -> None:
    producer = FakeProducer()
    s = replay.run(
        data_dir, DAY, DAY, producer, fast(), options=replay.ReplayOptions(replay_run_id="run-a")
    )
    assert (s.sent, s.dead_lettered, s.failed) == (12, 0, 0)
    assert s.delivered == 12 and sum(s.per_partition.values()) == 12
    topics = {t for t, _, _ in producer.sent}
    assert topics == {telemetry.TOPIC_RAW}
    msgs = [TelemetryMessage.model_validate_json(v) for _, _, v in producer.sent]
    assert [m.key() for m in msgs] == [k for _, k, _ in producer.sent]
    order = [(m.interval_start, m.cell_id) for m in msgs]
    assert order == sorted(order)
    assert {m.replay_run_id for m in msgs} == {"run-a"}
    gap = [m for m in msgs if m.data_gap]
    assert len(gap) == 1 and gap[0].cell_id == "CELL-1-2" and gap[0].latency_ms is None


def test_replay_is_deterministic_per_partition(data_dir: Path) -> None:
    def keys_by_partition(run_id: str) -> dict[int, list[str]]:
        producer = FakeProducer()
        replay.run(
            data_dir, DAY, DAY, producer, fast(), options=replay.ReplayOptions(replay_run_id=run_id)
        )
        out: dict[int, list[str]] = {}
        for _, key, _ in producer.sent:
            out.setdefault(zlib.crc32(key.encode()) % PARTITIONS, []).append(key)
        return out

    assert keys_by_partition("run-a") == keys_by_partition("run-b")


def test_rejects_go_to_dlq_topic() -> None:
    producer = FakeProducer()
    reject = replay.Reject(key="square-99|x", reason="square not in the topology", row={"a": 1})
    s = replay.send([reject], producer, replay.Summary("run-1"), fast())
    assert (s.sent, s.dead_lettered) == (0, 1)
    assert producer.sent[0][0] == telemetry.TOPIC_DLQ
    assert s.per_partition == {}  # DLQ deliveries are not counted as telemetry


def test_full_local_queue_is_retried(data_dir: Path) -> None:
    producer = FakeProducer(full_once=True)
    s = replay.run(data_dir, DAY, DAY, producer, fast())
    assert s.sent == 12 and len(producer.sent) == 12


def test_undelivered_messages_counted_as_failed(data_dir: Path) -> None:
    s = replay.run(data_dir, DAY, DAY, FakeProducer(undeliverable=2), fast())
    assert s.failed == 2 and "still queued" in s.errors[0]


def test_delivery_errors_counted() -> None:
    summary = replay.Summary("run-1")

    class ErrProducer(FakeProducer):
        def flush(self, timeout: float) -> int:
            for cb, msg in self._pending:
                cb("broker down", msg)
            self._pending.clear()
            return 0

    s = replay.send([telemetry_message()], ErrProducer(), summary, fast())
    assert s.failed == 1 and s.errors == ["broker down"]


def test_kafka_config_is_idempotent_and_ordered() -> None:
    cfg = replay.kafka_config(kafka.DEFAULT_BOOTSTRAP)
    assert cfg["enable.idempotence"] is True and cfg["acks"] == "all"
    assert int(cfg["max.in.flight.requests.per.connection"]) <= 5


def test_main_refuses_test_week_before_connecting(caplog: pytest.LogCaptureFixture) -> None:
    assert replay.main(["--start", "2013-12-20"]) == 1
    assert "test-week" in caplog.text


def test_cell_sites_loaded_from_topology(data_dir: Path) -> None:
    sites = topology.load_cell_sites(data_dir / "processed" / "topology_nodes.csv")
    assert sites == SITES
    assert pl.Series(list(sites)).str.starts_with("CELL-").all()
