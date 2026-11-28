"""The `alarms` message: one cell, one interval the promoted detector flagged (milestone 2,
step 8; specs/03-detection.md).

Written by the Spark job (`streaming.job`) to Kafka `alarms` (key = `alarm_id`) and Postgres
`alarms`; read by the correlator (milestone 3). Encoding: UTF-8 JSON with `schema_version`.

`alarm_id` is deterministic (`alarm_id`): the same replay run, cell, interval, and detector
version always give the same ID, so a re-scored micro-batch after a restart writes the same alarm
again instead of a new one. Alarms come only from detector output, never from ground truth
(Rule 6); on replays with injected faults the faults behind them are SYNTHETIC.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from common import telemetry
from common.domain import load_domain

SCHEMA_VERSION: Literal[1] = 1
TOPIC = "alarms"  # created by infra/kafka/create-topics.sh
# Fixed for good: changing it changes every alarm ID.
_NAMESPACE = uuid.UUID("27636bde-9576-4300-bcfc-4a432ee275ce")


def detector_version(model: str, version: str) -> str:
    """How an alarm names the registered model version that raised it, e.g. `detector/1`."""
    return f"{model}/{version}"


def alarm_id(run: str, cell_id: str, interval_start: datetime, detector: str) -> str:
    """`ALM-<uuid5>` of (replay run, cell, interval, detector version)."""
    name = f"{run}|{telemetry.message_key(cell_id, interval_start)}|{detector}"
    return load_domain().entity("Alarm").make_id(uuid=uuid.uuid5(_NAMESPACE, name))


class AlarmMessage(BaseModel):
    """One `alarms` message. Invalid alarms raise `pydantic.ValidationError`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = SCHEMA_VERSION
    alarm_id: str
    replay_run_id: Annotated[str, Field(min_length=1, max_length=64)]
    cell_id: str
    site_id: str
    interval_start: datetime
    score: Annotated[float, Field(allow_inf_nan=False)]
    kpis: Annotated[tuple[str, ...], Field(min_length=1)]  # contributing KPIs, largest first
    detector_version: Annotated[str, Field(min_length=1)]

    _cell_id = field_validator("cell_id")(telemetry.check_cell_id)
    _site_id = field_validator("site_id")(telemetry.check_site_id)
    _interval = field_validator("interval_start")(telemetry.check_interval_start)

    @field_validator("alarm_id")
    @classmethod
    def _alarm_id(cls, v: str) -> str:
        if not load_domain().entity("Alarm").matches(v):
            raise ValueError("not an Alarm id")
        return v

    @field_validator("kpis")
    @classmethod
    def _known_kpis(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        if unknown := set(v) - set(load_domain().kpis):
            raise ValueError(f"unknown KPIs {sorted(unknown)}")
        return v

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")
