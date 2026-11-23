"""The `telemetry.raw` message: one cell, one 10-minute interval (specs/02-telemetry.md).

Shared by the replay producer (milestone 1, step 7), the Postgres sink (step 8), and the Spark
job (milestone 2). Encoding: UTF-8 JSON, one object per Kafka message, with `schema_version`.

Traffic fields are REAL (Milan, scaled, summed over country codes). `load_ratio`, `latency_ms`
and `drop_rate` are MODELED from load (`producer.kpi_model`) and are null exactly when the source
had no row for the square and interval (`data_gap`).

Field names are checked against the domain model (`check_against_domain`), so the Milan field
list and the modeled-KPI catalog stay the single source (Rule 15).
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from common.domain import load_domain
from common.domain.loader import MILAN_FIELDS, KpiSource

SCHEMA_VERSION: Literal[1] = 1
TOPIC_RAW = "telemetry.raw"  # created by infra/kafka/create-topics.sh
TOPIC_DLQ = "telemetry.dlq"
INTERVAL_MINUTES = 10

_Activity = Annotated[float, Field(ge=0.0, allow_inf_nan=False)]
_Modeled = Annotated[float | None, Field(default=None, ge=0.0, allow_inf_nan=False)]

TRAFFIC_FIELDS = ("sms_in", "sms_out", "call_in", "call_out", "internet")
MODELED_FIELDS = ("load_ratio", "latency_ms", "drop_rate")


class TelemetryMessage(BaseModel):
    """One `telemetry.raw` message. Invalid rows raise `pydantic.ValidationError`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = SCHEMA_VERSION
    replay_run_id: Annotated[str, Field(min_length=1, max_length=64)]
    cell_id: str
    site_id: str
    square_id: Annotated[int, Field(ge=1, le=10_000)]
    interval_start: datetime
    sms_in: _Activity
    sms_out: _Activity
    call_in: _Activity
    call_out: _Activity
    internet: _Activity
    data_gap: bool
    load_ratio: _Modeled
    latency_ms: Annotated[float | None, Field(default=None, gt=0.0, allow_inf_nan=False)]
    drop_rate: Annotated[float | None, Field(default=None, ge=0.0, le=1.0, allow_inf_nan=False)]

    @field_validator("cell_id")
    @classmethod
    def _cell_id(cls, v: str) -> str:
        cell = load_domain().topology_types()[-1]
        if not cell.matches(v):
            raise ValueError(f"not a {cell.name} id")
        return v

    @field_validator("site_id")
    @classmethod
    def _site_id(cls, v: str) -> str:
        site = load_domain().topology_types()[-2]
        if not site.matches(v):
            raise ValueError(f"not a {site.name} id")
        return v

    @field_validator("interval_start")
    @classmethod
    def _utc_on_grid(cls, v: datetime) -> datetime:
        if v.tzinfo is None or v.utcoffset() != UTC.utcoffset(None):
            raise ValueError("interval_start must be timezone-aware UTC")
        if v.second or v.microsecond or v.minute % INTERVAL_MINUTES:
            raise ValueError(f"interval_start must be on the {INTERVAL_MINUTES}-minute grid")
        return v.astimezone(UTC)

    @model_validator(mode="after")
    def _gap_rule(self) -> TelemetryMessage:
        modeled = [getattr(self, f) for f in MODELED_FIELDS]
        if self.data_gap and any(m is not None for m in modeled):
            raise ValueError("data_gap rows carry no modeled values")
        if not self.data_gap and any(m is None for m in modeled):
            raise ValueError("modeled values are required unless data_gap")
        if self.data_gap and any(getattr(self, f) != 0.0 for f in TRAFFIC_FIELDS):
            raise ValueError("data_gap rows have zero traffic")
        return self

    def key(self) -> str:
        return message_key(self.cell_id, self.interval_start)

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


def message_key(cell_id: str, interval_start: datetime) -> str:
    """Kafka key `cell_id|interval_start` (UTC, ISO 8601): one key per cell and interval."""
    return f"{cell_id}|{interval_start.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}"


def check_against_domain() -> None:
    """Fail loudly if the schema and the domain model disagree on field names."""
    if frozenset(TRAFFIC_FIELDS) != MILAN_FIELDS:
        raise ValueError("TRAFFIC_FIELDS differ from the Milan fields in the domain model")
    modeled = {k.name for k in load_domain().kpis.values() if k.source is KpiSource.MODELED}
    if frozenset(MODELED_FIELDS) != modeled:
        raise ValueError(f"MODELED_FIELDS differ from the modeled KPIs in kpis.yaml: {modeled}")
    for f in (*TRAFFIC_FIELDS, *MODELED_FIELDS):
        if f not in TelemetryMessage.model_fields:
            raise ValueError(f"TelemetryMessage lacks field {f!r}")


def finite_or_none(v: object) -> object:
    """JSON-safe copy of a value for the dead-letter topic (NaN/inf become strings)."""
    if isinstance(v, float) and not math.isfinite(v):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v
