"""The `alarms` message and its deterministic ID (milestone 2, step 8)."""

from __future__ import annotations

from datetime import timedelta, timezone

import pytest
from pydantic import ValidationError

from common import alarms
from common.alarms import AlarmMessage
from common.domain import load_domain
from tests.factories import ALARM_AT, alarm_message

T = ALARM_AT


def test_alarm_id_is_deterministic_and_changes_with_every_part() -> None:
    first = alarms.alarm_id("run-1", "CELL-1-1", T, "detector/1")
    assert load_domain().entity("Alarm").matches(first)
    assert alarms.alarm_id("run-1", "CELL-1-1", T, "detector/1") == first
    same_instant = T.astimezone(timezone(timedelta(hours=1)))
    assert alarms.alarm_id("run-1", "CELL-1-1", same_instant, "detector/1") == first
    others = {
        alarms.alarm_id("run-2", "CELL-1-1", T, "detector/1"),
        alarms.alarm_id("run-1", "CELL-1-2", T, "detector/1"),
        alarms.alarm_id("run-1", "CELL-1-1", T + timedelta(minutes=10), "detector/1"),
        alarms.alarm_id("run-1", "CELL-1-1", T, "detector/2"),
    }
    assert first not in others
    assert len(others) == 4


def test_detector_version_names_model_and_version() -> None:
    assert alarms.detector_version("detector", "3") == "detector/3"


def test_message_round_trips_as_json() -> None:
    m = alarm_message()
    assert AlarmMessage.model_validate_json(m.to_json()) == m
    assert m.schema_version == alarms.SCHEMA_VERSION


@pytest.mark.parametrize(
    ("update", "fragment"),
    [
        ({"alarm_id": "ALM-1"}, "not an Alarm id"),
        ({"kpis": ("traffic_internet", "made_up")}, "unknown KPIs"),
        ({"kpis": ()}, "at least 1"),
        ({"cell_id": "SITE-1"}, "not a Cell id"),
        ({"site_id": "CELL-1-1"}, "not a Site id"),
        ({"interval_start": T.replace(tzinfo=None)}, "timezone-aware UTC"),
        ({"interval_start": T + timedelta(minutes=5)}, "grid"),
        ({"score": float("nan")}, "finite"),
        ({"extra": 1}, "Extra inputs"),
    ],
)
def test_invalid_alarms_are_rejected(update: dict[str, object], fragment: str) -> None:
    with pytest.raises(ValidationError, match=fragment):
        alarm_message(**update)
