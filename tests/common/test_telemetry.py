from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from pydantic import ValidationError

from common import telemetry
from common.telemetry import TelemetryMessage

T0 = datetime(2013, 11, 4, 23, 0, tzinfo=UTC)


def row(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "replay_run_id": "run-1",
        "cell_id": "CELL-1-1",
        "site_id": "SITE-1",
        "square_id": 4455,
        "interval_start": T0,
        "sms_in": 1.0,
        "sms_out": 2.0,
        "call_in": 3.0,
        "call_out": 4.0,
        "internet": 400.5,
        "data_gap": False,
        "load_ratio": 0.42,
        "latency_ms": 34.5,
        "drop_rate": 0.002,
    }
    return {**base, **over}


GAP = {
    "data_gap": True,
    **dict.fromkeys(telemetry.TRAFFIC_FIELDS, 0.0),
    **dict.fromkeys(telemetry.MODELED_FIELDS, None),
}


def test_schema_matches_domain_model() -> None:
    telemetry.check_against_domain()


def test_valid_message_json_and_key() -> None:
    msg = TelemetryMessage(**row())
    doc = json.loads(msg.to_json())
    assert doc["schema_version"] == 1
    assert doc["interval_start"] == "2013-11-04T23:00:00Z"
    assert doc["cell_id"] == "CELL-1-1" and doc["site_id"] == "SITE-1"
    assert TelemetryMessage.model_validate_json(msg.to_json()) == msg
    assert msg.key() == "CELL-1-1|2013-11-04T23:00:00Z"


def test_key_is_utc_whatever_the_input_zone() -> None:
    cet = timezone(timedelta(hours=1))
    assert telemetry.message_key("CELL-1-1", T0.astimezone(cet)) == "CELL-1-1|2013-11-04T23:00:00Z"


def test_gap_row_is_valid_with_null_modeled_values() -> None:
    msg = TelemetryMessage(**row(**GAP))
    assert msg.load_ratio is None and msg.latency_ms is None and msg.drop_rate is None


@pytest.mark.parametrize(
    ("over", "fragment"),
    [
        ({"internet": -1.0}, "internet"),
        ({"internet": math.nan}, "internet"),
        ({"latency_ms": math.inf}, "latency_ms"),
        ({"drop_rate": 1.5}, "drop_rate"),
        ({"cell_id": "SITE-1"}, "cell_id"),
        ({"site_id": "CELL-1-1"}, "site_id"),
        ({"square_id": 0}, "square_id"),
        ({"interval_start": T0.replace(tzinfo=None)}, "UTC"),
        ({"interval_start": T0 + timedelta(minutes=5)}, "grid"),
        ({"load_ratio": None}, "modeled values are required"),
        ({"data_gap": True}, "no modeled values"),
        ({**GAP, "internet": 3.0}, "zero traffic"),
        ({"replay_run_id": ""}, "replay_run_id"),
        ({"surprise": 1}, "surprise"),
    ],
)
def test_invalid_rows_rejected(over: dict[str, Any], fragment: str) -> None:
    with pytest.raises(ValidationError, match=fragment):
        TelemetryMessage(**row(**over))


def test_finite_or_none_makes_rows_json_safe() -> None:
    assert telemetry.finite_or_none(math.nan) == "nan"
    assert telemetry.finite_or_none(1.5) == 1.5
    assert telemetry.finite_or_none(T0) == "2013-11-04T23:00:00+00:00"
