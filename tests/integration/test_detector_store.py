"""`streaming.job.PgStore` against the running Postgres (milestone 2, step 8): alarms are stored
once by ID with the heartbeat in the same transaction. Writes are rolled back."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
import pytest

from streaming import job
from tests.factories import ALARM_AT, alarm_message
from tests.integration.conftest import unique

pytestmark = pytest.mark.integration

T = ALARM_AT


def beat(job_id: str, batch: int, scored: int, late: int, latest: datetime | None) -> job.Heartbeat:
    return job.Heartbeat(
        job_id=job_id,
        batch_id=batch,
        detector_version="detector/1",
        rows_scored=scored,
        rows_late=late,
        alarms=1,
        latest_interval=latest,
    )


def test_alarms_are_stored_once_with_their_heartbeat(writer_conn: psycopg.Connection[Any]) -> None:
    run, job_id = unique("store"), unique("job")
    first = alarm_message(run=run, score=3.141592653589793)
    with writer_conn.transaction(force_rollback=True):
        store = job.PgStore(writer_conn)
        assert store.known([first.alarm_id]) == set()
        store.commit([first], beat(job_id, 0, 144, 2, T))
        assert store.known([first.alarm_id, "ALM-other"]) == {first.alarm_id}
        store.commit(
            [alarm_message(run=run, score=9.0)],
            job.Heartbeat(job_id, 1, "detector/1", 0, 0, 1, None),
        )
        row = writer_conn.execute(
            "SELECT score, kpis, interval_start FROM alarms WHERE alarm_id = %s", (first.alarm_id,)
        ).fetchone()
        assert row == (first.score, list(first.kpis), T)  # the first write stays, exactly
        beats = writer_conn.execute(
            "SELECT batch_id, rows_scored, rows_late, alarms FROM detector_heartbeats"
            " WHERE job_id = %s ORDER BY batch_id",
            (job_id,),
        ).fetchall()
        assert beats == [(0, 144, 2, 1), (1, 0, 0, 1)]
