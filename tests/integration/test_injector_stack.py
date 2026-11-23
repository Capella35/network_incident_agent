"""Change events into the running Postgres as `pipeline_writer` (rolled back)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import psycopg
import pytest

from producer import injector as inj
from tests.integration.conftest import unique

pytestmark = pytest.mark.integration


def test_change_events_upsert_per_run(writer_conn: psycopg.Connection[Any]) -> None:
    run = unique("it-changes")
    t = datetime(2013, 12, 2, 9, 0, tzinfo=UTC)
    events = [
        inj.ChangeEvent("CHG-1", "SITE-1", t),
        inj.ChangeEvent("CHG-2", "SITE-2", t),
    ]
    with writer_conn.transaction() as outer:
        inj.write_change_events(writer_conn, run, events)
        inj.write_change_events(writer_conn, run, events)  # re-run: upsert, no duplicates
        rows = writer_conn.execute(
            "SELECT change_id, site_id FROM change_events WHERE replay_run_id = %s ORDER BY 1",
            (run,),
        ).fetchall()
        raise psycopg.Rollback(outer)
    assert rows == [("CHG-1", "SITE-1"), ("CHG-2", "SITE-2")]
