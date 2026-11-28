"""`evaluation.detect_check.store` against the running Postgres (milestone 2, step 10): the
writer role inserts a day's totals once; the readonly role (Grafana's) sees the latest check of
each day through the view. Writes are rolled back."""

from __future__ import annotations

import dataclasses
from typing import Any

import psycopg
import pytest

from common.db import DbSettings, Role
from evaluation import detect_check as dc
from tests.factories import check_report
from tests.integration.conftest import unique

pytestmark = pytest.mark.integration


def test_report_is_stored_once_and_the_view_shows_the_latest(
    writer_conn: psycopg.Connection[Any],
) -> None:
    first = dataclasses.replace(check_report([]), run=unique("check"))
    later = dataclasses.replace(check_report(["x"]), run=unique("check"))
    with writer_conn.transaction(force_rollback=True):
        dc.store(writer_conn, first)
        dc.store(writer_conn, first)  # a re-run of the same check: stored once
        n = writer_conn.execute(
            "SELECT count(*) FROM detection_reports WHERE replay_run_id = %s", (first.run,)
        ).fetchone()
        assert n == (1,)
        writer_conn.execute(  # a later check of the same day
            "INSERT INTO detection_reports SELECT %s, day, detector_version, faults, unobservable,"
            " detected, delay_median, delay_low, delay_high, incidents, false_incidents, false,"
            " reported_at + interval '1 second' FROM detection_reports WHERE replay_run_id = %s",
            (later.run, first.run),
        )
        latest = writer_conn.execute(
            "SELECT replay_run_id, parity FROM detection_reports_latest WHERE day = %s",
            (first.day,),
        ).fetchone()
        assert latest == (later.run, False)


def test_readonly_role_can_read_but_not_write_reports() -> None:
    with DbSettings.from_env(Role.READONLY).connect() as conn:
        conn.execute("SELECT count(*) FROM detection_reports_latest").fetchone()
        refused = (psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)
        with pytest.raises(refused):
            dc.store(conn, dataclasses.replace(check_report([]), run=unique("ro")))
