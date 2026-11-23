"""Checks against the running local stack (`make up`); run with `make test-integration`.

Every write happens inside a transaction that is rolled back, so the dev database is unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg
import pytest

from common.db import DbConfigError, DbSettings, Role
from producer import grid, topology, topology_db
from tests.producer.conftest import write_grid

pytestmark = pytest.mark.integration


def _settings(role: Role) -> DbSettings:
    try:
        return DbSettings.from_env(role)
    except DbConfigError as exc:
        pytest.skip(f"stack credentials not in the environment: {exc}")


@pytest.fixture(scope="module")
def rows(tmp_path_factory: pytest.TempPathFactory) -> topology_db.TopologyRows:
    out = tmp_path_factory.mktemp("topo")
    write_grid(out / "grid.geojson")
    squares = grid.load_squares(out / "grid.geojson")
    subset = grid.select_block(
        squares, grid.nearest_square(squares, *grid.DUOMO_LON_LAT), grid.SUBSET_BLOCK
    )
    topology.write(
        topology.build(squares, subset, topology.load_config()), out, {"synthetic": True}
    )
    return topology_db.read_topology(out)


def test_writer_replaces_topology_atomically(rows: topology_db.TopologyRows) -> None:
    with (
        _settings(Role.PIPELINE_WRITER).connect() as conn,
        conn.transaction(force_rollback=True),
        conn.cursor() as cur,
    ):
        topology_db.load(conn, rows)
        topology_db.load(conn, rows)  # idempotent
        cur.execute("SELECT count(*) FROM topology_nodes")
        assert cur.fetchone() == (len(rows.nodes),)
        cur.execute("SELECT count(*) FROM topology_edges")
        assert cur.fetchone() == (len(rows.edges),)
        cur.execute("SELECT synthetic FROM topology_meta")
        assert cur.fetchone() == (True,)


def test_writer_cannot_delete_telemetry_or_create_tables() -> None:
    with _settings(Role.PIPELINE_WRITER).connect() as conn:
        for sql in ("DELETE FROM telemetry_raw", "CREATE TABLE intruder (x int)"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege), conn.transaction():
                conn.execute(sql)


def test_readonly_can_read_but_never_write() -> None:
    """Guardrail S4: the role Grafana and the ops agent use cannot change anything."""
    now = datetime.now(UTC)
    with _settings(Role.READONLY).connect() as conn:
        conn.execute("SELECT count(*) FROM telemetry_raw").fetchone()
        conn.rollback()
        writes = [
            ("INSERT INTO topology_meta (synthetic, meta) VALUES (true, '{}')", ()),
            (
                "INSERT INTO telemetry_raw (replay_run_id, cell_id, interval_start, square_id, "
                "site_id, sms_in, sms_out, call_in, call_out, internet, data_gap, schema_version) "
                "VALUES ('t', 'CELL-1-1', %s, 1, 'SITE-1', 0, 0, 0, 0, 0, true, 1)",
                (now,),
            ),
            ("DELETE FROM topology_nodes", ()),
        ]
        for sql, params in writes:
            with pytest.raises(
                (psycopg.errors.InsufficientPrivilege, psycopg.errors.ReadOnlySqlTransaction)
            ):
                conn.execute(sql, params)
            conn.rollback()
