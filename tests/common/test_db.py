from __future__ import annotations

import pytest

from common.db import DbConfigError, DbSettings, Role

ENV = {
    "POSTGRES_HOST": "db.local",
    "POSTGRES_PORT": "6543",
    "POSTGRES_DB": "netops_test",
    "PIPELINE_WRITER_PASSWORD": "fake-writer-password",
    "READONLY_PASSWORD": "fake-readonly-password",
}


def test_from_env_picks_role_password() -> None:
    w = DbSettings.from_env(Role.PIPELINE_WRITER, ENV)
    r = DbSettings.from_env(Role.READONLY, ENV)
    assert (w.host, w.port, w.dbname) == ("db.local", 6543, "netops_test")
    assert w.password == ENV["PIPELINE_WRITER_PASSWORD"]
    assert r.password == ENV["READONLY_PASSWORD"]


def test_defaults() -> None:
    s = DbSettings.from_env(Role.READONLY, {"READONLY_PASSWORD": "x"})
    assert (s.host, s.port, s.dbname) == ("127.0.0.1", 5433, "netops")


def test_password_never_in_repr() -> None:
    s = DbSettings.from_env(Role.PIPELINE_WRITER, ENV)
    assert "fake-writer-password" not in repr(s)


def test_missing_password_is_a_clear_error() -> None:
    with pytest.raises(DbConfigError, match="READONLY_PASSWORD is not set"):
        DbSettings.from_env(Role.READONLY, {"PIPELINE_WRITER_PASSWORD": "x"})


def test_bad_port() -> None:
    with pytest.raises(DbConfigError, match="POSTGRES_PORT"):
        DbSettings.from_env(Role.READONLY, {**ENV, "POSTGRES_PORT": "abc"})
