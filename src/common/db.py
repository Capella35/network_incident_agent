"""Postgres connection settings for the application roles (created by infra/db/000_roles.sh).

Settings come from environment variables (filled from `.env` by the Makefile); the password is
kept out of `repr()` so it never lands in a log line or an error message.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

import psycopg
from psycopg import sql


class Role(StrEnum):
    PIPELINE_WRITER = "pipeline_writer"
    READONLY = "readonly"


_PASSWORD_VAR = {
    Role.PIPELINE_WRITER: "PIPELINE_WRITER_PASSWORD",
    Role.READONLY: "READONLY_PASSWORD",
}


class DbConfigError(RuntimeError):
    pass


@dataclass(frozen=True)
class DbSettings:
    host: str
    port: int
    dbname: str
    role: Role
    password: str = field(repr=False)

    @classmethod
    def from_env(cls, role: Role, env: Mapping[str, str] | None = None) -> DbSettings:
        env = os.environ if env is None else env
        var = _PASSWORD_VAR[role]
        password = env.get(var, "")
        if not password:
            raise DbConfigError(f"{var} is not set; run through `make` so .env is loaded")
        try:
            port = int(env.get("POSTGRES_PORT", "5433"))
        except ValueError:
            raise DbConfigError("POSTGRES_PORT is not an integer") from None
        return cls(
            host=env.get("POSTGRES_HOST", "127.0.0.1"),
            port=port,
            dbname=env.get("POSTGRES_DB", "netops"),
            role=role,
            password=password,
        )

    def connect(self) -> psycopg.Connection[tuple[object, ...]]:
        return psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=str(self.role),
            password=self.password,
            connect_timeout=5,
            application_name="network-incident-agent",
        )


def insert_into(table: str, columns: Sequence[str]) -> sql.Composed:
    """`INSERT INTO table (columns) VALUES (%s, ...)`, identifiers quoted; callers append their
    ON CONFLICT clause."""
    return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() * len(columns)),
    )
