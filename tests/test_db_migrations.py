"""Schema files after 001_init.sql must be idempotent: `make db-migrate` re-applies all of them
to an existing database, and Postgres runs them again on every fresh volume."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.factories import DB_DIR

MIGRATIONS = sorted(p for p in DB_DIR.glob("0*.sql") if p.name != "001_init.sql")

# Statement patterns that would fail, or change data, on a second run. Checked per statement.
_I = re.IGNORECASE | re.DOTALL
_NOT_IDEMPOTENT = (
    (r"^CREATE\s+(UNIQUE\s+)?INDEX\s+(?!IF\s+NOT\s+EXISTS)", "CREATE INDEX without IF NOT EXISTS"),
    (r"^CREATE\s+(TABLE|SCHEMA|SEQUENCE)\s+(?!IF\s+NOT\s+EXISTS)", "CREATE without IF NOT EXISTS"),
    (r"^CREATE\s+(VIEW|FUNCTION|PROCEDURE|TRIGGER)\b", "CREATE … without OR REPLACE"),
    (r"^CREATE\s+(ROLE|TYPE|USER)\b", "CREATE ROLE/TYPE/USER"),
    (
        r"^ALTER\s+TABLE\b.*\bADD\s+COLUMN\s+(?!IF\s+NOT\s+EXISTS)",
        "ADD COLUMN without IF NOT EXISTS",
    ),
    (r"^ALTER\s+TABLE\b.*\bADD\s+CONSTRAINT\b", "ADD CONSTRAINT"),
    (r"^ALTER\s+TABLE\b.*\bRENAME\b", "RENAME"),
    (r"^ALTER\s+TABLE\b.*\bDROP\s+COLUMN\s+(?!IF\s+EXISTS)", "DROP COLUMN without IF EXISTS"),
    (r"^(DROP|TRUNCATE|DELETE)\b", "DROP/TRUNCATE/DELETE"),
    (r"^INSERT\s+INTO\b(?!.*\bON\s+CONFLICT\b)", "INSERT without ON CONFLICT"),
)


def statements(sql: str) -> list[str]:
    """Top-level statements, comments and psql meta-commands (\\set) removed."""
    body = re.sub(r"--[^\n]*", "", sql)
    body = "\n".join(line for line in body.splitlines() if not line.lstrip().startswith("\\"))
    return [st.strip() for st in body.split(";") if st.strip()]


def problems(sql: str) -> list[str]:
    return [
        label
        for st in statements(sql)
        for pattern, label in _NOT_IDEMPOTENT
        if re.search(pattern, st, _I)
    ]


def test_there_are_migrations() -> None:
    assert MIGRATIONS, "expected infra/db/002_*.sql and later"


@pytest.mark.parametrize("path", MIGRATIONS, ids=lambda p: p.name)
def test_migration_is_idempotent(path: Path) -> None:
    sql = path.read_text(encoding="utf-8")
    assert problems(sql) == [], f"{path.name} would not survive a second run"
    assert r"\set ON_ERROR_STOP on" in sql, f"{path.name} must stop on the first error"


@pytest.mark.parametrize(
    ("sql", "bad"),
    [
        ("CREATE TABLE t (x int);", True),
        ("CREATE TABLE IF NOT EXISTS t (x int);", False),
        ("CREATE INDEX i ON t (x);", True),
        ("DROP TABLE t;", True),
        ("INSERT INTO t VALUES (1);", True),
        ("INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;", False),
        ("-- DROP TABLE t (a comment)\nGRANT SELECT ON t TO readonly;", False),
        # each statement on its own: a later ON CONFLICT does not excuse an earlier INSERT
        ("INSERT INTO a VALUES (1);\nINSERT INTO b VALUES (2) ON CONFLICT DO NOTHING;", True),
        ("CREATE UNIQUE INDEX u ON t (x);", True),
        ("CREATE UNIQUE INDEX IF NOT EXISTS u ON t (x);", False),
        ("ALTER TABLE t ADD COLUMN c text;", True),
        ("ALTER TABLE t ADD COLUMN IF NOT EXISTS c text;", False),
        ("ALTER TABLE t ADD CONSTRAINT k CHECK (x > 0);", True),
        ('ALTER TABLE "T" RENAME TO u;', True),
        ("CREATE VIEW v AS SELECT 1;", True),
        ("CREATE OR REPLACE VIEW v AS SELECT 1;", False),
        ("COMMENT ON TABLE t IS 'drop table; is fine in a string';", False),
    ],
)
def test_checker_spots_non_idempotent_sql(sql: str, *, bad: bool) -> None:
    assert bool(problems(sql)) is bad
