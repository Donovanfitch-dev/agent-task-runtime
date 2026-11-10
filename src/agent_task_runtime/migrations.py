"""Versioned, run-once schema migrations tracked in ``schema_migrations``."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from .errors import MigrationError


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str


MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        1,
        "core_tasks_and_event_log",
        """
        CREATE TABLE tasks (
            seq              INTEGER PRIMARY KEY AUTOINCREMENT,
            id               TEXT NOT NULL UNIQUE,
            kind             TEXT NOT NULL,
            payload          TEXT NOT NULL,
            state            TEXT NOT NULL CHECK (state IN
                ('queued','leased','succeeded','failed','dead_lettered','cancelled')),
            priority         INTEGER NOT NULL DEFAULT 0,
            idempotency_key  TEXT UNIQUE,
            attempt          INTEGER NOT NULL DEFAULT 0,
            max_attempts     INTEGER NOT NULL CHECK (max_attempts >= 1),
            not_before       REAL NOT NULL,
            lease_owner      TEXT,
            lease_expires_at REAL,
            last_error       TEXT,
            result           TEXT,
            created_at       REAL NOT NULL,
            updated_at       REAL NOT NULL
        );
        CREATE INDEX idx_tasks_claim ON tasks (state, not_before, priority DESC, seq);
        CREATE INDEX idx_tasks_lease ON tasks (state, lease_expires_at);

        CREATE TABLE task_events (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id    TEXT NOT NULL REFERENCES tasks (id),
            at         REAL NOT NULL,
            actor      TEXT NOT NULL,
            event_type TEXT NOT NULL,
            from_state TEXT,
            to_state   TEXT NOT NULL,
            attempt    INTEGER NOT NULL,
            detail     TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX idx_task_events_task ON task_events (task_id, id);

        CREATE TRIGGER task_events_no_update BEFORE UPDATE ON task_events
        BEGIN
            SELECT RAISE(ABORT, 'task_events is append-only');
        END;
        CREATE TRIGGER task_events_no_delete BEFORE DELETE ON task_events
        BEGIN
            SELECT RAISE(ABORT, 'task_events is append-only');
        END;
        """,
    ),
    Migration(
        2,
        "checkpoints",
        """
        CREATE TABLE checkpoints (
            task_id    TEXT NOT NULL REFERENCES tasks (id),
            step       TEXT NOT NULL,
            attempt    INTEGER NOT NULL,
            data       TEXT NOT NULL,
            created_at REAL NOT NULL,
            PRIMARY KEY (task_id, step)
        );
        """,
    ),
    Migration(
        3,
        "transactional_outbox",
        """
        CREATE TABLE outbox (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id         TEXT NOT NULL REFERENCES tasks (id),
            event_type      TEXT NOT NULL,
            payload         TEXT NOT NULL,
            idempotency_key TEXT NOT NULL UNIQUE,
            status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending','delivered')),
            attempts        INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL NOT NULL,
            last_error      TEXT,
            created_at      REAL NOT NULL,
            delivered_at    REAL
        );
        CREATE INDEX idx_outbox_due ON outbox (status, next_attempt_at, id);
        """,
    ),
)


def split_statements(script: str) -> list[str]:
    """Split a SQL script into statements, keeping CREATE TRIGGER bodies intact."""
    statements: list[str] = []
    start = 0
    for index, char in enumerate(script):
        if char != ";":
            continue
        chunk = script[start : index + 1]
        # complete_statement() is False inside an unfinished CREATE TRIGGER ... BEGIN ... END.
        if sqlite3.complete_statement(chunk):
            statements.append(chunk.strip())
            start = index + 1
    leftover = script[start:].strip()
    if leftover:
        raise MigrationError(f"incomplete SQL statement in migration: {leftover[:60]!r}")
    return statements


def applied_versions(conn: sqlite3.Connection) -> list[int]:
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'"
    ).fetchone()
    if not exists:
        return []
    return [
        row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")
    ]


def migrate(
    conn: sqlite3.Connection,
    now: Callable[[], float],
    migrations: Sequence[Migration] = MIGRATIONS,
) -> list[int]:
    """Apply any migrations not yet recorded. Returns the versions applied now.

    Each migration runs in its own ``BEGIN IMMEDIATE`` transaction together with
    its bookkeeping row, so a failure leaves the database at the previous
    version. The applied set is re-read inside the transaction, so two
    processes racing to migrate cannot apply the same version twice.
    The connection must be in autocommit mode (``isolation_level=None``).
    """
    known = {m.version for m in migrations}
    if len(known) != len(migrations):
        raise MigrationError("duplicate migration versions")

    newly_applied: list[int] = []
    for migration in sorted(migrations, key=lambda m: m.version):
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at REAL NOT NULL)"
            )
            done = set(applied_versions(conn))
            unknown = done - known
            if unknown:
                raise MigrationError(
                    f"database has migrations this code does not know: {sorted(unknown)}"
                )
            if migration.version not in done:
                for statement in split_statements(migration.sql):
                    conn.execute(statement)
                conn.execute(
                    "INSERT INTO schema_migrations (version, name, applied_at) VALUES (?, ?, ?)",
                    (migration.version, migration.name, now()),
                )
                newly_applied.append(migration.version)
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")
    return newly_applied
