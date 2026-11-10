from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from agent_task_runtime import MigrationError, TaskStore
from agent_task_runtime.migrations import (
    MIGRATIONS,
    Migration,
    applied_versions,
    migrate,
    split_statements,
)

from .helpers import run_in_threads


def raw_conn(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path, isolation_level=None)


def test_all_migrations_are_recorded_once(store: TaskStore, db_path: Path) -> None:
    conn = raw_conn(db_path)
    try:
        assert applied_versions(conn) == [m.version for m in MIGRATIONS]
        rows = conn.execute(
            "SELECT version, name FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert rows == [(m.version, m.name) for m in MIGRATIONS]
    finally:
        conn.close()


def test_reopening_does_not_reapply(make_store, db_path: Path) -> None:
    make_store().close()
    conn = raw_conn(db_path)
    before = conn.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    conn.close()
    make_store().close()
    conn = raw_conn(db_path)
    after = conn.execute("SELECT version, applied_at FROM schema_migrations").fetchall()
    assert before == after
    assert migrate(conn, lambda: 0.0) == []
    conn.close()


def test_a_failing_migration_rolls_back_completely(tmp_path: Path) -> None:
    conn = raw_conn(tmp_path / "m.db")
    good = Migration(1, "good", "CREATE TABLE a (x INTEGER);")
    bad = Migration(2, "bad", "CREATE TABLE b (x INTEGER); INSERT INTO missing_table VALUES (1);")
    with pytest.raises(sqlite3.OperationalError):
        migrate(conn, lambda: 0.0, [good, bad])
    assert applied_versions(conn) == [1]
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert "a" in tables and "b" not in tables  # half-applied migration left nothing behind
    # Fixing the migration and re-running applies only the missing version.
    fixed = Migration(2, "fixed", "CREATE TABLE b (x INTEGER);")
    assert migrate(conn, lambda: 0.0, [good, fixed]) == [2]
    conn.close()


def test_database_newer_than_code_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "future.db"
    TaskStore(path).close()
    conn = raw_conn(path)
    conn.execute("INSERT INTO schema_migrations VALUES (99, 'from_the_future', 0)")
    conn.close()
    with pytest.raises(MigrationError, match="99"):
        TaskStore(path)


def test_duplicate_versions_are_rejected(tmp_path: Path) -> None:
    conn = raw_conn(tmp_path / "d.db")
    with pytest.raises(MigrationError):
        migrate(conn, lambda: 0.0, [Migration(1, "a", "SELECT 1;"), Migration(1, "b", "SELECT 1;")])
    conn.close()


def test_statement_splitter_keeps_trigger_bodies_whole() -> None:
    script = (
        "CREATE TABLE t (x);\n"
        "CREATE TRIGGER g BEFORE UPDATE ON t\nBEGIN\n  SELECT 1;\n  SELECT 2;\nEND;\n"
        "INSERT INTO t VALUES (1); INSERT INTO t VALUES (2);"
    )
    statements = split_statements(script)
    assert len(statements) == 4 and statements[1].startswith("CREATE TRIGGER")
    assert "SELECT 2;" in statements[1]


def test_racing_processes_migrate_a_fresh_database_once(tmp_path: Path) -> None:
    path = tmp_path / "race.db"
    run_in_threads(6, path, lambda s, i: None)
    conn = raw_conn(path)
    try:
        assert applied_versions(conn) == [m.version for m in MIGRATIONS]
        assert conn.execute("SELECT COUNT(*) FROM schema_migrations").fetchone() == (
            len(MIGRATIONS),
        )
    finally:
        conn.close()
