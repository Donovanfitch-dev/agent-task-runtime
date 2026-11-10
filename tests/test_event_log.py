from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from agent_task_runtime import ManualClock, TaskStore


def test_full_lifecycle_is_audited_with_actor_time_and_transition(
    store: TaskStore, clock: ManualClock
) -> None:
    store.enqueue("k", task_id="t")
    clock.advance(1)
    leased = store.claim("worker-A")
    assert leased is not None
    clock.advance(1)
    store.fail("t", leased.fencing_token, "boom")
    clock.advance(5)
    leased = store.claim("worker-B")
    assert leased is not None
    clock.advance(1)
    store.complete("t", leased.fencing_token, "ok")

    log = [
        (e.actor, e.event_type, e.from_state, e.to_state, e.attempt, e.at)
        for e in store.list_events("t")
    ]
    assert log == [
        ("client", "enqueued", None, "queued", 0, 1000.0),
        ("worker-A", "claimed", "queued", "leased", 1, 1001.0),
        ("worker-A", "retry_scheduled", "leased", "queued", 1, 1002.0),
        ("worker-B", "claimed", "queued", "leased", 2, 1007.0),
        ("worker-B", "succeeded", "leased", "succeeded", 2, 1008.0),
    ]
    retry = store.list_events("t")[2]
    assert retry.detail["error"] == "boom" and retry.detail["delay_seconds"] == 1.0


def test_reaper_and_cancel_are_attributed(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k", task_id="t")
    store.claim("worker-A")
    clock.advance(31)
    store.reap_expired()
    store.cancel("t", actor="operator", reason="obsolete")
    types = [(e.actor, e.event_type) for e in store.list_events("t")]
    assert types[-2:] == [("reaper", "lease_expired"), ("operator", "cancelled")]
    assert store.list_events("t")[-1].detail == {"reason": "obsolete"}


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE task_events SET actor = 'mallory'",
        "DELETE FROM task_events",
    ],
)
def test_event_log_is_append_only_at_the_database_level(
    store: TaskStore, db_path: Path, sql: str
) -> None:
    store.enqueue("k")
    raw = sqlite3.connect(db_path)
    try:
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            raw.execute(sql)
    finally:
        raw.close()
    assert len(store.list_events()) == 1
