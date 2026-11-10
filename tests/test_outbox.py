from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

import pytest

from agent_task_runtime import (
    DedupSink,
    FlakySink,
    ListSink,
    LostAckSink,
    ManualClock,
    OutboxDispatcher,
    OutboxEvent,
    TaskState,
    TaskStore,
)


def finish_one(store: TaskStore, result: object = "done", task_id: str = "t1") -> str:
    store.enqueue("k", task_id=task_id)
    leased = store.claim("w")
    assert leased is not None
    store.complete(leased.id, leased.fencing_token, result)
    return leased.id


def test_terminal_state_and_outbox_event_are_written_together(store: TaskStore) -> None:
    task_id = finish_one(store, {"answer": 42})
    [event] = store.list_outbox(task_id)
    assert event.event_type == "task.succeeded"
    assert event.status == "pending"
    assert event.idempotency_key == f"{task_id}:task.succeeded"
    assert event.payload["result"] == {"answer": 42}
    assert event.payload["task_id"] == task_id


def test_failed_transaction_leaves_neither_terminal_state_nor_event(
    store: TaskStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Atomicity: if the outbox insert blows up, the state change is rolled back too."""
    store.enqueue("k", task_id="t1")
    leased = store.claim("w")
    assert leased is not None
    events_before = store.list_events("t1")

    def boom(*args: object, **kwargs: object) -> None:
        raise RuntimeError("disk full (simulated)")

    monkeypatch.setattr(store, "_emit_outbox", boom)
    with pytest.raises(RuntimeError):
        store.complete("t1", leased.fencing_token, "x")
    monkeypatch.undo()

    task = store.get_task("t1")
    assert task.state is TaskState.LEASED and task.result is None
    assert store.list_events("t1") == events_before
    assert store.list_outbox("t1") == []
    # And the original lease is still usable afterwards.
    assert store.complete("t1", leased.fencing_token, "x").state is TaskState.SUCCEEDED
    assert len(store.list_outbox("t1")) == 1


@pytest.mark.parametrize("terminal", ["failed", "dead_lettered", "cancelled"])
def test_every_terminal_state_emits_exactly_one_event(store: TaskStore, terminal: str) -> None:
    store.enqueue("k", task_id="t", max_attempts=1)
    if terminal == "cancelled":
        store.cancel("t")
    else:
        leased = store.claim("w")
        assert leased is not None
        store.fail("t", leased.fencing_token, "x", retryable=(terminal == "dead_lettered"))
    assert [e.event_type for e in store.list_outbox("t")] == [f"task.{terminal}"]


def test_non_terminal_transitions_emit_nothing(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    leased = store.claim("w")
    assert leased is not None
    store.fail("t", leased.fencing_token, "retry me")  # -> queued
    assert store.list_outbox("t") == []


def test_dispatcher_delivers_and_marks_delivered(store: TaskStore) -> None:
    finish_one(store)
    sink = ListSink()
    report = OutboxDispatcher(store, sink).run_once()
    assert len(report.delivered) == 1 and report.failed == []
    assert len(sink.received) == 1
    [event] = store.list_outbox()
    assert event.status == "delivered" and event.attempts == 1 and event.delivered_at is not None
    assert OutboxDispatcher(store, sink).run_once().delivered == []  # nothing left
    assert len(sink.received) == 1


def test_delivery_failures_back_off_then_succeed(store: TaskStore, clock: ManualClock) -> None:
    finish_one(store)
    inner = ListSink()
    sink = FlakySink(inner, failures=2)
    dispatcher = OutboxDispatcher(store, sink)

    t0 = clock.now()
    first = dispatcher.run_once()
    assert len(first.failed) == 1
    assert first.failed[0][2] == t0 + 1.0  # first backoff
    assert dispatcher.run_once().failed == []  # not due yet: sink not even called
    assert sink.attempts == 1

    clock.advance(1.0)
    second = dispatcher.run_once()
    assert second.failed[0][2] == clock.now() + 2.0  # doubled
    clock.advance(1.9)
    assert dispatcher.run_once().delivered == []  # still backing off
    assert sink.attempts == 2

    clock.advance(0.1)
    third = dispatcher.run_once()
    assert len(third.delivered) == 1
    assert len(inner.received) == 1
    [event] = store.list_outbox()
    assert event.status == "delivered" and event.attempts == 3


def test_failed_delivery_records_the_error(store: TaskStore) -> None:
    finish_one(store)
    OutboxDispatcher(store, FlakySink(ListSink(), failures=1)).run_once()
    [event] = store.list_outbox()
    assert event.status == "pending" and event.attempts == 1
    assert event.last_error is not None and "SinkUnavailableError" in event.last_error


def test_nothing_is_lost_across_a_process_restart(
    make_store: Callable[..., TaskStore], clock: ManualClock
) -> None:
    first = make_store()
    task_id = finish_one(first)
    # Dispatcher fails once, then the whole process "dies".
    OutboxDispatcher(first, FlakySink(ListSink(), failures=1)).run_once()
    first.close()

    reopened = make_store()  # same database file
    [event] = reopened.list_outbox(task_id)
    assert event.status == "pending" and event.attempts == 1  # state survived
    sink = ListSink()
    dispatcher = OutboxDispatcher(reopened, sink)
    assert dispatcher.run_once().delivered == []  # backoff deadline survived too
    clock.advance(1.0)
    assert len(dispatcher.run_once().delivered) == 1
    assert [e.idempotency_key for e in sink.received] == [f"{task_id}:task.succeeded"]


def test_committed_task_state_survives_restart_via_raw_file_reopen(
    make_store: Callable[..., TaskStore], db_path: Path
) -> None:
    store = make_store()
    finish_one(store, {"v": 1})
    store.close()
    raw = sqlite3.connect(db_path)
    try:
        assert raw.execute("SELECT state FROM tasks").fetchall() == [("succeeded",)]
        assert raw.execute("SELECT COUNT(*) FROM outbox").fetchone() == (1,)
    finally:
        raw.close()


def test_redelivery_carries_the_same_idempotency_key(store: TaskStore, clock: ManualClock) -> None:
    finish_one(store)
    inner = ListSink()
    dispatcher = OutboxDispatcher(store, LostAckSink(inner, times=2))
    for _ in range(3):
        dispatcher.run_once()
        clock.advance(10)
    keys = [e.idempotency_key for e in inner.received]
    assert len(keys) == 3 and len(set(keys)) == 1  # at-least-once: duplicates, one stable key


def test_dedup_sink_gives_effectively_once_processing(store: TaskStore, clock: ManualClock) -> None:
    finish_one(store)
    effects: list[OutboxEvent] = []
    dedup = DedupSink(effects.append)
    dispatcher = OutboxDispatcher(store, LostAckSink(dedup, times=1))

    dispatcher.run_once()  # effect applied, ack lost -> recorded as a failure
    assert len(effects) == 1 and store.list_outbox(status="pending")
    clock.advance(10)
    dispatcher.run_once()  # redelivery: duplicate absorbed, now acked
    assert len(effects) == 1
    assert dedup.duplicates_ignored == 1
    assert store.list_outbox(status="pending") == []
    dedup.close()


def test_dedup_sink_does_not_record_a_key_when_the_handler_fails() -> None:
    calls = []

    def handler(event: OutboxEvent) -> None:
        calls.append(event.idempotency_key)
        if len(calls) == 1:
            raise RuntimeError("handler failed")

    dedup = DedupSink(handler)
    event = OutboxEvent(
        1, "t", "task.succeeded", {}, "t:task.succeeded", "pending", 0, 0.0, None, 0.0, None
    )
    with pytest.raises(RuntimeError):
        dedup.deliver(event)
    dedup.deliver(event)  # retried and applied, because no key was recorded
    dedup.deliver(event)  # now a duplicate
    assert len(calls) == 2 and dedup.duplicates_ignored == 1
    dedup.close()


def test_dedup_state_can_live_in_a_file_and_survive_consumer_restart(tmp_path: Path) -> None:
    path = str(tmp_path / "consumer.db")
    event = OutboxEvent(
        1, "t", "task.succeeded", {}, "t:task.succeeded", "pending", 0, 0.0, None, 0.0, None
    )
    applied: list[str] = []
    first = DedupSink(lambda e: applied.append(e.idempotency_key), path)
    first.deliver(event)
    first.close()
    second = DedupSink(lambda e: applied.append(e.idempotency_key), path)
    second.deliver(event)
    second.close()
    assert applied == ["t:task.succeeded"]
