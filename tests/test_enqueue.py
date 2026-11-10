from __future__ import annotations

from pathlib import Path

import pytest

from agent_task_runtime import IdempotencyConflictError, ManualClock, TaskStore

from .helpers import run_in_threads


def test_same_key_returns_the_existing_task(store: TaskStore) -> None:
    first = store.enqueue("k", {"a": 1}, idempotency_key="job-1")
    second = store.enqueue("k", {"a": 1}, idempotency_key="job-1")
    assert first.created is True and second.created is False
    assert first.task.id == second.task.id
    assert len(store.list_tasks()) == 1
    assert len(store.list_events()) == 1  # no second 'enqueued' audit row


def test_payload_key_order_does_not_matter(store: TaskStore) -> None:
    store.enqueue("k", {"a": 1, "b": 2}, idempotency_key="j")
    assert store.enqueue("k", {"b": 2, "a": 1}, idempotency_key="j").created is False


def test_key_reuse_with_a_different_payload_or_kind_is_a_conflict(store: TaskStore) -> None:
    store.enqueue("k", {"a": 1}, idempotency_key="j")
    with pytest.raises(IdempotencyConflictError):
        store.enqueue("k", {"a": 2}, idempotency_key="j")
    with pytest.raises(IdempotencyConflictError):
        store.enqueue("other", {"a": 1}, idempotency_key="j")


def test_keyless_enqueues_are_distinct(store: TaskStore) -> None:
    a = store.enqueue("k", {"a": 1})
    b = store.enqueue("k", {"a": 1})
    assert a.task.id != b.task.id


def test_idempotency_survives_the_task_finishing(store: TaskStore) -> None:
    first = store.enqueue("k", idempotency_key="j").task
    leased = store.claim("w")
    assert leased is not None
    store.complete(first.id, leased.fencing_token)
    again = store.enqueue("k", idempotency_key="j")
    assert again.created is False and again.task.state.value == "succeeded"


def test_concurrent_enqueues_with_one_key_create_one_task(
    db_path: Path, clock: ManualClock, store: TaskStore
) -> None:
    results = run_in_threads(
        12,
        db_path,
        lambda s, i: s.enqueue("k", {"n": 1}, idempotency_key="shared"),
        clock=clock,
    )
    assert sum(r.created for r in results) == 1
    assert len({r.task.id for r in results}) == 1
    assert len(store.list_tasks()) == 1


def test_max_attempts_defaults_to_policy_and_can_be_overridden(store: TaskStore) -> None:
    assert store.enqueue("k").task.max_attempts == 5
    assert store.enqueue("k", max_attempts=2).task.max_attempts == 2
    with pytest.raises(ValueError):
        store.enqueue("k", max_attempts=0)


def test_payload_round_trips(store: TaskStore) -> None:
    payload = {"nested": {"list": [1, 2, {"x": None}]}, "text": "café"}
    task = store.enqueue("k", payload).task
    assert store.get_task(task.id).payload == payload
