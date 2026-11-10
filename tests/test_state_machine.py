from __future__ import annotations

import itertools

import pytest

from agent_task_runtime import (
    IllegalTransitionError,
    ManualClock,
    StaleLeaseError,
    TaskNotFoundError,
    TaskState,
    TaskStore,
)
from agent_task_runtime.models import ALLOWED_TRANSITIONS, TERMINAL_STATES, check_transition


@pytest.mark.parametrize(("src", "dst"), list(itertools.product(TaskState, TaskState)))
def test_transition_table_is_enforced_for_every_pair(src: TaskState, dst: TaskState) -> None:
    if dst in ALLOWED_TRANSITIONS[src]:
        check_transition("t", src, dst)
    else:
        with pytest.raises(IllegalTransitionError):
            check_transition("t", src, dst)


def test_terminal_states_have_no_exits() -> None:
    for state in TERMINAL_STATES:
        assert ALLOWED_TRANSITIONS[state] == frozenset()


def test_happy_path(store: TaskStore) -> None:
    task = store.enqueue("k", {"a": 1}).task
    assert task.state is TaskState.QUEUED and task.attempt == 0
    leased = store.claim("w1")
    assert leased is not None and leased.state is TaskState.LEASED and leased.attempt == 1
    done = store.complete(task.id, leased.fencing_token, {"ok": True})
    assert done.state is TaskState.SUCCEEDED
    assert done.result == {"ok": True}
    assert done.lease_owner is None and done.lease_expires_at is None


def test_retryable_failure_requeues_with_not_before(store: TaskStore, clock: ManualClock) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    failed = store.fail(task.id, leased.fencing_token, "boom")
    assert failed.state is TaskState.QUEUED
    assert failed.not_before == clock.now() + 1.0  # base delay, no jitter (FixedRng(0))
    assert failed.last_error == "boom"
    assert store.claim("w1") is None  # backing off
    clock.advance(1.0)
    again = store.claim("w1")
    assert again is not None and again.attempt == 2


def test_non_retryable_failure_is_terminal(store: TaskStore) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    failed = store.fail(task.id, leased.fencing_token, "bad input", retryable=False)
    assert failed.state is TaskState.FAILED
    assert len(store.list_outbox(task.id)) == 1


def test_attempts_exhausted_dead_letters(store: TaskStore, clock: ManualClock) -> None:
    task = store.enqueue("k", max_attempts=2).task
    for expected_attempt in (1, 2):
        leased = store.claim("w1")
        assert leased is not None and leased.attempt == expected_attempt
        result = store.fail(task.id, leased.fencing_token, "boom")
        clock.advance(100)
    assert result.state is TaskState.DEAD_LETTERED
    assert store.claim("w1") is None
    assert [e.event_type for e in store.list_outbox(task.id)] == ["task.dead_lettered"]


def test_cancel_queued_task(store: TaskStore) -> None:
    task = store.enqueue("k").task
    cancelled = store.cancel(task.id, reason="not needed")
    assert cancelled.state is TaskState.CANCELLED
    assert store.claim("w1") is None


def test_cancel_leased_task_invalidates_the_workers_token(store: TaskStore) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    store.cancel(task.id)
    with pytest.raises(StaleLeaseError):
        store.complete(task.id, leased.fencing_token)
    assert store.get_task(task.id).state is TaskState.CANCELLED


def test_cannot_cancel_a_terminal_task(store: TaskStore) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    store.complete(task.id, leased.fencing_token)
    with pytest.raises(IllegalTransitionError):
        store.cancel(task.id)


def test_cannot_complete_a_task_that_was_never_leased(store: TaskStore) -> None:
    task = store.enqueue("k").task
    with pytest.raises(StaleLeaseError):
        store.complete(task.id, 1)
    assert store.get_task(task.id).state is TaskState.QUEUED


def test_cannot_complete_twice(store: TaskStore) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    store.complete(task.id, leased.fencing_token)
    with pytest.raises(StaleLeaseError):
        store.complete(task.id, leased.fencing_token)


def test_unknown_task(store: TaskStore) -> None:
    with pytest.raises(TaskNotFoundError):
        store.get_task("nope")
    with pytest.raises(TaskNotFoundError):
        store.complete("nope", 1)


def test_release_returns_task_to_queue_immediately(store: TaskStore) -> None:
    task = store.enqueue("k").task
    leased = store.claim("w1")
    assert leased is not None
    released = store.release(task.id, leased.fencing_token, "shutting down")
    assert released.state is TaskState.QUEUED
    again = store.claim("w2")
    assert again is not None and again.attempt == 2
