from __future__ import annotations

from collections.abc import Callable

import pytest

from agent_task_runtime import (
    LeaseExpiredError,
    ManualClock,
    StaleLeaseError,
    TaskState,
    TaskStore,
)

from .helpers import snapshot


def test_each_claim_increments_the_fencing_token(store: TaskStore, clock: ManualClock) -> None:
    task = store.enqueue("k").task
    tokens = []
    for _ in range(3):
        leased = store.claim("w")
        assert leased is not None
        tokens.append(leased.fencing_token)
        clock.advance(31)
        store.reap_expired()
    assert tokens == [1, 2, 3]
    assert store.get_task(task.id).attempt == 3


def test_heartbeat_extends_the_lease(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k")
    leased = store.claim("w")
    assert leased is not None
    clock.advance(20)
    renewed = store.heartbeat(leased.id, leased.fencing_token)
    assert renewed.lease_expires_at == clock.now() + 30
    clock.advance(20)  # t=40 > original deadline of 30
    assert store.reap_expired() == []
    clock.advance(11)  # t=51 >= renewed deadline of 50
    assert [t.id for t in store.reap_expired()] == [leased.id]


def test_heartbeat_after_deadline_is_rejected_even_with_current_token(
    store: TaskStore, clock: ManualClock
) -> None:
    store.enqueue("k")
    leased = store.claim("w")
    assert leased is not None
    clock.advance(31)
    before = snapshot(store, leased.id)
    with pytest.raises(LeaseExpiredError) as info:
        store.heartbeat(leased.id, leased.fencing_token)
    assert isinstance(info.value, StaleLeaseError)
    assert snapshot(store, leased.id) == before


def test_complete_after_deadline_but_before_reclaim_is_accepted(
    store: TaskStore, clock: ManualClock
) -> None:
    """Nobody else holds the task, so finished work is kept rather than thrown away."""
    store.enqueue("k")
    leased = store.claim("w")
    assert leased is not None
    clock.advance(500)
    done = store.complete(leased.id, leased.fencing_token, "late but valid")
    assert done.state is TaskState.SUCCEEDED


@pytest.fixture
def zombie_scenario(store: TaskStore, clock: ManualClock) -> tuple[str, int, int]:
    """Worker A claims (token 1), its lease expires, B re-claims (token 2)."""
    store.enqueue("k", task_id="t1")
    a = store.claim("worker-A")
    assert a is not None
    store.save_checkpoint("t1", a.fencing_token, "plan", {"by": "A"})
    clock.advance(31)
    assert len(store.reap_expired()) == 1
    b = store.claim("worker-B")
    assert b is not None and b.fencing_token == 2
    return "t1", a.fencing_token, b.fencing_token


@pytest.mark.parametrize(
    "operation",
    [
        lambda s, tid, tok: s.complete(tid, tok, "zombie result"),
        lambda s, tid, tok: s.fail(tid, tok, "zombie failure"),
        lambda s, tid, tok: s.fail(tid, tok, "zombie failure", retryable=False),
        lambda s, tid, tok: s.heartbeat(tid, tok),
        lambda s, tid, tok: s.save_checkpoint(tid, tok, "plan", {"by": "zombie"}),
        lambda s, tid, tok: s.release(tid, tok, "zombie release"),
    ],
    ids=["complete", "fail", "fail-permanent", "heartbeat", "checkpoint", "release"],
)
def test_zombie_operations_are_rejected_and_change_nothing(
    store: TaskStore,
    zombie_scenario: tuple[str, int, int],
    operation: Callable[[TaskStore, str, int], object],
) -> None:
    task_id, stale_token, current_token = zombie_scenario
    before = snapshot(store, task_id)
    with pytest.raises(StaleLeaseError) as info:
        operation(store, task_id, stale_token)
    assert info.value.presented_token == stale_token
    assert info.value.current_token == current_token
    assert snapshot(store, task_id) == before
    assert store.load_checkpoints(task_id) == {"plan": {"by": "A"}}


def test_new_owner_can_finish_after_zombie_is_rejected(
    store: TaskStore, zombie_scenario: tuple[str, int, int]
) -> None:
    task_id, stale, current = zombie_scenario
    with pytest.raises(StaleLeaseError):
        store.complete(task_id, stale, "zombie")
    done = store.complete(task_id, current, "from B")
    assert done.state is TaskState.SUCCEEDED and done.result == "from B"


def test_zombie_cannot_resurrect_a_finished_task(
    store: TaskStore, zombie_scenario: tuple[str, int, int]
) -> None:
    task_id, stale, current = zombie_scenario
    store.complete(task_id, current, "from B")
    before = snapshot(store, task_id)
    with pytest.raises(StaleLeaseError):
        store.fail(task_id, stale, "zombie failure")
    assert snapshot(store, task_id) == before
    assert store.get_task(task_id).state is TaskState.SUCCEEDED


def test_reaper_requeues_expired_leases_and_counts_the_attempt(
    store: TaskStore, clock: ManualClock
) -> None:
    store.enqueue("k")
    leased = store.claim("w")
    assert leased is not None
    clock.advance(30)  # deadline is inclusive
    [reclaimed] = store.reap_expired()
    assert reclaimed.state is TaskState.QUEUED
    assert reclaimed.attempt == 1
    assert reclaimed.lease_owner is None and reclaimed.lease_expires_at is None
    assert "expired" in (reclaimed.last_error or "")
    assert store.claim("w2") is not None  # immediately claimable: a crash earns no backoff


def test_reaper_ignores_live_and_terminal_tasks(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k")
    store.enqueue("k")
    live = store.claim("w")
    finished = store.claim("w")
    assert live is not None and finished is not None
    store.complete(finished.id, finished.fencing_token)
    clock.advance(29)
    assert store.reap_expired() == []


def test_reaper_dead_letters_when_attempts_are_exhausted(
    store: TaskStore, clock: ManualClock
) -> None:
    task = store.enqueue("k", max_attempts=1).task
    assert store.claim("w") is not None
    clock.advance(31)
    [reclaimed] = store.reap_expired()
    assert reclaimed.state is TaskState.DEAD_LETTERED
    assert [e.event_type for e in store.list_outbox(task.id)] == ["task.dead_lettered"]
    assert store.claim("w") is None


def test_reaping_is_idempotent(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k")
    assert store.claim("w") is not None
    clock.advance(31)
    assert len(store.reap_expired()) == 1
    assert store.reap_expired() == []
