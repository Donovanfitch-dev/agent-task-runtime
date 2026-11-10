from __future__ import annotations

from pathlib import Path

from agent_task_runtime import ManualClock, TaskState, TaskStore

from .helpers import run_in_threads


def test_empty_queue_returns_none(store: TaskStore) -> None:
    assert store.claim("w1") is None


def test_priority_then_fifo(store: TaskStore) -> None:
    low1 = store.enqueue("k", priority=0).task
    high1 = store.enqueue("k", priority=10).task
    low2 = store.enqueue("k", priority=0).task
    high2 = store.enqueue("k", priority=10).task
    order = []
    while (t := store.claim("w1")) is not None:
        order.append(t.id)
    assert order == [high1.id, high2.id, low1.id, low2.id]


def test_not_before_is_respected_and_does_not_block_others(
    store: TaskStore, clock: ManualClock
) -> None:
    later = store.enqueue("k", priority=99, not_before=clock.now() + 50).task
    now_task = store.enqueue("k", priority=0).task
    first = store.claim("w1")
    assert first is not None and first.id == now_task.id
    assert store.claim("w1") is None
    clock.advance(50)
    second = store.claim("w1")
    assert second is not None and second.id == later.id


def test_kind_filter(store: TaskStore) -> None:
    store.enqueue("a")
    b = store.enqueue("b").task
    claimed = store.claim("w1", kinds=["b"])
    assert claimed is not None and claimed.id == b.id
    assert store.claim("w1", kinds=["b"]) is None


def test_claim_sets_lease_fields(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k")
    task = store.claim("w1", lease_seconds=12)
    assert task is not None
    assert task.lease_owner == "w1"
    assert task.lease_expires_at == clock.now() + 12


def test_only_one_of_many_concurrent_claimers_wins_a_single_task(
    db_path: Path, clock: ManualClock, store: TaskStore
) -> None:
    store.enqueue("k", task_id="only")
    results = run_in_threads(
        16, db_path, lambda s, i: s.claim(f"w{i}"), clock=clock, lease_seconds=30.0
    )
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    assert winners[0].attempt == 1
    final = store.get_task("only")
    assert final.state is TaskState.LEASED
    assert final.lease_owner == winners[0].lease_owner


def test_every_task_is_claimed_exactly_once_under_contention(
    db_path: Path, clock: ManualClock, store: TaskStore
) -> None:
    ids = [store.enqueue("k", task_id=f"t{i:03}").task.id for i in range(60)]

    def drain(s: TaskStore, i: int) -> list[str]:
        mine = []
        while (t := s.claim(f"w{i}")) is not None:
            mine.append(t.id)
        return mine

    per_worker = run_in_threads(8, db_path, drain, clock=clock, lease_seconds=30.0)
    claimed = [tid for mine in per_worker for tid in mine]
    assert sorted(claimed) == sorted(ids)  # none lost, none duplicated
    assert len(set(claimed)) == len(claimed)
    assert all(store.get_task(t).attempt == 1 for t in ids)
    assert store.stats()["tasks"]["leased"] == 60
