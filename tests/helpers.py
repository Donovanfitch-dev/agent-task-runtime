from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from agent_task_runtime import TaskStore

T = TypeVar("T")


class FixedRng:
    """A 'random' source that always returns the same value (default 0.0 = no jitter)."""

    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def random(self) -> float:
        return self.value


def snapshot(store: TaskStore, task_id: str) -> tuple[Any, ...]:
    """Everything observable about a task, for 'this operation changed nothing' assertions."""
    return (
        store.get_task(task_id),
        store.list_events(task_id),
        store.load_checkpoints(task_id),
        store.list_outbox(task_id),
    )


def run_in_threads(
    n: int, db_path: Path, fn: Callable[[TaskStore, int], T], **store_kwargs: Any
) -> list[T]:
    """Run ``fn(store, index)`` in ``n`` threads, each with its own connection.

    All threads start together (barrier). Exceptions in any thread are re-raised.
    """
    barrier = threading.Barrier(n)
    results: list[Any] = [None] * n
    errors: list[BaseException] = []

    def target(i: int) -> None:
        try:
            with TaskStore(db_path, **store_kwargs) as s:
                barrier.wait(timeout=30)
                results[i] = fn(s, i)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=target, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    if errors:
        raise errors[0]
    return results
