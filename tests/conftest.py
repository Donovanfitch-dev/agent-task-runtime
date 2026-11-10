from __future__ import annotations

import contextlib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from agent_task_runtime import ManualClock, TaskStore

from .helpers import FixedRng


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(1000.0)


@pytest.fixture
def rng() -> FixedRng:
    return FixedRng(0.0)


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "tasks.db"


@pytest.fixture
def make_store(
    db_path: Path, clock: ManualClock, rng: FixedRng
) -> Iterator[Callable[..., TaskStore]]:
    """Factory for stores on the shared test database; all are closed at teardown."""
    created: list[TaskStore] = []

    def factory(**overrides: Any) -> TaskStore:
        kwargs: dict[str, Any] = {"clock": clock, "rng": rng, "lease_seconds": 30.0}
        kwargs.update(overrides)
        store = TaskStore(db_path, **kwargs)
        created.append(store)
        return store

    yield factory
    for store in created:
        with contextlib.suppress(Exception):  # the test may have closed it already
            store.close()


@pytest.fixture
def store(make_store: Callable[..., TaskStore]) -> TaskStore:
    return make_store()
