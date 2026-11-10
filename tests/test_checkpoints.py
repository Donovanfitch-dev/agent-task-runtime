from __future__ import annotations

from collections.abc import Callable

import pytest

from agent_task_runtime import (
    ManualClock,
    SimulatedAgentExecutor,
    SimulatedCrash,
    StaleLeaseError,
    TaskContext,
    TaskState,
    TaskStore,
    Worker,
)


def test_checkpoints_are_saved_in_order_and_overwritten_per_step(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    leased = store.claim("w")
    assert leased is not None
    token = leased.fencing_token
    store.save_checkpoint("t", token, "a", 1)
    store.save_checkpoint("t", token, "b", 2)
    store.save_checkpoint("t", token, "a", 10)
    loaded = store.load_checkpoints("t")
    assert loaded == {"a": 10, "b": 2}
    assert list(loaded) == ["a", "b"]


def test_checkpoint_requires_a_current_lease(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    with pytest.raises(StaleLeaseError):
        store.save_checkpoint("t", 1, "a", 1)  # never claimed
    assert store.load_checkpoints("t") == {}


def test_retried_attempt_resumes_from_checkpoints_instead_of_restarting(
    make_store: Callable[..., TaskStore], clock: ManualClock
) -> None:
    store = make_store()
    store.enqueue("agent", {"topic": "t"}, task_id="job")
    started: list[tuple[int, str]] = []

    def crash_on_first_attempt(ctx: TaskContext, step: str) -> None:
        started.append((ctx.task.attempt, step))
        if ctx.task.attempt == 1 and step == "analyze":
            raise SimulatedCrash

    executor = SimulatedAgentExecutor(on_step=crash_on_first_attempt)
    with pytest.raises(SimulatedCrash):
        Worker(store, executor, worker_id="A").run_once()

    # Hard crash: still leased, completed steps are durable.
    assert store.get_task("job").state is TaskState.LEASED
    assert list(store.load_checkpoints("job")) == ["plan", "gather"]

    clock.advance(31)
    result = Worker(store, executor, worker_id="B").run_once()

    assert result is not None and result.outcome == "succeeded"
    assert result.task is not None
    assert result.task.attempt == 2
    assert result.task.result["resumed"] == ["plan", "gather"]
    assert result.task.result["ran"] == ["analyze", "draft", "review"]
    # 'plan' and 'gather' were started exactly once across both attempts.
    assert started.count((1, "plan")) == 1 and started.count((2, "plan")) == 0
    assert [s for a, s in started if a == 2] == ["analyze", "draft", "review"]


def test_checkpoints_survive_a_process_restart(
    make_store: Callable[..., TaskStore], db_path
) -> None:
    first = make_store()
    first.enqueue("k", task_id="t")
    leased = first.claim("w")
    assert leased is not None
    first.save_checkpoint("t", leased.fencing_token, "plan", {"x": [1, 2]})
    first.close()

    reopened = make_store()
    assert reopened.load_checkpoints("t") == {"plan": {"x": [1, 2]}}


def test_simulated_executor_is_deterministic(make_store: Callable[..., TaskStore]) -> None:
    store = make_store()
    for i in range(2):
        store.enqueue("agent", {"topic": "same"}, task_id=f"t{i}")
    results = Worker(store, SimulatedAgentExecutor(), worker_id="w").run_until_idle()
    digests = [r.task.result["digests"] for r in results if r.task]
    assert len(digests) == 2 and digests[0] == digests[1]
