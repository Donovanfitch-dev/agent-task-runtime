from __future__ import annotations

from collections.abc import Callable
from typing import Any

from agent_task_runtime import (
    AlwaysFailingExecutor,
    FlakyExecutor,
    ManualClock,
    PermanentError,
    RoutingExecutor,
    SimulatedAgentExecutor,
    TaskContext,
    TaskState,
    TaskStore,
    Worker,
)


class RecordingExecutor:
    def __init__(self, result: Any = "ok") -> None:
        self.result = result
        self.seen: list[str] = []

    def execute(self, ctx: TaskContext) -> Any:
        self.seen.append(ctx.task.id)
        return self.result


def test_success_path(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    worker = Worker(store, RecordingExecutor({"v": 1}), worker_id="w")
    result = worker.run_once()
    assert result is not None and result.outcome == "succeeded"
    assert store.get_task("t").result == {"v": 1}
    assert worker.run_once() is None


def test_run_until_idle_processes_in_priority_order(store: TaskStore) -> None:
    store.enqueue("k", task_id="low", priority=0)
    store.enqueue("k", task_id="high", priority=9)
    executor = RecordingExecutor()
    results = Worker(store, executor, worker_id="w").run_until_idle()
    assert executor.seen == ["high", "low"] and len(results) == 2


def test_transient_failures_are_retried_with_backoff_until_success(
    store: TaskStore, clock: ManualClock
) -> None:
    store.enqueue("k", task_id="t")
    worker = Worker(store, FlakyExecutor(fail_times=2), worker_id="w")

    first = worker.run_once()
    assert first is not None and first.outcome == "retry_scheduled"
    assert "RetryableError" in (first.error or "")
    assert worker.run_once() is None  # backing off (1s)
    clock.advance(1)
    second = worker.run_once()
    assert second is not None and second.outcome == "retry_scheduled"
    clock.advance(1.9)
    assert worker.run_once() is None  # backing off (2s)
    clock.advance(0.1)
    third = worker.run_once()
    assert third is not None and third.outcome == "succeeded"
    assert store.get_task("t").result == {"recovered_on_attempt": 3}


def test_permanent_errors_are_not_retried(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    result = Worker(store, AlwaysFailingExecutor(retryable=False), worker_id="w").run_once()
    assert result is not None and result.outcome == "failed"
    assert store.get_task("t").state is TaskState.FAILED


def test_persistent_retryable_errors_end_in_the_dead_letter_state(
    store: TaskStore, clock: ManualClock
) -> None:
    store.enqueue("k", task_id="t", max_attempts=3)
    worker = Worker(store, AlwaysFailingExecutor(), worker_id="w")
    outcomes = []
    for _ in range(3):
        clock.advance(100)
        result = worker.run_once()
        assert result is not None
        outcomes.append(result.outcome)
    assert outcomes == ["retry_scheduled", "retry_scheduled", "dead_lettered"]
    assert store.get_task("t").attempt == 3


def test_routing_executor_dispatches_by_kind_and_rejects_unknown_kinds(store: TaskStore) -> None:
    store.enqueue("known", task_id="a")
    store.enqueue("mystery", task_id="b")
    executor = RoutingExecutor({"known": RecordingExecutor("A")})
    Worker(store, executor, worker_id="w").run_until_idle()
    assert store.get_task("a").state is TaskState.SUCCEEDED
    assert store.get_task("b").state is TaskState.FAILED
    assert "no executor registered" in (store.get_task("b").last_error or "")


def test_worker_only_takes_the_kinds_it_is_told_to(store: TaskStore) -> None:
    store.enqueue("a", task_id="ta")
    store.enqueue("b", task_id="tb")
    Worker(store, RecordingExecutor(), worker_id="w", kinds=["b"]).run_until_idle()
    assert store.get_task("ta").state is TaskState.QUEUED
    assert store.get_task("tb").state is TaskState.SUCCEEDED


def test_worker_reaps_expired_leases_before_claiming(store: TaskStore, clock: ManualClock) -> None:
    store.enqueue("k", task_id="t")
    assert store.claim("dead-worker") is not None
    clock.advance(31)
    result = Worker(store, RecordingExecutor(), worker_id="w").run_once()
    assert result is not None and result.outcome == "succeeded"
    assert store.get_task("t").attempt == 2


def test_graceful_shutdown_releases_the_in_flight_task_and_stops(store: TaskStore) -> None:
    store.enqueue("agent", {"topic": "x"}, task_id="t1")
    store.enqueue("agent", {"topic": "y"}, task_id="t2")
    holder: dict[str, Worker] = {}

    def stop_during_second_step(ctx: TaskContext, step: str) -> None:
        if step == "gather":
            holder["w"].stop()  # e.g. a SIGTERM arrives mid-task

    worker = Worker(store, SimulatedAgentExecutor(on_step=stop_during_second_step), worker_id="w")
    holder["w"] = worker
    results = worker.run_until_idle()

    assert [r.outcome for r in results] == ["released"]  # loop stopped; t2 never claimed
    t1 = store.get_task("t1")
    assert t1.state is TaskState.QUEUED and t1.lease_owner is None
    assert list(store.load_checkpoints("t1")) == ["plan", "gather"]  # progress kept
    assert store.get_task("t2").state is TaskState.QUEUED
    assert [e.event_type for e in store.list_events("t1")][-1] == "released"

    # A fresh worker resumes the released task from its checkpoints.
    resumed = Worker(store, SimulatedAgentExecutor(), worker_id="w2").run_once()
    assert resumed is not None and resumed.task is not None
    assert resumed.task.result["resumed"] == ["plan", "gather"]


def test_executor_that_ignores_stop_finishes_its_task_then_the_loop_exits(
    store: TaskStore,
) -> None:
    store.enqueue("k", task_id="t1")
    store.enqueue("k", task_id="t2")
    holder: dict[str, Worker] = {}

    class StopsButFinishes:
        def execute(self, ctx: TaskContext) -> str:
            holder["w"].stop()
            return "finished anyway"

    holder["w"] = Worker(store, StopsButFinishes(), worker_id="w")
    results = holder["w"].run_until_idle()
    assert [r.outcome for r in results] == ["succeeded"]
    assert store.get_task("t2").state is TaskState.QUEUED


def test_run_forever_exits_when_stopped(store: TaskStore) -> None:
    store.enqueue("k", task_id="t")
    holder: dict[str, Worker] = {}

    class StopAfterThis:
        def execute(self, ctx: TaskContext) -> str:
            holder["w"].stop()
            return "done"

    holder["w"] = Worker(store, StopAfterThis(), worker_id="w")
    holder["w"].run_forever(poll_interval=60)  # would hang if stop were ignored
    assert store.get_task("t").state is TaskState.SUCCEEDED

    pre_stopped = Worker(store, RecordingExecutor(), worker_id="w2")
    pre_stopped.stop()
    pre_stopped.run_forever(poll_interval=60)  # returns immediately


def test_worker_whose_lease_was_stolen_reports_lease_lost_and_writes_nothing(
    make_store: Callable[..., TaskStore], clock: ManualClock
) -> None:
    store_a = make_store()
    store_b = make_store()  # second connection = second worker process
    store_a.enqueue("k", task_id="t")
    stolen: dict[str, Any] = {}

    class SlowThenStolen:
        def execute(self, ctx: TaskContext) -> str:
            clock.advance(31)  # A stalls past its lease...
            store_b.reap_expired()  # ...the reaper requeues it...
            stolen["task"] = store_b.claim("worker-B")  # ...and B takes over.
            return "A's late result"

    result = Worker(store_a, SlowThenStolen(), worker_id="worker-A", reap=False).run_once()
    assert result is not None and result.outcome == "lease_lost"
    task = store_a.get_task("t")
    assert task.state is TaskState.LEASED and task.lease_owner == "worker-B"
    assert task.result is None
    assert store_a.list_outbox("t") == []


def test_permanent_error_type_is_part_of_the_public_api() -> None:
    assert issubclass(PermanentError, Exception)
