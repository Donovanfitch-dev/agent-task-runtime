"""Worker loop and the pluggable ``Executor`` protocol."""

from __future__ import annotations

import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .errors import PermanentError, ShutdownRequested, StaleLeaseError
from .models import Task
from .store import TaskStore


class TaskContext:
    """What an executor sees: the task, its resumable checkpoints, and fenced helpers."""

    def __init__(self, store: TaskStore, task: Task, worker_id: str, stop: threading.Event) -> None:
        self._store = store
        self._stop = stop
        self.task = task
        self.worker_id = worker_id
        self.fencing_token = task.fencing_token
        #: Checkpoints from earlier attempts (and this one, as they are saved).
        self.checkpoints: dict[str, Any] = store.load_checkpoints(task.id)

    def checkpoint(self, step: str, data: Any) -> None:
        """Durably record a finished step. Raises ``StaleLeaseError`` for a zombie."""
        self._store.save_checkpoint(self.task.id, self.fencing_token, step, data)
        self.checkpoints[step] = data

    def heartbeat(self) -> None:
        """Renew the lease. Raises ``StaleLeaseError`` / ``LeaseExpiredError`` if lost."""
        self._store.heartbeat(self.task.id, self.fencing_token)

    @property
    def stop_requested(self) -> bool:
        return self._stop.is_set()

    def raise_if_stopping(self) -> None:
        """Cooperative shutdown point: hand the task back if the worker is stopping."""
        if self._stop.is_set():
            raise ShutdownRequested("worker is shutting down")


class Executor(Protocol):
    """Runs one attempt of a task. Return a JSON-serialisable result, or raise.

    * ``PermanentError``     -> task fails without retry
    * ``ShutdownRequested``  -> task is released back to the queue
    * any other ``Exception`` (including ``RetryableError``) -> retried with backoff
    """

    def execute(self, ctx: TaskContext) -> Any: ...


@dataclass(frozen=True)
class RunResult:
    task_id: str
    #: succeeded | retry_scheduled | failed | dead_lettered | released | lease_lost
    outcome: str
    task: Task | None = None
    error: str | None = None


class Worker:
    """Claims tasks one at a time and runs them through an executor."""

    def __init__(
        self,
        store: TaskStore,
        executor: Executor,
        *,
        worker_id: str,
        lease_seconds: float | None = None,
        kinds: Sequence[str] | None = None,
        reap: bool = True,
        on_claim: Callable[[Task], None] | None = None,
    ) -> None:
        self.store = store
        self.executor = executor
        self.worker_id = worker_id
        self.lease_seconds = lease_seconds
        self.kinds = kinds
        self.reap = reap
        self.on_claim = on_claim
        self._stop = threading.Event()

    def stop(self) -> None:
        """Request graceful shutdown: finish (or release) the current task, then exit."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def run_once(self) -> RunResult | None:
        """Reap expired leases, claim one task and run it. ``None`` if nothing was eligible.

        A ``BaseException`` that is not an ``Exception`` (for example the demo's
        ``SimulatedCrash``, or ``KeyboardInterrupt``) is deliberately not
        caught: the task stays leased, exactly as if the process had died, and
        the reaper recovers it after the lease expires.
        """
        if self.reap:
            self.store.reap_expired(actor=f"reaper@{self.worker_id}")
        task = self.store.claim(self.worker_id, lease_seconds=self.lease_seconds, kinds=self.kinds)
        if task is None:
            return None
        if self.on_claim is not None:
            self.on_claim(task)
        token = task.fencing_token
        ctx = TaskContext(self.store, task, self.worker_id, self._stop)
        try:
            result = self.executor.execute(ctx)
            final = self.store.complete(task.id, token, result)
            return RunResult(task.id, "succeeded", final)
        except StaleLeaseError as exc:
            # We lost the lease while working. The new owner is responsible now.
            return RunResult(task.id, "lease_lost", None, str(exc))
        except ShutdownRequested as exc:
            reason = str(exc)
            return self._finish(task, lambda: self.store.release(task.id, token, reason), None)
        except PermanentError as exc:
            msg = f"{type(exc).__name__}: {exc}"
            return self._finish(
                task, lambda: self.store.fail(task.id, token, msg, retryable=False), msg
            )
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            return self._finish(task, lambda: self.store.fail(task.id, token, msg), msg)

    def _finish(self, task: Task, action: Callable[[], Task], error: str | None) -> RunResult:
        try:
            final = action()
        except StaleLeaseError as exc:
            return RunResult(task.id, "lease_lost", None, str(exc))
        outcome = {
            "queued": "retry_scheduled" if error else "released",
            "failed": "failed",
            "dead_lettered": "dead_lettered",
        }[final.state.value]
        return RunResult(task.id, outcome, final, error)

    def run_until_idle(self, max_tasks: int | None = None) -> list[RunResult]:
        """Run until no eligible task remains (or ``max_tasks`` ran, or stop was requested)."""
        results: list[RunResult] = []
        while not self._stop.is_set() and (max_tasks is None or len(results) < max_tasks):
            result = self.run_once()
            if result is None:
                break
            results.append(result)
        return results

    def run_forever(self, poll_interval: float = 1.0) -> None:
        """Poll until :meth:`stop` is called. Idle waits are interruptible by ``stop``."""
        while not self._stop.is_set():
            if self.run_once() is None:
                self._stop.wait(poll_interval)
