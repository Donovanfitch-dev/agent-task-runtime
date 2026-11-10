"""Simulated executors. No network, no model calls: everything is deterministic."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from typing import Any

from .errors import PermanentError, RetryableError
from .worker import Executor, TaskContext

DEFAULT_STEPS: tuple[str, ...] = ("plan", "gather", "analyze", "draft", "review")


class SimulatedCrash(BaseException):
    """Raised by tests and the demo to emulate a hard process crash.

    It derives from ``BaseException`` so that the worker's ``except Exception``
    handlers do not catch it: the task is left leased and un-failed, just as if
    the process had been killed mid-step.
    """


def _simulate_step(step: str, topic: str) -> dict[str, str]:
    digest = hashlib.sha256(f"{step}:{topic}".encode()).hexdigest()[:8]
    return {"step": step, "note": f"simulated '{step}' output for '{topic}'", "digest": digest}


class SimulatedAgentExecutor:
    """A multi-step "agent" job that checkpoints after every step.

    On a retried attempt, steps that already have a checkpoint are skipped, so
    the job resumes where the previous attempt stopped. ``on_step`` is a hook
    called just before each step's work; tests and the demo use it to inject a
    crash. ``sleep`` is injected (default: do nothing) so "work" costs no time.
    """

    def __init__(
        self,
        steps: tuple[str, ...] = DEFAULT_STEPS,
        *,
        step_seconds: float = 0.0,
        sleep: Callable[[float], None] | None = None,
        on_step: Callable[[TaskContext, str], None] | None = None,
    ) -> None:
        self.steps = steps
        self.step_seconds = step_seconds
        self.sleep = sleep
        self.on_step = on_step

    def execute(self, ctx: TaskContext) -> dict[str, Any]:
        topic = str(ctx.task.payload.get("topic", "untitled"))
        resumed = [s for s in self.steps if s in ctx.checkpoints]
        ran: list[str] = []
        for step in self.steps:
            if step in ctx.checkpoints:
                continue
            ctx.raise_if_stopping()
            if self.on_step is not None:
                self.on_step(ctx, step)
            if self.sleep is not None and self.step_seconds:
                self.sleep(self.step_seconds)
            ctx.checkpoint(step, _simulate_step(step, topic))
            ctx.heartbeat()
            ran.append(step)
        digests = [ctx.checkpoints[s]["digest"] for s in self.steps]
        return {"topic": topic, "ran": ran, "resumed": resumed, "digests": digests}


class FlakyExecutor:
    """Fails with a retryable error for the first ``fail_times`` attempts, then succeeds."""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times

    def execute(self, ctx: TaskContext) -> dict[str, Any]:
        if ctx.task.attempt <= self.fail_times:
            raise RetryableError(f"simulated transient failure on attempt {ctx.task.attempt}")
        return {"recovered_on_attempt": ctx.task.attempt}


class AlwaysFailingExecutor:
    """Always fails. ``retryable`` selects retry-until-dead-letter or immediate failure."""

    def __init__(self, retryable: bool = True) -> None:
        self.retryable = retryable

    def execute(self, ctx: TaskContext) -> Any:
        if self.retryable:
            raise RetryableError("simulated persistent failure")
        raise PermanentError("simulated permanent failure")


class RoutingExecutor:
    """Dispatches to a per-``kind`` executor; unknown kinds fail permanently."""

    def __init__(self, routes: Mapping[str, Executor]) -> None:
        self.routes = dict(routes)

    def execute(self, ctx: TaskContext) -> Any:
        executor = self.routes.get(ctx.task.kind)
        if executor is None:
            raise PermanentError(f"no executor registered for kind {ctx.task.kind!r}")
        return executor.execute(ctx)
