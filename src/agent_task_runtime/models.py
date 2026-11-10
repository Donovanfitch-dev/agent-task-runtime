"""Domain types and the task state machine."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .errors import IllegalTransitionError


class TaskState(StrEnum):
    QUEUED = "queued"
    LEASED = "leased"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"
    CANCELLED = "cancelled"


TERMINAL_STATES: frozenset[TaskState] = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.DEAD_LETTERED, TaskState.CANCELLED}
)

#: The complete set of legal transitions. Every state change in the store is
#: validated against this table before it is written.
ALLOWED_TRANSITIONS: dict[TaskState, frozenset[TaskState]] = {
    TaskState.QUEUED: frozenset({TaskState.LEASED, TaskState.CANCELLED}),
    TaskState.LEASED: frozenset(
        {
            TaskState.SUCCEEDED,
            TaskState.FAILED,
            TaskState.DEAD_LETTERED,
            TaskState.QUEUED,  # retry, lease expiry, or voluntary release
            TaskState.CANCELLED,
        }
    ),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.FAILED: frozenset(),
    TaskState.DEAD_LETTERED: frozenset(),
    TaskState.CANCELLED: frozenset(),
}


def check_transition(task_id: str, src: TaskState, dst: TaskState) -> None:
    """Raise :class:`IllegalTransitionError` unless ``src -> dst`` is allowed."""
    if dst not in ALLOWED_TRANSITIONS[src]:
        raise IllegalTransitionError(
            f"illegal transition for {task_id}: {src.value} -> {dst.value}"
        )


@dataclass(frozen=True)
class Task:
    """An immutable snapshot of a task row."""

    seq: int
    id: str
    kind: str
    payload: dict[str, Any]
    state: TaskState
    priority: int
    idempotency_key: str | None
    attempt: int
    max_attempts: int
    not_before: float
    lease_owner: str | None
    lease_expires_at: float | None
    last_error: str | None
    result: Any
    created_at: float
    updated_at: float
    #: Duration of the current/last claim's lease, so renewals keep it.
    lease_seconds: float | None = None
    #: Graceful releases so far. They advance the token but not the retry budget.
    releases: int = 0

    @property
    def fencing_token(self) -> int:
        """The token a worker must present; equal to the claim count."""
        return self.attempt

    @property
    def attempts_used(self) -> int:
        """Retry budget consumed: claims that ended in failure or a lost lease.

        ``attempt`` counts every claim (it is the fencing token); a graceful
        release is a claim that did not use up the budget.
        """
        return self.attempt - self.releases


@dataclass(frozen=True)
class EnqueueResult:
    task: Task
    created: bool


@dataclass(frozen=True)
class TaskEvent:
    """One row of the append-only audit log."""

    id: int
    task_id: str
    at: float
    actor: str
    event_type: str
    from_state: str | None
    to_state: str
    attempt: int
    detail: dict[str, Any]


@dataclass(frozen=True)
class OutboxEvent:
    """A pending or delivered outbox row."""

    id: int
    task_id: str
    event_type: str
    payload: dict[str, Any]
    idempotency_key: str
    status: str
    attempts: int
    next_attempt_at: float
    last_error: str | None
    created_at: float
    delivered_at: float | None
