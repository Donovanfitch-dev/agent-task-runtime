"""Exception types raised by the runtime."""

from __future__ import annotations


class AgentTaskRuntimeError(Exception):
    """Base class for every error raised intentionally by this package."""


class TaskNotFoundError(AgentTaskRuntimeError):
    """The referenced task id does not exist."""


class IllegalTransitionError(AgentTaskRuntimeError):
    """The requested state change is not permitted by the task state machine."""


class StaleLeaseError(AgentTaskRuntimeError):
    """A fenced operation was attempted with a token that is no longer current.

    This is the "zombie worker" signal: the caller once held a lease, but the
    task has since been re-claimed (higher token), finished, or cancelled.
    Raising this error guarantees that nothing was written.
    """

    def __init__(
        self,
        task_id: str,
        presented_token: int,
        current_token: int,
        current_state: str,
        message: str | None = None,
    ) -> None:
        self.task_id = task_id
        self.presented_token = presented_token
        self.current_token = current_token
        self.current_state = current_state
        super().__init__(
            message
            or (
                f"stale lease on {task_id}: presented fencing token {presented_token}, "
                f"current token is {current_token} and task state is {current_state}"
            )
        )


class LeaseExpiredError(StaleLeaseError):
    """The token is still current, but the lease deadline has passed (heartbeat only)."""


class IdempotencyConflictError(AgentTaskRuntimeError):
    """An idempotency key was reused with a different kind or payload."""


class MigrationError(AgentTaskRuntimeError):
    """Schema migration failed or the database is newer than this code."""


class ExecutorError(AgentTaskRuntimeError):
    """Base class for errors an executor raises to classify a failure."""


class RetryableError(ExecutorError):
    """The attempt failed but the task may be retried."""


class PermanentError(ExecutorError):
    """The attempt failed and retrying cannot help; do not retry."""


class ShutdownRequested(ExecutorError):
    """Raised by an executor to hand the task back because the worker is stopping."""
