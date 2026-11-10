"""agent-task-runtime: a crash-safe, lease-based task runtime for long-running agent jobs.

An independent reference implementation, written from scratch for
demonstration; not extracted from any production system.
"""

from .clock import Clock, ManualClock, SystemClock
from .errors import (
    AgentTaskRuntimeError,
    DuplicateTaskIdError,
    IdempotencyConflictError,
    IllegalTransitionError,
    LeaseExpiredError,
    MigrationError,
    PermanentError,
    RetryableError,
    ShutdownRequested,
    StaleLeaseError,
    TaskNotFoundError,
)
from .executors import (
    AlwaysFailingExecutor,
    FlakyExecutor,
    RoutingExecutor,
    SimulatedAgentExecutor,
    SimulatedCrash,
)
from .models import EnqueueResult, OutboxEvent, Task, TaskEvent, TaskState
from .outbox import OutboxDispatcher
from .retry import Backoff, RetryPolicy
from .sinks import DedupSink, FlakySink, ListSink, LostAckSink, Sink
from .store import TaskStore
from .worker import Executor, RunResult, TaskContext, Worker

__version__ = "0.1.0"

__all__ = [
    "AgentTaskRuntimeError",
    "AlwaysFailingExecutor",
    "Backoff",
    "Clock",
    "DedupSink",
    "DuplicateTaskIdError",
    "EnqueueResult",
    "Executor",
    "FlakyExecutor",
    "FlakySink",
    "IdempotencyConflictError",
    "IllegalTransitionError",
    "LeaseExpiredError",
    "ListSink",
    "LostAckSink",
    "ManualClock",
    "MigrationError",
    "OutboxDispatcher",
    "OutboxEvent",
    "PermanentError",
    "RetryPolicy",
    "RetryableError",
    "RoutingExecutor",
    "RunResult",
    "ShutdownRequested",
    "SimulatedAgentExecutor",
    "SimulatedCrash",
    "Sink",
    "StaleLeaseError",
    "SystemClock",
    "Task",
    "TaskContext",
    "TaskEvent",
    "TaskNotFoundError",
    "TaskState",
    "TaskStore",
    "Worker",
    "__version__",
]
