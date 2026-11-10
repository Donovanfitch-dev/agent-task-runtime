"""The task store: every state transition, as one SQLite transaction each.

Concurrency model
-----------------
A ``TaskStore`` owns one SQLite connection and must be used from the thread
that created it (``sqlite3`` enforces this). To run many concurrent workers,
give each its own ``TaskStore`` on the same database file. Every mutating
operation runs inside ``BEGIN IMMEDIATE`` so SQLite's single-writer lock
serialises them: a claim's SELECT and UPDATE can never interleave with another
claim, and a state transition is always written together with its audit-log
row (and, for terminal states, its outbox row) or not at all.
"""

from __future__ import annotations

import json
import random
import sqlite3
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from os import PathLike
from typing import Any

from .clock import Clock, SystemClock
from .errors import (
    IdempotencyConflictError,
    LeaseExpiredError,
    StaleLeaseError,
    TaskNotFoundError,
)
from .migrations import migrate
from .models import (
    EnqueueResult,
    OutboxEvent,
    Task,
    TaskEvent,
    TaskState,
    check_transition,
)
from .retry import RandomSource, RetryPolicy

SYSTEM_ACTOR = "system"


def _dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _task_from_row(row: sqlite3.Row) -> Task:
    return Task(
        seq=row["seq"],
        id=row["id"],
        kind=row["kind"],
        payload=json.loads(row["payload"]),
        state=TaskState(row["state"]),
        priority=row["priority"],
        idempotency_key=row["idempotency_key"],
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        not_before=row["not_before"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        last_error=row["last_error"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _outbox_from_row(row: sqlite3.Row) -> OutboxEvent:
    return OutboxEvent(
        id=row["id"],
        task_id=row["task_id"],
        event_type=row["event_type"],
        payload=json.loads(row["payload"]),
        idempotency_key=row["idempotency_key"],
        status=row["status"],
        attempts=row["attempts"],
        next_attempt_at=row["next_attempt_at"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        delivered_at=row["delivered_at"],
    )


class TaskStore:
    """Durable task state, leases, checkpoints, audit log and outbox."""

    def __init__(
        self,
        path: str | PathLike[str],
        *,
        clock: Clock | None = None,
        rng: RandomSource | None = None,
        retry_policy: RetryPolicy | None = None,
        lease_seconds: float = 30.0,
        busy_timeout: float = 30.0,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        self.clock: Clock = clock or SystemClock()
        self.rng: RandomSource = rng or random.Random()
        self.retry_policy = retry_policy or RetryPolicy()
        self.lease_seconds = lease_seconds
        self._conn = sqlite3.connect(str(path), isolation_level=None, timeout=busy_timeout)
        self._conn.row_factory = sqlite3.Row
        self._enable_wal()
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        migrate(self._conn, self.clock.now)

    # ------------------------------------------------------------------ plumbing

    def _enable_wal(self) -> None:
        """Switch the file to WAL mode (persistent once set).

        The very first switch needs an exclusive lock and SQLite does not always
        apply the busy timeout to it, so concurrent first-time openers can see
        "database is locked". Retry briefly; later opens are a no-op.
        """
        for attempt in range(100):
            try:
                self._conn.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or attempt == 99:
                    raise
                time.sleep(0.05)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> TaskStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """One write transaction. Takes the write lock up front (BEGIN IMMEDIATE)."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def _load(self, task_id: str) -> Task:
        row = self._conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFoundError(f"no such task: {task_id}")
        return _task_from_row(row)

    def _log(
        self,
        task_id: str,
        now: float,
        actor: str,
        event_type: str,
        from_state: TaskState | None,
        to_state: TaskState,
        attempt: int,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_events"
            " (task_id, at, actor, event_type, from_state, to_state, attempt, detail)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                task_id,
                now,
                actor,
                event_type,
                from_state.value if from_state else None,
                to_state.value,
                attempt,
                _dumps(detail or {}),
            ),
        )

    def _emit_outbox(self, task: Task, now: float, event_type: str, body: dict[str, Any]) -> None:
        """Write an outbox row. Called inside the same transaction as the transition."""
        payload = {"task_id": task.id, "kind": task.kind, "attempt": task.attempt, **body}
        self._conn.execute(
            "INSERT INTO outbox"
            " (task_id, event_type, payload, idempotency_key, next_attempt_at, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (task.id, event_type, _dumps(payload), f"{task.id}:{event_type}", now, now),
        )

    def _require_lease(self, task_id: str, token: int) -> Task:
        """Load a task and verify the caller still holds its current lease."""
        task = self._load(task_id)
        if task.state is not TaskState.LEASED or task.attempt != token:
            raise StaleLeaseError(task_id, token, task.attempt, task.state.value)
        return task

    # ------------------------------------------------------------------ enqueue

    def enqueue(
        self,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 0,
        idempotency_key: str | None = None,
        max_attempts: int | None = None,
        not_before: float | None = None,
        task_id: str | None = None,
        actor: str = "client",
    ) -> EnqueueResult:
        """Create a task, or return the existing one for a repeated idempotency key.

        Reusing a key with a different ``kind`` or ``payload`` raises
        :class:`IdempotencyConflictError` rather than silently returning a task
        that does not match what the caller asked for.
        """
        payload = payload if payload is not None else {}
        attempts_cap = max_attempts if max_attempts is not None else self.retry_policy.max_attempts
        if attempts_cap < 1:
            raise ValueError("max_attempts must be >= 1")
        with self._tx() as conn:
            now = self.clock.now()
            if idempotency_key is not None:
                row = conn.execute(
                    "SELECT * FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row is not None:
                    existing = _task_from_row(row)
                    if existing.kind != kind or _dumps(existing.payload) != _dumps(payload):
                        raise IdempotencyConflictError(
                            f"idempotency key {idempotency_key!r} was already used "
                            "with a different kind or payload"
                        )
                    return EnqueueResult(existing, created=False)
            new_id = task_id or uuid.uuid4().hex
            conn.execute(
                "INSERT INTO tasks (id, kind, payload, state, priority, idempotency_key,"
                " max_attempts, not_before, created_at, updated_at)"
                " VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)",
                (
                    new_id,
                    kind,
                    _dumps(payload),
                    priority,
                    idempotency_key,
                    attempts_cap,
                    now if not_before is None else not_before,
                    now,
                    now,
                ),
            )
            self._log(new_id, now, actor, "enqueued", None, TaskState.QUEUED, 0)
            return EnqueueResult(self._load(new_id), created=True)

    # -------------------------------------------------------------------- claim

    def claim(
        self,
        worker_id: str,
        *,
        lease_seconds: float | None = None,
        kinds: Sequence[str] | None = None,
    ) -> Task | None:
        """Atomically lease the next eligible task, or return ``None``.

        Eligible means ``queued`` and ``not_before <= now``. Order is highest
        ``priority`` first, then oldest enqueue (``seq``) first. The claim
        increments ``attempt``, which doubles as the fencing token.
        """
        duration = lease_seconds if lease_seconds is not None else self.lease_seconds
        if duration <= 0:
            raise ValueError("lease_seconds must be positive")
        sql = "SELECT * FROM tasks WHERE state = 'queued' AND not_before <= ?"
        params: list[Any] = []
        with self._tx() as conn:
            now = self.clock.now()
            params.append(now)
            if kinds:
                sql += f" AND kind IN ({','.join('?' * len(kinds))})"
                params.extend(kinds)
            sql += " ORDER BY priority DESC, seq ASC LIMIT 1"
            row = conn.execute(sql, params).fetchone()
            if row is None:
                return None
            task = _task_from_row(row)
            check_transition(task.id, task.state, TaskState.LEASED)
            token = task.attempt + 1
            conn.execute(
                "UPDATE tasks SET state = 'leased', attempt = ?, lease_owner = ?,"
                " lease_expires_at = ?, updated_at = ? WHERE id = ?",
                (token, worker_id, now + duration, now, task.id),
            )
            self._log(
                task.id,
                now,
                worker_id,
                "claimed",
                TaskState.QUEUED,
                TaskState.LEASED,
                token,
                {"lease_expires_at": now + duration},
            )
            return self._load(task.id)

    # ---------------------------------------------------------------- heartbeat

    def heartbeat(self, task_id: str, token: int, *, lease_seconds: float | None = None) -> Task:
        """Renew a lease. Rejected for stale tokens and for already-expired leases."""
        duration = lease_seconds if lease_seconds is not None else self.lease_seconds
        with self._tx() as conn:
            now = self.clock.now()
            task = self._require_lease(task_id, token)
            if task.lease_expires_at is not None and task.lease_expires_at <= now:
                raise LeaseExpiredError(
                    task_id,
                    token,
                    task.attempt,
                    task.state.value,
                    f"lease on {task_id} expired at {task.lease_expires_at} (now {now}); "
                    "token is still current but the worker must stop",
                )
            conn.execute(
                "UPDATE tasks SET lease_expires_at = ?, updated_at = ? WHERE id = ?",
                (now + duration, now, task_id),
            )
            return self._load(task_id)

    # -------------------------------------------------------------- checkpoints

    def save_checkpoint(self, task_id: str, token: int, step: str, data: Any) -> None:
        """Persist the output of one step. Fenced: a zombie cannot overwrite progress."""
        with self._tx() as conn:
            now = self.clock.now()
            self._require_lease(task_id, token)
            conn.execute(
                "INSERT INTO checkpoints (task_id, step, attempt, data, created_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT (task_id, step) DO UPDATE SET"
                " attempt = excluded.attempt, data = excluded.data,"
                " created_at = excluded.created_at",
                (task_id, step, token, _dumps(data), now),
            )

    def load_checkpoints(self, task_id: str) -> dict[str, Any]:
        """Return ``{step: data}`` in the order the steps were first saved."""
        rows = self._conn.execute(
            "SELECT step, data FROM checkpoints WHERE task_id = ? ORDER BY rowid", (task_id,)
        ).fetchall()
        return {row["step"]: json.loads(row["data"]) for row in rows}

    # ------------------------------------------------------- finishing a task

    def complete(
        self, task_id: str, token: int, result: Any = None, *, actor: str | None = None
    ) -> Task:
        """Mark a leased task succeeded and write its outbox event, atomically.

        Accepted whenever the token is current and the task is still leased,
        even if the lease deadline has passed but nobody has reclaimed it yet:
        the work is done and no other worker holds the task.
        """
        with self._tx() as conn:
            now = self.clock.now()
            task = self._require_lease(task_id, token)
            check_transition(task_id, task.state, TaskState.SUCCEEDED)
            conn.execute(
                "UPDATE tasks SET state = 'succeeded', result = ?, lease_owner = NULL,"
                " lease_expires_at = NULL, last_error = NULL, updated_at = ? WHERE id = ?",
                (_dumps(result), now, task_id),
            )
            self._log(
                task_id,
                now,
                actor or task.lease_owner or SYSTEM_ACTOR,
                "succeeded",
                TaskState.LEASED,
                TaskState.SUCCEEDED,
                task.attempt,
            )
            final = self._load(task_id)
            self._emit_outbox(final, now, "task.succeeded", {"result": result})
            return final

    def fail(
        self,
        task_id: str,
        token: int,
        error: str,
        *,
        retryable: bool = True,
        actor: str | None = None,
    ) -> Task:
        """Record a failed attempt.

        * non-retryable            -> ``failed`` (terminal, outbox event)
        * retryable, attempts left -> ``queued`` with a backoff ``not_before``
        * retryable, exhausted     -> ``dead_lettered`` (terminal, outbox event)
        """
        with self._tx() as conn:
            now = self.clock.now()
            task = self._require_lease(task_id, token)
            who = actor or task.lease_owner or SYSTEM_ACTOR
            if not retryable:
                target, event_type = TaskState.FAILED, "failed"
            elif task.attempt >= task.max_attempts:
                target, event_type = TaskState.DEAD_LETTERED, "dead_lettered"
            else:
                target, event_type = TaskState.QUEUED, "retry_scheduled"
            check_transition(task_id, task.state, target)
            detail: dict[str, Any] = {"error": error, "retryable": retryable}
            not_before = task.not_before
            if target is TaskState.QUEUED:
                delay = self.retry_policy.delay(task.attempt, self.rng)
                not_before = now + delay
                detail["delay_seconds"] = delay
                detail["not_before"] = not_before
            conn.execute(
                "UPDATE tasks SET state = ?, not_before = ?, last_error = ?, lease_owner = NULL,"
                " lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                (target.value, not_before, error, now, task_id),
            )
            self._log(task_id, now, who, event_type, TaskState.LEASED, target, task.attempt, detail)
            final = self._load(task_id)
            if target is not TaskState.QUEUED:
                self._emit_outbox(final, now, f"task.{target.value}", {"error": error})
            return final

    def release(self, task_id: str, token: int, reason: str, *, actor: str | None = None) -> Task:
        """Hand a leased task back to the queue immediately, without backoff.

        Used for graceful shutdown. The attempt already counts toward
        ``max_attempts`` because the attempt number is the fencing token.
        """
        with self._tx() as conn:
            now = self.clock.now()
            task = self._require_lease(task_id, token)
            check_transition(task_id, task.state, TaskState.QUEUED)
            conn.execute(
                "UPDATE tasks SET state = 'queued', lease_owner = NULL, lease_expires_at = NULL,"
                " updated_at = ? WHERE id = ?",
                (now, task_id),
            )
            self._log(
                task_id,
                now,
                actor or task.lease_owner or SYSTEM_ACTOR,
                "released",
                TaskState.LEASED,
                TaskState.QUEUED,
                task.attempt,
                {"reason": reason},
            )
            return self._load(task_id)

    def cancel(self, task_id: str, *, actor: str = "client", reason: str = "") -> Task:
        """Cancel a queued or leased task. A leased worker's token goes stale."""
        with self._tx() as conn:
            now = self.clock.now()
            task = self._load(task_id)
            check_transition(task_id, task.state, TaskState.CANCELLED)
            conn.execute(
                "UPDATE tasks SET state = 'cancelled', lease_owner = NULL,"
                " lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                (now, task_id),
            )
            self._log(
                task_id,
                now,
                actor,
                "cancelled",
                task.state,
                TaskState.CANCELLED,
                task.attempt,
                {"reason": reason},
            )
            final = self._load(task_id)
            self._emit_outbox(final, now, "task.cancelled", {"reason": reason})
            return final

    # ------------------------------------------------------------------- reaper

    def reap_expired(self, *, actor: str = "reaper") -> list[Task]:
        """Reclaim every lease whose deadline has passed.

        The task returns to ``queued`` immediately (a crash is not the task's
        fault, so no backoff), or is dead-lettered if its attempts are used up.
        Returns the tasks as they are after reaping.
        """
        reclaimed: list[Task] = []
        with self._tx() as conn:
            now = self.clock.now()
            rows = conn.execute(
                "SELECT * FROM tasks WHERE state = 'leased' AND lease_expires_at <= ? ORDER BY seq",
                (now,),
            ).fetchall()
            for row in rows:
                task = _task_from_row(row)
                exhausted = task.attempt >= task.max_attempts
                target = TaskState.DEAD_LETTERED if exhausted else TaskState.QUEUED
                check_transition(task.id, task.state, target)
                message = f"lease held by {task.lease_owner} expired at {task.lease_expires_at}"
                conn.execute(
                    "UPDATE tasks SET state = ?, last_error = ?, lease_owner = NULL,"
                    " lease_expires_at = NULL, updated_at = ? WHERE id = ?",
                    (target.value, message, now, task.id),
                )
                self._log(
                    task.id,
                    now,
                    actor,
                    "dead_lettered" if exhausted else "lease_expired",
                    TaskState.LEASED,
                    target,
                    task.attempt,
                    {"previous_owner": task.lease_owner, "error": message},
                )
                final = self._load(task.id)
                if exhausted:
                    self._emit_outbox(final, now, "task.dead_lettered", {"error": message})
                reclaimed.append(final)
        return reclaimed

    # ------------------------------------------------------------------ queries

    def get_task(self, task_id: str) -> Task:
        return self._load(task_id)

    def list_tasks(self, state: TaskState | None = None) -> list[Task]:
        if state is None:
            rows = self._conn.execute("SELECT * FROM tasks ORDER BY seq").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM tasks WHERE state = ? ORDER BY seq", (state.value,)
            ).fetchall()
        return [_task_from_row(r) for r in rows]

    def list_events(self, task_id: str | None = None) -> list[TaskEvent]:
        if task_id is None:
            rows = self._conn.execute("SELECT * FROM task_events ORDER BY id").fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM task_events WHERE task_id = ? ORDER BY id", (task_id,)
            ).fetchall()
        return [
            TaskEvent(
                id=r["id"],
                task_id=r["task_id"],
                at=r["at"],
                actor=r["actor"],
                event_type=r["event_type"],
                from_state=r["from_state"],
                to_state=r["to_state"],
                attempt=r["attempt"],
                detail=json.loads(r["detail"]),
            )
            for r in rows
        ]

    def stats(self) -> dict[str, Any]:
        tasks = {state.value: 0 for state in TaskState}
        for row in self._conn.execute("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state"):
            tasks[row["state"]] = row["n"]
        outbox = {"pending": 0, "delivered": 0}
        for row in self._conn.execute("SELECT status, COUNT(*) AS n FROM outbox GROUP BY status"):
            outbox[row["status"]] = row["n"]
        events = self._conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
        return {"tasks": tasks, "outbox": outbox, "events": events}

    # ------------------------------------------------------------------- outbox

    def get_outbox_event(self, event_id: int) -> OutboxEvent:
        row = self._conn.execute("SELECT * FROM outbox WHERE id = ?", (event_id,)).fetchone()
        if row is None:
            raise TaskNotFoundError(f"no such outbox event: {event_id}")
        return _outbox_from_row(row)

    def list_outbox(
        self, task_id: str | None = None, status: str | None = None
    ) -> list[OutboxEvent]:
        sql, params = "SELECT * FROM outbox WHERE 1=1", []
        if task_id is not None:
            sql += " AND task_id = ?"
            params.append(task_id)
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        rows = self._conn.execute(sql + " ORDER BY id", params).fetchall()
        return [_outbox_from_row(r) for r in rows]

    def outbox_due(self, limit: int = 50) -> list[OutboxEvent]:
        """Pending events whose next attempt time has arrived, oldest first."""
        rows = self._conn.execute(
            "SELECT * FROM outbox WHERE status = 'pending' AND next_attempt_at <= ?"
            " ORDER BY id LIMIT ?",
            (self.clock.now(), limit),
        ).fetchall()
        return [_outbox_from_row(r) for r in rows]

    def outbox_mark_delivered(self, event_id: int) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE outbox SET status = 'delivered', attempts = attempts + 1,"
                " delivered_at = ?, last_error = NULL WHERE id = ?",
                (self.clock.now(), event_id),
            )

    def outbox_record_failure(self, event_id: int, error: str, retry_at: float) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE outbox SET attempts = attempts + 1, last_error = ?,"
                " next_attempt_at = ? WHERE id = ?",
                (error, retry_at, event_id),
            )
