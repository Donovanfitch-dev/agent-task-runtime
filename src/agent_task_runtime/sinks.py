"""Outbox sinks: where delivered events go.

A sink is anything with ``deliver(event)``. It signals failure by raising.
Delivery is at-least-once, so a sink may see the same event more than once and
must be idempotent on ``event.idempotency_key``. :class:`DedupSink` shows how.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Protocol

from .models import OutboxEvent


class Sink(Protocol):
    def deliver(self, event: OutboxEvent) -> None: ...


class SinkUnavailableError(Exception):
    """Raised by simulated sinks to stand in for a downstream outage."""


class AckLostError(Exception):
    """The sink applied the event but the acknowledgement never reached the dispatcher."""


class ListSink:
    """Collects every delivery it receives, duplicates included."""

    def __init__(self) -> None:
        self.received: list[OutboxEvent] = []

    def deliver(self, event: OutboxEvent) -> None:
        self.received.append(event)


class DedupSink:
    """Applies each idempotency key at most once, recording keys in its own SQLite file.

    Two modes, with different guarantees:

    * ``transactional=False`` (default): ``handler(event)`` is called, then the
      key is recorded, inside one transaction. A handler that raises leaves no
      key, so the event is retried; a repeated key is acknowledged without
      re-running the handler. But the handler's *effect* is outside that
      transaction. If the process dies after the effect and before the commit,
      the effect happens again on redelivery. This mode narrows duplicates; it
      does not make an external effect effectively-once.
    * ``transactional=True``: ``handler(event, conn)`` receives this sink's
      connection while the transaction is open, so a handler that writes its
      effect to the same database commits the effect and the key atomically.
      A crash or error before commit rolls back both, and redelivery applies
      the effect exactly once. This is the effectively-once pattern, and it
      only holds for effects stored in that same database.
    """

    def __init__(
        self,
        handler: Callable[..., None],
        db_path: str = ":memory:",
        *,
        transactional: bool = False,
    ) -> None:
        self._handler = handler
        self._transactional = transactional
        self._conn = sqlite3.connect(db_path, isolation_level=None)
        self._conn.execute("CREATE TABLE IF NOT EXISTS processed (key TEXT PRIMARY KEY)")
        self.duplicates_ignored = 0

    def deliver(self, event: OutboxEvent) -> None:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            seen = self._conn.execute(
                "SELECT 1 FROM processed WHERE key = ?", (event.idempotency_key,)
            ).fetchone()
            if seen:
                self.duplicates_ignored += 1
            else:
                if self._transactional:
                    self._handler(event, self._conn)
                else:
                    self._handler(event)
                self._conn.execute(
                    "INSERT INTO processed (key) VALUES (?)", (event.idempotency_key,)
                )
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.execute("COMMIT")

    def close(self) -> None:
        self._conn.close()


class FlakySink:
    """Simulates an outage: the first ``failures`` deliveries raise, the rest pass through."""

    def __init__(self, inner: Sink, failures: int) -> None:
        self._inner = inner
        self.remaining_failures = failures
        self.attempts = 0

    def deliver(self, event: OutboxEvent) -> None:
        self.attempts += 1
        if self.remaining_failures > 0:
            self.remaining_failures -= 1
            raise SinkUnavailableError("downstream unavailable (simulated)")
        self._inner.deliver(event)


class LostAckSink:
    """Simulates a lost acknowledgement.

    The first ``times`` deliveries are applied by the inner sink and *then*
    raise, so the dispatcher believes they failed and sends them again. This is
    the canonical source of duplicates in at-least-once delivery.
    """

    def __init__(self, inner: Sink, times: int = 1) -> None:
        self._inner = inner
        self.remaining = times

    def deliver(self, event: OutboxEvent) -> None:
        self._inner.deliver(event)
        if self.remaining > 0:
            self.remaining -= 1
            raise AckLostError("sink applied the event but the acknowledgement was lost")
