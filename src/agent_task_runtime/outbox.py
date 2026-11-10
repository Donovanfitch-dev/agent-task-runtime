"""At-least-once outbox dispatcher."""

from __future__ import annotations

from dataclasses import dataclass, field

from .models import OutboxEvent
from .retry import Backoff, DelayPolicy
from .sinks import Sink
from .store import TaskStore

#: Delivery backoff: 1s, 2s, 4s ... capped at 5 minutes, with jitter. There is no
#: attempt limit: undeliverable events are retried at the capped rate forever.
DEFAULT_DELIVERY_BACKOFF = Backoff(base_delay=1.0, multiplier=2.0, max_delay=300.0, jitter=0.5)


@dataclass
class DispatchReport:
    delivered: list[OutboxEvent] = field(default_factory=list)
    #: (event, error text, time of next attempt)
    failed: list[tuple[OutboxEvent, str, float]] = field(default_factory=list)
    #: (event, error text): bookkeeping failures for one event. The batch carried on.
    errors: list[tuple[OutboxEvent, str]] = field(default_factory=list)


class OutboxDispatcher:
    """Reads due outbox rows and hands them to a sink.

    The outbox row is marked delivered only *after* the sink returns, so a
    crash between the two causes a re-send, never a loss. Failures are
    recorded with an exponential-backoff ``next_attempt_at``; events are
    retried indefinitely (see the limitations section of the README).
    Ordering across events is not guaranteed once failures occur.
    """

    def __init__(
        self,
        store: TaskStore,
        sink: Sink,
        *,
        policy: DelayPolicy | None = None,
        batch_size: int = 50,
    ) -> None:
        self._store = store
        self._sink = sink
        self._policy = policy or DEFAULT_DELIVERY_BACKOFF
        self._batch_size = batch_size

    def run_once(self) -> DispatchReport:
        """Attempt every due event. One event's problems never stop the others.

        A sink failure is recorded with a backoff. If even the bookkeeping for
        one event fails (for example a database error), that is reported in
        ``DispatchReport.errors`` and the loop moves on to the next event; the
        row stays as it was and is picked up again on a later pass.
        """
        report = DispatchReport()
        for event in self._store.outbox_due(self._batch_size):
            try:
                self._deliver_one(event, report)
            except Exception as exc:
                report.errors.append((event, f"{type(exc).__name__}: {exc}"))
        return report

    def _deliver_one(self, event: OutboxEvent, report: DispatchReport) -> None:
        try:
            self._sink.deliver(event)
        except Exception as exc:
            try:
                delay = self._policy.delay(event.attempts + 1, self._store.rng)
            except Exception:  # a broken custom policy must not wedge delivery
                delay = DEFAULT_DELIVERY_BACKOFF.max_delay
            retry_at = self._store.clock.now() + delay
            error = f"{type(exc).__name__}: {exc}"
            self._store.outbox_record_failure(event.id, error, retry_at)
            report.failed.append((event, error, retry_at))
        else:
            self._store.outbox_mark_delivered(event.id)
            report.delivered.append(event)
