"""Deterministic scripted simulation behind ``python -m agent_task_runtime demo``.

Everything runs on a virtual clock and a seeded RNG, so the output is
byte-for-byte reproducible and nothing sleeps.
"""

from __future__ import annotations

import contextlib
import random
import tempfile
from collections.abc import Callable
from pathlib import Path

from .clock import ManualClock
from .errors import StaleLeaseError
from .executors import SimulatedAgentExecutor, SimulatedCrash
from .models import OutboxEvent, Task, TaskState
from .outbox import OutboxDispatcher
from .retry import RetryPolicy
from .sinks import DedupSink, FlakySink, LostAckSink
from .store import TaskStore
from .worker import TaskContext, Worker

TASK_ID = "task-demo-1"
LEASE_SECONDS = 30.0
STEP_SECONDS = 2.0


def run_demo(out: Callable[[str], None] = print, db_path: str | Path | None = None) -> int:
    clock = ManualClock(0.0)
    rng = random.Random(7)
    policy = RetryPolicy(max_attempts=5, base_delay=1.0, multiplier=2.0, max_delay=60.0, jitter=0.5)

    def say(actor: str, message: str) -> None:
        out(f"[t={clock.now():6.1f}s] {actor:<9} {message}")

    def heading(title: str) -> None:
        out("")
        out(f"--- {title} ---")

    with tempfile.TemporaryDirectory(prefix="agent-task-runtime-demo-") as tmp:
        path = Path(db_path) if db_path else Path(tmp) / "demo.db"

        def open_store() -> TaskStore:
            return TaskStore(
                path, clock=clock, rng=rng, retry_policy=policy, lease_seconds=LEASE_SECONDS
            )

        out("agent-task-runtime demo: virtual clock, seeded RNG, no network, no sleeping")
        out(f"lease = {LEASE_SECONDS:.0f}s, each simulated agent step = {STEP_SECONDS:.0f}s")
        store = open_store()

        # 1. enqueue ----------------------------------------------------------
        heading("1. enqueue (idempotent)")
        first = store.enqueue(
            "agent.research",
            {"topic": "example-quarterly-report"},
            priority=5,
            idempotency_key="report:example-q1",
            task_id=TASK_ID,
        )
        say("client", f"enqueue {first.task.id} priority=5 -> created={first.created}")
        again = store.enqueue(
            "agent.research",
            {"topic": "example-quarterly-report"},
            priority=5,
            idempotency_key="report:example-q1",
        )
        say(
            "client",
            f"enqueue same idempotency key -> created={again.created}, same id: "
            f"{again.task.id == first.task.id}",
        )

        # 2. worker A claims and crashes -------------------------------------
        heading("2. worker-A claims, works, then crashes mid-task")
        zombie_token: dict[str, int] = {}

        def a_claimed(task: Task) -> None:
            zombie_token["value"] = task.fencing_token
            say(
                "worker-A",
                f"claimed {task.id}: attempt={task.attempt}, fencing token={task.fencing_token}, "
                f"lease until t={task.lease_expires_at:.1f}s",
            )

        def a_hook(_: TaskContext, step: str) -> None:
            if step == "analyze":
                say("worker-A", f"CRASH before step '{step}' (process dies; lease not released)")
                raise SimulatedCrash
            say("worker-A", f"running step '{step}' ...")

        worker_a = Worker(
            store,
            SimulatedAgentExecutor(step_seconds=STEP_SECONDS, sleep=clock.advance, on_step=a_hook),
            worker_id="worker-A",
            reap=False,
            on_claim=a_claimed,
        )
        with contextlib.suppress(SimulatedCrash):
            worker_a.run_once()
        saved = list(store.load_checkpoints(TASK_ID))
        say(
            "store",
            f"checkpoints on disk: {saved}; task state={store.get_task(TASK_ID).state.value}",
        )

        # 3. lease expiry and reaping ----------------------------------------
        heading("3. lease expires; reaper reclaims")
        clock.set(20.0)
        reclaimed = store.reap_expired()
        say(
            "reaper",
            f"scan: {len(reclaimed)} expired leases (lease still valid until "
            f"t={store.get_task(TASK_ID).lease_expires_at:.1f}s)",
        )
        clock.set(36.0)
        for task in store.reap_expired():
            say(
                "reaper",
                f"lease expired -> {task.id} back to '{task.state.value}' "
                f"(attempts used: {task.attempt}/{task.max_attempts})",
            )

        # 4. worker B takes over and resumes ---------------------------------
        heading("4. worker-B takes over and resumes from checkpoint")

        def b_claimed(task: Task) -> None:
            say(
                "worker-B",
                f"claimed {task.id}: attempt={task.attempt}, fencing token={task.fencing_token}",
            )
            say("worker-B", f"resuming; checkpoints found: {list(store.load_checkpoints(task.id))}")

        def b_hook(_: TaskContext, step: str) -> None:
            say("worker-B", f"running step '{step}' ...")

        worker_b = Worker(
            store,
            SimulatedAgentExecutor(step_seconds=STEP_SECONDS, sleep=clock.advance, on_step=b_hook),
            worker_id="worker-B",
            reap=False,
            on_claim=b_claimed,
        )
        result = worker_b.run_once()
        assert result is not None and result.outcome == "succeeded", result
        assert result.task is not None
        say(
            "worker-B",
            f"completed {TASK_ID}; result.ran={result.task.result['ran']}, "
            f"resumed={result.task.result['resumed']}",
        )
        say("store", "terminal state AND outbox event were committed in one transaction")

        # 5. zombie A wakes up ------------------------------------------------
        heading("5. zombie worker-A wakes up and tries to finish")
        clock.advance(4.0)
        before = store.get_task(TASK_ID)
        stale = zombie_token["value"]
        for label, call in (
            ("heartbeat", lambda: store.heartbeat(TASK_ID, stale)),
            ("checkpoint", lambda: store.save_checkpoint(TASK_ID, stale, "review", {"x": 1})),
            ("complete", lambda: store.complete(TASK_ID, stale, {"from": "zombie"})),
        ):
            try:
                call()
            except StaleLeaseError as exc:
                say("zombie-A", f"{label}(token={stale}) REJECTED: {exc}")
            else:  # pragma: no cover - would be a fencing bug
                raise AssertionError(f"zombie {label} was accepted")
        after = store.get_task(TASK_ID)
        assert before == after
        say(
            "store",
            f"unchanged: state={after.state.value}, attempt={after.attempt}, "
            f"result.ran={after.result['ran']}",
        )

        # 6. process restart + outbox delivery -------------------------------
        heading("6. process restart, then outbox delivery (at-least-once)")
        say("process", "closing the database and reopening the file (simulated restart)")
        store.close()
        store = open_store()
        stats = store.stats()
        say(
            "store",
            f"after restart: outbox pending={stats['outbox']['pending']}, "
            f"delivered={stats['outbox']['delivered']}",
        )

        applied: list[OutboxEvent] = []

        def apply_effect(event: OutboxEvent) -> None:
            applied.append(event)
            say("sink", f"EFFECT applied for key={event.idempotency_key}")

        dedup = DedupSink(apply_effect)
        sink = FlakySink(LostAckSink(dedup, times=1), failures=2)
        dispatcher = OutboxDispatcher(store, sink)

        for _ in range(5):
            report = dispatcher.run_once()
            if not report.delivered and not report.failed:
                pending = store.list_outbox(status="pending")
                if not pending:
                    break
                wake = min(e.next_attempt_at for e in pending)
                say("dispatch", f"nothing due; backing off until t={wake:.1f}s")
                clock.set(wake)
                continue
            for event, error, retry_at in report.failed:
                say(
                    "dispatch",
                    f"delivery of {event.event_type} FAILED ({error}); retry at t={retry_at:.1f}s",
                )
                clock.set(retry_at)
            for event in report.delivered:
                dup = (
                    " (duplicate delivery ignored by the dedup sink)"
                    if (dedup.duplicates_ignored)
                    else ""
                )
                say("dispatch", f"delivered {event.event_type} key={event.idempotency_key}{dup}")
        done = store.list_outbox(status="delivered")
        say(
            "dispatch",
            f"outbox: {len(done)} delivered, "
            f"{len(store.list_outbox(status='pending'))} pending; "
            f"sink attempts={sink.attempts}, effects applied={len(applied)}, "
            f"duplicates ignored={dedup.duplicates_ignored}",
        )
        dedup.close()

        # 7. audit trail ------------------------------------------------------
        heading("7. append-only audit log for the task")
        for ev in store.list_events(TASK_ID):
            arrow = f"{ev.from_state or '-'} -> {ev.to_state}"
            out(
                f"  #{ev.id:<2} t={ev.at:6.1f}s  {ev.actor:<16} {ev.event_type:<14} "
                f"{arrow:<20} attempt={ev.attempt}"
            )

        final = store.get_task(TASK_ID)
        assert final.state is TaskState.SUCCEEDED and final.attempt == 2
        assert len(applied) == 1 and dedup.duplicates_ignored == 1
        out("")
        out(
            "invariants held: 1 terminal state, 1 effect applied, 1 duplicate absorbed, "
            "0 zombie writes"
        )
        store.close()
    return 0
