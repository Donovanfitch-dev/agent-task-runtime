"""Command-line interface: ``python -m agent_task_runtime <command>``."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from collections.abc import Sequence
from typing import Any

from .demo import run_demo
from .errors import AgentTaskRuntimeError
from .executors import (
    AlwaysFailingExecutor,
    FlakyExecutor,
    RoutingExecutor,
    SimulatedAgentExecutor,
)
from .models import OutboxEvent
from .outbox import OutboxDispatcher
from .store import TaskStore
from .worker import Worker

DEFAULT_DB = "agent-task-runtime.db"


class PrintSink:
    """Writes each delivered event to stdout as one JSON line."""

    def deliver(self, event: OutboxEvent) -> None:
        print(
            json.dumps(
                {
                    "key": event.idempotency_key,
                    "event_type": event.event_type,
                    "payload": event.payload,
                },
                sort_keys=True,
            ),
            flush=True,
        )


def _add_db(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--db", default=DEFAULT_DB, help=f"SQLite file (default: {DEFAULT_DB})")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="agent_task_runtime",
        description="Crash-safe, lease-based task runtime (independent reference implementation).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="run the deterministic crash/takeover/fencing simulation")
    demo.add_argument("--db", default=None, help="keep the demo database at this path")

    enq = sub.add_parser("enqueue", help="enqueue a task")
    _add_db(enq)
    enq.add_argument("--kind", required=True)
    enq.add_argument("--payload", default="{}", help="JSON object (default: {})")
    enq.add_argument("--priority", type=int, default=0)
    enq.add_argument("--key", default=None, help="idempotency key")
    enq.add_argument("--max-attempts", type=int, default=None)

    stats = sub.add_parser("stats", help="show task and outbox counts")
    _add_db(stats)

    inspect = sub.add_parser("inspect", help="list tasks, or show one task in detail")
    _add_db(inspect)
    inspect.add_argument("task_id", nargs="?", default=None)

    work = sub.add_parser("work", help="run a worker with the simulated executors")
    _add_db(work)
    work.add_argument("--worker-id", default="worker-1")
    work.add_argument("--once", action="store_true", help="drain the queue then exit")
    work.add_argument("--lease-seconds", type=float, default=30.0)
    work.add_argument("--poll-interval", type=float, default=1.0)

    dispatch = sub.add_parser("dispatch", help="deliver outbox events to stdout as JSON lines")
    _add_db(dispatch)
    dispatch.add_argument("--once", action="store_true", help="one pass then exit")
    dispatch.add_argument("--poll-interval", type=float, default=1.0)
    return parser


def _cmd_enqueue(args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(f"error: --payload is not valid JSON: {exc}", file=sys.stderr)
        return 2
    if not isinstance(payload, dict):
        print("error: --payload must be a JSON object", file=sys.stderr)
        return 2
    with TaskStore(args.db) as store:
        res = store.enqueue(
            args.kind,
            payload,
            priority=args.priority,
            idempotency_key=args.key,
            max_attempts=args.max_attempts,
        )
    status = "created" if res.created else "already exists (idempotent)"
    print(f"{res.task.id} {status} kind={res.task.kind} state={res.task.state.value}")
    return 0


def _cmd_stats(args: argparse.Namespace) -> int:
    with TaskStore(args.db) as store:
        stats = store.stats()
    print("tasks:")
    for state, count in stats["tasks"].items():
        print(f"  {state:<14} {count}")
    print("outbox:")
    for status, count in stats["outbox"].items():
        print(f"  {status:<14} {count}")
    print(f"audit events: {stats['events']}")
    return 0


def _cmd_inspect(args: argparse.Namespace) -> int:
    with TaskStore(args.db) as store:
        if args.task_id is None:
            for task in store.list_tasks():
                print(
                    f"{task.id}  {task.state.value:<13} kind={task.kind} "
                    f"prio={task.priority} attempt={task.attempt} "
                    f"used={task.attempts_used}/{task.max_attempts}"
                )
            return 0
        task = store.get_task(args.task_id)
        detail: dict[str, Any] = {
            "id": task.id,
            "kind": task.kind,
            "state": task.state.value,
            "priority": task.priority,
            "attempt": task.attempt,
            "attempts_used": task.attempts_used,
            "releases": task.releases,
            "max_attempts": task.max_attempts,
            "not_before": task.not_before,
            "lease_owner": task.lease_owner,
            "lease_expires_at": task.lease_expires_at,
            "last_error": task.last_error,
            "payload": task.payload,
            "result": task.result,
        }
        print(json.dumps(detail, indent=2, sort_keys=True))
        print("checkpoints:", list(store.load_checkpoints(task.id)))
        print("events:")
        for ev in store.list_events(task.id):
            print(
                f"  #{ev.id} t={ev.at:.3f} {ev.actor} {ev.event_type} "
                f"{ev.from_state or '-'} -> {ev.to_state} attempt={ev.attempt}"
            )
        print("outbox:")
        for ob in store.list_outbox(task.id):
            print(f"  #{ob.id} {ob.event_type} status={ob.status} attempts={ob.attempts}")
    return 0


def _cmd_work(args: argparse.Namespace) -> int:
    executor = RoutingExecutor(
        {
            "agent.research": SimulatedAgentExecutor(),
            "demo.flaky": FlakyExecutor(fail_times=2),
            "demo.fail": AlwaysFailingExecutor(retryable=False),
        }
    )
    with TaskStore(args.db, lease_seconds=args.lease_seconds) as store:
        worker = Worker(store, executor, worker_id=args.worker_id)
        if args.once:
            for res in worker.run_until_idle():
                print(f"{res.task_id} -> {res.outcome}" + (f" ({res.error})" if res.error else ""))
            return 0
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: worker.stop())
        print(f"{args.worker_id} polling; Ctrl-C for graceful shutdown", file=sys.stderr)
        worker.run_forever(args.poll_interval)
        print(f"{args.worker_id} stopped cleanly", file=sys.stderr)
    return 0


def _cmd_dispatch(args: argparse.Namespace) -> int:
    stop = False

    def request_stop(*_: object) -> None:
        nonlocal stop
        stop = True

    with TaskStore(args.db) as store:
        dispatcher = OutboxDispatcher(store, PrintSink())
        if args.once:
            dispatcher.run_once()
            return 0
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, request_stop)
        while not stop:
            report = dispatcher.run_once()
            if not report.delivered:
                time.sleep(args.poll_interval)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "demo":
            return run_demo(db_path=args.db)
        handlers = {
            "enqueue": _cmd_enqueue,
            "stats": _cmd_stats,
            "inspect": _cmd_inspect,
            "work": _cmd_work,
            "dispatch": _cmd_dispatch,
        }
        return handlers[args.command](args)
    except AgentTaskRuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
