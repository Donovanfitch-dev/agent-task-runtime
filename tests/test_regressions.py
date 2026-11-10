"""Regression tests for defects found in review. Each class names the defect."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from agent_task_runtime import (
    Backoff,
    DedupSink,
    DuplicateTaskIdError,
    FlakySink,
    ListSink,
    ManualClock,
    OutboxDispatcher,
    OutboxEvent,
    RetryPolicy,
    ShutdownRequested,
    SimulatedCrash,
    StaleLeaseError,
    TaskContext,
    TaskState,
    TaskStore,
    Worker,
)
from agent_task_runtime.migrations import MIGRATIONS, migrate

from .helpers import FixedRng


def make_event(key: str = "t:task.succeeded", event_id: int = 1) -> OutboxEvent:
    return OutboxEvent(event_id, "t", "task.succeeded", {}, key, "pending", 0, 0.0, None, 0.0, None)


class TestHeartbeatKeepsClaimDuration:
    """RT-1: a heartbeat used to replace a per-claim lease duration with the store default."""

    def test_direct_heartbeat_renews_with_the_claim_duration(
        self, store: TaskStore, clock: ManualClock
    ) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w", lease_seconds=600)  # store default is 30
        assert leased is not None and leased.lease_seconds == 600
        clock.advance(100)
        renewed = store.heartbeat("t", leased.fencing_token)
        assert renewed.lease_expires_at == clock.now() + 600

    def test_explicit_duration_still_overrides(self, store: TaskStore, clock: ManualClock) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w", lease_seconds=600)
        assert leased is not None
        renewed = store.heartbeat("t", leased.fencing_token, lease_seconds=5)
        assert renewed.lease_expires_at == clock.now() + 5

    def test_task_context_heartbeat_keeps_the_workers_duration(
        self, store: TaskStore, clock: ManualClock
    ) -> None:
        store.enqueue("k", task_id="t")
        seen: dict[str, Any] = {}

        class Beats:
            def execute(self, ctx: TaskContext) -> str:
                seen["context_duration"] = ctx.lease_seconds
                clock.advance(100)
                ctx.heartbeat()
                seen["expires"] = store.get_task("t").lease_expires_at
                seen["now"] = clock.now()
                return "ok"

        Worker(store, Beats(), worker_id="w", lease_seconds=600).run_once()
        assert seen["context_duration"] == 600
        assert seen["expires"] == seen["now"] + 600

    def test_default_claim_uses_the_store_default_and_renews_with_it(
        self, store: TaskStore, clock: ManualClock
    ) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w")
        assert leased is not None and leased.lease_seconds == 30
        assert store.heartbeat("t", leased.fencing_token).lease_expires_at == clock.now() + 30

    def test_database_created_before_the_column_existed_is_upgraded(
        self, tmp_path: Path, clock: ManualClock
    ) -> None:
        path = tmp_path / "old.db"
        conn = sqlite3.connect(path, isolation_level=None)
        assert migrate(conn, clock.now, MIGRATIONS[:3]) == [1, 2, 3]
        conn.execute(
            "INSERT INTO tasks (id, kind, payload, state, max_attempts, not_before,"
            " created_at, updated_at) VALUES ('old', 'k', '{}', 'queued', 5, 0, 0, 0)"
        )
        conn.close()
        with TaskStore(path, clock=clock, rng=FixedRng()) as upgraded:
            task = upgraded.get_task("old")
            assert task.releases == 0 and task.lease_seconds is None
            claimed = upgraded.claim("w", lease_seconds=60)
            assert claimed is not None and claimed.lease_seconds == 60


class TestBackoffSaturates:
    """RT-2: multiplier ** (attempt - 1) used to raise OverflowError for large attempts."""

    @pytest.mark.parametrize("attempt", [1024, 1025, 5000, 10**6, 10**12])
    def test_delay_is_the_cap_for_any_attempt_count(self, attempt: int) -> None:
        backoff = Backoff(base_delay=1.0, multiplier=2.0, max_delay=300.0, jitter=0.5)
        assert backoff.delay(attempt, FixedRng(0.0)) == 300.0
        assert 150.0 < backoff.delay(attempt, FixedRng(0.999)) <= 300.0

    def test_retry_policy_and_integer_multiplier_also_saturate(self) -> None:
        assert RetryPolicy(max_delay=60.0, jitter=0.0).delay(10**9, FixedRng()) == 60.0
        assert Backoff(multiplier=2, max_delay=7.0).delay(10**9, FixedRng()) == 7.0  # type: ignore[arg-type]

    def test_zero_base_delay_stays_zero(self) -> None:
        assert Backoff(base_delay=0.0).delay(10**6, FixedRng()) == 0.0

    def test_dispatcher_survives_an_event_with_a_huge_attempt_count(
        self, store: TaskStore, db_path: Path, clock: ManualClock
    ) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w")
        assert leased is not None
        store.complete("t", leased.fencing_token, "ok")
        raw = sqlite3.connect(db_path)
        raw.execute("UPDATE outbox SET attempts = 5000")
        raw.commit()
        raw.close()

        dispatcher = OutboxDispatcher(store, FlakySink(ListSink(), failures=1))
        report = dispatcher.run_once()  # used to raise OverflowError before recording anything
        assert report.errors == [] and len(report.failed) == 1
        assert report.failed[0][2] == clock.now() + 300.0  # saturated at the cap
        [event] = store.list_outbox()
        assert event.attempts == 5001 and event.next_attempt_at == clock.now() + 300.0
        clock.advance(300)
        assert len(dispatcher.run_once().delivered) == 1

    def test_one_bad_event_never_stops_the_batch(
        self, store: TaskStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for task_id in ("a", "b"):
            store.enqueue("k", task_id=task_id)
            leased = store.claim("w")
            assert leased is not None
            store.complete(task_id, leased.fencing_token, "ok")

        class FailsForA:
            def __init__(self) -> None:
                self.delivered: list[str] = []

            def deliver(self, event: OutboxEvent) -> None:
                if event.task_id == "a":
                    raise RuntimeError("sink rejects a")
                self.delivered.append(event.task_id)

        real = store.outbox_record_failure

        def broken_bookkeeping(event_id: int, error: str, retry_at: float) -> None:
            if event_id == 1:
                raise sqlite3.OperationalError("disk I/O error (simulated)")
            real(event_id, error, retry_at)

        monkeypatch.setattr(store, "outbox_record_failure", broken_bookkeeping)
        sink = FailsForA()
        report = OutboxDispatcher(store, sink).run_once()
        assert sink.delivered == ["b"] and len(report.delivered) == 1
        assert len(report.errors) == 1 and "disk I/O error" in report.errors[0][1]

    def test_a_broken_custom_policy_cannot_wedge_delivery(self, store: TaskStore) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w")
        assert leased is not None
        store.complete("t", leased.fencing_token, "ok")

        class Broken:
            def delay(self, attempt: int, rng: Any) -> float:
                raise ArithmeticError("bad policy")

        report = OutboxDispatcher(store, FlakySink(ListSink(), 1), policy=Broken()).run_once()
        assert len(report.failed) == 1 and report.errors == []


class TestReleaseAndTheAttemptCap:
    """RT-3: release used to let attempt run past max_attempts.

    Decision: a release advances the fencing token but does not consume retry
    budget; the cap applies to failed or lost attempts (attempts_used).
    """

    def test_repeated_releases_advance_the_token_but_not_the_budget(self, store: TaskStore) -> None:
        store.enqueue("k", task_id="t", max_attempts=1)
        tokens = []
        for _ in range(3):
            leased = store.claim("w")
            assert leased is not None
            tokens.append(leased.fencing_token)
            released = store.release("t", leased.fencing_token, "deploy")
            assert released.state is TaskState.QUEUED
        final = store.claim("w")
        assert final is not None
        assert tokens == [1, 2, 3] and final.fencing_token == 4
        assert final.releases == 3 and final.attempts_used == 1
        # The cap is enforced where budget is spent: a real failure now dead-letters.
        assert store.fail("t", 4, "boom").state is TaskState.DEAD_LETTERED

    def test_a_released_token_is_stale(self, store: TaskStore) -> None:
        store.enqueue("k", task_id="t")
        leased = store.claim("w")
        assert leased is not None
        store.release("t", leased.fencing_token, "stop")
        with pytest.raises(StaleLeaseError):
            store.complete("t", leased.fencing_token)

    def test_failure_budget_counts_only_real_failures(
        self, store: TaskStore, clock: ManualClock
    ) -> None:
        store.enqueue("k", task_id="t", max_attempts=2)
        a = store.claim("w")
        assert a is not None
        store.release("t", a.fencing_token, "stop")
        b = store.claim("w")
        assert b is not None and b.attempt == 2
        first = store.fail("t", b.fencing_token, "boom")
        assert first.state is TaskState.QUEUED  # 1 of 2 used, despite attempt == 2
        assert first.not_before == clock.now() + 1.0  # backoff exponent uses attempts_used
        clock.advance(10)
        c = store.claim("w")
        assert c is not None and c.attempt == 3
        assert store.fail("t", c.fencing_token, "boom").state is TaskState.DEAD_LETTERED

    def test_reaper_uses_the_same_budget(self, store: TaskStore, clock: ManualClock) -> None:
        store.enqueue("k", task_id="t", max_attempts=2)
        a = store.claim("w")
        assert a is not None
        store.release("t", a.fencing_token, "stop")
        assert store.claim("w") is not None
        clock.advance(31)
        [reaped] = store.reap_expired()
        assert reaped.state is TaskState.QUEUED  # 1 of 2 used
        assert store.claim("w") is not None
        clock.advance(31)
        [reaped] = store.reap_expired()
        assert reaped.state is TaskState.DEAD_LETTERED

    def test_graceful_shutdown_through_the_worker_does_not_burn_budget(
        self, store: TaskStore
    ) -> None:
        store.enqueue("k", task_id="t", max_attempts=1)

        class Stops:
            def execute(self, ctx: TaskContext) -> None:
                raise ShutdownRequested("deploy")

        worker = Worker(store, Stops(), worker_id="w")
        for _ in range(3):
            result = worker.run_once()
            assert result is not None and result.outcome == "released"
        task = store.get_task("t")
        assert task.state is TaskState.QUEUED and task.attempts_used == 0


class TestResultRecordingErrors:
    """RT-4: a store error from complete() used to be treated as an executor failure."""

    def test_unserialisable_result_fails_once_without_rerunning_the_executor(
        self, store: TaskStore
    ) -> None:
        store.enqueue("k", task_id="t")
        calls = []

        class ReturnsJunk:
            def execute(self, ctx: TaskContext) -> object:
                calls.append(ctx.task.attempt)
                return object()  # not JSON-serialisable

        worker = Worker(store, ReturnsJunk(), worker_id="w")
        result = worker.run_once()
        assert result is not None and result.outcome == "failed"
        assert "ResultNotRecordable" in (result.error or "")
        assert worker.run_once() is None  # nothing is retried
        assert calls == [1]
        task = store.get_task("t")
        assert task.state is TaskState.FAILED and task.attempt == 1
        assert [e.event_type for e in store.list_outbox("t")] == ["task.failed"]

    def test_infrastructure_errors_propagate_and_leave_the_task_leased(
        self, store: TaskStore, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store.enqueue("k", task_id="t")

        class Fine:
            def execute(self, ctx: TaskContext) -> str:
                return "ok"

        def locked(*args: object, **kwargs: object) -> None:
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(store, "complete", locked)
        with pytest.raises(sqlite3.OperationalError):
            Worker(store, Fine(), worker_id="w").run_once()
        monkeypatch.undo()
        assert store.get_task("t").state is TaskState.LEASED  # reaper recovers it later


class TestLeaseLostWhileFinishing:
    """The failure and release paths in the worker must also tolerate a stolen lease."""

    @pytest.mark.parametrize("raises", [RuntimeError("boom"), ShutdownRequested("stop")])
    def test_lost_lease_is_reported_and_nothing_is_written(
        self,
        make_store: Callable[..., TaskStore],
        clock: ManualClock,
        raises: Exception,
    ) -> None:
        store_a, store_b = make_store(), make_store()
        store_a.enqueue("k", task_id="t")

        class StolenThenFails:
            def execute(self, ctx: TaskContext) -> None:
                clock.advance(31)
                store_b.reap_expired()
                assert store_b.claim("worker-B") is not None
                raise raises

        result = Worker(store_a, StolenThenFails(), worker_id="worker-A", reap=False).run_once()
        assert result is not None and result.outcome == "lease_lost"
        task = store_a.get_task("t")
        assert task.state is TaskState.LEASED and task.lease_owner == "worker-B"
        assert task.last_error is not None and "expired" in task.last_error  # from the reaper
        assert [e.event_type for e in store_a.list_events("t")][-1] == "claimed"


class TestSmallStoreFixes:
    def test_duplicate_explicit_task_id_is_a_domain_error(self, store: TaskStore) -> None:
        store.enqueue("k", task_id="same")
        events_before = store.list_events()
        with pytest.raises(DuplicateTaskIdError):
            store.enqueue("k", task_id="same")
        assert store.list_events() == events_before and len(store.list_tasks()) == 1

    def test_checkpoint_after_deadline_is_accepted_but_heartbeat_is_not(
        self, store: TaskStore, clock: ManualClock
    ) -> None:
        """Rule: operations that record finished work are accepted while the token is
        current; operations that extend ownership are not."""
        store.enqueue("k", task_id="t")
        leased = store.claim("w")
        assert leased is not None
        clock.advance(31)
        store.save_checkpoint("t", leased.fencing_token, "step", {"done": True})
        assert store.load_checkpoints("t") == {"step": {"done": True}}
        with pytest.raises(StaleLeaseError):
            store.heartbeat("t", leased.fencing_token)
        # Once reclaimed, the old token can no longer checkpoint.
        store.reap_expired()
        store.claim("w2")
        with pytest.raises(StaleLeaseError):
            store.save_checkpoint("t", leased.fencing_token, "step", {"done": False})


class TestDedupSinkTransactionalMode:
    """The effect joins the dedup transaction only when the handler is given the connection."""

    @staticmethod
    def effect_count(path: Path) -> int:
        conn = sqlite3.connect(path)
        try:
            exists = conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'effects'").fetchone()
            return conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0] if exists else 0
        finally:
            conn.close()

    @staticmethod
    def keys(path: Path) -> list[str]:
        conn = sqlite3.connect(path)
        try:
            return [r[0] for r in conn.execute("SELECT key FROM processed")]
        finally:
            conn.close()

    def test_crash_between_effect_and_commit_leaves_neither_effect_nor_key(
        self, tmp_path: Path
    ) -> None:
        path = tmp_path / "consumer.db"
        crash = {"armed": True}

        def handler(event: OutboxEvent, conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE IF NOT EXISTS effects (key TEXT)")
            conn.execute("INSERT INTO effects VALUES (?)", (event.idempotency_key,))
            if crash["armed"]:
                crash["armed"] = False
                raise SimulatedCrash  # after the effect, before the commit

        sink = DedupSink(handler, str(path), transactional=True)
        with pytest.raises(SimulatedCrash):
            sink.deliver(make_event())
        assert self.effect_count(path) == 0 and self.keys(path) == []

        sink.deliver(make_event())  # redelivery applies the effect
        sink.deliver(make_event())  # duplicate is absorbed
        sink.close()
        assert self.effect_count(path) == 1
        assert self.keys(path) == ["t:task.succeeded"]
        assert sink.duplicates_ignored == 1

    def test_effect_and_key_survive_a_consumer_restart_together(self, tmp_path: Path) -> None:
        path = tmp_path / "consumer.db"

        def handler(event: OutboxEvent, conn: sqlite3.Connection) -> None:
            conn.execute("CREATE TABLE IF NOT EXISTS effects (key TEXT)")
            conn.execute("INSERT INTO effects VALUES (?)", (event.idempotency_key,))

        for _ in range(2):  # second round models a restarted consumer
            sink = DedupSink(handler, str(path), transactional=True)
            sink.deliver(make_event())
            sink.close()
        assert self.effect_count(path) == 1

    def test_plain_mode_cannot_protect_an_effect_outside_the_database(self) -> None:
        """The narrower guarantee, demonstrated: an external effect repeats after a crash."""
        external_effects: list[str] = []
        crash = {"armed": True}

        def handler(event: OutboxEvent) -> None:
            external_effects.append(event.idempotency_key)  # e.g. an HTTP call
            if crash["armed"]:
                crash["armed"] = False
                raise SimulatedCrash

        sink = DedupSink(handler)
        with pytest.raises(SimulatedCrash):
            sink.deliver(make_event())
        sink.deliver(make_event())
        assert external_effects == ["t:task.succeeded", "t:task.succeeded"]
        sink.close()
