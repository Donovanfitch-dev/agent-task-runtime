from __future__ import annotations

import random

import pytest

from agent_task_runtime import ManualClock, RetryPolicy, TaskStore

from .helpers import FixedRng


def test_exponential_growth_and_cap_without_jitter() -> None:
    policy = RetryPolicy(base_delay=1.0, multiplier=2.0, max_delay=60.0, jitter=0.5)
    delays = [policy.delay(n, FixedRng(0.0)) for n in range(1, 9)]
    assert delays == [1, 2, 4, 8, 16, 32, 60, 60]


def test_jitter_scales_the_delay_deterministically() -> None:
    policy = RetryPolicy(base_delay=4.0, jitter=0.5)
    assert policy.delay(1, FixedRng(0.5)) == 4.0 * (1 - 0.5 * 0.5)
    assert policy.delay(1, FixedRng(0.0)) == 4.0


def test_jitter_stays_within_bounds_and_never_exceeds_the_cap() -> None:
    policy = RetryPolicy(base_delay=1.0, multiplier=2.0, max_delay=10.0, jitter=0.4)
    rng = random.Random(1234)
    for attempt in range(1, 15):
        raw = min(10.0, 2.0 ** (attempt - 1))
        for _ in range(100):
            d = policy.delay(attempt, rng)
            assert raw * 0.6 <= d <= raw <= 10.0


def test_seeded_rng_gives_a_reproducible_schedule() -> None:
    policy = RetryPolicy()
    first = [policy.delay(n, random.Random(99)) for n in range(1, 6)]
    second = [policy.delay(n, random.Random(99)) for n in range(1, 6)]
    assert first == second


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_attempts": 0},
        {"base_delay": -1},
        {"max_delay": -1},
        {"multiplier": 0.5},
        {"jitter": 1.5},
        {"jitter": -0.1},
    ],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_attempt_numbers_start_at_one() -> None:
    with pytest.raises(ValueError):
        RetryPolicy().delay(0, FixedRng())


def test_store_applies_the_backoff_schedule(store: TaskStore, clock: ManualClock) -> None:
    task = store.enqueue("k", max_attempts=5).task
    not_befores = []
    for _ in range(3):
        leased = store.claim("w")
        assert leased is not None
        failed = store.fail(task.id, leased.fencing_token, "boom")
        not_befores.append(failed.not_before - clock.now())
        clock.set(failed.not_before)
    assert not_befores == [1.0, 2.0, 4.0]


def test_store_backoff_is_reproducible_with_a_seeded_rng(make_store, clock: ManualClock) -> None:
    def schedule(seed: int) -> list[float]:
        s = make_store(rng=random.Random(seed))
        out = []
        t = s.enqueue("k", max_attempts=9).task
        for _ in range(4):
            clock.advance(1000)
            leased = s.claim("w")
            assert leased is not None
            out.append(s.fail(t.id, leased.fencing_token, "x").not_before - clock.now())
        s.cancel(t.id)
        return out

    assert schedule(5) == pytest.approx(schedule(5))
    assert schedule(5) != pytest.approx(schedule(6))
