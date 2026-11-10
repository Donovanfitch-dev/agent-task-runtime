"""Retry policy: exponential backoff with bounded jitter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class RandomSource(Protocol):
    """Anything with ``random() -> float in [0, 1)``; ``random.Random`` qualifies."""

    def random(self) -> float: ...


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with a cap and multiplicative jitter.

    ``delay(n)`` for the n-th failed attempt (n >= 1) is::

        raw    = min(max_delay, base_delay * multiplier ** (n - 1))
        delay  = raw * (1 - jitter * u)        with u drawn from [0, 1)

    so the delay lies in ``(raw * (1 - jitter), raw]``. Jitter only ever
    shortens the delay, which keeps ``max_delay`` a true upper bound.
    """

    max_attempts: int = 5
    base_delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    jitter: float = 0.5

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")

    def delay(self, attempt: int, rng: RandomSource) -> float:
        if attempt < 1:
            raise ValueError("attempt numbers start at 1")
        raw = min(self.max_delay, self.base_delay * self.multiplier ** (attempt - 1))
        return raw * (1.0 - self.jitter * rng.random())
