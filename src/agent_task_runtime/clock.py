"""Injectable time source.

All runtime code asks a ``Clock`` for the current time instead of calling
``time.time()`` directly. Tests and the demo use :class:`ManualClock`, so no
test ever sleeps.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """Anything with a ``now()`` returning seconds as a float."""

    def now(self) -> float: ...


class SystemClock:
    """Wall-clock time (seconds since the Unix epoch)."""

    def now(self) -> float:
        return time.time()


class ManualClock:
    """A clock that only moves when told to. Used by tests and the demo."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("a clock cannot move backwards")
        self._now += seconds
        return self._now

    def set(self, value: float) -> None:
        self._now = float(value)
