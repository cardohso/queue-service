"""Turning a job outcome into the next state. Pure functions: tested with a seeded RNG."""

import random
from dataclasses import dataclass
from typing import Any, Literal

from queue_service.errors import classify


def full_jitter(attempt: int, *, base_s: float, cap_s: float, rng: random.Random) -> float:
    """AWS "full jitter": uniform in [0, min(cap, base * 2^(attempt-1))].

    Spreading retries uniformly stops a burst of failures from coming back as a synchronized
    burst (thundering herd), at the cost of some retries happening sooner than plain backoff.
    """
    if attempt < 1:
        raise ValueError("attempt starts at 1")
    return rng.uniform(0.0, min(cap_s, base_s * 2 ** (attempt - 1)))


@dataclass(frozen=True, slots=True)
class Decision:
    kind: Literal["done", "retry", "dead"]
    result: dict[str, Any] | None = None
    delay_s: float = 0.0
    reason: str | None = None  # dead-letter reason: "permanent" or "exhausted"
    error_class: str | None = None  # exception type name, kept in the attempt history
    error: str | None = None


def decide_success(result: dict[str, Any]) -> Decision:
    return Decision("done", result=result)


def decide_failure(
    exc: BaseException,
    *,
    attempt: int,
    max_attempts: int,
    base_s: float,
    cap_s: float,
    rng: random.Random,
) -> Decision:
    error_class = type(exc).__name__
    error = str(exc)[:2000]
    if classify(exc) == "permanent":
        return Decision("dead", reason="permanent", error_class=error_class, error=error)
    if attempt >= max_attempts:
        return Decision("dead", reason="exhausted", error_class=error_class, error=error)
    delay = full_jitter(attempt, base_s=base_s, cap_s=cap_s, rng=rng)
    return Decision("retry", delay_s=delay, error_class=error_class, error=error)
