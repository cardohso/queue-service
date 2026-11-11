import random

import pytest
from pydantic import BaseModel, ValidationError

from queue_service.errors import PermanentError, TransientError, classify
from queue_service.retry import decide_failure, full_jitter


class _M(BaseModel):
    x: int


def _validation_error() -> ValidationError:
    try:
        _M.model_validate({"x": "nope"})
    except ValidationError as exc:
        return exc
    raise AssertionError("unreachable")


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (PermanentError("x"), "permanent"),
        (_validation_error(), "permanent"),
        (TransientError("x"), "transient"),
        (TimeoutError(), "transient"),
        (KeyError("bug"), "transient"),  # unknown -> transient (decided 2026-11-11)
    ],
)
def test_classify(exc: BaseException, expected: str) -> None:
    assert classify(exc) == expected


def test_full_jitter_bounds_double_then_cap() -> None:
    # Draw many samples per attempt: every one must lie in [0, min(cap, base*2^(n-1))].
    rng = random.Random(0)
    for attempt, upper in [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 10.0), (9, 10.0)]:
        samples = [full_jitter(attempt, base_s=1.0, cap_s=10.0, rng=rng) for _ in range(2000)]
        assert all(0.0 <= s <= upper for s in samples)
        # ...and actually spread across the window, not clustered (that's the point of jitter)
        assert max(samples) > 0.9 * upper
        assert min(samples) < 0.1 * upper


def test_full_jitter_is_deterministic_with_a_seed() -> None:
    a = [full_jitter(3, base_s=0.5, cap_s=30, rng=random.Random(42)) for _ in range(3)]
    b = [full_jitter(3, base_s=0.5, cap_s=30, rng=random.Random(42)) for _ in range(3)]
    assert a == b


def test_full_jitter_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError):
        full_jitter(0, base_s=1, cap_s=1, rng=random.Random(0))


def _decide(exc: BaseException, attempt: int, max_attempts: int = 3) -> tuple[str, str | None]:
    d = decide_failure(
        exc,
        attempt=attempt,
        max_attempts=max_attempts,
        base_s=1,
        cap_s=10,
        rng=random.Random(0),
    )
    return d.kind, d.reason


def test_permanent_dead_letters_on_first_attempt() -> None:
    assert _decide(PermanentError("x"), attempt=1) == ("dead", "permanent")


def test_transient_retries_until_the_last_attempt_then_exhausts() -> None:
    assert _decide(TransientError("x"), attempt=1) == ("retry", None)
    assert _decide(TransientError("x"), attempt=2) == ("retry", None)
    assert _decide(TransientError("x"), attempt=3) == ("dead", "exhausted")


def test_failure_records_exception_type_for_the_dlq() -> None:
    d = decide_failure(
        KeyError("k"), attempt=3, max_attempts=3, base_s=1, cap_s=1, rng=random.Random(0)
    )
    assert (d.error_class, d.reason) == ("KeyError", "exhausted")
