import pytest

from queue_service.admission import MAX_RETRY_AFTER_S, Admission


def test_admits_at_or_below_limit() -> None:
    a = Admission(max_depth=100, drain_per_s=10)
    a.update(100, "redis")
    assert a.retry_after() is None


@pytest.mark.parametrize(
    ("depth", "expected"),
    [
        (101, 1),  # tiny excess still waits at least 1 s
        (200, 10),  # 100 over / 10 per s
        (205, 11),  # rounds up, never tells the client to come back too early
        (10_000, MAX_RETRY_AFTER_S),  # capped so one overload can't park clients for minutes
    ],
)
def test_retry_after_scales_with_excess_and_is_clamped(depth: int, expected: int) -> None:
    a = Admission(max_depth=100, drain_per_s=10)
    a.update(depth, "redis")
    assert a.retry_after() == expected


def test_concurrency_limit_sheds_beyond_the_limit_and_frees_slots() -> None:
    from queue_service.admission import ConcurrencyLimit, Overloaded

    lim = ConcurrencyLimit(2)
    with lim.slot(), lim.slot():
        assert lim.in_use == 2
        with pytest.raises(Overloaded), lim.slot():
            pass
    assert lim.in_use == 0  # released even though the third was rejected
    with lim.slot():  # and usable again
        pass
