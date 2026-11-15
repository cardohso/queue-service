"""Postgres resource exhaustion: classified as infrastructure, start-up waits instead of dying."""

import asyncio
from typing import Any

import asyncpg
import pytest

from queue_service import db
from queue_service.config import Settings
from queue_service.errors import DB_INFRA_ERRORS
from queue_service.worker import Worker
from tests.fakes import FakeBroker, FakeClock, FakeStore
from tests.test_worker_metrics import Scripted

EXHAUSTION = [
    asyncpg.exceptions.TooManyConnectionsError("sorry, too many clients already"),
    asyncpg.exceptions.CannotConnectNowError("the database system is starting up"),
    asyncpg.exceptions.InsufficientResourcesError("out of shared memory"),
    asyncpg.exceptions.DiskFullError("could not extend file"),
    asyncpg.exceptions.OutOfMemoryError("out of memory"),
]


@pytest.mark.parametrize("exc", EXHAUSTION, ids=lambda e: type(e).__name__)
def test_resource_exhaustion_is_an_infrastructure_error(exc: BaseException) -> None:
    assert isinstance(exc, DB_INFRA_ERRORS)


def test_integrity_violations_are_not_infrastructure_errors() -> None:
    # They mean a bug or a broken invariant: they must surface, not be waited out.
    assert not isinstance(asyncpg.exceptions.UniqueViolationError("dup"), DB_INFRA_ERRORS)
    assert not isinstance(asyncpg.exceptions.CheckViolationError("bad"), DB_INFRA_ERRORS)


class ExhaustedStore(FakeStore):
    def __init__(self, clock: FakeClock, exc: BaseException) -> None:
        super().__init__(clock)
        self._exc = exc

    async def claim(self, *args: Any, **kwargs: Any) -> Any:
        raise self._exc


@pytest.mark.parametrize("exc", EXHAUSTION, ids=lambda e: type(e).__name__)
async def test_handler_pauses_instead_of_crashing_and_leaves_the_message_unacked(
    exc: BaseException,
) -> None:
    clock = FakeClock()
    store, broker = ExhaustedStore(clock, exc), FakeBroker(clock)
    w = Worker(Settings(), store, broker, Scripted(), worker_id="w1")
    await broker.ensure_group()
    job_id = FakeStore(clock).accept("io_sleep", {"duration_ms": 1}, 3)
    await broker.publish(job_id, 1)
    (msg,) = await broker.read("w1", 1, 0)

    await w._handle(msg)  # must not raise: a crash here strands the entry until the lease ends

    assert w.stats["db_unavailable"] == 1
    assert w._db_paused_until > asyncio.get_running_loop().time()  # fetching backs off
    assert broker.pending == 1  # still in the PEL: redelivered, no attempt charged


# ---- start-up retry --------------------------------------------------------------------------


class FakePool:
    closed = False

    async def close(self) -> None:
        self.closed = True


def patch_db(
    monkeypatch: pytest.MonkeyPatch,
    create_errors: list[BaseException],
    migrate_errors: list[BaseException],
) -> dict[str, Any]:
    state: dict[str, Any] = {"creates": 0, "migrates": 0, "pools": []}

    async def create_pool(url: str, *, max_size: int = 10) -> FakePool:
        state["creates"] += 1
        if create_errors:
            raise create_errors.pop(0)
        pool = FakePool()
        state["pools"].append(pool)
        return pool

    async def migrate(pool: Any) -> None:
        state["migrates"] += 1
        if migrate_errors:
            raise migrate_errors.pop(0)

    monkeypatch.setattr(db, "create_pool", create_pool)
    monkeypatch.setattr(db, "migrate", migrate)
    return state


async def test_startup_waits_for_a_full_database_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    full = asyncpg.exceptions.TooManyConnectionsError("sorry, too many clients already")
    state = patch_db(monkeypatch, [full, full], [])
    pool = await db.open_pool_with_retry("x", max_size=5, initial_delay_s=0.0)
    assert pool is state["pools"][0]
    assert state["creates"] == 3  # two refusals, then in


async def test_startup_closes_a_pool_whose_migration_failed_before_retrying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = patch_db(monkeypatch, [], [asyncpg.exceptions.CannotConnectNowError("starting")])
    pool = await db.open_pool_with_retry("x", max_size=5, initial_delay_s=0.0)
    assert state["pools"][0].closed is True  # no leaked connections
    assert pool is state["pools"][1]


async def test_startup_stops_waiting_when_told_to_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    full = asyncpg.exceptions.TooManyConnectionsError("full")
    patch_db(monkeypatch, [full] * 1000, [])
    stop = asyncio.Event()
    task = asyncio.create_task(
        db.open_pool_with_retry("x", max_size=5, stop=stop, initial_delay_s=30)
    )
    for _ in range(20):
        await asyncio.sleep(0)  # let it fail once and start waiting
    stop.set()  # SIGTERM while waiting for Postgres
    assert await asyncio.wait_for(task, 1) is None


async def test_startup_gives_up_after_the_deadline_and_raises_the_real_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    full = asyncpg.exceptions.TooManyConnectionsError("full")
    patch_db(monkeypatch, [full] * 1000, [])
    with pytest.raises(asyncpg.exceptions.TooManyConnectionsError):
        await db.open_pool_with_retry("x", max_size=5, give_up_after_s=0.0, initial_delay_s=0.0)


async def test_startup_does_not_swallow_non_infrastructure_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    patch_db(monkeypatch, [], [asyncpg.exceptions.SyntaxOrAccessError("bad schema")])
    with pytest.raises(asyncpg.exceptions.SyntaxOrAccessError):
        await db.open_pool_with_retry("x", max_size=5, initial_delay_s=0.0)
