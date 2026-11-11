"""Store contract: every test runs against FakeStore AND PgStore and must behave identically.

Time is controlled the same way for both: the adapter edits the timestamp columns directly
(e.g. "this lease expired 1 s ago"), instead of waiting.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from queue_service.db import create_pool, migrate
from queue_service.store import Claim, PgStore, Rejected, Store, replay_dead
from tests.fakes import FakeClock, FakeStore

LEASE = 30.0
GRACE = 5.0


class Backend(Protocol):
    @property
    def store(self) -> Store: ...

    async def accept(self, max_attempts: int = 3) -> UUID: ...
    async def expire_lease(self, job_id: UUID) -> None: ...
    async def make_old(self, job_id: UUID) -> None: ...
    async def make_retry_due(self, job_id: UUID) -> None: ...
    async def job(self, job_id: UUID) -> dict[str, Any]: ...
    async def result_count(self, job_id: UUID) -> int: ...
    async def replay(self, job_id: UUID) -> None: ...


_COLS = ("state", "attempts", "max_attempts", "epoch", "dead_reason", "stream_entry_id")


@dataclass
class FakeBackend:
    store: FakeStore

    async def accept(self, max_attempts: int = 3) -> UUID:
        return self.store.accept("io_sleep", {"duration_ms": 1}, max_attempts)

    async def expire_lease(self, job_id: UUID) -> None:
        self.store.jobs[job_id].lease_until = self.store.clock.now() - 1

    async def make_old(self, job_id: UUID) -> None:
        j = self.store.jobs[job_id]
        j.created_at -= 3600
        if j.dispatched_at is not None:
            j.dispatched_at -= 3600
        if j.lease_until is not None:
            j.lease_until -= 3600

    async def make_retry_due(self, job_id: UUID) -> None:
        self.store.jobs[job_id].next_attempt_at = self.store.clock.now() - 1

    async def job(self, job_id: UUID) -> dict[str, Any]:
        j = self.store.jobs[job_id]
        return {c: getattr(j, c) for c in _COLS}

    async def result_count(self, job_id: UUID) -> int:
        return int(job_id in self.store.results)

    async def replay(self, job_id: UUID) -> None:
        self.store.replay_dead([job_id], max_attempts=3)


@dataclass
class PgBackend:
    store: PgStore
    pool: Any

    async def _sql(self, q: str, *args: Any) -> None:
        async with self.pool.acquire() as c:
            await c.execute(q, *args)

    async def accept(self, max_attempts: int = 3) -> UUID:
        async with self.pool.acquire() as c:
            job_id: UUID = await c.fetchval(
                """INSERT INTO jobs (request_hash, type, schema_version, payload, max_attempts)
                   VALUES ('h', 'io_sleep', 1, '{"duration_ms": 1}', $1) RETURNING id""",
                max_attempts,
            )
        return job_id

    async def expire_lease(self, job_id: UUID) -> None:
        await self._sql(
            "UPDATE jobs SET lease_until = now() - interval '1 s' WHERE id = $1", job_id
        )

    async def make_old(self, job_id: UUID) -> None:
        await self._sql(
            """UPDATE jobs SET created_at = created_at - interval '1 h',
                   dispatched_at = dispatched_at - interval '1 h',
                   lease_until = lease_until - interval '1 h' WHERE id = $1""",
            job_id,
        )

    async def make_retry_due(self, job_id: UUID) -> None:
        await self._sql(
            "UPDATE jobs SET next_attempt_at = now() - interval '1 s' WHERE id = $1", job_id
        )

    async def job(self, job_id: UUID) -> dict[str, Any]:
        async with self.pool.acquire() as c:
            row = await c.fetchrow(f"SELECT {', '.join(_COLS)} FROM jobs WHERE id = $1", job_id)
        assert row is not None
        return dict(row)

    async def result_count(self, job_id: UUID) -> int:
        async with self.pool.acquire() as c:
            return int(await c.fetchval("SELECT count(*) FROM results WHERE job_id = $1", job_id))

    async def replay(self, job_id: UUID) -> None:
        await replay_dead(self.pool, job_ids=[job_id], max_attempts=3)


@pytest_asyncio.fixture(params=["fake", "postgres"])
async def b(request: pytest.FixtureRequest, pg_url: str, clean: None) -> AsyncIterator[Backend]:
    if request.param == "fake":
        yield FakeBackend(FakeStore(FakeClock()))
        return
    pool = await create_pool(pg_url, max_size=4)
    await migrate(pool)
    yield PgBackend(PgStore(pool, 2.0), pool)
    await pool.close()


async def _claim(b: Backend, job_id: UUID, who: str = "w1", epoch: int = 1) -> Claim:
    c = await b.store.claim(job_id, epoch, who, LEASE)
    assert isinstance(c, Claim), c
    return c


async def test_claim_is_exclusive_while_the_lease_is_valid(b: Backend) -> None:
    job_id = await b.accept()
    c = await _claim(b, job_id)
    assert c.attempt == 1
    assert (await b.job(job_id))["state"] == "running"
    assert await b.store.claim(job_id, 1, "w2", LEASE) == Rejected("running_elsewhere")


async def test_missing_job_is_rejected(b: Backend) -> None:
    assert await b.store.claim(uuid4(), 1, "w1", LEASE) == Rejected("missing")


async def test_complete_is_final_and_produces_one_result(b: Backend) -> None:
    job_id = await b.accept()
    c = await _claim(b, job_id)
    assert await b.store.complete(c, {"ok": 1}) is True
    assert (await b.job(job_id))["state"] == "done"
    assert await b.result_count(job_id) == 1
    assert await b.store.claim(job_id, 1, "w2", LEASE) == Rejected("done")
    assert await b.store.complete(c, {"ok": 2}) is False  # second commit is fenced out


async def test_takeover_after_expiry_fences_out_the_old_holder(b: Backend) -> None:
    job_id = await b.accept()
    old = await _claim(b, job_id, "old")
    await b.expire_lease(job_id)
    new = await _claim(b, job_id, "new")
    assert new.attempt == 2
    assert await b.store.heartbeat(old, LEASE) is False
    assert await b.store.complete(old, {}) is False
    assert await b.store.retry_later(old, 1.0, None, None) is False
    assert await b.store.dead_letter(old, "permanent", None, None) is False
    assert await b.store.release(old) is False
    assert await b.store.heartbeat(new, LEASE) is True
    assert await b.store.complete(new, {}) is True


async def test_takeover_on_last_attempt_dead_letters(b: Backend) -> None:
    job_id = await b.accept(max_attempts=1)
    await _claim(b, job_id)
    await b.expire_lease(job_id)
    assert await b.store.claim(job_id, 1, "w2", LEASE) == Rejected("crashed_out", True)
    j = await b.job(job_id)
    assert (j["state"], j["dead_reason"], j["attempts"]) == ("dead", "crashed_max_attempts", 1)


async def test_retry_wait_is_dispatched_only_when_due(b: Backend) -> None:
    job_id = await b.accept()
    c = await _claim(b, job_id)
    assert await b.store.retry_later(c, 3600.0, "TransientError", "x") is True
    assert (await b.job(job_id))["state"] == "retry_wait"
    assert await b.store.take_dispatch_batch(10, GRACE) == []
    await b.make_retry_due(job_id)
    [item] = await b.store.take_dispatch_batch(10, GRACE)
    assert (item.job_id, item.entry_id) == (job_id, None)  # None: must publish
    assert (await b.job(job_id))["state"] == "queued"
    assert (await _claim(b, job_id, "w2")).attempt == 2


async def test_dead_letter_records_reason(b: Backend) -> None:
    job_id = await b.accept()
    c = await _claim(b, job_id)
    assert await b.store.dead_letter(c, "permanent", "PermanentError", "x") is True
    j = await b.job(job_id)
    assert (j["state"], j["dead_reason"]) == ("dead", "permanent")
    assert await b.store.claim(job_id, 1, "w2", LEASE) == Rejected("dead")


async def test_release_refunds_budget_and_keeps_the_token(b: Backend) -> None:
    job_id = await b.accept(max_attempts=3)
    c = await _claim(b, job_id)
    assert await b.store.release(c) is True
    j = await b.job(job_id)
    assert (j["state"], j["attempts"], j["max_attempts"]) == ("queued", 1, 4)
    assert await b.store.complete(c, {}) is False
    assert (await _claim(b, job_id, "w2")).attempt == 2


async def test_replay_starts_a_new_epoch_and_rejects_old_messages(b: Backend) -> None:
    job_id = await b.accept()
    c = await _claim(b, job_id)
    await b.store.dead_letter(c, "permanent", None, None)
    await b.replay(job_id)
    j = await b.job(job_id)
    assert (j["state"], j["epoch"], j["attempts"]) == ("queued", 2, 0)
    assert await b.store.claim(job_id, 1, "w1", LEASE) == Rejected("stale_epoch")
    assert (await _claim(b, job_id, epoch=2)).attempt == 1


async def test_dispatch_skips_fresh_jobs_and_stamps_picked_ones(b: Backend) -> None:
    job_id = await b.accept()
    await b.store.set_entry(job_id, 1, "1-0")
    assert await b.store.take_dispatch_batch(10, GRACE) == []  # just created
    await b.make_old(job_id)
    [item] = await b.store.take_dispatch_batch(10, GRACE)
    assert item.entry_id == "1-0"  # recorded entry: publish only if it is gone
    assert await b.store.take_dispatch_batch(10, GRACE) == []  # stamped: peers skip it


async def test_dispatch_picks_running_jobs_with_long_expired_leases(b: Backend) -> None:
    job_id = await b.accept()
    await _claim(b, job_id)
    assert await b.store.take_dispatch_batch(10, GRACE) == []
    await b.make_old(job_id)  # lease expired an hour ago, never re-dispatched
    [item] = await b.store.take_dispatch_batch(10, GRACE)
    assert item.job_id == job_id
    assert (await b.job(job_id))["state"] == "running"  # still claimable by takeover
