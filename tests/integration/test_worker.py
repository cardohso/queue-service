"""Worker behaviour against real Postgres and Redis (DESIGN.md s5.2-s10).

Waiting is done with ``wait_until`` (poll a condition with a deadline), never a fixed sleep.
Crashed workers are simulated by a "ghost" consumer that reads and claims a job and then
never does anything again: exactly what a kill -9 leaves behind. The real kill -9 tests are
in step 7.
"""

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import pytest_asyncio
from pydantic import TypeAdapter
from redis.asyncio import Redis

from queue_service.broker import RedisBroker
from queue_service.config import Settings
from queue_service.db import create_pool, migrate
from queue_service.executor import Executor
from queue_service.ingest import accept_job
from queue_service.schemas import JobRequest
from queue_service.store import Claim, PgStore, replay_dead
from queue_service.worker import Worker

_jobs: TypeAdapter[Any] = TypeAdapter(JobRequest)

FAST = {
    "lease_s": 1.0,
    "block_ms": 50,
    "reclaim_interval_s": 0.1,
    "dispatch_interval_s": 0.05,
    "dispatch_grace_s": 0.3,
    "backoff_base_s": 0.01,
    "backoff_cap_s": 0.05,
    "drain_deadline_s": 2.0,
    "job_timeout_s": 5.0,
    "cpu_workers": 1,
    "max_in_flight": 8,
}


async def wait_until(
    pred: Callable[[], Awaitable[bool]], timeout_s: float = 15.0, what: str = "condition"
) -> float:
    """Poll ``pred`` until true; return elapsed seconds. Fails with a clear message."""
    start = time.monotonic()
    while not await pred():
        if time.monotonic() - start > timeout_s:
            raise AssertionError(f"timed out after {timeout_s}s waiting for {what}")
        await asyncio.sleep(0.02)
    return time.monotonic() - start


@dataclass
class Env:
    pg_url: str
    redis_url: str
    pool: asyncpg.Pool[Any]
    redis: Redis
    store: PgStore
    broker: RedisBroker
    running: list[tuple[Worker, asyncio.Task[None], Executor]] = field(default_factory=list)

    def settings(self, **overrides: Any) -> Settings:
        return Settings(database_url=self.pg_url, redis_url=self.redis_url, **{**FAST, **overrides})

    def start(self, **overrides: Any) -> Worker:
        s = self.settings(**overrides)
        executor = Executor(s.cpu_workers)
        w = Worker(s, PgStore(self.pool, 2.0), self.broker, executor)
        self.running.append((w, asyncio.create_task(w.run()), executor))
        return w

    async def stop(self, w: Worker) -> None:
        for worker, task, executor in self.running:
            if worker is w:
                w.stop()
                await asyncio.wait_for(task, 10)
                executor.close()
        self.running = [r for r in self.running if r[0] is not w]

    async def submit(
        self, job: dict[str, Any], *, publish: bool = True, max_attempts: int = 5
    ) -> UUID:
        accepted = await accept_job(
            self.pool,
            _jobs.validate_python(job),
            None,
            max_attempts=max_attempts,
            acquire_timeout_s=2,
        )
        if publish:
            entry = await self.broker.publish(accepted.job_id, accepted.epoch)
            await self.store.set_entry(accepted.job_id, accepted.epoch, entry)
            async with self.pool.acquire() as c:
                await c.execute(
                    "UPDATE jobs SET dispatched_at = now() WHERE id = $1", accepted.job_id
                )
        return accepted.job_id

    async def job(self, job_id: UUID) -> asyncpg.Record:
        async with self.pool.acquire() as c:
            row: asyncpg.Record | None = await c.fetchrow(
                "SELECT * FROM jobs WHERE id = $1", job_id
            )
        assert row is not None
        return row

    async def state_is(self, job_id: UUID, state: str) -> bool:
        return bool((await self.job(job_id))["state"] == state)

    async def outcomes(self, job_id: UUID) -> list[str]:
        async with self.pool.acquire() as c:
            rows = await c.fetch(
                "SELECT outcome FROM job_attempts WHERE job_id = $1 ORDER BY epoch, attempt", job_id
            )
        return [r["outcome"] for r in rows]

    async def result_count(self, job_id: UUID) -> int:
        async with self.pool.acquire() as c:
            return int(await c.fetchval("SELECT count(*) FROM results WHERE job_id = $1", job_id))

    async def stream_is_empty(self) -> bool:
        """Nothing left in the stream and nothing pending: every entry was acked + deleted."""
        length = await self.redis.xlen("jobs")
        groups = await self.redis.xinfo_groups("jobs")
        pending = sum(int(g["pending"]) for g in groups)
        return bool(length == 0 and pending == 0)


@pytest_asyncio.fixture
async def env(pg_url: str, redis_url: str, clean: None) -> AsyncIterator[Env]:
    pool = await create_pool(pg_url, max_size=20)
    await migrate(pool)
    redis = Redis.from_url(redis_url, decode_responses=True)
    broker = RedisBroker(redis, "jobs", "workers", "jobs:dead")
    await broker.ensure_group()
    e = Env(pg_url, redis_url, pool, redis, PgStore(pool, 2.0), broker)
    yield e
    for w, _, _ in list(e.running):
        await e.stop(w)
    await redis.aclose()
    await pool.close()


def io(ms: int = 5) -> dict[str, Any]:
    return {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": ms}}


def fail(kind: str, failures: int = 1) -> dict[str, Any]:
    return {"type": "fail", "schema_version": 1, "payload": {"kind": kind, "failures": failures}}


def cpu(iterations: int) -> dict[str, Any]:
    return {"type": "cpu_hash", "schema_version": 1, "payload": {"iterations": iterations}}


# ---- happy path and retries --------------------------------------------------------------


async def test_job_is_done_once_and_entry_is_acked_and_deleted(env: Env) -> None:
    job_id = await env.submit(io())
    env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done")
    assert (await env.job(job_id))["attempts"] == 1
    assert await env.result_count(job_id) == 1
    await wait_until(env.stream_is_empty, what="stream drained")


async def test_transient_failures_retry_with_backoff_then_succeed(env: Env) -> None:
    job_id = await env.submit(fail("transient", failures=2))
    env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done")
    assert await env.outcomes(job_id) == ["retry", "retry", "done"]
    assert await env.result_count(job_id) == 1


async def test_permanent_failure_dead_letters_immediately_with_reason(env: Env) -> None:
    job_id = await env.submit(fail("permanent"))
    env.start()
    await wait_until(lambda: env.state_is(job_id, "dead"), what="dead")
    row = await env.job(job_id)
    assert (row["attempts"], row["dead_reason"]) == (1, "permanent")
    assert await env.outcomes(job_id) == ["dead:permanent"]
    dlq: list[Any] = await env.redis.xrange("jobs:dead")  # type: ignore[assignment]
    assert [f["job_id"] for _, f in dlq] == [str(job_id)]


async def test_exhausted_retries_dead_letter_with_full_history(env: Env) -> None:
    job_id = await env.submit(fail("transient", failures=10), max_attempts=3)
    env.start()
    await wait_until(lambda: env.state_is(job_id, "dead"), what="dead")
    assert (await env.job(job_id))["dead_reason"] == "exhausted"
    assert await env.outcomes(job_id) == ["retry", "retry", "dead:exhausted"]
    async with env.pool.acquire() as c:
        classes = await c.fetch("SELECT error_class FROM job_attempts WHERE job_id = $1", job_id)
    assert {r["error_class"] for r in classes} == {"TransientError"}


# ---- duplicates, fencing --------------------------------------------------------------------


async def test_duplicate_deliveries_execute_once(env: Env) -> None:
    job_id = await env.submit(io())
    for _ in range(2):  # two extra copies of the same job in the stream
        await env.broker.publish(job_id, 1)
    w = env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done")
    await wait_until(env.stream_is_empty, what="all copies acked")
    assert (await env.job(job_id))["attempts"] == 1
    assert await env.result_count(job_id) == 1
    assert w.stats["duplicate_deliveries"] == 2


async def test_fence_rejects_a_zombie_after_takeover(env: Env) -> None:
    job_id = await env.submit(io(), publish=False)
    zombie = await env.store.claim(job_id, 1, "zombie", lease_s=30)
    assert isinstance(zombie, Claim)
    async with env.pool.acquire() as c:  # zombie stalls past its lease
        await c.execute(
            "UPDATE jobs SET lease_until = now() - interval '1 s' WHERE id = $1", job_id
        )
    successor = await env.store.claim(job_id, 1, "successor", lease_s=30)
    assert isinstance(successor, Claim)
    assert successor.attempt == 2

    assert await env.store.heartbeat(zombie, 30) is False
    assert await env.store.complete(zombie, {"by": "zombie"}) is False
    assert await env.store.complete(successor, {"by": "successor"}) is True
    async with env.pool.acquire() as c:
        assert await c.fetchval("SELECT result->>'by' FROM results WHERE job_id = $1", job_id) == (
            "successor"
        )
    assert await env.outcomes(job_id) == ["lost_lease", "done"]


# ---- crash recovery -------------------------------------------------------------------------


async def _ghost_holds(env: Env, job_id: UUID, lease_s: float = 1.0) -> None:
    """A worker that read and claimed the job, then died (kill -9)."""
    msgs = await env.broker.read("ghost", 10, 100)
    assert [m.job_id for m in msgs] == [job_id]
    assert isinstance(await env.store.claim(job_id, 1, "ghost", lease_s=lease_s), Claim)


async def test_dead_workers_job_is_reclaimed_after_the_lease(env: Env) -> None:
    job_id = await env.submit(io())
    await _ghost_holds(env, job_id)
    w = env.start()
    elapsed = await wait_until(lambda: env.state_is(job_id, "done"), what="reclaimed and done")
    assert (await env.job(job_id))["attempts"] == 2  # the crash counted as an attempt
    assert await env.outcomes(job_id) == ["lost_lease", "done"]
    assert w.stats["reclaimed"] >= 1
    assert elapsed >= 0.5  # not before the lease could have expired (lease_s=1.0)


async def test_job_that_keeps_killing_workers_is_dead_lettered(env: Env) -> None:
    job_id = await env.submit(io(), max_attempts=2)
    await _ghost_holds(env, job_id)
    async with env.pool.acquire() as c:  # pretend this was already the last allowed attempt
        await c.execute("UPDATE jobs SET attempts = 2 WHERE id = $1", job_id)
        await c.execute("UPDATE job_attempts SET attempt = 2 WHERE job_id = $1", job_id)
    env.start()
    await wait_until(lambda: env.state_is(job_id, "dead"), what="dead")
    assert (await env.job(job_id))["dead_reason"] == "crashed_max_attempts"
    assert await env.redis.xlen("jobs:dead") == 1


# ---- dispatcher: Redis is not the source of truth -------------------------------------------


async def test_undispatched_job_is_published_by_the_dispatcher(env: Env) -> None:
    job_id = await env.submit(io(), publish=False)  # API committed, then Redis was down
    w = env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done")
    assert w.stats["republished"] >= 1


async def test_redis_wipe_loses_nothing(env: Env) -> None:
    lost = [await env.submit(io()) for _ in range(5)]
    await env.redis.flushall()  # stream, group and PEL all gone
    # The API keeps publishing before any worker recreates the group: these entries land in
    # a group-less stream. If the group were recreated at "$" they would be skipped, and the
    # dispatcher would never republish them (their entries exist), so they would be stuck.
    after = [await env.submit(io()) for _ in range(5)]
    ids = lost + after
    env.start()

    async def all_done() -> bool:
        return all([await env.state_is(j, "done") for j in ids])

    await wait_until(all_done, what="all 10 done")
    assert [await env.result_count(j) for j in ids] == [1] * 10


async def test_dispatcher_does_not_republish_entries_still_in_the_stream(env: Env) -> None:
    job_id = await env.submit(io())
    async with env.pool.acquire() as c:  # old enough to be a dispatch candidate
        await c.execute(
            "UPDATE jobs SET dispatched_at = now() - interval '1 h' WHERE id = $1", job_id
        )
    w = Worker(env.settings(), env.store, env.broker, Executor(1))
    assert await w.dispatch_once() == 0
    assert await env.redis.xlen("jobs") == 1  # no duplicate created


# ---- graceful shutdown, replay ----------------------------------------------------------------


async def test_sigterm_releases_unfinished_job_without_charging_an_attempt(env: Env) -> None:
    job_id = await env.submit(io(ms=10_000))
    w = env.start(drain_deadline_s=0.2)
    await wait_until(lambda: env.state_is(job_id, "running"), what="running")
    await env.stop(w)

    row = await env.job(job_id)
    assert row["state"] == "queued"
    assert (row["attempts"], row["max_attempts"]) == (1, 6)  # budget refunded, token kept
    assert await env.outcomes(job_id) == ["released"]
    assert await env.redis.xlen("jobs") == 1  # old entry acked+deleted, new one published

    async with env.pool.acquire() as c:
        await c.execute("UPDATE jobs SET payload = '{\"duration_ms\": 5}' WHERE id = $1", job_id)
    env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done by next worker")
    assert (await env.job(job_id))["attempts"] == 2


async def test_replay_runs_a_dead_job_again_as_a_new_epoch(env: Env) -> None:
    job_id = await env.submit(fail("transient", failures=3), max_attempts=2)
    env.start()
    await wait_until(lambda: env.state_is(job_id, "dead"), what="dead")

    assert await replay_dead(env.pool, job_ids=[job_id], max_attempts=5) == [job_id]
    await wait_until(lambda: env.state_is(job_id, "done"), what="done after replay")
    row = await env.job(job_id)
    assert row["epoch"] == 2
    assert await env.outcomes(job_id) == [
        "retry",
        "dead:exhausted",  # epoch 1 ends dead...
        "retry",
        "retry",
        "retry",
        "done",  # ...epoch 2 ends done
    ]


# ---- CPU jobs -------------------------------------------------------------------------------


async def test_cpu_job_runs_in_the_process_pool(env: Env) -> None:
    job_id = await env.submit(cpu(1000))
    env.start()
    await wait_until(lambda: env.state_is(job_id, "done"), what="done")
    async with env.pool.acquire() as c:
        digest = await c.fetchval("SELECT result->>'digest' FROM results WHERE job_id = $1", job_id)
    assert isinstance(digest, str)
    assert len(digest) == 64


async def test_cpu_timeout_kills_the_child_and_frees_the_slot(env: Env) -> None:
    # One CPU process. The first job would run for minutes; if the timeout did not kill the
    # child, the second job could never get the process and would time out too.
    stuck = await env.submit(cpu(50_000_000), max_attempts=1)
    env.start(job_timeout_s=0.5, cpu_workers=1)
    await wait_until(lambda: env.state_is(stuck, "dead"), what="stuck job dead")
    assert (await env.job(stuck))["dead_reason"] == "exhausted"

    quick = await env.submit(cpu(1000))
    await wait_until(lambda: env.state_is(quick, "done"), timeout_s=5, what="next CPU job done")
