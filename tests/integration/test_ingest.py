"""Ingest guarantees against real Postgres and Redis (DESIGN.md s5.1, s7)."""

import asyncio
from collections.abc import Callable
from typing import Any

import httpx
from redis.asyncio import Redis

from tests.integration.conftest import MakeClient

IO_JOB = {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": 5}}


async def _count(db: Callable[[], Any]) -> int:
    conn = await db()
    return int(await conn.fetchval("SELECT count(*) FROM jobs"))


async def test_202_means_committed_and_dispatched(
    make_client: MakeClient, db: Callable[[], Any], redis_url: str
) -> None:
    client, _ = await make_client()
    resp = await client.post("/jobs", json=IO_JOB)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    conn = await db()
    row = await conn.fetchrow(
        "SELECT state, attempts, dispatched_at FROM jobs WHERE id = $1", job_id
    )
    assert row is not None
    assert (row["state"], row["attempts"]) == ("queued", 0)
    assert row["dispatched_at"] is not None

    redis = Redis.from_url(redis_url, decode_responses=True)
    entries: list[Any] = await redis.xrange("jobs")  # type: ignore[assignment]
    await redis.aclose()
    assert [fields["job_id"] for _, fields in entries] == [job_id]

    got = await client.get(f"/jobs/{job_id}")
    assert got.json()["state"] == "queued"
    assert (await client.get("/jobs/00000000-0000-0000-0000-000000000000")).status_code == 404


async def test_same_key_returns_same_job_and_dispatches_once(
    make_client: MakeClient, db: Callable[[], Any], redis_url: str
) -> None:
    client, _ = await make_client()
    headers = {"Idempotency-Key": "k-1"}
    first = await client.post("/jobs", json=IO_JOB, headers=headers)
    second = await client.post("/jobs", json=IO_JOB, headers=headers)
    assert first.status_code == second.status_code == 202
    assert first.json()["job_id"] == second.json()["job_id"]
    assert await _count(db) == 1

    redis = Redis.from_url(redis_url)
    assert await redis.xlen("jobs") == 1
    await redis.aclose()


async def test_concurrent_submissions_with_one_key_create_one_job(
    make_client: MakeClient, db: Callable[[], Any]
) -> None:
    client, _ = await make_client()
    resps = await asyncio.gather(
        *(client.post("/jobs", json=IO_JOB, headers={"Idempotency-Key": "race"}) for _ in range(50))
    )
    assert {r.status_code for r in resps} == {202}
    assert len({r.json()["job_id"] for r in resps}) == 1
    assert await _count(db) == 1


async def test_same_key_different_body_is_409(
    make_client: MakeClient, db: Callable[[], Any]
) -> None:
    client, _ = await make_client()
    headers = {"Idempotency-Key": "k-2"}
    assert (await client.post("/jobs", json=IO_JOB, headers=headers)).status_code == 202
    other = {**IO_JOB, "payload": {"duration_ms": 6}}
    assert (await client.post("/jobs", json=other, headers=headers)).status_code == 409
    assert await _count(db) == 1


async def test_without_key_each_submission_is_a_new_job(
    make_client: MakeClient, db: Callable[[], Any]
) -> None:
    client, _ = await make_client()
    a = await client.post("/jobs", json=IO_JOB)
    b = await client.post("/jobs", json=IO_JOB)
    assert a.json()["job_id"] != b.json()["job_id"]
    assert await _count(db) == 2


async def test_invalid_payload_is_422_and_writes_nothing(
    make_client: MakeClient, db: Callable[[], Any]
) -> None:
    client, _ = await make_client()
    for bad in (
        {**IO_JOB, "schema_version": 2},
        {**IO_JOB, "type": "unknown"},
        {**IO_JOB, "payload": {"duration_ms": -1}},
    ):
        assert (await client.post("/jobs", json=bad)).status_code == 422
    assert await _count(db) == 0


async def test_429_with_retry_after_when_over_limit_then_recovers(
    make_client: MakeClient, db: Callable[[], Any], redis_url: str
) -> None:
    client, app = await make_client(max_depth=5, assumed_drain_per_s=1.0)
    redis = Redis.from_url(redis_url)
    for _ in range(10):  # simulate a backlog nobody is consuming
        await redis.xadd("jobs", {"job_id": "x", "epoch": "1"})
    await app.state.deps.sampler.sample_once()

    resp = await client.post("/jobs", json=IO_JOB)
    assert resp.status_code == 429
    assert resp.headers["Retry-After"] == "5"  # 5 over the limit at 1 job/s
    assert await _count(db) == 0  # rejected means not accepted, not half-written

    await redis.delete("jobs")  # backlog drains
    await app.state.deps.sampler.sample_once()
    assert (await client.post("/jobs", json=IO_JOB)).status_code == 202
    await redis.aclose()


async def test_redis_down_still_accepts_and_leaves_job_for_the_dispatcher(
    make_client: MakeClient, db: Callable[[], Any]
) -> None:
    client, app = await make_client(redis_url="redis://127.0.0.1:1/0")
    assert app.state.deps.admission.source == "postgres"  # fell back from the Redis signal

    resp = await client.post("/jobs", json=IO_JOB)
    assert resp.status_code == 202

    conn = await db()
    row = await conn.fetchrow(
        "SELECT state, dispatched_at FROM jobs WHERE id = $1", resp.json()["job_id"]
    )
    assert row is not None
    assert row["state"] == "queued"
    assert row["dispatched_at"] is None  # the dispatcher will publish it (step 4)


async def test_saturated_db_pool_returns_503_fast_then_recovers(
    make_client: MakeClient,
) -> None:
    client, app = await make_client(db_pool_max_size=1, db_acquire_timeout_s=0.1)
    async with app.state.deps.pool.acquire():  # hold the only connection
        resp = await client.post("/jobs", json=IO_JOB)
        assert resp.status_code == 503
        assert resp.headers["Retry-After"] == "1"
    assert (await client.post("/jobs", json=IO_JOB)).status_code == 202


async def test_ingest_never_blocks_on_unreachable_redis_for_long(
    make_client: MakeClient,
) -> None:
    client, _ = await make_client(redis_url="redis://10.255.255.1:6379/0", redis_timeout_s=0.2)
    async with asyncio.timeout(3):  # connect timeout bounds the dispatch attempt
        resp: httpx.Response = await client.post("/jobs", json=IO_JOB)
    assert resp.status_code == 202
