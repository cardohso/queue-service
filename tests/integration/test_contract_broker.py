"""Broker contract: every test runs against FakeBroker AND RedisBroker."""

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from uuid import uuid4

import pytest
import pytest_asyncio
from redis.asyncio import Redis

from queue_service.broker import Broker, RedisBroker
from tests.fakes import FakeBroker, FakeClock


@dataclass
class B:
    broker: Broker
    wipe: Callable[[], Awaitable[None]]


@pytest_asyncio.fixture(params=["fake", "redis"])
async def b(request: pytest.FixtureRequest, redis_url: str, clean: None) -> AsyncIterator[B]:
    if request.param == "fake":
        fake = FakeBroker(FakeClock())

        async def wipe_fake() -> None:
            fake.wipe()

        await fake.ensure_group()
        yield B(fake, wipe_fake)
        return
    redis = Redis.from_url(redis_url, decode_responses=True)
    real = RedisBroker(redis, "jobs", "workers", "jobs:dead")
    await real.ensure_group()

    async def wipe_real() -> None:
        await redis.flushall()

    yield B(real, wipe_real)
    await redis.aclose()


async def test_each_entry_is_delivered_to_one_consumer_once(b: B) -> None:
    job = uuid4()
    eid = await b.broker.publish(job, 1)
    [m] = await b.broker.read("c1", 10, 10)
    assert (m.entry_id, m.job_id, m.epoch) == (eid, job, 1)
    assert await b.broker.read("c2", 10, 10) == []
    assert await b.broker.read("c1", 10, 10) == []


async def test_read_respects_count(b: B) -> None:
    for _ in range(5):
        await b.broker.publish(uuid4(), 1)
    assert len(await b.broker.read("c1", 2, 10)) == 2
    assert len(await b.broker.read("c1", 10, 10)) == 3


async def test_ack_deletes_the_entry(b: B) -> None:
    eid = await b.broker.publish(uuid4(), 1)
    await b.broker.read("c1", 10, 10)
    assert await b.broker.entries_exist([eid]) == {eid: True}
    await b.broker.ack(eid)
    assert await b.broker.entries_exist([eid]) == {eid: False}
    assert await b.broker.autoclaim("c2", 0, 10) == []


async def test_unacked_entry_can_be_autoclaimed_once_idle(b: B) -> None:
    job = uuid4()
    await b.broker.publish(job, 1)
    await b.broker.read("dead-worker", 10, 10)
    assert await b.broker.autoclaim("c2", 3_600_000, 10) == []  # not idle for an hour
    [m] = await b.broker.autoclaim("c2", 0, 10)
    assert m.job_id == job


async def test_unread_entry_is_not_autoclaimed(b: B) -> None:
    await b.broker.publish(uuid4(), 1)
    assert await b.broker.autoclaim("c1", 0, 10) == []


async def test_after_a_wipe_entries_published_before_group_recreation_are_delivered(b: B) -> None:
    await b.broker.publish(uuid4(), 1)
    await b.wipe()
    survivor = uuid4()
    await b.broker.publish(survivor, 1)  # stream recreated, but no group yet
    assert await b.broker.read("c1", 10, 10) == []  # NOGROUP -> group recreated
    [m] = await b.broker.read("c1", 10, 10)
    assert m.job_id == survivor


async def test_entries_exist_is_false_for_unknown_ids(b: B) -> None:
    assert await b.broker.entries_exist(["1-0"]) == {"1-0": False}
    assert await b.broker.entries_exist([]) == {}
