"""Real Postgres and Redis via testcontainers; one container pair per test session."""

from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from contextlib import AsyncExitStack
from typing import Any

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from redis.asyncio import Redis
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer

from queue_service.api import create_app
from queue_service.config import Settings

pytestmark = pytest.mark.integration

MakeClient = Callable[..., Coroutine[Any, Any, tuple[httpx.AsyncClient, FastAPI]]]


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    with PostgresContainer("postgres:17-alpine", driver=None) as pg:
        yield pg.get_connection_url()


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    with RedisContainer("redis:8-alpine") as r:
        yield f"redis://{r.get_container_host_ip()}:{r.get_exposed_port(6379)}/0"


@pytest_asyncio.fixture
async def clean(pg_url: str, redis_url: str) -> AsyncIterator[None]:
    """Fresh state per test. The app recreates the tables when it starts."""
    conn = await asyncpg.connect(pg_url)
    try:
        await conn.execute("DROP TABLE IF EXISTS results, job_attempts, jobs")
    finally:
        await conn.close()
    redis = Redis.from_url(redis_url)
    try:
        await redis.flushall()
    finally:
        await redis.aclose()
    yield


@pytest_asyncio.fixture
async def make_client(pg_url: str, redis_url: str, clean: None) -> AsyncIterator[MakeClient]:
    """Start an app against the containers. Overrides are Settings fields.

    The background sampler is effectively off (long interval); tests call
    ``app.state.deps.sampler.sample_once()`` so depth changes are explicit, not timing-based.
    """
    stack = AsyncExitStack()

    async def make(**overrides: Any) -> tuple[httpx.AsyncClient, FastAPI]:
        overrides.setdefault("redis_url", redis_url)
        settings = Settings(database_url=pg_url, sample_interval_s=3600.0, **overrides)
        app = create_app(settings)
        await stack.enter_async_context(app.router.lifespan_context(app))
        client = await stack.enter_async_context(
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
        )
        return client, app

    yield make
    await stack.aclose()


@pytest_asyncio.fixture
async def db(pg_url: str, clean: None) -> AsyncIterator[Callable[[], Any]]:
    """Open a direct connection for assertions about what is really stored."""
    conns: list[asyncpg.Connection[Any]] = []

    async def connect() -> asyncpg.Connection[Any]:
        c = await asyncpg.connect(pg_url)
        conns.append(c)
        return c

    yield connect
    for c in conns:
        await c.close()
