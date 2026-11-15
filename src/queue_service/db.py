"""Postgres schema and connection pool. Postgres is the source of truth (DESIGN.md s2)."""

import asyncio
import json
import logging
from typing import Any

import asyncpg

from queue_service.errors import DB_INFRA_ERRORS

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text UNIQUE,
    request_hash    text NOT NULL,
    type            text NOT NULL,
    schema_version  integer NOT NULL,
    payload         jsonb NOT NULL,
    state           text NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'retry_wait', 'done', 'dead')),
    attempts        integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL CHECK (max_attempts > 0),
    epoch           integer NOT NULL DEFAULT 1,
    lease_owner     text,
    lease_until     timestamptz,
    next_attempt_at timestamptz,
    dispatched_at   timestamptz,
    -- Redis entry of the latest dispatch; the dispatcher republishes only if it is gone.
    stream_entry_id text,
    dead_reason     text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS jobs_expired_lease_idx
    ON jobs (lease_until) WHERE state = 'running';

-- Dispatcher scans (DESIGN.md s5.3): due retries and queued-but-undispatched jobs.
CREATE INDEX IF NOT EXISTS jobs_retry_due_idx
    ON jobs (next_attempt_at) WHERE state = 'retry_wait';
CREATE INDEX IF NOT EXISTS jobs_undispatched_idx
    ON jobs (created_at) WHERE state = 'queued';

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id      uuid NOT NULL REFERENCES jobs (id),
    epoch       integer NOT NULL,
    attempt     integer NOT NULL,
    worker_id   text NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text,
    error_class text,
    error       text,
    PRIMARY KEY (job_id, epoch, attempt)
);

-- At most one result per job: the idempotent-effects guarantee (DESIGN.md s1).
CREATE TABLE IF NOT EXISTS results (
    job_id       uuid PRIMARY KEY REFERENCES jobs (id),
    epoch        integer NOT NULL,
    result       jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now()
);
"""

_MIGRATION_LOCK_ID = 727_274_001


async def _init_connection(conn: asyncpg.Connection[Any]) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog", format="text"
    )


async def create_pool(url: str, *, max_size: int = 10) -> asyncpg.Pool[Any]:
    return await asyncpg.create_pool(url, min_size=1, max_size=max_size, init=_init_connection)


async def migrate(pool: asyncpg.Pool[Any]) -> None:
    """Create tables if missing. An advisory lock makes concurrent starts safe."""
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", _MIGRATION_LOCK_ID)
        try:
            await conn.execute(SCHEMA)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", _MIGRATION_LOCK_ID)


async def open_pool_with_retry(
    url: str,
    *,
    max_size: int,
    stop: asyncio.Event | None = None,
    give_up_after_s: float | None = None,
    initial_delay_s: float = 0.2,
    max_delay_s: float = 5.0,
) -> asyncpg.Pool[Any] | None:
    """Connect and create the schema, retrying infrastructure errors with capped back-off.

    A process that starts while Postgres is full, restarting or unreachable (a scale-up, a
    rolling deploy, a database failover) should wait for it, not crash and rely on its
    supervisor to retry. Returns None if ``stop`` is set while waiting. With
    ``give_up_after_s`` the last error is raised after that long (API start-up).
    """
    loop = asyncio.get_running_loop()
    started = loop.time()
    delay = initial_delay_s
    while True:
        pool: asyncpg.Pool[Any] | None = None
        try:
            pool = await create_pool(url, max_size=max_size)
            await migrate(pool)
            return pool
        except DB_INFRA_ERRORS as exc:
            if pool is not None:
                await pool.close()
            if give_up_after_s is not None and loop.time() - started > give_up_after_s:
                raise
            log.warning("db_connect_retry", extra={"error": repr(exc), "retry_in_s": delay})
            if stop is None:
                await asyncio.sleep(delay)
            else:
                try:
                    async with asyncio.timeout(delay):
                        await stop.wait()
                    return None
                except TimeoutError:
                    pass
            delay = min(delay * 2, max_delay_s)


async def warn_if_pool_exceeds_server(pool: asyncpg.Pool[Any], pool_max: int, who: str) -> None:
    """Log the connection budget. N workers x pool size must fit under max_connections."""
    try:
        async with pool.acquire(timeout=5) as conn:
            server_max = int(await conn.fetchval("SHOW max_connections"))
    except DB_INFRA_ERRORS:
        return
    level = logging.WARNING if pool_max * 2 > server_max else logging.INFO
    log.log(
        level,
        "db_connection_budget",
        extra={"who": who, "pool_max": pool_max, "server_max_connections": server_max},
    )
