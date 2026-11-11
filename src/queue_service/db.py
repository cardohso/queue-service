"""Postgres schema and connection pool. Postgres is the source of truth (DESIGN.md s2)."""

import json
from typing import Any

import asyncpg

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
