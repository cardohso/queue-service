"""Accepting jobs: the durable commit, then a best-effort dispatch (DESIGN.md s5.1)."""

import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import RedisError

from queue_service.schemas import AnyJob, request_hash

log = logging.getLogger(__name__)


class IdempotencyConflict(Exception):
    """The idempotency key was already used with a different request body."""


@dataclass(frozen=True, slots=True)
class Accepted:
    job_id: UUID
    epoch: int
    created: bool  # False when an earlier submission with the same key is returned


async def accept_job(
    pool: asyncpg.Pool[Any],
    job: AnyJob,
    idempotency_key: str | None,
    *,
    max_attempts: int,
    acquire_timeout_s: float,
) -> Accepted:
    """Insert the job row; return once it is durably committed.

    A single INSERT is its own transaction, so when this returns the row is committed. Two
    concurrent requests with the same key race on the UNIQUE constraint: ON CONFLICT DO NOTHING
    makes the loser wait for the winner's commit, then see no row to insert and read the winner's.
    """
    digest = request_hash(job)
    async with pool.acquire(timeout=acquire_timeout_s) as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO jobs (idempotency_key, request_hash, type, schema_version, payload,
                              max_attempts)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING id, epoch
            """,
            idempotency_key,
            digest,
            job.type,
            job.schema_version,
            job.payload.model_dump(mode="json"),
            max_attempts,
        )
        if row is not None:
            return Accepted(row["id"], row["epoch"], created=True)

        existing = await conn.fetchrow(
            "SELECT id, epoch, request_hash FROM jobs WHERE idempotency_key = $1",
            idempotency_key,
        )
    if existing is None:  # unreachable: rows are never deleted while the key is in use
        raise RuntimeError("idempotency conflict without an existing row")
    if existing["request_hash"] != digest:
        raise IdempotencyConflict(idempotency_key)
    return Accepted(existing["id"], existing["epoch"], created=False)


class RedisPublisher:
    """Best-effort XADD. Failure is fine: the dispatcher republishes from Postgres."""

    def __init__(self, redis: Redis, stream: str) -> None:
        self._redis = redis
        self._stream = stream

    async def publish(self, job_id: UUID, epoch: int) -> str | None:
        """Return the stream entry ID, or None if Redis is unavailable."""
        try:
            entry_id = await self._redis.xadd(
                self._stream, {"job_id": str(job_id), "epoch": str(epoch)}
            )
        except RedisError, OSError, TimeoutError:
            log.warning("dispatch_failed", extra={"job_id": str(job_id)})
            return None
        return str(entry_id)


async def mark_dispatched(
    pool: asyncpg.Pool[Any], job_id: UUID, entry_id: str, *, acquire_timeout_s: float
) -> None:
    """Record where the job was published, so the dispatcher can tell if that entry is lost."""
    try:
        async with pool.acquire(timeout=acquire_timeout_s) as conn:
            await conn.execute(
                "UPDATE jobs SET dispatched_at = now(), stream_entry_id = $2 WHERE id = $1",
                job_id,
                entry_id,
            )
    except asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError:
        # Harmless: an unmarked job is simply republished later, and the claim is conditional.
        log.warning("mark_dispatched_failed", extra={"job_id": str(job_id)})
