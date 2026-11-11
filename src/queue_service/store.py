"""The job state machine in Postgres (DESIGN.md s4, s5.2, s5.3).

Every write after a claim is *fenced*: it only applies if (epoch, attempts, lease_owner) still
match what this worker claimed. ``attempts`` is the fencing token and only ever increases
within an epoch, so a worker that lost its lease (crash, GC pause, partition) can never
overwrite the attempt that replaced it.
"""

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

import asyncpg
from asyncpg.pool import PoolConnectionProxy

type Conn = asyncpg.Connection[Any] | PoolConnectionProxy[Any]


@dataclass(frozen=True, slots=True)
class Claim:
    job_id: UUID
    epoch: int
    attempt: int  # fencing token
    max_attempts: int
    worker_id: str
    type: str
    payload: dict[str, Any]


@dataclass(frozen=True, slots=True)
class Rejected:
    """The delivery is not ours to run. The message is acked and dropped."""

    # missing | stale_epoch | done | dead | retry_wait | running_elsewhere | crashed_out
    # | needs_isolation (a takeover the caller did not allow: the message is NOT acked)
    reason: str
    dead_lettered: bool = False  # True when this claim attempt itself moved the job to dead


@dataclass(frozen=True, slots=True)
class DispatchItem:
    job_id: UUID
    epoch: int
    entry_id: str | None  # None means "must publish"; otherwise publish only if entry is gone


class Store(Protocol):
    async def ping(self) -> None: ...
    async def claim(
        self,
        job_id: UUID,
        epoch: int,
        worker_id: str,
        lease_s: float,
        *,
        allow_takeover: bool = True,
    ) -> Claim | Rejected: ...
    async def heartbeat(self, claim: Claim, lease_s: float) -> bool: ...
    async def complete(self, claim: Claim, result: dict[str, Any]) -> bool: ...
    async def retry_later(
        self, claim: Claim, delay_s: float, error_class: str | None, error: str | None
    ) -> bool: ...
    async def dead_letter(
        self, claim: Claim, reason: str, error_class: str | None, error: str | None
    ) -> bool: ...
    async def release(self, claim: Claim) -> bool: ...
    async def take_dispatch_batch(self, limit: int, grace_s: float) -> list[DispatchItem]: ...
    async def set_entry(self, job_id: UUID, epoch: int, entry_id: str) -> None: ...


_FENCE = "id = $1 AND epoch = $2 AND attempts = $3 AND lease_owner = $4 AND state = 'running'"


def _updated(status: str) -> bool:
    return status.split()[-1] == "1"


class PgStore:
    def __init__(self, pool: asyncpg.Pool[Any], acquire_timeout_s: float) -> None:
        self._pool = pool
        self._timeout = acquire_timeout_s

    async def ping(self) -> None:
        async with self._pool.acquire(timeout=self._timeout) as conn:
            await conn.execute("SELECT 1")

    async def claim(
        self,
        job_id: UUID,
        epoch: int,
        worker_id: str,
        lease_s: float,
        *,
        allow_takeover: bool = True,
    ) -> Claim | Rejected:
        """Claim a job. A takeover (the previous holder crashed) happens only if allowed;
        otherwise the job is reported as ``needs_isolation`` and nothing changes."""
        async with self._pool.acquire(timeout=self._timeout) as conn, conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT state, epoch, attempts, max_attempts, type, payload, lease_owner,
                       (lease_until IS NOT NULL AND lease_until < now()) AS lease_expired
                FROM jobs WHERE id = $1 FOR UPDATE
                """,
                job_id,
            )
            if row is None:
                return Rejected("missing")
            if row["epoch"] != epoch:
                return Rejected("stale_epoch")
            state = row["state"]
            takeover = state == "running" and row["lease_expired"]
            if state == "running" and not takeover:
                return Rejected("running_elsewhere")
            if state != "queued" and not takeover:
                return Rejected(state)
            if takeover and not allow_takeover:
                return Rejected("needs_isolation")

            if takeover:
                # The previous holder died (or stalled past its lease). Its attempt counts:
                # that is what stops a job that kills workers from crash-looping forever.
                await conn.execute(
                    """
                    UPDATE job_attempts SET finished_at = now(), outcome = 'lost_lease'
                    WHERE job_id = $1 AND epoch = $2 AND attempt = $3 AND finished_at IS NULL
                    """,
                    job_id,
                    epoch,
                    row["attempts"],
                )
                if row["attempts"] >= row["max_attempts"]:
                    await conn.execute(
                        """
                        UPDATE jobs SET state = 'dead', dead_reason = 'crashed_max_attempts',
                               lease_owner = NULL, lease_until = NULL, updated_at = now()
                        WHERE id = $1
                        """,
                        job_id,
                    )
                    return Rejected("crashed_out", dead_lettered=True)

            attempt = await conn.fetchval(
                """
                UPDATE jobs SET state = 'running', attempts = attempts + 1, lease_owner = $2,
                       lease_until = now() + make_interval(secs => $3), updated_at = now()
                WHERE id = $1
                RETURNING attempts
                """,
                job_id,
                worker_id,
                lease_s,
            )
            await conn.execute(
                "INSERT INTO job_attempts (job_id, epoch, attempt, worker_id) VALUES ($1,$2,$3,$4)",
                job_id,
                epoch,
                attempt,
                worker_id,
            )
            return Claim(
                job_id=job_id,
                epoch=epoch,
                attempt=attempt,
                max_attempts=row["max_attempts"],
                worker_id=worker_id,
                type=row["type"],
                payload=row["payload"],
            )

    async def heartbeat(self, claim: Claim, lease_s: float) -> bool:
        async with self._pool.acquire(timeout=self._timeout) as conn:
            status = await conn.execute(
                f"UPDATE jobs SET lease_until = now() + make_interval(secs => $5) WHERE {_FENCE}",
                *_fence_args(claim),
                lease_s,
            )
        return _updated(status)

    async def complete(self, claim: Claim, result: dict[str, Any]) -> bool:
        async with self._pool.acquire(timeout=self._timeout) as conn, conn.transaction():
            if not await self._finish(conn, claim, "done", None, None):
                return False
            # Plain INSERT on purpose: the fence already guarantees one winner, so a unique
            # violation here would mean the invariant is broken and must fail loudly.
            await conn.execute(
                "INSERT INTO results (job_id, epoch, result) VALUES ($1, $2, $3)",
                claim.job_id,
                claim.epoch,
                result,
            )
            await _close_attempt(conn, claim, "done", None, None)
            return True

    async def retry_later(
        self, claim: Claim, delay_s: float, error_class: str | None, error: str | None
    ) -> bool:
        async with self._pool.acquire(timeout=self._timeout) as conn, conn.transaction():
            if not await self._finish(conn, claim, "retry_wait", delay_s, None):
                return False
            await _close_attempt(conn, claim, "retry", error_class, error)
            return True

    async def dead_letter(
        self, claim: Claim, reason: str, error_class: str | None, error: str | None
    ) -> bool:
        async with self._pool.acquire(timeout=self._timeout) as conn, conn.transaction():
            if not await self._finish(conn, claim, "dead", None, reason):
                return False
            await _close_attempt(conn, claim, f"dead:{reason}", error_class, error)
            return True

    async def release(self, claim: Claim) -> bool:
        """Give a job back on graceful shutdown without charging it an attempt.

        We refund budget (max_attempts + 1) instead of decrementing ``attempts``, because the
        fencing token must never go backwards: a reused token could let a stale write through.
        """
        async with self._pool.acquire(timeout=self._timeout) as conn, conn.transaction():
            status = await conn.execute(
                f"""
                UPDATE jobs SET state = 'queued', max_attempts = max_attempts + 1,
                       lease_owner = NULL, lease_until = NULL, dispatched_at = now(),
                       stream_entry_id = NULL, updated_at = now()
                WHERE {_FENCE}
                """,
                *_fence_args(claim),
            )
            if not _updated(status):
                return False
            await _close_attempt(conn, claim, "released", None, None)
            return True

    async def take_dispatch_batch(self, limit: int, grace_s: float) -> list[DispatchItem]:
        """Pick jobs that may need (re)publishing, and stamp them so peers skip them.

        Stamping ``dispatched_at`` works as a short dispatch lease: N workers can run this
        loop concurrently (SKIP LOCKED + the stamp) without publishing the same job N times.
        """
        async with self._pool.acquire(timeout=self._timeout) as conn:
            rows = await conn.fetch(
                """
                WITH c AS (
                    SELECT id FROM jobs
                    WHERE (state = 'retry_wait' AND next_attempt_at <= now())
                       OR (state = 'queued'
                           AND coalesce(dispatched_at, created_at)
                               < now() - make_interval(secs => $2))
                       OR (state = 'running'
                           AND lease_until < now() - make_interval(secs => $2)
                           AND coalesce(dispatched_at, created_at)
                               < now() - make_interval(secs => $2))
                    ORDER BY coalesce(next_attempt_at, dispatched_at, created_at)
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE jobs j SET
                    -- SET expressions read the OLD row, so these CASEs all test the old state.
                    stream_entry_id = CASE WHEN j.state = 'retry_wait' THEN NULL
                                           ELSE j.stream_entry_id END,
                    next_attempt_at = CASE WHEN j.state = 'retry_wait' THEN NULL
                                           ELSE j.next_attempt_at END,
                    state = CASE WHEN j.state = 'retry_wait' THEN 'queued' ELSE j.state END,
                    dispatched_at = now(),
                    updated_at = now()
                FROM c WHERE j.id = c.id
                RETURNING j.id, j.epoch, j.stream_entry_id
                """,
                limit,
                grace_s,
            )
        return [DispatchItem(r["id"], r["epoch"], r["stream_entry_id"]) for r in rows]

    async def set_entry(self, job_id: UUID, epoch: int, entry_id: str) -> None:
        async with self._pool.acquire(timeout=self._timeout) as conn:
            await conn.execute(
                "UPDATE jobs SET stream_entry_id = $3 WHERE id = $1 AND epoch = $2",
                job_id,
                epoch,
                entry_id,
            )

    async def _finish(
        self,
        conn: Conn,
        claim: Claim,
        state: str,
        delay_s: float | None,
        reason: str | None,
    ) -> bool:
        status = await conn.execute(
            f"""
            UPDATE jobs SET state = $5, lease_owner = NULL, lease_until = NULL,
                   next_attempt_at = CASE WHEN $6::float8 IS NULL THEN NULL
                                          ELSE now() + make_interval(secs => $6::float8) END,
                   dead_reason = $7, updated_at = now()
            WHERE {_FENCE}
            """,
            *_fence_args(claim),
            state,
            delay_s,
            reason,
        )
        return _updated(status)


def _fence_args(claim: Claim) -> tuple[UUID, int, int, str]:
    return (claim.job_id, claim.epoch, claim.attempt, claim.worker_id)


async def _close_attempt(
    conn: Conn,
    claim: Claim,
    outcome: str,
    error_class: str | None,
    error: str | None,
) -> None:
    await conn.execute(
        """
        UPDATE job_attempts SET finished_at = now(), outcome = $4, error_class = $5, error = $6
        WHERE job_id = $1 AND epoch = $2 AND attempt = $3
        """,
        claim.job_id,
        claim.epoch,
        claim.attempt,
        outcome,
        error_class,
        error,
    )


async def replay_dead(
    pool: asyncpg.Pool[Any], *, job_ids: list[UUID] | None, max_attempts: int
) -> list[UUID]:
    """Move dead jobs back to queued as a new epoch. The dispatcher publishes them.

    The invariant is per (job_id, epoch): the old epoch keeps its final state ``dead`` in the
    attempt history, and the new epoch must reach its own final state.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE jobs SET state = 'queued', epoch = epoch + 1, attempts = 0,
                   max_attempts = $2, dead_reason = NULL, dispatched_at = NULL,
                   stream_entry_id = NULL, next_attempt_at = NULL, updated_at = now()
            WHERE state = 'dead' AND ($1::uuid[] IS NULL OR id = ANY($1::uuid[]))
            RETURNING id
            """,
            job_ids,
            max_attempts,
        )
    return [r["id"] for r in rows]
