"""Edge back-pressure: decide whether to admit a request, from a sampled queue-depth signal.

The depth is sampled in the background, so a request costs one comparison and no I/O. The limit
is therefore soft: depth can overshoot by whatever is accepted between two samples.
"""

import asyncio
import logging
import math
from typing import Any

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import RedisError

log = logging.getLogger(__name__)

MAX_RETRY_AFTER_S = 30


class Admission:
    def __init__(self, max_depth: int, drain_per_s: float) -> None:
        self._max_depth = max_depth
        self._drain_per_s = drain_per_s
        self.depth = 0
        self.source = "none"

    def update(self, depth: int, source: str) -> None:
        self.depth = depth
        self.source = source

    def retry_after(self) -> int | None:
        """Seconds the client should wait, or None to admit the request."""
        if self.depth <= self._max_depth:
            return None
        excess = self.depth - self._max_depth
        return max(1, min(MAX_RETRY_AFTER_S, math.ceil(excess / self._drain_per_s)))


class DepthSampler:
    """Measures queue depth: consumer-group lag + pending from Redis, else Postgres backlog.

    Redis is the primary signal because it is what KEDA scales on in phase 2, which keeps
    admission and autoscaling consistent. If Redis is unreachable the API must still admit
    work (DESIGN.md s7), so we fall back to counting unfinished jobs in Postgres.
    """

    def __init__(
        self,
        *,
        admission: Admission,
        redis: Redis,
        pool: asyncpg.Pool[Any],
        stream: str,
        group: str,
        interval_s: float,
    ) -> None:
        self._admission = admission
        self._redis = redis
        self._pool = pool
        self._stream = stream
        self._group = group
        self._interval_s = interval_s

    async def sample_once(self) -> None:
        try:
            self._admission.update(await self._redis_depth(), "redis")
            return
        except RedisError, OSError, TimeoutError:
            log.warning("depth_sample_redis_failed")
        try:
            async with self._pool.acquire(timeout=1.0) as conn:
                backlog = await conn.fetchval(
                    "SELECT count(*) FROM jobs WHERE state IN ('queued', 'running', 'retry_wait')"
                )
            self._admission.update(int(backlog), "postgres")
        except asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError:
            # Keep the last known depth; request handling reports 503 on its own.
            log.warning("depth_sample_postgres_failed")

    async def _redis_depth(self) -> int:
        try:
            groups = await self._redis.xinfo_groups(self._stream)
        except RedisError as exc:
            if "no such key" in str(exc).lower():  # stream not created yet: nothing queued
                return 0
            raise
        for g in groups:
            if g["name"] == self._group:
                lag = g.get("lag")
                if lag is not None:
                    return int(lag) + int(g["pending"])
                break  # Redis reports lag=None when it cannot tell; use stream length instead
        return int(await self._redis.xlen(self._stream))

    async def run(self) -> None:
        # The caller takes the first sample at startup, so sleep first: no sample races startup.
        while True:
            await asyncio.sleep(self._interval_s)
            await self.sample_once()
