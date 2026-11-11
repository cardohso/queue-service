"""FastAPI ingest service."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI, Header, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from queue_service.admission import Admission, DepthSampler
from queue_service.config import Settings
from queue_service.db import create_pool, migrate
from queue_service.ingest import (
    IdempotencyConflict,
    RedisPublisher,
    accept_job,
    mark_dispatched,
)
from queue_service.metrics import ApiMetrics, LoopLag
from queue_service.schemas import JobAccepted, JobRequest, JobStatus

log = logging.getLogger(__name__)

_DB_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError)


@dataclass(slots=True)
class Deps:
    settings: Settings
    pool: asyncpg.Pool[Any]
    redis: Redis
    publisher: RedisPublisher
    admission: Admission
    sampler: DepthSampler
    metrics: ApiMetrics
    loop_lag: LoopLag


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    pool = await create_pool(settings.database_url, max_size=settings.db_pool_max_size)
    await migrate(pool)
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.redis_timeout_s,
        socket_timeout=settings.redis_timeout_s,
    )
    metrics = ApiMetrics()
    admission = Admission(
        settings.max_depth, settings.assumed_drain_per_s, on_update=metrics.record_depth
    )
    loop_lag = LoopLag(metrics.loop_lag)
    sampler = DepthSampler(
        admission=admission,
        redis=redis,
        pool=pool,
        stream=settings.stream,
        group=settings.group,
        interval_s=settings.sample_interval_s,
    )
    app.state.deps = Deps(
        settings,
        pool,
        redis,
        RedisPublisher(redis, settings.stream),
        admission,
        sampler,
        metrics,
        loop_lag,
    )
    await sampler.sample_once()
    tasks = [asyncio.create_task(sampler.run()), asyncio.create_task(loop_lag.run())]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await redis.aclose()
        await pool.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(title="queue-service", lifespan=_lifespan)
    app.state.settings = settings or Settings.from_env()

    @app.post("/jobs", status_code=202, response_model=JobAccepted)
    async def submit_job(
        request: Request,
        job: JobRequest,
        idempotency_key: Annotated[str | None, Header(min_length=1, max_length=255)] = None,
    ) -> JobAccepted:
        deps: Deps = request.app.state.deps
        if job.type == "poison" and not deps.settings.fault_injection:
            raise HTTPException(422, detail="fault injection is disabled")
        retry_after = deps.admission.retry_after()
        if retry_after is not None:
            deps.metrics.rejected.labels("429").inc()
            raise HTTPException(
                429,
                detail=f"queue depth {deps.admission.depth} over limit",
                headers={"Retry-After": str(retry_after)},
            )
        try:
            accepted = await accept_job(
                deps.pool,
                job,
                idempotency_key,
                max_attempts=deps.settings.max_attempts,
                acquire_timeout_s=deps.settings.db_acquire_timeout_s,
            )
        except IdempotencyConflict:
            deps.metrics.rejected.labels("409").inc()
            raise HTTPException(
                409, detail="Idempotency-Key reused with a different body"
            ) from None
        except _DB_ERRORS:
            log.exception("accept_failed")
            deps.metrics.rejected.labels("503").inc()
            raise HTTPException(
                503, detail="database unavailable", headers={"Retry-After": "1"}
            ) from None

        # The job is durable. Dispatch is best-effort: if Redis is down or we crash here,
        # the dispatcher republishes it from Postgres (DESIGN.md s5.3).
        if not accepted.created:
            deps.metrics.replays.inc()
        if accepted.created:
            deps.metrics.accepted.inc()
            entry_id = await deps.publisher.publish(accepted.job_id, accepted.epoch)
            if entry_id is not None:
                await mark_dispatched(
                    deps.pool,
                    accepted.job_id,
                    entry_id,
                    acquire_timeout_s=deps.settings.db_acquire_timeout_s,
                )
        return JobAccepted(job_id=accepted.job_id, state="queued")

    @app.get("/jobs/{job_id}", response_model=JobStatus)
    async def get_job(request: Request, job_id: UUID) -> JobStatus:
        deps: Deps = request.app.state.deps
        try:
            async with deps.pool.acquire(timeout=deps.settings.db_acquire_timeout_s) as conn:
                row = await conn.fetchrow(
                    "SELECT id, type, state, attempts, epoch FROM jobs WHERE id = $1", job_id
                )
        except _DB_ERRORS:
            raise HTTPException(503, detail="database unavailable") from None
        if row is None:
            raise HTTPException(404, detail="job not found")
        return JobStatus(
            job_id=row["id"],
            type=row["type"],
            state=row["state"],
            attempts=row["attempts"],
            epoch=row["epoch"],
        )

    @app.get("/healthz")
    async def healthz(request: Request) -> Response:
        """Liveness: the event loop answers and has not been blocked for seconds."""
        alive = request.app.state.deps.loop_lag.is_alive()
        return Response(
            "ok\n" if alive else "event loop stalled\n", status_code=200 if alive else 503
        )

    @app.get("/readyz")
    async def readyz(request: Request) -> Response:
        """Readiness = can accept jobs = Postgres reachable.

        Redis is reported but does NOT gate readiness: with Postgres as the source of truth
        the API keeps accepting while Redis is down (DESIGN.md s7). Failing readiness on a
        Redis blip would take every API pod out of rotation for no benefit.
        """
        deps: Deps = request.app.state.deps
        try:
            async with deps.pool.acquire(timeout=deps.settings.db_acquire_timeout_s) as conn:
                await conn.execute("SELECT 1")
        except _DB_ERRORS:
            return Response("postgres unavailable\n", status_code=503)
        redis_state = "ok" if deps.admission.source == "redis" else "degraded"
        return Response(f"ok redis={redis_state}\n", status_code=200)

    @app.get("/metrics")
    async def metrics(request: Request) -> Response:
        reg = request.app.state.deps.metrics.registry
        return Response(generate_latest(reg), media_type=CONTENT_TYPE_LATEST)

    return app
