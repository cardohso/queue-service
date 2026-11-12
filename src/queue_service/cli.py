"""Command line: ``qs worker`` and ``qs replay``."""

import argparse
import asyncio
import logging
import signal
from uuid import UUID

import uvicorn
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis

from queue_service.api import create_app
from queue_service.broker import RedisBroker
from queue_service.config import Settings
from queue_service.db import create_pool, migrate
from queue_service.executor import Executor
from queue_service.logs import configure_logging
from queue_service.ops import Response, start_ops_server, text
from queue_service.store import PgStore, replay_dead
from queue_service.worker import Worker

log = logging.getLogger(__name__)


async def run_worker(settings: Settings) -> None:
    # One connection per in-flight job plus one for its heartbeat, plus loops: a heartbeat
    # that can't get a connection lets a live job's lease expire (a needless duplicate).
    pool = await create_pool(settings.database_url, max_size=2 * settings.max_in_flight + 4)
    await migrate(pool)
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.worker_redis_timeout_s,
        socket_timeout=settings.worker_redis_timeout_s,
    )
    executor = Executor(settings.cpu_workers, fault_injection=settings.fault_injection)
    worker = Worker(
        settings,
        PgStore(pool, settings.worker_db_acquire_timeout_s),
        RedisBroker(redis, settings.stream, settings.group, settings.dead_stream),
        executor,
        worker_id=settings.worker_id or None,
    )

    async def healthz() -> Response:
        return text(200, "ok\n") if worker.alive() else text(503, "event loop stalled\n")

    async def readyz() -> Response:
        return text(200, "ready\n") if await worker.ready() else text(503, "not ready\n")

    async def metrics() -> Response:
        return 200, CONTENT_TYPE_LATEST, generate_latest(worker.metrics.registry)

    ops = await start_ops_server(
        settings.ops_host,
        settings.ops_port,
        {"/healthz": healthz, "/readyz": readyz, "/metrics": metrics},
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    log.info("worker_started", extra={"worker_id": worker.worker_id, "ops_port": settings.ops_port})
    try:
        await worker.run()
    finally:
        ops.close()
        executor.close()
        await redis.aclose()
        await pool.close()
    log.info("worker_stopped", extra={"stats": dict(worker.stats)})


async def run_replay(settings: Settings, job_ids: list[UUID] | None) -> list[UUID]:
    pool = await create_pool(settings.database_url, max_size=2)
    try:
        return await replay_dead(pool, job_ids=job_ids, max_attempts=settings.max_attempts)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="qs")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("worker", help="run a worker until SIGTERM")
    sub.add_parser("api", help="run the ingest API")
    rp = sub.add_parser("replay", help="move dead-lettered jobs back to queued (new epoch)")
    group = rp.add_mutually_exclusive_group(required=True)
    group.add_argument("--job-id", action="append", type=UUID, dest="job_ids")
    group.add_argument("--all-dead", action="store_true")
    args = parser.parse_args()

    settings = Settings.from_env()
    configure_logging(settings.log_level, json_output=settings.log_json)
    if args.cmd == "worker":
        asyncio.run(run_worker(settings))
    elif args.cmd == "api":
        # log_config=None: keep our JSON logging instead of uvicorn's own handlers.
        uvicorn.run(
            create_app(settings),
            host=settings.api_host,
            port=settings.api_port,
            log_config=None,
        )
    else:
        replayed = asyncio.run(run_replay(settings, None if args.all_dead else args.job_ids))
        print(f"replayed {len(replayed)} job(s)")
        for job_id in replayed:
            print(job_id)


if __name__ == "__main__":
    main()
