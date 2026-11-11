"""Command line: ``qs worker`` and ``qs replay``."""

import argparse
import asyncio
import logging
import signal
from uuid import UUID

from redis.asyncio import Redis

from queue_service.broker import RedisBroker
from queue_service.config import Settings
from queue_service.db import create_pool, migrate
from queue_service.executor import Executor
from queue_service.store import PgStore, replay_dead
from queue_service.worker import Worker


async def run_worker(settings: Settings) -> None:
    pool = await create_pool(settings.database_url, max_size=settings.max_in_flight + 4)
    await migrate(pool)
    redis = Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=settings.worker_redis_timeout_s,
        socket_timeout=settings.worker_redis_timeout_s,
    )
    executor = Executor(settings.cpu_workers)
    worker = Worker(
        settings,
        PgStore(pool, settings.worker_db_acquire_timeout_s),
        RedisBroker(redis, settings.stream, settings.group, settings.dead_stream),
        executor,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, worker.stop)
    logging.info("worker_started id=%s", worker.worker_id)
    try:
        await worker.run()
    finally:
        executor.close()
        await redis.aclose()
        await pool.close()
    logging.info("worker_stopped stats=%s", dict(worker.stats))


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
    rp = sub.add_parser("replay", help="move dead-lettered jobs back to queued (new epoch)")
    group = rp.add_mutually_exclusive_group(required=True)
    group.add_argument("--job-id", action="append", type=UUID, dest="job_ids")
    group.add_argument("--all-dead", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    settings = Settings.from_env()
    if args.cmd == "worker":
        asyncio.run(run_worker(settings))
    else:
        replayed = asyncio.run(run_replay(settings, None if args.all_dead else args.job_ids))
        print(f"replayed {len(replayed)} job(s)")
        for job_id in replayed:
            print(job_id)


if __name__ == "__main__":
    main()
