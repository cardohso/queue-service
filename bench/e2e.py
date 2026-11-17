"""End-to-end capacity of the real system vs number of worker processes.

Real Postgres and Redis (containers), real `qs worker` processes, real fenced commits. For
each (job kind, worker count): start the workers, pre-load a backlog of N jobs straight into
Postgres and the stream (so the API is not the bottleneck: this measures the workers), then
measure until every job is final.

  throughput   N / (last attempt finished - first attempt started)   (jobs per second)
  latency      per attempt, claim -> committed (job_attempts.finished_at - started_at), p50/95/99
               This is service latency including real overhead: Postgres round trips,
               event-loop scheduling among max_in_flight concurrent jobs, process-pool hand-off.
               It is NOT end-to-end latency from submission (everything is queued at once, so
               that would just measure the backlog).

I/O jobs: 50 ms wait.  CPU jobs: ~10 ms of sha256 each (same calibration as bench.executors),
one CPU child process per worker, so scaling workers scales cores.
Every configuration is repeated RUNS times; tables show median and min-max.

Run: ``uv run python -m bench.e2e``  (needs Docker; about 5 minutes)
"""

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Any

import asyncpg
from redis.asyncio import Redis
from tests.failure.harness import Cluster, Infra, wait_until

from bench.common import machine_info, percentile, save, settle
from bench.executors import calibrate_cpu_iterations
from queue_service.schemas import stream_for

RUNS = int(os.environ.get("QS_BENCH_RUNS", "3"))
WORKERS = (1, 2, 4)
N_JOBS = {"io": 2000, "cpu": 600}
MAX_IN_FLIGHT = 32
IO_MS = 50


def payload(kind: str, iterations: int) -> tuple[str, str]:
    if kind == "io":
        return "io_sleep", f'{{"duration_ms": {IO_MS}}}'
    return "cpu_hash", f'{{"iterations": {iterations}}}'


async def preload(infra: Infra, kind: str, n: int, iterations: int) -> None:
    job_type, body = payload(kind, iterations)
    conn = await asyncpg.connect(infra.pg_url)
    try:
        rows = await conn.fetch(
            """INSERT INTO jobs (request_hash, type, schema_version, payload, max_attempts,
                                 dispatched_at)
               SELECT 'bench', $1, 1, $2::jsonb, 3, now() FROM generate_series(1, $3)
               RETURNING id""",
            job_type,
            body,
            n,
        )
    finally:
        await conn.close()
    redis = Redis.from_url(infra.redis_url, decode_responses=True)
    try:
        pipe = redis.pipeline(transaction=False)
        for r in rows:
            pipe.xadd(stream_for("jobs", job_type), {"job_id": str(r["id"]), "epoch": "1"})
        await pipe.execute()
    finally:
        await redis.aclose()


async def reset(infra: Infra) -> None:
    conn = await asyncpg.connect(infra.pg_url)
    try:
        await conn.execute("DROP TABLE IF EXISTS results, job_attempts, jobs CASCADE")
    finally:
        await conn.close()
    redis = Redis.from_url(infra.redis_url)
    try:
        await redis.flushall()  # workers recreate the group on NOGROUP
    finally:
        await redis.aclose()


async def clear_jobs(infra: Infra) -> None:
    """Between runs: empty tables (the schema stays) and the stream."""
    conn = await asyncpg.connect(infra.pg_url)
    try:
        await conn.execute("TRUNCATE results, job_attempts, jobs CASCADE")
    finally:
        await conn.close()
    redis = Redis.from_url(infra.redis_url)
    try:
        await redis.flushall()  # workers recreate the group on NOGROUP
    finally:
        await redis.aclose()


async def measure(infra: Infra, n: int) -> dict[str, Any]:
    conn = await asyncpg.connect(infra.pg_url)
    try:

        async def all_final() -> bool:
            row = await conn.fetchrow("SELECT count(*) AS n FROM jobs WHERE state = 'done'")
            return bool(row and row["n"] == n)

        await wait_until(all_final, 300, f"{n} jobs done", 0.05)
        span = await conn.fetchrow(
            "SELECT min(started_at) AS t0, max(finished_at) AS t1 FROM job_attempts"
        )
        durs = [
            float(r["d"])  # extract() returns numeric -> Decimal, which json can't write
            for r in await conn.fetch(
                "SELECT extract(epoch FROM finished_at - started_at) AS d FROM job_attempts"
            )
        ]
        extra = await conn.fetchval("SELECT count(*) - $1 FROM job_attempts", n)
    finally:
        await conn.close()
    assert span is not None
    elapsed = (span["t1"] - span["t0"]).total_seconds()
    return {
        "jobs": n,
        "elapsed_s": elapsed,
        "throughput": n / elapsed,
        "p50_ms": percentile(durs, 0.50) * 1000,
        "p95_ms": percentile(durs, 0.95) * 1000,
        "p99_ms": percentile(durs, 0.99) * 1000,
        "extra_executions": int(extra),
    }


async def main() -> None:
    load_at_start = settle()
    iterations = calibrate_cpu_iterations()
    infra = Infra().start()
    results: list[dict[str, Any]] = []
    try:
        with tempfile.TemporaryDirectory() as td:
            for kind in ("io", "cpu"):
                for w in WORKERS:
                    c = Cluster(infra, Path(td) / f"{kind}-{w}")
                    c.log_dir.mkdir(parents=True)
                    await reset(infra)
                    env = {
                        "QS_MAX_IN_FLIGHT": str(MAX_IN_FLIGHT),
                        "QS_CPU_WORKERS": "1",
                        "QS_FAULT_INJECTION": "0",
                        "QS_DISPATCH_GRACE_S": "600",  # nothing here needs re-dispatching
                        "QS_JOB_TIMEOUT_S": "60",
                        "QS_LEASE_S": "30",
                    }
                    try:
                        await c.api()  # creates the schema
                        for i in range(w):
                            await c.worker(f"w{i}", env)
                        for run in range(1, RUNS + 1):
                            await clear_jobs(infra)
                            await asyncio.sleep(0.5)  # let workers notice NOGROUP / go idle
                            await preload(infra, kind, N_JOBS[kind], iterations)
                            r = await measure(infra, N_JOBS[kind])
                            results.append({"kind": kind, "workers": w, "run": run, **r})
                            print(
                                f"{kind:<3} W={w} run {run}: {r['throughput']:7.1f} jobs/s "
                                f"p50 {r['p50_ms']:.0f} p99 {r['p99_ms']:.0f} ms "
                                f"extra {r['extra_executions']}",
                                flush=True,
                            )
                    finally:
                        c.stop_all()
    finally:
        infra.stop()
    path = save(
        "e2e",
        {
            "machine": machine_info(),
            "load_avg_at_start": load_at_start,
            "params": {
                "runs": RUNS,
                "workers": list(WORKERS),
                "jobs": N_JOBS,
                "max_in_flight_per_worker": MAX_IN_FLIGHT,
                "io_wait_ms": IO_MS,
                "cpu_job_iterations": iterations,
                "cpu_children_per_worker": 1,
            },
            "results": results,
        },
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main())
