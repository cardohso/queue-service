"""Event-loop lag: proof that CPU jobs in the process pool do not block the event loop.

Thresholds are deliberately wide (blocking ~0.5 s vs. a 0.15 s bound) so a noisy CI machine
cannot flip the result: the signal is an order of magnitude, not a fine measurement. The
benchmark in step 8 reports the precise numbers.
"""

import asyncio
import time
from uuid import uuid4

from prometheus_client import CollectorRegistry, Histogram

from queue_service.executor import Executor, cpu_hash
from queue_service.metrics import LoopLag
from queue_service.store import Claim

ITERATIONS = 2_000_000  # roughly half a second of hashing on a modern laptop


def _lag() -> LoopLag:
    hist = Histogram("lag", "x", registry=CollectorRegistry(), buckets=(0.01, 0.1, 1))
    return LoopLag(hist, interval_s=0.02)


async def _warm_up(lag: LoopLag) -> asyncio.Task[None]:
    task = asyncio.create_task(lag.run())
    await asyncio.sleep(0.2)  # let it tick a few times: healthy baseline
    lag.max_lag_s = 0.0
    return task


async def test_a_blocking_call_shows_up_as_loop_lag() -> None:
    lag = _lag()
    task = await _warm_up(lag)
    started = time.monotonic()
    cpu_hash(ITERATIONS)  # runs on the event loop thread: nothing else can run
    blocked_for = time.monotonic() - started
    await asyncio.sleep(0.1)
    task.cancel()
    assert blocked_for > 0.15, "workload too small to prove anything on this machine"
    assert lag.max_lag_s > 0.5 * blocked_for


def _claim(iterations: int) -> Claim:
    return Claim(
        job_id=uuid4(),
        epoch=1,
        attempt=1,
        max_attempts=3,
        worker_id="w",
        type="cpu_hash",
        payload={"iterations": iterations},
    )


async def test_cpu_job_in_the_process_pool_does_not_block_the_loop() -> None:
    executor = Executor(cpu_workers=1)
    lag = _lag()
    try:
        await executor.run(_claim(10), 30)  # spawn the child first: its start-up is not the job
        task = await _warm_up(lag)
        started = time.monotonic()
        await executor.run(_claim(ITERATIONS), 30)
        ran_for = time.monotonic() - started
        task.cancel()
    finally:
        executor.close()
    assert ran_for > 0.15, "workload too small to prove anything on this machine"
    assert lag.max_lag_s < 0.15, f"loop lagged {lag.max_lag_s:.3f}s during a {ran_for:.2f}s job"
