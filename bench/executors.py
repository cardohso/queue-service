"""asyncio vs threads vs processes, for I/O-bound and CPU-bound jobs (DESIGN.md s8).

Question answered: when W jobs run concurrently, how much total work gets done and how long
does each job take, depending on how the work is executed?

Method (identical for every mode so the numbers are comparable)
* W "slots" each run jobs back to back for a fixed window (DURATION_S), all stopping at the
  same absolute deadline. Throughput = jobs completed inside the window / window length.
* Latency of a job = its own wall time from start to finish (queueing is excluded on purpose:
  here the question is overhead and contention, not backlog). p50/p95/p99 pooled per run.
* I/O job: wait 20 ms (a stand-in for a network call; not a real socket).
* CPU job: repeated sha256, sized by calibration to about 10 ms of single-thread CPU.
* asyncio mode runs the CPU job *inline on the event loop* (no offload) because that is the
  thing being measured. The offload case is the `processes` mode.
* Every configuration is repeated RUNS times; the report shows median and min-max.
* The process pool is created and warmed before the clock starts.

Run: ``uv run python -m bench.executors``  (about 4 minutes)
"""

import asyncio
import hashlib
import multiprocessing as mp
import os
import sys
import threading
import time
from typing import Any

from bench.common import machine_info, percentile, save

DURATION_S = float(sys.argv[1]) if len(sys.argv) > 1 else 2.0
RUNS = int(os.environ.get("QS_BENCH_RUNS", "5"))
WORKERS = (1, 2, 4, 8)
IO_WAIT_S = 0.020
CPU_TARGET_S = 0.010
MODES = ("asyncio", "threads", "processes")
KINDS = ("io", "cpu")


def cpu_work(iterations: int) -> None:
    digest = b"seed"
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()


def calibrate_cpu_iterations() -> int:
    """Iterations for about CPU_TARGET_S of CPU. Best of 5 longer repeats: a single short
    sample once landed on a slower core and under-sized the job by 1.7x."""
    n = 100_000
    cpu_work(n)  # warm up (frequency ramp, caches)
    best = min(_timed(n) for _ in range(5))
    return max(1, round(CPU_TARGET_S / (best / n)))


def _timed(n: int) -> float:
    t = time.perf_counter()
    cpu_work(n)
    return time.perf_counter() - t


# ---- one slot per mode -----------------------------------------------------------------------


def _blocking_job(kind: str, iterations: int) -> None:
    if kind == "io":
        time.sleep(IO_WAIT_S)
    else:
        cpu_work(iterations)


def _loop_until(deadline: float, kind: str, iterations: int) -> list[float]:
    """Run jobs back to back until the wall-clock deadline (time.time(), shared by processes)."""
    lat: list[float] = []
    while time.time() < deadline:
        t = time.perf_counter()
        _blocking_job(kind, iterations)
        lat.append(time.perf_counter() - t)
    return lat


def run_threads(kind: str, workers: int, iterations: int, deadline: float) -> list[float]:
    results: list[list[float]] = [[] for _ in range(workers)]

    def slot(i: int) -> None:
        results[i] = _loop_until(deadline, kind, iterations)

    threads = [threading.Thread(target=slot, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return [x for r in results for x in r]


def run_asyncio(
    kind: str, workers: int, iterations: int, deadline: float
) -> tuple[list[float], float]:
    max_lag = 0.0

    async def probe() -> None:
        nonlocal max_lag
        loop = asyncio.get_running_loop()
        while True:
            expected = loop.time() + 0.01
            await asyncio.sleep(0.01)
            max_lag = max(max_lag, loop.time() - expected)

    async def slot() -> list[float]:
        lat: list[float] = []
        while time.time() < deadline:
            t = time.perf_counter()
            if kind == "io":
                await asyncio.sleep(IO_WAIT_S)
            else:
                cpu_work(iterations)  # inline: blocks the loop. That is the point.
            lat.append(time.perf_counter() - t)
        return lat

    async def main() -> list[float]:
        p = asyncio.create_task(probe())
        out = await asyncio.gather(*(slot() for _ in range(workers)))
        p.cancel()
        return [x for r in out for x in r]

    lat = asyncio.run(main())
    return lat, max_lag


def _proc_slot(kind: str, iterations: int, deadline: float) -> list[float]:
    return _loop_until(deadline, kind, iterations)


def run_processes(
    pool: Any, kind: str, workers: int, iterations: int, deadline: float
) -> list[float]:
    asyncs = [pool.apply_async(_proc_slot, (kind, iterations, deadline)) for _ in range(workers)]
    return [x for a in asyncs for x in a.get()]


# ---- driver ----------------------------------------------------------------------------------


def one_run(
    mode: str, kind: str, workers: int, iterations: int, pools: dict[int, Any]
) -> dict[str, Any]:
    start_at = time.time() + (1.0 if mode == "processes" else 0.05)  # let slots get ready
    deadline = start_at + DURATION_S
    lag_ms: float | None = None
    while time.time() < start_at:
        time.sleep(0.001)
    if mode == "threads":
        lat = run_threads(kind, workers, iterations, deadline)
    elif mode == "asyncio":
        lat, lag = run_asyncio(kind, workers, iterations, deadline)
        lag_ms = lag * 1000
    else:
        lat = run_processes(pools[workers], kind, workers, iterations, deadline)
    return {
        "mode": mode,
        "kind": kind,
        "workers": workers,
        "jobs": len(lat),
        "throughput": len(lat) / DURATION_S,
        "p50_ms": percentile(lat, 0.50) * 1000,
        "p95_ms": percentile(lat, 0.95) * 1000,
        "p99_ms": percentile(lat, 0.99) * 1000,
        "loop_lag_max_ms": lag_ms,
    }


def main() -> None:
    load_at_start = os.getloadavg()  # 1/5/15 min: this is a shared laptop, record how busy it was
    iterations = calibrate_cpu_iterations()
    print(f"CPU job = {iterations} sha256 iterations (~{CPU_TARGET_S * 1000:.0f} ms)", flush=True)
    ctx = mp.get_context("spawn")
    pools: dict[int, Any] = {}
    for w in WORKERS:
        pools[w] = ctx.Pool(w)
        pools[w].map(cpu_work, [10] * w)  # warm: children started and imports done
    results: list[dict[str, Any]] = []
    try:
        for kind in KINDS:
            for mode in MODES:
                for w in WORKERS:
                    for run in range(1, RUNS + 1):
                        r = one_run(mode, kind, w, iterations, pools) | {"run": run}
                        results.append(r)
                    last = [
                        x
                        for x in results
                        if (x["mode"], x["kind"], x["workers"]) == (mode, kind, w)
                    ]
                    tp = sorted(x["throughput"] for x in last)
                    print(
                        f"{kind:<3} {mode:<9} W={w}: {tp[len(tp) // 2]:8.1f} jobs/s "
                        f"[{tp[0]:.1f}-{tp[-1]:.1f}] p99 {last[-1]['p99_ms']:.1f} ms",
                        flush=True,
                    )
    finally:
        for p in pools.values():
            p.terminate()
    path = save(
        "executors",
        {
            "machine": machine_info(),
            "load_avg_at_start": load_at_start,
            "params": {
                "duration_s": DURATION_S,
                "runs": RUNS,
                "workers": list(WORKERS),
                "io_wait_ms": IO_WAIT_S * 1000,
                "cpu_job_iterations": iterations,
                "cpu_job_target_ms": CPU_TARGET_S * 1000,
            },
            "results": results,
        },
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
