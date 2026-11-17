"""How many CPU jobs should one worker take at once? (DESIGN.md s8.1)

One worker with one CPU process, a backlog of CPU jobs, ``QS_MAX_CPU_IN_FLIGHT`` = 1..8.
Too few slots leave the process idle during claim and commit round trips; too many just move
the queue back inside the worker (latency up, throughput flat).

Run: ``PYTHONPATH=. uv run python -m bench.cpu_slots``  (needs Docker; about 5 minutes)
"""

import asyncio
import tempfile
from pathlib import Path
from typing import Any

from tests.failure.harness import Cluster, Infra

from bench.common import machine_info, save, settle
from bench.e2e import clear_jobs, measure, preload, reset
from bench.executors import calibrate_cpu_iterations

SLOTS = (1, 2, 3, 4, 8)
RUNS = 3
JOBS = 600


async def main() -> None:
    load = settle()
    iterations = calibrate_cpu_iterations()
    infra = Infra().start()
    results: list[dict[str, Any]] = []
    try:
        with tempfile.TemporaryDirectory() as td:
            for slots in SLOTS:
                c = Cluster(infra, Path(td) / f"slots-{slots}")
                c.log_dir.mkdir(parents=True)
                await reset(infra)
                env = {
                    "QS_MAX_CPU_IN_FLIGHT": str(slots),
                    "QS_CPU_WORKERS": "1",
                    "QS_FAULT_INJECTION": "0",
                    "QS_DISPATCH_GRACE_S": "600",
                    "QS_JOB_TIMEOUT_S": "60",
                    "QS_LEASE_S": "30",
                }
                try:
                    await c.api()
                    await c.worker("w0", env)
                    for run in range(1, RUNS + 1):
                        await clear_jobs(infra)
                        await asyncio.sleep(0.5)
                        await preload(infra, "cpu", JOBS, iterations)
                        r = await measure(infra, JOBS)
                        results.append({"slots": slots, "run": run, **r})
                        print(f"slots {slots} run {run}: {r['throughput']:.1f} jobs/s "
                              f"p50 {r['p50_ms']:.1f} ms", flush=True)  # fmt: skip
                finally:
                    c.stop_all()
    finally:
        infra.stop()
    path = save(
        "cpu_slots",
        {
            "machine": machine_info(),
            "load_avg_at_start": load,
            "params": {"jobs": JOBS, "runs": RUNS, "slots": list(SLOTS),
                       "cpu_job_iterations": iterations, "cpu_children": 1},
            "results": results,
        },
    )  # fmt: skip
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main())
