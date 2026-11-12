"""Open-loop load generator, run as a separate process.

Why a separate process: generating hundreds of requests per second from the pytest process
itself made the *generator* fall behind (its event loop ran 270-400 ms late while the API's
was idle), so the test measured the client, not the server. Several generator processes, each
with a bounded connection pool, keep the offered load honest.

Usage: python -m tests.failure.loadgen URL RATE SECONDS KEY_PREFIX OUT_JSON
Writes {"results": [[status, latency_s, job_id_or_null, has_retry_after], ...],
        "send_lateness": [...], "elapsed_s": float}
Latency is measured from each request's *scheduled* time (no coordinated omission).
"""

import asyncio
import json
import sys
import time
from typing import Any

import httpx

JOB = {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": 50}}


async def main(url: str, rate: float, seconds: float, prefix: str) -> dict[str, Any]:
    results: list[list[Any]] = []
    lateness: list[float] = []

    async def one(client: httpx.AsyncClient, i: int, scheduled: float) -> None:
        lateness.append(time.monotonic() - scheduled)
        try:
            r = await client.post("/jobs", json=JOB, headers={"Idempotency-Key": f"{prefix}-{i}"})
        except httpx.HTTPError as exc:
            results.append(
                [f"error:{type(exc).__name__}", time.monotonic() - scheduled, None, False]
            )
            return
        job_id = r.json()["job_id"] if r.status_code == 202 else None
        results.append(
            [r.status_code, time.monotonic() - scheduled, job_id, "retry-after" in r.headers]
        )

    limits = httpx.Limits(max_connections=64, max_keepalive_connections=64)
    async with httpx.AsyncClient(base_url=url, timeout=30, limits=limits) as client:
        start = time.monotonic()
        tasks = []
        for i in range(int(rate * seconds)):
            scheduled = start + i / rate
            delay = scheduled - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            tasks.append(asyncio.create_task(one(client, i, scheduled)))
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start
    return {"results": results, "send_lateness": lateness, "elapsed_s": elapsed}


if __name__ == "__main__":
    u, r, s, p, o = sys.argv[1:6]
    data = asyncio.run(main(u, float(r), float(s), p))
    with open(o, "w") as f:  # written after the loop is done: no blocking I/O inside it
        json.dump(data, f)
