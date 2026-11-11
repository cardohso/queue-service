"""Runs job handlers: I/O jobs on the event loop, CPU jobs in a process pool (DESIGN.md s8).

Why pebble instead of ``concurrent.futures.ProcessPoolExecutor``:
1. Cancelling a stdlib future does not stop a task that is already running, so a timed-out
   CPU job would keep its process busy forever. Pebble terminates the child.
2. One crashed child breaks a stdlib pool for every job (``BrokenProcessPool``). Pebble
   replaces the dead child and fails only that task (``ProcessExpired``).
"""

import asyncio
import hashlib
import os
from typing import Any

from pebble import ProcessPool

from queue_service.errors import PermanentError, TransientError
from queue_service.schemas import CpuHashPayload, FailPayload, IoSleepPayload, PoisonPayload
from queue_service.store import Claim


def cpu_hash(iterations: int) -> str:
    """CPU-bound and GIL-holding: the benchmark workload for step 8."""
    digest = b"seed"
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()


def _child_exit() -> str:
    os._exit(1)  # the child dies without a Python exception: like a segfault or OOM kill


def _spin() -> str:
    while True:
        pass


class Executor:
    def __init__(self, cpu_workers: int, *, fault_injection: bool = False) -> None:
        self._pool = ProcessPool(max_workers=cpu_workers)
        self._fault_injection = fault_injection

    async def run(self, claim: Claim, timeout_s: float) -> dict[str, Any]:
        if claim.type == "io_sleep":
            io = IoSleepPayload.model_validate(claim.payload)
            await asyncio.sleep(io.duration_ms / 1000)
            return {"slept_ms": io.duration_ms}

        if claim.type == "cpu_hash":
            cpu = CpuHashPayload.model_validate(claim.payload)
            # Pebble's own timeout kills the child; the caller's asyncio timeout also cancels
            # the future, which pebble turns into terminating the child.
            future = self._pool.schedule(cpu_hash, args=(cpu.iterations,), timeout=timeout_s)
            return {"digest": await asyncio.wrap_future(future)}

        if claim.type == "fail":
            f = FailPayload.model_validate(claim.payload)
            if f.kind == "permanent":
                raise PermanentError("injected permanent failure")
            if claim.attempt <= f.failures:
                raise TransientError(f"injected transient failure {claim.attempt}/{f.failures}")
            return {"succeeded_on_attempt": claim.attempt}

        if claim.type == "poison":
            if not self._fault_injection:
                raise PermanentError("fault injection is disabled on this worker")
            p = PoisonPayload.model_validate(claim.payload)
            if p.mode == "worker_exit":
                os._exit(137)  # the whole worker dies mid-job, like kill -9 / OOM
            target = _child_exit if p.mode == "child_exit" else _spin
            future = self._pool.schedule(target, timeout=timeout_s)
            await asyncio.wrap_future(future)
            return {}

        raise PermanentError(f"unknown job type {claim.type!r}")

    def close(self) -> None:
        self._pool.stop()  # type: ignore[no-untyped-call]
        self._pool.join(timeout=5)
