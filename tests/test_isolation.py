"""Suspect jobs run alone (DESIGN.md s9.1)."""

import asyncio

from prometheus_client import CollectorRegistry

from queue_service.config import Settings
from queue_service.isolation import IsolationLock
from queue_service.metrics import WorkerMetrics
from queue_service.store import Claim
from queue_service.worker import Worker
from tests.fakes import FakeBroker, FakeClock, FakeStore

# ---- the lock --------------------------------------------------------------------------------


async def _settle() -> None:
    for _ in range(20):
        await asyncio.sleep(0)


async def test_shared_holders_run_together() -> None:
    lock, inside = IsolationLock(), 0
    peak = 0
    gate = asyncio.Event()

    async def job() -> None:
        nonlocal inside, peak
        async with lock.shared():
            inside += 1
            peak = max(peak, inside)
            await gate.wait()
            inside -= 1

    tasks = [asyncio.create_task(job()) for _ in range(3)]
    await _settle()
    assert peak == 3
    gate.set()
    await asyncio.gather(*tasks)


async def test_exclusive_waits_for_shared_and_blocks_new_shared() -> None:
    lock = IsolationLock()
    events: list[str] = []
    release_first = asyncio.Event()

    async def normal(name: str, gate: asyncio.Event | None) -> None:
        async with lock.shared():
            events.append(f"{name} start")
            if gate:
                await gate.wait()
            events.append(f"{name} end")

    async def suspect() -> None:
        async with lock.exclusive():
            events.append("suspect start")
            await asyncio.sleep(0)
            events.append("suspect end")

    first = asyncio.create_task(normal("n1", release_first))
    await _settle()
    s = asyncio.create_task(suspect())
    await _settle()
    assert lock.isolating  # a suspect is waiting: the worker must stop pulling work
    late = asyncio.create_task(normal("n2", None))  # arrives while the suspect waits
    await _settle()
    assert events == ["n1 start"]  # neither the suspect nor the late job got in
    release_first.set()
    await asyncio.gather(first, s, late)
    assert events == ["n1 start", "n1 end", "suspect start", "suspect end", "n2 start", "n2 end"]
    assert not lock.isolating


async def test_cancelled_exclusive_waiter_does_not_wedge_the_lock() -> None:
    lock = IsolationLock()
    hold = asyncio.Event()

    async def normal() -> None:
        async with lock.shared():
            await hold.wait()

    n = asyncio.create_task(normal())
    await _settle()
    s = asyncio.create_task(lock.exclusive().__aenter__())
    await _settle()
    s.cancel()  # e.g. graceful shutdown while waiting
    await asyncio.gather(s, return_exceptions=True)
    assert not lock.isolating
    async with asyncio.timeout(1):  # new shared holders are admitted again
        async with lock.shared():
            pass
    hold.set()
    await n


# ---- the worker ------------------------------------------------------------------------------


class Gated:
    def __init__(self) -> None:
        self.gates: dict[object, asyncio.Event] = {}
        self.running: set[object] = set()
        self.max_concurrent_with_suspect = 0
        self.suspect: object = None

    async def run(self, claim: Claim, timeout_s: float) -> dict[str, object]:
        self.running.add(claim.job_id)
        if self.suspect in self.running:
            self.max_concurrent_with_suspect = max(
                self.max_concurrent_with_suspect, len(self.running)
            )
        gate = self.gates.get(claim.job_id)
        if gate is not None:
            await gate.wait()
        self.running.discard(claim.job_id)
        return {}


async def test_a_job_whose_last_attempt_crashed_runs_alone() -> None:
    clock = FakeClock()
    store, broker, ex = FakeStore(clock), FakeBroker(clock), Gated()
    reg = CollectorRegistry()
    w = Worker(
        Settings(lease_s=10.0, commit_retry_s=0.0), store, broker, ex,
        worker_id="w1", metrics=WorkerMetrics(reg),
    )  # fmt: skip
    await broker.ensure_group()

    # A suspect: claimed by a worker that then died; its lease has expired.
    suspect_id = store.accept("io_sleep", {"duration_ms": 1}, 3)
    await broker.publish(suspect_id, 1)
    (s_msg,) = await broker.read("ghost", 1, 0)
    assert isinstance(await store.claim(suspect_id, 1, "ghost", 10.0), Claim)
    clock.advance(11)
    ex.suspect = suspect_id

    # A normal job already running on this worker.
    normal_id = store.accept("io_sleep", {"duration_ms": 1}, 3)
    await broker.publish(normal_id, 1)
    (n_msg,) = await broker.read("w1", 1, 0)
    ex.gates[normal_id] = asyncio.Event()
    normal = asyncio.create_task(w._handle(n_msg))
    await _settle()
    assert normal_id in ex.running

    suspect = asyncio.create_task(w._handle(s_msg))
    await _settle()
    assert suspect_id not in ex.running, "the suspect must wait for the normal job to finish"
    assert w._isolation.isolating

    ex.gates[normal_id].set()
    await asyncio.gather(normal, suspect)
    assert store.jobs[suspect_id].state == "done"
    assert store.jobs[suspect_id].attempts == 2  # the crash still counted, once
    assert ex.max_concurrent_with_suspect == 1  # it really ran alone
    assert reg.get_sample_value("qs_isolated_runs_total") == 1
