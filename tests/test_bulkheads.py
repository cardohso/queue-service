"""Per-kind streams and slot budgets (DESIGN.md s8.1).

The property being protected: a CPU job is claimed only when a CPU process is free to run it,
so its lease and timeout never run while it merely queues, and CPU jobs cannot use up the
slots I/O jobs need. Excess CPU jobs wait in Redis, unread, where any worker can take them.
"""

import asyncio
from uuid import UUID

from queue_service.config import Settings
from queue_service.schemas import job_kind, stream_for
from queue_service.store import Claim
from queue_service.worker import Worker
from tests.fakes import FakeBroker, FakeClock, FakeStore


def test_job_types_map_to_kinds_and_streams() -> None:
    assert [job_kind(t) for t in ("io_sleep", "fail", "cpu_hash", "poison")] == [
        "io",
        "io",
        "cpu",
        "cpu",
    ]
    assert stream_for("jobs", "cpu_hash") == "jobs:cpu"
    assert stream_for("jobs", "io_sleep") == "jobs:io"


class Gated:
    """Every job blocks until released; records what is running at once."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.running: dict[str, int] = {"io": 0, "cpu": 0}
        self.peak: dict[str, int] = {"io": 0, "cpu": 0}
        self.started = 0
        self.changed = asyncio.Event()

    async def run(self, claim: Claim, timeout_s: float) -> dict[str, object]:
        k = job_kind(claim.type)
        self.running[k] += 1
        self.peak[k] = max(self.peak[k], self.running[k])
        self.started += 1
        self.changed.set()
        await self.release.wait()
        self.running[k] -= 1
        return {}


class Rig:
    def __init__(self, cpu_slots: int = 1, io_slots: int = 8) -> None:
        self.clock = FakeClock()
        self.store = FakeStore(self.clock)
        self.io, self.cpu = FakeBroker(self.clock, "io"), FakeBroker(self.clock, "cpu")
        self.ex = Gated()
        self.worker = Worker(
            Settings(
                max_in_flight=io_slots, cpu_workers=cpu_slots, max_cpu_in_flight=cpu_slots,
                block_ms=5,
                dispatch_interval_s=3600.0, reclaim_interval_s=3600.0, commit_retry_s=0.0,
            ),
            self.store, {"io": self.io, "cpu": self.cpu}, self.ex, worker_id="w1",
        )  # fmt: skip

    async def submit(self, job_type: str, n: int) -> list[UUID]:
        ids = []
        broker = self.cpu if job_kind(job_type) == "cpu" else self.io
        for _ in range(n):
            job_id = self.store.accept(job_type, {}, 3)
            await broker.publish(job_id, 1)
            ids.append(job_id)
        return ids

    async def wait_started(self, n: int) -> None:
        async with asyncio.timeout(5):
            while self.ex.started < n:
                self.ex.changed.clear()
                await self.ex.changed.wait()


async def test_cpu_jobs_beyond_the_cpu_budget_are_not_claimed_and_wait_unread() -> None:
    rig = Rig(cpu_slots=1)
    await rig.cpu.ensure_group()
    await rig.io.ensure_group()
    cpu_ids = await rig.submit("cpu_hash", 5)
    io_ids = await rig.submit("io_sleep", 4)
    task = asyncio.create_task(rig.worker.run())

    await rig.wait_started(5)  # 1 CPU + 4 I/O: everything the budgets allow
    for _ in range(50):  # give the loops every chance to over-fetch
        await asyncio.sleep(0)
    states = [rig.store.jobs[j].state for j in cpu_ids]
    assert states.count("running") == 1, states  # only the one that can run right now
    assert states.count("queued") == 4
    assert all(
        rig.store.jobs[j].attempts == 0 for j in cpu_ids if rig.store.jobs[j].state == "queued"
    )
    assert rig.cpu.pending == 1  # the other 4 were never read: still in Redis for anyone
    assert rig.ex.running == {"io": 4, "cpu": 1}  # CPU jobs did not take I/O slots

    rig.ex.release.set()
    async with asyncio.timeout(5):
        while not all(rig.store.jobs[j].state == "done" for j in cpu_ids + io_ids):
            rig.ex.changed.clear()
            await asyncio.wait_for(rig.ex.changed.wait(), 1)
    rig.worker.stop()
    await task
    assert rig.ex.peak["cpu"] == 1  # never more CPU jobs at once than CPU processes


async def test_io_jobs_flow_while_the_cpu_budget_is_full() -> None:
    rig = Rig(cpu_slots=1, io_slots=8)
    await rig.cpu.ensure_group()
    await rig.io.ensure_group()
    await rig.submit("cpu_hash", 10)
    await rig.submit("io_sleep", 8)
    task = asyncio.create_task(rig.worker.run())
    await rig.wait_started(9)  # 1 CPU + all 8 I/O despite 10 CPU jobs queued first
    assert rig.ex.running == {"io": 8, "cpu": 1}
    rig.ex.release.set()
    rig.worker.stop()
    await task


async def test_dispatcher_and_release_publish_to_the_jobs_own_stream() -> None:
    rig = Rig()
    await rig.cpu.ensure_group()
    await rig.io.ensure_group()
    cpu_job = rig.store.accept("cpu_hash", {}, 3)
    io_job = rig.store.accept("io_sleep", {}, 3)
    rig.clock.advance(3600)  # both are old and were never dispatched (API crashed)
    assert await rig.worker.dispatch_once() == 2
    assert [j for j, _ in rig.cpu.stream.values()] == [cpu_job]
    assert [j for j, _ in rig.io.stream.values()] == [io_job]

    # graceful release republishes to the job's own stream too
    (msg,) = await rig.cpu.read("w1", 1, 0)
    claim = await rig.store.claim(cpu_job, 1, "w1", 10.0)
    assert isinstance(claim, Claim)
    await rig.worker._release(claim, msg)
    assert [j for j, _ in rig.cpu.stream.values()] == [cpu_job]  # old acked, new one here
    assert [j for j, _ in rig.io.stream.values()] == [io_job]


def test_default_cpu_budget_is_two_slots_per_cpu_process() -> None:
    # One running, one on deck: measured to recover most of a process's throughput without
    # turning the worker back into a long queue (DESIGN.md s8.1).
    clock = FakeClock()
    w = Worker(
        Settings(cpu_workers=3, max_in_flight=32),
        FakeStore(clock), {"io": FakeBroker(clock, "io"), "cpu": FakeBroker(clock, "cpu")},
        Gated(), worker_id="w",
    )  # fmt: skip
    assert w._limits == {"io": 32, "cpu": 6}
    w2 = Worker(
        Settings(cpu_workers=3, max_cpu_in_flight=3),
        FakeStore(clock), {"io": FakeBroker(clock, "io"), "cpu": FakeBroker(clock, "cpu")},
        Gated(), worker_id="w",
    )  # fmt: skip
    assert w2._limits["cpu"] == 3  # explicit setting wins
