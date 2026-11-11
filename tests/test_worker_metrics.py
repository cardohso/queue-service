"""Worker metrics and health, driven through the real Worker on in-memory fakes."""

import asyncio
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from queue_service.broker import Message
from queue_service.config import Settings
from queue_service.errors import PermanentError, TransientError
from queue_service.metrics import WorkerMetrics
from queue_service.store import Claim
from queue_service.worker import Worker
from tests.fakes import FakeBroker, FakeClock, FakeStore

CFG = Settings(lease_s=10.0, backoff_base_s=1.0, backoff_cap_s=2.0, commit_retry_s=0.0)


class Scripted:
    def __init__(self) -> None:
        self.behaviour: Any = {"ok": 1}  # a dict result, or an exception instance to raise
        self.gate: asyncio.Event | None = None
        self.on_run: Any = None

    async def run(self, claim: Claim, timeout_s: float) -> dict[str, object]:
        if self.on_run is not None:
            await self.on_run(claim)
        if self.gate is not None:
            await self.gate.wait()
        if isinstance(self.behaviour, BaseException):
            raise self.behaviour
        return dict(self.behaviour)


class Rig:
    def __init__(self, job_type: str = "io_sleep") -> None:
        self.clock = FakeClock()
        self.store = FakeStore(self.clock)
        self.broker = FakeBroker(self.clock)
        self.executor = Scripted()
        self.registry = CollectorRegistry()
        self.worker = Worker(
            CFG, self.store, self.broker, self.executor,
            worker_id="w1", metrics=WorkerMetrics(self.registry),
        )  # fmt: skip
        self.job_type = job_type

    async def message(self, max_attempts: int = 3) -> Message:
        await self.broker.ensure_group()
        job_id = self.store.accept(self.job_type, {"duration_ms": 1}, max_attempts)
        await self.broker.publish(job_id, 1)
        (msg,) = await self.broker.read("w1", 1, 0)
        return msg

    def sample(self, name: str, **labels: str) -> float:
        return self.registry.get_sample_value(name, labels) or 0.0


async def test_success_is_counted_timed_and_in_flight_returns_to_zero() -> None:
    rig = Rig()
    await rig.worker._handle(await rig.message())
    assert rig.sample("qs_jobs_total", outcome="done") == 1
    assert rig.sample("qs_job_duration_seconds_count", kind="io", outcome="done") == 1
    assert rig.sample("qs_jobs_in_flight", kind="io") == 0


async def test_in_flight_gauge_is_one_while_the_job_runs() -> None:
    rig = Rig()
    rig.executor.gate = asyncio.Event()
    task = asyncio.create_task(rig.worker._handle(await rig.message()))
    for _ in range(100):  # wait for the job to be inside the executor
        if rig.sample("qs_jobs_in_flight", kind="io") == 1:
            break
        await asyncio.sleep(0)
    assert rig.sample("qs_jobs_in_flight", kind="io") == 1
    rig.executor.gate.set()
    await task
    assert rig.sample("qs_jobs_in_flight", kind="io") == 0


async def test_cpu_jobs_are_labelled_cpu() -> None:
    rig = Rig(job_type="cpu_hash")
    rig.executor.gate = asyncio.Event()
    task = asyncio.create_task(rig.worker._handle(await rig.message()))
    for _ in range(100):
        if rig.sample("qs_jobs_in_flight", kind="cpu") == 1:
            break
        await asyncio.sleep(0)
    assert rig.sample("qs_jobs_in_flight", kind="cpu") == 1
    assert rig.sample("qs_jobs_in_flight", kind="io") == 0
    rig.executor.gate.set()
    await task


async def test_transient_failure_counts_a_retry_with_its_exception_type() -> None:
    rig = Rig()
    rig.executor.behaviour = TransientError("boom")
    await rig.worker._handle(await rig.message())
    assert rig.sample("qs_jobs_total", outcome="retry") == 1
    assert rig.sample("qs_job_retries_total", error_class="TransientError") == 1
    assert rig.sample("qs_jobs_total", outcome="done") == 0


async def test_permanent_failure_counts_a_dead_letter_with_its_reason() -> None:
    rig = Rig()
    rig.executor.behaviour = PermanentError("no")
    await rig.worker._handle(await rig.message())
    assert rig.sample("qs_jobs_total", outcome="dead") == 1
    assert rig.sample("qs_dead_letter_total", reason="permanent") == 1


async def test_exhausted_retries_are_dead_lettered_as_exhausted() -> None:
    rig = Rig()
    rig.executor.behaviour = KeyError("bug")  # unknown -> transient
    await rig.worker._handle(await rig.message(max_attempts=1))
    assert rig.sample("qs_dead_letter_total", reason="exhausted") == 1
    assert rig.sample("qs_job_retries_total", error_class="KeyError") == 0


async def test_duplicate_delivery_is_counted_not_executed() -> None:
    rig = Rig()
    msg = await rig.message()
    await rig.worker._handle(msg)
    await rig.broker.publish(msg.job_id, 1)  # type: ignore[arg-type]
    (dup,) = await rig.broker.read("w1", 1, 0)
    await rig.worker._handle(dup)
    assert rig.sample("qs_duplicate_deliveries_total") == 1
    assert rig.sample("qs_jobs_total", outcome="done") == 1  # still exactly one execution


async def test_fenced_write_is_counted_when_the_job_was_taken_over() -> None:
    rig = Rig()

    async def take_over(claim: Claim) -> None:  # lease expires, another worker claims it
        rig.store.jobs[claim.job_id].lease_until = rig.clock.now() - 1
        assert isinstance(await rig.store.claim(claim.job_id, claim.epoch, "usurper", 30.0), Claim)

    rig.executor.on_run = take_over
    await rig.worker._handle(await rig.message())
    assert rig.sample("qs_fenced_writes_total") == 1
    assert rig.sample("qs_jobs_total", outcome="done") == 0  # our result was not committed


async def test_crash_loop_dead_letter_is_counted() -> None:
    rig = Rig()
    msg = await rig.message(max_attempts=1)
    claim = await rig.store.claim(msg.job_id, 1, "ghost", 10.0)  # type: ignore[arg-type]
    assert isinstance(claim, Claim)
    rig.clock.advance(11)  # the ghost's lease expires; it used its only attempt
    await rig.worker._handle(msg)
    assert rig.sample("qs_dead_letter_total", reason="crashed_max_attempts") == 1
    assert rig.sample("qs_jobs_total", outcome="dead") == 1


async def test_db_outage_on_claim_is_counted_and_nothing_is_acked() -> None:
    rig = Rig()
    msg = await rig.message()
    rig.store.down = True
    await rig.worker._handle(msg)
    assert rig.sample("qs_db_unavailable_total") == 1
    assert rig.broker.pending == 1  # still in the PEL, will be reclaimed


# ---- health ------------------------------------------------------------------------------


async def test_ready_requires_postgres_and_redis_and_not_draining() -> None:
    rig = Rig()
    assert await rig.worker.ready() is True
    rig.store.down = True
    assert await rig.worker.ready() is False
    rig.store.down = False
    rig.broker.down = True
    assert await rig.worker.ready() is False
    rig.broker.down = False
    assert await rig.worker.ready() is True
    rig.worker.stop()
    assert await rig.worker.ready() is False  # draining: stop sending traffic/work


async def test_alive_goes_false_when_the_event_loop_tick_is_stale() -> None:
    rig = Rig()
    assert rig.worker.alive() is True  # not started: booting is not hung
    loop = asyncio.get_running_loop()
    rig.worker.loop_lag.last_tick = loop.time() - 60  # the loop has not turned for a minute
    assert rig.worker.alive() is False
    rig.worker.loop_lag.last_tick = loop.time()
    assert rig.worker.alive() is True


@pytest.mark.parametrize("name", ["qs_event_loop_lag_seconds_count"])
async def test_loop_lag_histogram_is_registered(name: str) -> None:
    rig = Rig()
    assert rig.registry.get_sample_value(name) == 0.0
