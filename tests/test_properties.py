"""Property-based test of the core claim (DESIGN.md s1, s12.2).

Hypothesis generates random sequences of: submits (incl. API crashes before/after XADD),
deliveries with random outcomes, worker crashes at each step of the ack order, workers that
stall past their lease and come back (zombies), clock jumps, Redis wipes, Postgres and Redis
outages, duplicate deliveries and DLQ replays.

The code under test is the REAL ``Worker`` (claim -> execute -> fenced commit -> ack, retries,
reclaim, dispatcher). Only Postgres and Redis are replaced by fakes whose behaviour is pinned
to the real systems by the contract tests.

Checked after every step:
  * a job is ``done`` exactly when it has a result, and it has at most one result
  * attempts never exceed the budget; a running job always has a lease owner
Checked at the end, after healing every fault and letting time pass:
  * every accepted job is in exactly one final state (done or dead): none lost, none stuck
  * the stream is empty and nothing is pending: every delivery was eventually acked

Coverage evidence: run with ``--hypothesis-show-statistics`` to see how often each fault path
(fenced write, lost lease, crash loop, replay, wipe with work in flight...) was exercised.
"""

import asyncio
import contextlib
import os
import random
from collections import Counter
from typing import Any
from uuid import UUID

from hypothesis import HealthCheck, event, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule

from queue_service.broker import Message
from queue_service.config import Settings
from queue_service.errors import PermanentError, TransientError
from queue_service.store import Claim
from queue_service.worker import Worker
from tests.fakes import Down, FakeBroker, FakeClock, FakeStore

LEASE_S = 10.0
GRACE_S = 5.0
MAX_ATTEMPTS = 2  # small, so exhausted retries and crash loops actually happen
SETTINGS = Settings(
    lease_s=LEASE_S,
    dispatch_grace_s=GRACE_S,
    backoff_base_s=1.0,
    backoff_cap_s=8.0,
    commit_retry_s=0.0,  # don't sleep on a DB outage: give up at once (lease expiry recovers)
    job_timeout_s=600.0,
    dispatch_batch=1000,
)
FINAL = {"done", "dead"}
# Weighted towards transient so retries are exhausted in a meaningful share of runs.
OUTCOMES = ["success", "transient", "transient", "permanent"]


class Crash(BaseException):
    """Process death. BaseException so no ``except Exception`` in the worker can swallow it."""


class ScriptedExecutor:
    """What the job does on its next delivery is chosen by Hypothesis.

    "stall" models a worker that freezes mid-job (GC pause, partition, laptop lid): the call
    blocks until the test resumes it, possibly long after its lease expired.
    """

    def __init__(self) -> None:
        self.next = "success"
        self.stall_gates: tuple[asyncio.Event, asyncio.Event] | None = None

    async def run(self, claim: Claim, timeout_s: float) -> dict[str, object]:
        behaviour = self.next
        if behaviour.startswith("stall:"):
            assert self.stall_gates is not None
            entered, release = self.stall_gates
            self.stall_gates = None
            entered.set()
            await release.wait()
            behaviour = behaviour.removeprefix("stall:")
        if behaviour == "transient":
            raise TransientError("injected")
        if behaviour == "permanent":
            raise PermanentError("injected")
        if behaviour == "crash":
            raise Crash
        return {"attempt": claim.attempt}


class CrashingStore:
    """Wraps FakeStore; can die right AFTER an outcome commit (before the ack)."""

    def __init__(self, inner: FakeStore) -> None:
        self.inner = inner
        self.crash_after_commit = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    async def _maybe_crash(self, committed: bool) -> bool:
        if self.crash_after_commit:
            raise Crash
        return committed

    async def complete(self, claim: Claim, result: dict[str, Any]) -> bool:
        return await self._maybe_crash(await self.inner.complete(claim, result))

    async def retry_later(self, claim: Claim, *args: Any) -> bool:
        return await self._maybe_crash(await self.inner.retry_later(claim, *args))

    async def dead_letter(self, claim: Claim, *args: Any) -> bool:
        return await self._maybe_crash(await self.inner.dead_letter(claim, *args))


class ModelWorker(Worker):
    """The real Worker, minus the wall-clock heartbeat.

    The heartbeat sleeps in real seconds, which would make replays depend on how fast the
    machine runs. In the model every lease runs out unless the job finishes first, which is
    the harsher case. Heartbeats are covered by the integration tests.
    """

    async def _heartbeat(self, claim: Claim, message: Message) -> None:
        await asyncio.Event().wait()


class QueueMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.clock = FakeClock()
        self.store = FakeStore(self.clock)
        self.broker = FakeBroker(self.clock)
        self.executor = ScriptedExecutor()
        self.alive: list[tuple[Worker, CrashingStore]] = []
        self.everyone: list[Worker] = []  # incl. crashed ones, for coverage stats
        self.stalled: list[tuple[asyncio.Task[None], asyncio.Event, Worker]] = []
        self.outage_left = {"postgres": 0, "redis": 0}  # steps left
        self.faults: Counter[str] = Counter()
        self.accepted: set[UUID] = set()
        self.run(self.broker.ensure_group())
        self._spawn_worker()

    # ---- helpers ------------------------------------------------------------------------

    def run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def _spawn_worker(self) -> Worker:
        crashing = CrashingStore(self.store)
        n = len(self.everyone)
        worker = ModelWorker(
            SETTINGS,
            crashing,
            self.broker,
            self.executor,
            worker_id=f"w{n}",
            rng=random.Random(n),
        )
        self.alive.append((worker, crashing))
        self.everyone.append(worker)
        return worker

    def _pick(self, i: int) -> tuple[Worker, CrashingStore]:
        if not self.alive:
            self._spawn_worker()
        return self.alive[i % len(self.alive)]

    def _is_alive(self, worker: Worker) -> bool:
        return any(w is worker for w, _ in self.alive)

    def _kill(self, worker: Worker) -> None:
        self.alive = [(w, s) for w, s in self.alive if w is not worker]

    def _handle(self, worker: Worker, msg: Message) -> None:
        try:
            self.run(worker._handle(msg))
        except Crash:
            self.faults["crash"] += 1
            self._kill(worker)

    def _submit(self, api: str) -> None:
        try:
            job_id = self.store.accept("io_sleep", {"duration_ms": 1}, MAX_ATTEMPTS)
        except Down:
            return  # API answers 503: never accepted
        self.accepted.add(job_id)  # the row is committed: from here on it must not be lost
        if api == "crash_before_xadd":
            return
        try:
            entry = self.run(self.broker.publish(job_id, 1))
        except Down:
            return  # Redis down: best-effort dispatch failed
        if api == "ok":
            with contextlib.suppress(Down):
                self.store.mark_dispatched(job_id, entry)

    # ---- the API ------------------------------------------------------------------------

    @initialize(n=st.integers(1, 4))
    def seed_jobs(self, n: int) -> None:
        for _ in range(n):
            self._submit("ok")

    @rule(api=st.sampled_from(["ok", "ok", "crash_before_xadd", "crash_after_xadd"]))
    def submit(self, api: str) -> None:
        self._submit(api)

    # ---- workers --------------------------------------------------------------------------

    @rule(
        i=st.integers(0, 7),
        outcomes=st.lists(st.sampled_from(OUTCOMES), min_size=1, max_size=4),
        crash=st.sampled_from(["none", "none", "before_claim", "during_execute", "after_commit"]),
    )
    def deliver(self, i: int, outcomes: list[str], crash: str) -> None:
        """Read up to len(outcomes) messages; handle each; the crash (if any) hits the last."""
        worker, crashing = self._pick(i)
        try:
            msgs = self.run(self.broker.read(worker.worker_id, len(outcomes), 0))
        except Down:
            return
        for n, (msg, outcome) in enumerate(zip(msgs, outcomes, strict=False)):
            last = n == len(msgs) - 1
            if last and crash == "before_claim":  # read into its PEL, then died
                self.faults["crash"] += 1
                self._kill(worker)
                return
            self.executor.next = "crash" if last and crash == "during_execute" else outcome
            crashing.crash_after_commit = last and crash == "after_commit"
            try:
                self._handle(worker, msg)
            finally:
                crashing.crash_after_commit = False
            if not self._is_alive(worker):
                return

    @precondition(lambda self: len(self.stalled) < 2)
    @rule(i=st.integers(0, 7), outcome=st.sampled_from(OUTCOMES))
    def deliver_and_stall(self, i: int, outcome: str) -> None:
        """The worker claims the job, then freezes mid-execution without heartbeating."""
        worker, _ = self._pick(i)
        try:
            msgs = self.run(self.broker.read(worker.worker_id, 1, 0))
        except Down:
            return
        if not msgs:
            return
        self.executor.next = f"stall:{outcome}"
        entered, release = asyncio.Event(), asyncio.Event()
        self.executor.stall_gates = (entered, release)
        task = self.loop.create_task(worker._handle(msgs[0]))
        # Run until it is stuck inside the job, or finished early (e.g. claim rejected).
        # Waiting on an event, not a timeout, keeps every Hypothesis replay identical.
        waiter = self.loop.create_task(entered.wait())
        self.run(asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED))
        waiter.cancel()
        self.executor.stall_gates = None
        if task.done():
            return
        self.stalled.append((task, release, worker))
        self._kill(worker)  # frozen: it takes no further part until resumed

    @precondition(lambda self: bool(self.stalled))
    @rule()
    def resume_stalled(self) -> None:
        """The frozen worker wakes up and finishes: its commit must be fenced if replaced."""
        task, release, worker = self.stalled.pop(0)
        release.set()
        self.run(task)
        self.alive.append((worker, CrashingStore(self.store)))

    @rule(i=st.integers(0, 7), outcome=st.sampled_from(["success", "transient"]))
    def reclaim(self, i: int, outcome: str) -> None:
        worker, _ = self._pick(i)
        try:
            msgs = self.run(self.broker.autoclaim(worker.worker_id, int(LEASE_S * 1000), 10))
        except Down:
            return
        self.executor.next = outcome
        for m in msgs:
            if not self._is_alive(worker):
                break
            self._handle(worker, m)

    @rule(i=st.integers(0, 7))
    def dispatch(self, i: int) -> None:
        worker, _ = self._pick(i)
        with contextlib.suppress(Down):
            self.run(worker.dispatch_once())

    @rule()
    def start_worker(self) -> None:
        self._spawn_worker()

    # ---- the world ------------------------------------------------------------------------

    @rule(seconds=st.sampled_from([0.5, 2.0, 6.0, 11.0, 30.0]))
    def advance_clock(self, seconds: float) -> None:
        self.clock.advance(seconds)

    @rule()
    def redis_wipe(self) -> None:
        if self.broker.stream or self.broker.pending:
            self.faults["redis_wipe_with_work"] += 1
        self.broker.wipe()

    @rule(system=st.sampled_from(["postgres", "redis"]), steps=st.integers(1, 4))
    def outage(self, system: str, steps: int) -> None:
        self.outage_left[system] = steps
        self.faults[f"{system}_outage"] += 1

    @rule(data=st.data())
    def duplicate_delivery(self, data: st.DataObject) -> None:
        if not self.accepted:
            return
        job_id = data.draw(st.sampled_from(sorted(self.accepted)))
        with contextlib.suppress(Down):
            self.run(self.broker.publish(job_id, self.store.jobs[job_id].epoch))

    @rule()
    def replay_dead_letters(self) -> None:
        try:
            if self.store.replay_dead(None, MAX_ATTEMPTS):
                self.faults["replay"] += 1
        except Down:
            pass

    # ---- invariants ---------------------------------------------------------------------

    @invariant()
    def outages_heal(self) -> None:  # not a check: advances outage countdowns each step
        for system in self.outage_left:
            self.outage_left[system] = max(0, self.outage_left[system] - 1)
        self.store.down = self.outage_left["postgres"] > 0
        self.broker.down = self.outage_left["redis"] > 0

    @invariant()
    def done_iff_one_result(self) -> None:
        for job in self.store.jobs.values():
            assert (job.state == "done") == (job.id in self.store.results), job
            if job.state == "done":
                assert self.store.results[job.id][0] == job.epoch

    @invariant()
    def attempts_within_budget(self) -> None:
        for job in self.store.jobs.values():
            assert job.attempts <= job.max_attempts, job
            if job.state == "running":
                assert job.lease_owner is not None, job

    def teardown(self) -> None:
        """Heal everything, let time pass, and require every job to reach a final state."""
        try:
            self.outage_left = {"postgres": 0, "redis": 0}
            self.store.down = self.broker.down = False
            while self.stalled:  # zombies wake up before we judge the outcome
                self.resume_stalled()
            worker = self._spawn_worker()
            self.executor.next = "success"
            for _ in range(50):
                self.clock.advance(LEASE_S + GRACE_S + SETTINGS.backoff_cap_s + 1)
                self.run(worker.dispatch_once())
                for m in self.run(self.broker.autoclaim(worker.worker_id, 0, 10_000)):
                    self.run(worker._handle(m))
                while msgs := self.run(self.broker.read(worker.worker_id, 100, 0)):
                    for m in msgs:
                        self.run(worker._handle(m))
                if all(self.store.jobs[j].state in FINAL for j in self.accepted):
                    break

            stuck = {j: self.store.jobs[j].state for j in self.accepted}
            stuck = {j: s for j, s in stuck.items() if s not in FINAL}
            assert not stuck, f"jobs never reached a final state: {stuck}"
            assert set(self.store.jobs) == self.accepted  # nothing appeared from nowhere
            done = {j for j in self.accepted if self.store.jobs[j].state == "done"}
            assert set(self.store.results) == done
            assert self.broker.stream == {}, "unacked entries left in the stream"
            assert self.broker.pending == 0
            self._report()
        finally:
            for task, _, _ in self.stalled:
                task.cancel()
            self.loop.close()

    def _report(self) -> None:
        """Coverage evidence, printed by --hypothesis-show-statistics."""
        stats: Counter[str] = Counter(self.faults)
        for w in self.everyone:
            stats.update(w.stats)
        for key in (
            "fenced_writes",
            "lost_lease",
            "duplicate_deliveries",
            "crash",
            "redis_wipe_with_work",
            "republished",
            "reclaimed",
            "commit_gave_up",
            "db_unavailable",
            "postgres_outage",
            "redis_outage",
            "replay",
        ):
            if stats[key]:
                event(f"saw {key}")
        jobs = self.store.jobs.values()
        if any(j.state == "dead" for j in jobs):
            event("saw dead-lettered job")
        if any(j.dead_reason == "exhausted" for j in jobs):
            event("saw retries exhausted")
        if any(j.dead_reason == "crashed_max_attempts" for j in jobs):
            event("saw crash loop dead-lettered")


QueueMachine.TestCase.settings = settings(
    max_examples=int(os.environ.get("QS_HYPOTHESIS_EXAMPLES", "300")),
    stateful_step_count=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestQueueInvariant = QueueMachine.TestCase
