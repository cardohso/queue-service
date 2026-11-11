"""In-memory Store and Broker with a fake clock.

These are independent re-implementations of PgStore and RedisBroker semantics, NOT wrappers.
The contract tests (tests/integration/test_contract_*.py) run the same assertions against
both, so a fake that drifts from the real thing fails CI. Without that, the property-based
tests could pass for the wrong reason: they would be proving properties of the fakes.
"""

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from queue_service.broker import Message
from queue_service.store import Claim, DispatchItem, Rejected


class FakeClock:
    def __init__(self) -> None:
        self.t = 1_000.0

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class Down(OSError):
    """Simulated outage. OSError, so the worker treats it as infrastructure failure."""


# ---- store --------------------------------------------------------------------------------


@dataclass
class FakeJob:
    id: UUID
    type: str
    payload: dict[str, Any]
    max_attempts: int
    created_at: float
    state: str = "queued"
    attempts: int = 0
    epoch: int = 1
    lease_owner: str | None = None
    lease_until: float | None = None
    next_attempt_at: float | None = None
    dispatched_at: float | None = None
    stream_entry_id: str | None = None
    dead_reason: str | None = None


@dataclass
class FakeAttempt:
    worker_id: str
    outcome: str | None = None
    error_class: str | None = None


class FakeStore:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.jobs: dict[UUID, FakeJob] = {}
        self.results: dict[UUID, tuple[int, dict[str, Any]]] = {}
        self.attempts: dict[tuple[UUID, int, int], FakeAttempt] = {}
        self.down = False
        self._next_id = 0

    def _check(self) -> None:
        if self.down:
            raise Down("postgres unavailable")

    async def ping(self) -> None:
        self._check()

    # -- what the API does (not part of the Store protocol) --

    def accept(self, type_: str, payload: dict[str, Any], max_attempts: int) -> UUID:
        self._check()
        self._next_id += 1
        job_id = UUID(int=self._next_id)  # deterministic: Hypothesis replays must match
        self.jobs[job_id] = FakeJob(job_id, type_, payload, max_attempts, self.clock.now())
        return job_id

    def mark_dispatched(self, job_id: UUID, entry_id: str) -> None:
        self._check()
        job = self.jobs[job_id]
        job.dispatched_at = self.clock.now()
        job.stream_entry_id = entry_id

    def replay_dead(self, job_ids: list[UUID] | None, max_attempts: int) -> list[UUID]:
        self._check()
        replayed = []
        for job in self.jobs.values():
            if job.state == "dead" and (job_ids is None or job.id in job_ids):
                job.state, job.epoch, job.attempts = "queued", job.epoch + 1, 0
                job.max_attempts, job.dead_reason = max_attempts, None
                job.dispatched_at = job.stream_entry_id = job.next_attempt_at = None
                replayed.append(job.id)
        return replayed

    # -- Store protocol --

    async def claim(
        self, job_id: UUID, epoch: int, worker_id: str, lease_s: float
    ) -> Claim | Rejected:
        self._check()
        now = self.clock.now()
        job = self.jobs.get(job_id)
        if job is None:
            return Rejected("missing")
        if job.epoch != epoch:
            return Rejected("stale_epoch")
        expired = job.lease_until is not None and job.lease_until < now
        takeover = job.state == "running" and expired
        if job.state == "running" and not takeover:
            return Rejected("running_elsewhere")
        if job.state != "queued" and not takeover:
            return Rejected(job.state)
        if takeover:
            prev = self.attempts.get((job_id, epoch, job.attempts))
            if prev is not None and prev.outcome is None:
                prev.outcome = "lost_lease"
            if job.attempts >= job.max_attempts:
                job.state, job.dead_reason = "dead", "crashed_max_attempts"
                job.lease_owner = job.lease_until = None
                return Rejected("crashed_out", dead_lettered=True)
        job.state = "running"
        job.attempts += 1
        job.lease_owner, job.lease_until = worker_id, now + lease_s
        self.attempts[(job_id, epoch, job.attempts)] = FakeAttempt(worker_id)
        return Claim(
            job_id, epoch, job.attempts, job.max_attempts, worker_id, job.type, job.payload
        )

    def _fenced(self, c: Claim) -> FakeJob | None:
        job = self.jobs.get(c.job_id)
        if (
            job is None
            or job.epoch != c.epoch
            or job.attempts != c.attempt
            or job.lease_owner != c.worker_id
            or job.state != "running"
        ):
            return None
        return job

    def _finish(self, job: FakeJob, state: str, delay_s: float | None, reason: str | None) -> None:
        job.state = state
        job.lease_owner = job.lease_until = None
        job.next_attempt_at = None if delay_s is None else self.clock.now() + delay_s
        job.dead_reason = reason

    def _close(self, c: Claim, outcome: str, error_class: str | None) -> None:
        a = self.attempts[(c.job_id, c.epoch, c.attempt)]
        a.outcome, a.error_class = outcome, error_class

    async def heartbeat(self, claim: Claim, lease_s: float) -> bool:
        self._check()
        job = self._fenced(claim)
        if job is None:
            return False
        job.lease_until = self.clock.now() + lease_s
        return True

    async def complete(self, claim: Claim, result: dict[str, Any]) -> bool:
        self._check()
        job = self._fenced(claim)
        if job is None:
            return False
        if claim.job_id in self.results:  # mirrors the PRIMARY KEY on results.job_id
            raise AssertionError(f"unique violation: second result for {claim.job_id}")
        self._finish(job, "done", None, None)
        self.results[claim.job_id] = (claim.epoch, result)
        self._close(claim, "done", None)
        return True

    async def retry_later(
        self, claim: Claim, delay_s: float, error_class: str | None, error: str | None
    ) -> bool:
        self._check()
        job = self._fenced(claim)
        if job is None:
            return False
        self._finish(job, "retry_wait", delay_s, None)
        self._close(claim, "retry", error_class)
        return True

    async def dead_letter(
        self, claim: Claim, reason: str, error_class: str | None, error: str | None
    ) -> bool:
        self._check()
        job = self._fenced(claim)
        if job is None:
            return False
        self._finish(job, "dead", None, reason)
        self._close(claim, f"dead:{reason}", error_class)
        return True

    async def release(self, claim: Claim) -> bool:
        self._check()
        job = self._fenced(claim)
        if job is None:
            return False
        job.state, job.max_attempts = "queued", job.max_attempts + 1
        job.lease_owner = job.lease_until = job.stream_entry_id = None
        job.dispatched_at = self.clock.now()
        self._close(claim, "released", None)
        return True

    async def take_dispatch_batch(self, limit: int, grace_s: float) -> list[DispatchItem]:
        self._check()
        now = self.clock.now()

        def last_dispatch(j: FakeJob) -> float:
            return j.dispatched_at if j.dispatched_at is not None else j.created_at

        def candidate(j: FakeJob) -> bool:
            if j.state == "retry_wait":
                return j.next_attempt_at is not None and j.next_attempt_at <= now
            if j.state == "queued":
                return last_dispatch(j) < now - grace_s
            if j.state == "running":
                return (
                    j.lease_until is not None
                    and j.lease_until < now - grace_s
                    and last_dispatch(j) < now - grace_s
                )
            return False

        def order(j: FakeJob) -> float:
            return j.next_attempt_at if j.next_attempt_at is not None else last_dispatch(j)

        picked = sorted(filter(candidate, self.jobs.values()), key=order)[:limit]
        for j in picked:
            if j.state == "retry_wait":
                j.state, j.stream_entry_id, j.next_attempt_at = "queued", None, None
            j.dispatched_at = now
        return [DispatchItem(j.id, j.epoch, j.stream_entry_id) for j in picked]

    async def set_entry(self, job_id: UUID, epoch: int, entry_id: str) -> None:
        self._check()
        job = self.jobs.get(job_id)
        if job is not None and job.epoch == epoch:
            job.stream_entry_id = entry_id


# ---- broker -------------------------------------------------------------------------------


@dataclass
class _Pending:
    consumer: str
    delivered_at: float
    deliveries: int = 1


@dataclass
class _Group:
    last_delivered: int = 0
    pel: dict[int, _Pending] = field(default_factory=dict)


class FakeBroker:
    """Redis Streams semantics that the worker relies on: consumer group, PEL, XAUTOCLAIM."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.stream: dict[int, tuple[UUID, int]] = {}  # entry seq -> (job_id, epoch)
        self.group: _Group | None = None
        self.dead: list[tuple[UUID, int, str]] = []
        self._seq = 0  # survives wipe(): real IDs are time-based and keep increasing
        self.down = False

    def _check(self) -> None:
        if self.down:
            raise Down("redis unavailable")

    def wipe(self) -> None:
        """Redis restarted without persistence: stream, group and PEL are gone."""
        self.stream.clear()
        self.group = None
        self.dead.clear()

    @staticmethod
    def _id(seq: int) -> str:
        return f"{seq}-0"

    @staticmethod
    def _seq_of(entry_id: str) -> int:
        return int(entry_id.split("-")[0])

    def _msg(self, seq: int) -> Message:
        job_id, epoch = self.stream[seq]
        return Message(self._id(seq), job_id, epoch)

    async def ping(self) -> None:
        self._check()

    async def ensure_group(self) -> None:
        self._check()
        if self.group is None:
            self.group = _Group(last_delivered=0)  # created at "0", like RedisBroker

    async def read(self, consumer: str, count: int, block_ms: int) -> list[Message]:
        self._check()
        if self.group is None:  # NOGROUP -> recreate, return nothing this time
            await self.ensure_group()
            return []
        g = self.group
        seqs = sorted(s for s in self.stream if s > g.last_delivered)[:count]
        for s in seqs:
            g.pel[s] = _Pending(consumer, self.clock.now())
            g.last_delivered = s
        return [self._msg(s) for s in seqs]

    async def autoclaim(self, consumer: str, min_idle_ms: int, count: int) -> list[Message]:
        self._check()
        if self.group is None:
            await self.ensure_group()
            return []
        g, now = self.group, self.clock.now()
        out: list[Message] = []
        for s in sorted(g.pel):
            if len(out) >= count:
                break
            p = g.pel[s]
            if (now - p.delivered_at) * 1000 < min_idle_ms:
                continue
            if s not in self.stream:  # deleted entry: Redis drops it from the PEL
                del g.pel[s]
                continue
            p.consumer, p.delivered_at, p.deliveries = consumer, now, p.deliveries + 1
            out.append(self._msg(s))
        return out

    async def touch(self, consumer: str, entry_ids: list[str]) -> None:
        self._check()
        if self.group is None:
            return
        for eid in entry_ids:
            p = self.group.pel.get(self._seq_of(eid))
            if p is not None:
                p.consumer, p.delivered_at = consumer, self.clock.now()

    async def ack(self, entry_id: str) -> None:
        self._check()
        s = self._seq_of(entry_id)
        if self.group is not None:
            self.group.pel.pop(s, None)
        self.stream.pop(s, None)

    async def publish(self, job_id: UUID, epoch: int) -> str:
        self._check()
        self._seq += 1
        self.stream[self._seq] = (job_id, epoch)
        return self._id(self._seq)

    async def publish_dead(self, job_id: UUID, epoch: int, reason: str) -> None:
        self._check()
        self.dead.append((job_id, epoch, reason))

    async def entries_exist(self, entry_ids: list[str]) -> dict[str, bool]:
        self._check()
        return {eid: self._seq_of(eid) in self.stream for eid in entry_ids}

    @property
    def pending(self) -> int:
        return 0 if self.group is None else len(self.group.pel)
