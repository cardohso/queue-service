"""The worker: fetch, claim, execute, fenced commit, then ack (DESIGN.md s5.2).

The one rule everything else serves: **never ack before the outcome is committed**. Every
crash window then ends in redelivery, never loss; the conditional claim and the fenced commit
make redelivery harmless.
"""

import asyncio
import contextlib
import logging
import random
import socket
import time
import uuid
from collections import Counter
from typing import Protocol

from redis.exceptions import RedisError

from queue_service.broker import Broker, Message
from queue_service.config import Settings
from queue_service.errors import DB_INFRA_ERRORS
from queue_service.isolation import IsolationLock
from queue_service.logs import bind_job
from queue_service.metrics import LoopLag, WorkerMetrics, job_kind
from queue_service.retry import Decision, decide_failure, decide_success
from queue_service.store import Claim, Rejected, Store

log = logging.getLogger(__name__)

REDIS_INFRA_ERRORS: tuple[type[BaseException], ...] = (RedisError, OSError, TimeoutError)


class JobRunner(Protocol):
    async def run(self, claim: Claim, timeout_s: float) -> dict[str, object]: ...


def new_worker_id() -> str:
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


class Worker:
    def __init__(
        self,
        settings: Settings,
        store: Store,
        broker: Broker,
        executor: JobRunner,
        *,
        worker_id: str | None = None,
        rng: random.Random | None = None,
        metrics: WorkerMetrics | None = None,
    ) -> None:
        self._s = settings
        self._store = store
        self._broker = broker
        self._executor = executor
        self.worker_id = worker_id or new_worker_id()  # also the Redis consumer name
        self._rng = rng or random.Random()
        self._in_flight: set[asyncio.Task[None]] = set()
        self._slot_freed = asyncio.Event()
        self._stopping = asyncio.Event()
        self._isolation = IsolationLock()
        self._db_backoff_s = 0.0
        self._db_paused_until = 0.0
        self.metrics = metrics or WorkerMetrics()
        self.loop_lag = LoopLag(self.metrics.loop_lag)
        # Same events as the Prometheus counters; handy for tests and the exit log line.
        self.stats: Counter[str] = Counter()

    def _count(self, name: str, n: int = 1) -> None:
        self.stats[name] += n
        if (counter := self.metrics.simple_counter(name)) is not None:
            counter.inc(n)

    # ---- health ----------------------------------------------------------------------

    def alive(self) -> bool:
        """Liveness: the event loop is still turning (not blocked or deadlocked)."""
        return self.loop_lag.is_alive()

    async def ready(self) -> bool:
        """Readiness: able to do useful work: not draining, Postgres and Redis reachable."""
        if self._stopping.is_set():
            return False
        try:
            await asyncio.gather(self._store.ping(), self._broker.ping())
        except (*DB_INFRA_ERRORS, *REDIS_INFRA_ERRORS):
            return False
        return True

    # ---- lifecycle -------------------------------------------------------------------

    def stop(self) -> None:
        """SIGTERM handler: stop pulling; run() then drains in-flight jobs."""
        self._stopping.set()
        self._slot_freed.set()

    @property
    def in_flight(self) -> int:
        return len(self._in_flight)

    async def run(self) -> None:
        lag_task = asyncio.create_task(self.loop_lag.run(), name="loop-lag")
        await self._ensure_group()
        loops = [
            asyncio.create_task(self._fetch_loop(), name="fetch"),
            asyncio.create_task(self._reclaim_loop(), name="reclaim"),
            asyncio.create_task(self._dispatch_loop(), name="dispatch"),
        ]
        # Loops exit on their own once _stopping is set (fetch within block_ms). Not cancelling
        # them avoids dropping an XREADGROUP reply mid-flight, which would strand entries in
        # our PEL until the lease expires.
        await asyncio.gather(*loops)
        await self._drain()
        lag_task.cancel()

    async def _drain(self) -> None:
        if not self._in_flight:
            return
        _, pending = await asyncio.wait(set(self._in_flight), timeout=self._s.drain_deadline_s)
        for task in pending:
            task.cancel()  # _handle sees CancelledError while stopping and releases the job
        await asyncio.gather(*pending, return_exceptions=True)

    async def _ensure_group(self) -> None:
        delay = 0.1
        while not self._stopping.is_set():
            try:
                await self._broker.ensure_group()
                return
            except REDIS_INFRA_ERRORS as exc:
                log.warning("ensure_group_failed", extra={"error": repr(exc)})
                await self._sleep_or_stop(delay)
                delay = min(delay * 2, 5.0)

    # ---- loops -----------------------------------------------------------------------

    async def _fetch_loop(self) -> None:
        redis_delay = 0.1
        while not self._stopping.is_set():
            if await self._paused_for_db():
                continue
            free = self._s.max_in_flight - len(self._in_flight)
            if free <= 0:
                # Back-pressure inside the worker: pull nothing until a slot frees up.
                self._slot_freed.clear()
                if self._s.max_in_flight - len(self._in_flight) <= 0:
                    await self._slot_freed.wait()
                continue
            if self._isolation.isolating:
                # A suspect job is waiting to run alone: pulling new work onto this worker
                # would put more jobs next to it if it crashes the process again.
                self._slot_freed.clear()
                if self._isolation.isolating:
                    await self._slot_freed.wait()
                continue
            try:
                messages = await self._broker.read(self.worker_id, free, self._s.block_ms)
                redis_delay = 0.1
            except REDIS_INFRA_ERRORS as exc:
                log.warning("read_failed", extra={"error": repr(exc)})
                await self._sleep_or_stop(redis_delay)
                redis_delay = min(redis_delay * 2, 5.0)
                continue
            for m in messages:  # spawn even when stopping: they're in our PEL already
                self._spawn(m)

    async def _reclaim_loop(self) -> None:
        while not self._stopping.is_set():
            await self._sleep_or_stop(self._s.reclaim_interval_s)
            free = self._s.max_in_flight - len(self._in_flight)
            if self._stopping.is_set() or free <= 0 or self._db_is_paused():
                continue
            try:
                messages = await self._broker.autoclaim(
                    self.worker_id, int(self._s.lease_s * 1000), free
                )
            except REDIS_INFRA_ERRORS as exc:
                log.warning("autoclaim_failed", extra={"error": repr(exc)})
                continue
            self._count("reclaimed", len(messages))
            for m in messages:
                self._spawn(m)

    async def _dispatch_loop(self) -> None:
        while not self._stopping.is_set():
            await self._sleep_or_stop(self._s.dispatch_interval_s)
            if self._stopping.is_set():
                break
            try:
                await self.dispatch_once()
            except (*DB_INFRA_ERRORS, *REDIS_INFRA_ERRORS) as exc:
                log.warning("dispatch_failed", extra={"error": repr(exc)})

    async def dispatch_once(self) -> int:
        """Publish due retries and jobs whose stream entry is provably gone."""
        items = await self._store.take_dispatch_batch(
            self._s.dispatch_batch, self._s.dispatch_grace_s
        )
        if not items:
            return 0
        exists = await self._broker.entries_exist([i.entry_id for i in items if i.entry_id])
        published = 0
        for item in items:
            # An existing entry is still unprocessed (ack deletes it), so it will be delivered
            # or reclaimed: republishing would only create duplicates.
            if item.entry_id is not None and exists.get(item.entry_id, False):
                continue
            entry_id = await self._broker.publish(item.job_id, item.epoch)
            await self._store.set_entry(item.job_id, item.epoch, entry_id)
            published += 1
        self._count("republished", published)
        return published

    # ---- one message -----------------------------------------------------------------

    def _spawn(self, message: Message) -> None:
        task = asyncio.create_task(self._handle(message))
        self._in_flight.add(task)
        task.add_done_callback(self._on_done)

    def _on_done(self, task: asyncio.Task[None]) -> None:
        self._in_flight.discard(task)
        self._slot_freed.set()
        if not task.cancelled() and (exc := task.exception()) is not None:
            log.error("handler_crashed", exc_info=exc)

    async def _handle(self, message: Message) -> None:
        if message.job_id is None:
            await self._ack_quietly(message)
            return
        bind_job(job_id=str(message.job_id), epoch=message.epoch, worker_id=self.worker_id)
        async with self._isolation.shared():
            claimed = await self._claim(message, allow_takeover=False)
            if claimed is None:
                return
            if not (isinstance(claimed, Rejected) and claimed.reason == "needs_isolation"):
                await self._process(message, claimed)
                return
        # Its previous attempt crashed a worker, maybe because of this very job. Run it alone
        # (DESIGN.md s9.1): if it crashes us again, nothing else is running to share the blame.
        self._count("isolated_runs")
        log.warning("isolating_suspect_job")
        async with self._isolation.exclusive():
            claimed = await self._claim(message, allow_takeover=True)
            if claimed is None:
                return
            await self._process(message, claimed)

    async def _claim(self, message: Message, *, allow_takeover: bool) -> Claim | Rejected | None:
        """Step 2. None means Postgres is down: the entry stays in our PEL (not acked, no
        attempt charged) and fetching pauses; it is reclaimed after the lease."""
        assert message.job_id is not None
        try:
            claimed = await self._store.claim(
                message.job_id,
                message.epoch,
                self.worker_id,
                self._s.lease_s,
                allow_takeover=allow_takeover,
            )
        except DB_INFRA_ERRORS as exc:
            self._db_failed()
            log.warning("claim_db_unavailable", extra={"error": repr(exc)})
            return None
        self._db_ok()
        return claimed

    async def _process(self, message: Message, claimed: Claim | Rejected) -> None:
        if isinstance(claimed, Rejected):
            if claimed.dead_lettered:
                self.stats["dead"] += 1
                self.metrics.jobs.labels("dead").inc()
                self.metrics.dead.labels("crashed_max_attempts").inc()
                log.warning("job_dead", extra={"reason": "crashed_max_attempts"})
                await self._publish_dead_quietly(message, claimed.reason)
            else:
                self._count("duplicate_deliveries")
                log.info("duplicate_delivery", extra={"reason": claimed.reason})
            await self._ack_quietly(message)
            return

        bind_job(attempt=claimed.attempt, type=claimed.type)
        kind = job_kind(claimed.type)
        in_flight = self.metrics.in_flight.labels(kind)
        in_flight.inc()
        started = time.monotonic()
        # Step 3: execute, with a heartbeat that keeps the lease and the PEL entry fresh.
        heartbeat = asyncio.create_task(self._heartbeat(claimed, message))
        try:
            decision = await self._execute(claimed)
        except asyncio.CancelledError:
            heartbeat.cancel()
            if self._stopping.is_set():
                await self._release(claimed, message)
            raise
        finally:
            heartbeat.cancel()
            in_flight.dec()
        duration = time.monotonic() - started

        # Step 4: fenced commit. Steps 5-6: DLQ copy, then ack.
        committed = await self._commit(claimed, decision)
        if committed is None:
            return  # gave up: lease expiry hands the job to someone else; nothing acked
        if not committed:
            self._count("fenced_writes")
            log.warning("fenced_out")
        else:
            self.stats[decision.kind] += 1
            self.metrics.jobs.labels(decision.kind).inc()
            self.metrics.duration.labels(kind, decision.kind).observe(duration)
            if decision.kind == "retry":
                self.metrics.retries.labels(decision.error_class or "unknown").inc()
            log.info(
                "job_finished",
                extra={
                    "outcome": decision.kind,
                    "duration_ms": round(duration * 1000, 1),
                    "error_class": decision.error_class,
                    "retry_in_s": round(decision.delay_s, 3) if decision.kind == "retry" else None,
                },
            )
            if decision.kind == "dead":
                reason = decision.reason or "unknown"
                self.metrics.dead.labels(reason).inc()
                log.warning("job_dead", extra={"reason": reason, "error": decision.error})
                await self._publish_dead_quietly(message, reason)
        await self._ack_quietly(message)

    async def _execute(self, claim: Claim) -> Decision:
        try:
            async with asyncio.timeout(self._s.job_timeout_s):
                result = await self._executor.run(claim, self._s.job_timeout_s)
        except Exception as exc:  # job failures only: CancelledError is BaseException
            return decide_failure(
                exc,
                attempt=claim.attempt,
                max_attempts=claim.max_attempts,
                base_s=self._s.backoff_base_s,
                cap_s=self._s.backoff_cap_s,
                rng=self._rng,
            )
        return decide_success(dict(result))

    async def _commit(self, claim: Claim, d: Decision) -> bool | None:
        """True = committed, False = fenced out, None = DB unavailable past the retry budget."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._s.commit_retry_s
        delay = 0.05
        while True:
            try:
                if d.kind == "done":
                    return await self._store.complete(claim, d.result or {})
                if d.kind == "retry":
                    return await self._store.retry_later(claim, d.delay_s, d.error_class, d.error)
                return await self._store.dead_letter(
                    claim, d.reason or "unknown", d.error_class, d.error
                )
            except DB_INFRA_ERRORS:
                if loop.time() + delay > deadline:
                    log.error("commit_gave_up", extra={"job_id": str(claim.job_id)})
                    self._count("commit_gave_up")
                    return None
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1.0)

    async def _heartbeat(self, claim: Claim, message: Message) -> None:
        while True:
            await asyncio.sleep(self._s.lease_s / 3)
            try:
                still_ours = await self._store.heartbeat(claim, self._s.lease_s)
            except DB_INFRA_ERRORS as exc:
                # Not fatal on its own, but enough of these in a row let the lease expire.
                self._count("heartbeat_failed")
                log.warning("heartbeat_failed", extra={"error": repr(exc)})
                continue
            if not still_ours:
                # Someone took over. Our commit will be fenced out; let the job finish.
                self._count("lost_lease")
                return
            with contextlib.suppress(*REDIS_INFRA_ERRORS):
                await self._broker.touch(self.worker_id, [message.entry_id])

    async def _release(self, claim: Claim, message: Message) -> None:
        """Graceful shutdown: hand the job back now instead of after lease expiry.

        Order: release in DB -> publish a new entry -> ack the old one. A crash anywhere in
        between leaves at worst a duplicate delivery, never a lost job.
        """
        try:
            if await self._store.release(claim):
                entry_id = await self._broker.publish(claim.job_id, claim.epoch)
                await self._store.set_entry(claim.job_id, claim.epoch, entry_id)
                self._count("released")
                self.metrics.jobs.labels("released").inc()
            await self._broker.ack(message.entry_id)
        except (*DB_INFRA_ERRORS, *REDIS_INFRA_ERRORS) as exc:
            log.warning("release_failed", extra={"error": repr(exc)})

    # ---- helpers ---------------------------------------------------------------------

    async def _ack_quietly(self, message: Message) -> None:
        try:
            await self._broker.ack(message.entry_id)
        except REDIS_INFRA_ERRORS as exc:
            # Safe: the entry stays in our PEL, gets reclaimed, and the claim rejects it.
            log.warning("ack_failed", extra={"entry_id": message.entry_id, "error": repr(exc)})

    async def _publish_dead_quietly(self, message: Message, reason: str) -> None:
        if message.job_id is None:
            return
        try:
            await self._broker.publish_dead(message.job_id, message.epoch, reason)
        except REDIS_INFRA_ERRORS as exc:
            log.warning("dlq_publish_failed", extra={"error": repr(exc)})

    def _db_failed(self) -> None:
        self._db_backoff_s = min(max(self._db_backoff_s * 2, 0.1), 5.0)
        self._db_paused_until = asyncio.get_running_loop().time() + self._db_backoff_s
        self._count("db_unavailable")

    def _db_ok(self) -> None:
        self._db_backoff_s = 0.0

    def _db_is_paused(self) -> bool:
        return asyncio.get_running_loop().time() < self._db_paused_until

    async def _paused_for_db(self) -> bool:
        remaining = self._db_paused_until - asyncio.get_running_loop().time()
        if remaining <= 0:
            return False
        await self._sleep_or_stop(remaining)
        return True

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            async with asyncio.timeout(seconds):
                await self._stopping.wait()
        except TimeoutError:
            pass
