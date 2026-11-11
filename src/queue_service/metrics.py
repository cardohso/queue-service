"""Prometheus metrics, one registry per component (so tests and processes never collide).

Metric names are part of the phase-2 contract (alerts, dashboards, KEDA): change with care.
"""

import asyncio

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

# Job durations span ~1 ms (io_sleep) to ~minutes (CPU jobs).
_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120)
# Event-loop lag: sub-millisecond when healthy; >100 ms means something blocked the loop.
_LAG_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5)


def job_kind(job_type: str) -> str:
    """Label value for the in-flight gauge and duration histogram: cpu | io."""
    return "cpu" if job_type == "cpu_hash" else "io"


class LoopLag:
    """Measures how late the event loop wakes a task that asked to sleep ``interval_s``.

    A healthy loop wakes it within ~1 ms. A coroutine that blocks the loop (CPU work, a
    synchronous call) shows up directly as lag. This is the number that proves CPU jobs
    really run in the process pool. ``last_tick`` also drives liveness: if the loop is
    blocked for long enough the tick goes stale and /healthz fails.
    """

    def __init__(self, histogram: Histogram, interval_s: float = 0.1) -> None:
        self._hist = histogram
        self._interval_s = interval_s
        self.max_lag_s = 0.0
        self.last_tick: float | None = None

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        self.last_tick = loop.time()
        while True:
            expected = loop.time() + self._interval_s
            await asyncio.sleep(self._interval_s)
            now = loop.time()
            lag = max(0.0, now - expected)
            self._hist.observe(lag)
            self.max_lag_s = max(self.max_lag_s, lag)
            self.last_tick = now

    def is_alive(self, max_age_s: float = 5.0) -> bool:
        if self.last_tick is None:
            return True  # not started yet: a booting process is not a hung one
        return asyncio.get_running_loop().time() - self.last_tick < max_age_s


class WorkerMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.jobs = Counter(
            "qs_jobs_total",
            "Job attempts by outcome: done, retry, dead, released.",
            ["outcome"],
            registry=r,
        )
        self.in_flight = Gauge(
            "qs_jobs_in_flight", "Jobs currently executing.", ["kind"], registry=r
        )
        self.duration = Histogram(
            "qs_job_duration_seconds",
            "Execution time of one attempt (claim to decision).",
            ["kind", "outcome"],
            buckets=_DURATION_BUCKETS,
            registry=r,
        )
        self.retries = Counter(
            "qs_job_retries_total",
            "Retries scheduled, by exception type.",
            ["error_class"],
            registry=r,
        )
        self.dead = Counter(
            "qs_dead_letter_total", "Jobs dead-lettered, by reason.", ["reason"], registry=r
        )
        self.duplicates = Counter(
            "qs_duplicate_deliveries_total",
            "Deliveries rejected because the job was already claimed or finished.",
            registry=r,
        )
        self.fenced = Counter(
            "qs_fenced_writes_total",
            "Commits rejected by the fence (this worker had lost the job).",
            registry=r,
        )
        self.reclaimed = Counter(
            "qs_reclaimed_total", "Messages taken over from dead workers.", registry=r
        )
        self.republished = Counter(
            "qs_dispatcher_republished_total",
            "Jobs the dispatcher published (due retries and lost dispatches).",
            registry=r,
        )
        self.isolated = Counter(
            "qs_isolated_runs_total",
            "Jobs run alone because their previous attempt crashed a worker.",
            registry=r,
        )
        self.lost_lease = Counter(
            "qs_lost_lease_total", "Heartbeats that found the lease taken over.", registry=r
        )
        self.commit_gave_up = Counter(
            "qs_commit_gave_up_total",
            "Outcome commits abandoned because Postgres stayed unavailable.",
            registry=r,
        )
        self.db_unavailable = Counter(
            "qs_db_unavailable_total", "Claims that hit a Postgres outage.", registry=r
        )
        self.loop_lag = Histogram(
            "qs_event_loop_lag_seconds",
            "How late the event loop wakes a 100 ms sleep.",
            buckets=_LAG_BUCKETS,
            registry=r,
        )

    # Worker.stats names that map 1:1 onto a plain counter.
    def simple_counter(self, name: str) -> Counter | None:
        return {
            "duplicate_deliveries": self.duplicates,
            "fenced_writes": self.fenced,
            "reclaimed": self.reclaimed,
            "republished": self.republished,
            "lost_lease": self.lost_lease,
            "isolated_runs": self.isolated,
            "commit_gave_up": self.commit_gave_up,
            "db_unavailable": self.db_unavailable,
        }.get(name)


class ApiMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.accepted = Counter(
            "qs_api_jobs_accepted_total", "Jobs accepted (new rows, not replays).", registry=r
        )
        self.replays = Counter(
            "qs_api_idempotent_replays_total",
            "Submissions answered from an earlier job.",
            registry=r,
        )
        self.rejected = Counter(
            "qs_api_rejected_total",
            "Rejected submissions by status code: 409, 429, 503.",
            ["code"],
            registry=r,
        )
        self.queue_depth = Gauge(
            "qs_queue_depth",
            "Sampled queue depth used for admission control.",
            ["source"],
            registry=r,
        )
        self.queue_depth.labels("redis")  # materialise both series so dashboards never gap
        self.queue_depth.labels("postgres")
        self.loop_lag = Histogram(
            "qs_event_loop_lag_seconds",
            "How late the event loop wakes a 100 ms sleep.",
            buckets=_LAG_BUCKETS,
            registry=r,
        )

    def record_depth(self, depth: int, source: str) -> None:
        self.queue_depth.clear()
        self.queue_depth.labels(source).set(depth)
