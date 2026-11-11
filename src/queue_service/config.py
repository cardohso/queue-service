"""Runtime settings, read from environment variables with local-dev defaults."""

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = "postgresql://qs:qs_local_dev_only@localhost:5432/qs"
    redis_url: str = "redis://localhost:6379/0"
    stream: str = "jobs"
    group: str = "workers"
    # Edge back-pressure: reject with 429 when the queue depth exceeds this.
    max_depth: int = 1000
    # How often the background sampler refreshes the depth signal.
    sample_interval_s: float = 0.5
    # Used to turn "how far over the limit" into a Retry-After hint (refined in step 6
    # once workers export a measured drain rate).
    assumed_drain_per_s: float = 100.0
    # Fail fast with 503 instead of queueing behind a saturated DB pool.
    db_acquire_timeout_s: float = 0.2
    db_pool_max_size: int = 10
    # Short timeouts so a dead Redis can never stall request handling.
    redis_timeout_s: float = 0.5
    max_attempts: int = 5

    # --- worker (DESIGN.md s5.2-s10) ---
    dead_stream: str = "jobs:dead"
    max_in_flight: int = 32
    cpu_workers: int = os.cpu_count() or 2
    # Lease = visibility timeout. Heartbeat every lease/3 keeps live jobs from being reclaimed.
    lease_s: float = 30.0
    job_timeout_s: float = 20.0
    block_ms: int = 1000
    reclaim_interval_s: float = 5.0
    dispatch_interval_s: float = 0.5
    # A queued job is only checked for a lost stream entry once it is this old.
    dispatch_grace_s: float = 5.0
    dispatch_batch: int = 100
    backoff_base_s: float = 0.5
    backoff_cap_s: float = 30.0
    drain_deadline_s: float = 25.0
    # How long a worker keeps retrying a failed outcome commit before giving up the job to
    # lease expiry (it then gets redelivered; the fence stops a late double commit).
    commit_retry_s: float = 10.0
    worker_db_acquire_timeout_s: float = 2.0
    worker_redis_timeout_s: float = 5.0  # must exceed block_ms

    @classmethod
    def from_env(cls) -> Settings:
        d = cls()
        return cls(
            database_url=os.environ.get("QS_DATABASE_URL", d.database_url),
            redis_url=os.environ.get("QS_REDIS_URL", d.redis_url),
            stream=os.environ.get("QS_STREAM", d.stream),
            group=os.environ.get("QS_GROUP", d.group),
            max_depth=int(os.environ.get("QS_MAX_DEPTH", d.max_depth)),
            sample_interval_s=float(os.environ.get("QS_SAMPLE_INTERVAL_S", d.sample_interval_s)),
            assumed_drain_per_s=float(
                os.environ.get("QS_ASSUMED_DRAIN_PER_S", d.assumed_drain_per_s)
            ),
            db_acquire_timeout_s=float(
                os.environ.get("QS_DB_ACQUIRE_TIMEOUT_S", d.db_acquire_timeout_s)
            ),
            db_pool_max_size=int(os.environ.get("QS_DB_POOL_MAX_SIZE", d.db_pool_max_size)),
            redis_timeout_s=float(os.environ.get("QS_REDIS_TIMEOUT_S", d.redis_timeout_s)),
            max_attempts=int(os.environ.get("QS_MAX_ATTEMPTS", d.max_attempts)),
        )
