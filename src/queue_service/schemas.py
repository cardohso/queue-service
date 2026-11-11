"""Versioned job payload schemas.

Each job type carries an explicit ``schema_version``. Adding a v2 of a type means adding a new
model with ``schema_version: Literal[2]`` to the union; old clients keep working and unknown
versions are rejected with 422 instead of being queued and failing later in a worker.
"""

import hashlib
import json
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IoSleepPayload(_Strict):
    """I/O-bound demo job: waits, then succeeds."""

    duration_ms: int = Field(ge=0, le=60_000)


class CpuHashPayload(_Strict):
    """CPU-bound demo job: repeated hashing."""

    iterations: int = Field(ge=1, le=50_000_000)


class FailPayload(_Strict):
    """Failure-injection job: fails ``failures`` times, then succeeds.

    ``kind="permanent"`` ignores ``failures`` and always dead-letters.
    """

    kind: Literal["transient", "permanent"]
    failures: int = Field(default=1, ge=0, le=100)


class IoSleepJob(_Strict):
    type: Literal["io_sleep"]
    schema_version: Literal[1]
    payload: IoSleepPayload


class CpuHashJob(_Strict):
    type: Literal["cpu_hash"]
    schema_version: Literal[1]
    payload: CpuHashPayload


class FailJob(_Strict):
    type: Literal["fail"]
    schema_version: Literal[1]
    payload: FailPayload


class PoisonPayload(_Strict):
    """Fault injection for the failure tests. Rejected unless QS_FAULT_INJECTION=1.

    child_exit:  the CPU child process dies (segfault/OOM-kill of a child)
    worker_exit: the worker process itself dies (the job crash-loops every worker that runs it)
    hang:        the CPU child spins forever (only the timeout can stop it)
    """

    mode: Literal["child_exit", "worker_exit", "hang"]


class PoisonJob(_Strict):
    type: Literal["poison"]
    schema_version: Literal[1]
    payload: PoisonPayload


AnyJob = IoSleepJob | CpuHashJob | FailJob | PoisonJob
JobRequest = Annotated[AnyJob, Field(discriminator="type")]


class JobAccepted(BaseModel):
    job_id: UUID
    state: str


class JobStatus(BaseModel):
    job_id: UUID
    type: str
    state: str
    attempts: int
    epoch: int


def request_hash(job: AnyJob) -> str:
    """Stable fingerprint of a request, used to detect idempotency-key reuse with a new body."""
    canonical = json.dumps(job.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()
