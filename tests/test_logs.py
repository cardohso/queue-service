"""JSON logs and per-job context."""

import asyncio
import io
import json
import logging
from collections.abc import Iterator
from typing import Any

import pytest
import structlog

from queue_service.config import Settings
from queue_service.errors import TransientError
from queue_service.logs import bind_job, configure_logging
from queue_service.worker import Worker
from tests.fakes import FakeBroker, FakeClock, FakeStore
from tests.test_worker_metrics import Scripted


@pytest.fixture
def logbuf() -> Iterator[io.StringIO]:
    buf = io.StringIO()
    configure_logging("DEBUG", json_output=True, stream=buf)
    yield buf
    logging.getLogger().handlers.clear()
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()


def lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(ln) for ln in buf.getvalue().splitlines() if ln.strip()]


def test_stdlib_logging_with_extra_becomes_one_json_object_per_line(logbuf: io.StringIO) -> None:
    logging.getLogger("queue_service.test").warning("dispatch_failed", extra={"job_id": "j-1"})
    [rec] = lines(logbuf)
    assert rec["event"] == "dispatch_failed"
    assert rec["level"] == "warning"
    assert rec["job_id"] == "j-1"
    assert rec["logger"] == "queue_service.test"
    assert rec["timestamp"].endswith("Z")


def test_exceptions_are_rendered_into_the_json(logbuf: io.StringIO) -> None:
    try:
        raise RuntimeError("kaboom")
    except RuntimeError:
        logging.getLogger("x").exception("handler_crashed")
    [rec] = lines(logbuf)
    assert "RuntimeError: kaboom" in rec["exception"]


async def test_bound_context_is_per_task_and_never_leaks(logbuf: io.StringIO) -> None:
    log = logging.getLogger("ctx")

    async def handle(job_id: str) -> None:
        bind_job(job_id=job_id, attempt=1)
        await asyncio.sleep(0)  # interleave with the other task
        log.info("working")

    await asyncio.gather(handle("a"), handle("b"))
    log.info("outside")  # the parent context was never touched
    by_job = {r.get("job_id") for r in lines(logbuf) if r["event"] == "working"}
    assert by_job == {"a", "b"}
    assert "job_id" not in lines(logbuf)[-1]


async def test_every_log_line_of_a_handled_job_carries_its_ids(logbuf: io.StringIO) -> None:
    clock = FakeClock()
    store, broker, executor = FakeStore(clock), FakeBroker(clock), Scripted()
    executor.behaviour = TransientError("flaky")
    worker = Worker(
        Settings(commit_retry_s=0.0), store, broker, executor, worker_id="w9"
    )  # fmt: skip
    await broker.ensure_group()
    job_id = store.accept("io_sleep", {"duration_ms": 1}, 3)
    await broker.publish(job_id, 1)
    (msg,) = await broker.read("w9", 1, 0)
    await worker._handle(msg)

    recs = [r for r in lines(logbuf) if r["logger"] == "queue_service.worker"]
    assert [r["event"] for r in recs] == ["job_finished"]
    [rec] = recs
    assert (rec["job_id"], rec["epoch"], rec["attempt"], rec["worker_id"]) == (
        str(job_id), 1, 1, "w9",
    )  # fmt: skip
    assert rec["outcome"] == "retry"
    assert rec["error_class"] == "TransientError"
    assert rec["duration_ms"] >= 0
