"""The real `qs worker` process: it serves health and metrics, logs JSON, and exits 0 on SIGTERM."""

import asyncio
import json
import os
import signal
import socket
import sys
from typing import Any

import httpx
import pytest

from queue_service.ingest import accept_job
from queue_service.schemas import IoSleepJob
from tests.integration.conftest import MakeClient
from tests.integration.test_worker import wait_until


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def test_worker_process_serves_ops_endpoints_logs_json_and_exits_cleanly_on_sigterm(
    pg_url: str, redis_url: str, make_client: MakeClient
) -> None:
    client, app = await make_client()  # creates the schema; also gives us a pool to submit with
    port = _free_port()
    env = {
        **os.environ,
        "QS_DATABASE_URL": pg_url,
        "QS_REDIS_URL": redis_url,
        "QS_OPS_PORT": str(port),
        "QS_BLOCK_MS": "100",
        "QS_CPU_WORKERS": "1",
        "QS_DRAIN_DEADLINE_S": "2",
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "queue_service.cli", "worker",
        env=env, stderr=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
    )  # fmt: skip
    try:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as ops:

            async def ready() -> bool:
                try:
                    return (await ops.get("/readyz")).status_code == 200
                except httpx.TransportError:
                    return False

            await wait_until(ready, timeout_s=30, what="worker /readyz")
            assert (await ops.get("/healthz")).status_code == 200

            job = IoSleepJob.model_validate(
                {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": 5}}
            )
            accepted = await accept_job(
                app.state.deps.pool, job, None, max_attempts=3, acquire_timeout_s=2
            )
            await app.state.deps.publisher.publish(accepted.job_id, accepted.epoch)

            async def done() -> bool:
                r = await client.get(f"/jobs/{accepted.job_id}")
                return bool(r.json()["state"] == "done")

            await wait_until(done, timeout_s=30, what="job done by the worker process")
            metrics = (await ops.get("/metrics")).text
            assert 'qs_jobs_total{outcome="done"} 1.0' in metrics
            assert "qs_event_loop_lag_seconds_count" in metrics

        proc.send_signal(signal.SIGTERM)
        _stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    except BaseException:
        # Show what the worker said: a bare timeout tells us nothing about why.
        if proc.returncode is None:
            proc.kill()
        _, err = await proc.communicate()
        print("---- worker stderr (tail) ----\n" + err.decode()[-3000:])
        raise
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()

    assert proc.returncode == 0, stderr.decode()[-2000:]
    records: list[dict[str, Any]] = []
    for line in stderr.decode().splitlines():
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            pytest.fail(f"non-JSON line on stderr: {line!r}")
    events = [r["event"] for r in records]
    assert "worker_started" in events
    assert "worker_stopped" in events
    finished = [r for r in records if r["event"] == "job_finished"]
    assert [(r["job_id"], r["outcome"]) for r in finished] == [(str(accepted.job_id), "done")]
