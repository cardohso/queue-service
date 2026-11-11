"""API health and metrics endpoints against real Postgres and Redis."""

from tests.integration.conftest import MakeClient

IO_JOB = {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": 5}}


async def _metric(client: object, name: str, labels: str = "") -> float | None:
    resp = await client.get("/metrics")  # type: ignore[attr-defined]
    assert resp.status_code == 200
    for line in resp.text.splitlines():
        if line.startswith(f"{name}{labels} "):
            return float(line.rsplit(" ", 1)[1])
    return None


async def test_healthz_and_readyz_are_ok_when_dependencies_are_up(make_client: MakeClient) -> None:
    client, _ = await make_client()
    assert (await client.get("/healthz")).status_code == 200
    ready = await client.get("/readyz")
    assert (ready.status_code, ready.text.strip()) == (200, "ok redis=ok")


async def test_readyz_stays_ready_but_reports_degraded_when_redis_is_down(
    make_client: MakeClient,
) -> None:
    # Accepting jobs needs only Postgres (DESIGN.md s7): don't pull pods out of rotation.
    client, _ = await make_client(redis_url="redis://127.0.0.1:1/0")
    ready = await client.get("/readyz")
    assert (ready.status_code, ready.text.strip()) == (200, "ok redis=degraded")


async def test_readyz_is_503_when_postgres_cannot_serve(make_client: MakeClient) -> None:
    client, app = await make_client(db_pool_max_size=1, db_acquire_timeout_s=0.1)
    async with app.state.deps.pool.acquire():  # the only connection is busy
        assert (await client.get("/readyz")).status_code == 503
    assert (await client.get("/readyz")).status_code == 200


async def test_metrics_reflect_accepts_replays_conflicts_and_back_pressure(
    make_client: MakeClient,
) -> None:
    client, app = await make_client(max_depth=0)
    sampler = app.state.deps.sampler
    await sampler.sample_once()
    # Depth is 0, limit is 0: admitted. After one accepted job is dispatched, depth is 1: 429.
    h = {"Idempotency-Key": "k"}
    assert (await client.post("/jobs", json=IO_JOB, headers=h)).status_code == 202
    assert (await client.post("/jobs", json=IO_JOB, headers=h)).status_code == 202  # replay
    other = {**IO_JOB, "payload": {"duration_ms": 6}}
    assert (await client.post("/jobs", json=other, headers=h)).status_code == 409
    await sampler.sample_once()
    assert (await client.post("/jobs", json=IO_JOB)).status_code == 429

    assert await _metric(client, "qs_api_jobs_accepted_total") == 1
    assert await _metric(client, "qs_api_idempotent_replays_total") == 1
    assert await _metric(client, "qs_api_rejected_total", '{code="409"}') == 1
    assert await _metric(client, "qs_api_rejected_total", '{code="429"}') == 1
    assert await _metric(client, "qs_queue_depth", '{source="redis"}') == 1
    assert await _metric(client, "qs_event_loop_lag_seconds_count") is not None
