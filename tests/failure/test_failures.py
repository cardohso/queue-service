"""Failure tests (DESIGN.md s12.5). Each injects a real fault under load, then checks:

* every accepted job (every 202 the client got) ends in exactly one final state
* done <=> exactly one result row; attempt budget never exceeded
* every delivery is eventually acked (stream empty, nothing pending)

and reports the cost: extra executions (at-least-once duplicates) and recovery time.
Run: ``uv run pytest -m failure -s`` (``-s`` prints the result lines).
"""

import asyncio
import json
import os
import statistics
import sys
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from redis.asyncio import Redis

from tests.failure.harness import Cluster, Infra, Report, fail, io, poison, wait_until

LEASE_S = 2.0  # FAST_ENV QS_LEASE_S


async def _first_done_after(c: Cluster, t: datetime) -> float | None:
    rows = await c.q(
        "SELECT min(finished_at) AS t FROM job_attempts WHERE outcome = 'done' AND finished_at > $1",
        t,
    )
    first = rows[0]["t"]
    return None if first is None else (first - t).total_seconds()


async def _done_count(c: Cluster) -> int:
    rows = await c.q("SELECT count(*) AS n FROM jobs WHERE state = 'done'")
    return int(rows[0]["n"])


# ---- 1. kill -9 a worker mid-job ------------------------------------------------------------


async def test_kill9_worker_mid_job(make_infra: Callable[..., Infra], make_cluster: Any) -> None:
    infra = make_infra()
    c: Cluster = make_cluster(infra)
    a = await c.worker("A")
    await c.worker("B")
    await c.api()
    ids = await c.submit_many([io(300) for _ in range(200)], "kill9")

    await wait_until(lambda: _ge(c.running_on("A"), 4), 30, "worker A is running >= 4 jobs")
    held = await c.running_on("A")
    t_kill = await c.db_now()
    t_kill_host = datetime.now(UTC)
    a.kill9()

    await c.wait_final(ids, 60)
    await c.wait_stream_drained()
    rep = await c.check_invariant("kill9_worker_mid_job", ids)

    rows = await c.q(
        """SELECT max(nxt.finished_at) AS t FROM job_attempts cut
           JOIN job_attempts nxt ON nxt.job_id = cut.job_id AND nxt.epoch = cut.epoch
                                AND nxt.attempt = cut.attempt + 1
           WHERE cut.outcome = 'lost_lease' AND cut.worker_id = 'A'""",
    )
    rep.recovery_s = (rows[0]["t"] - t_kill).total_seconds() if rows[0]["t"] else None
    rep.notes |= {"jobs_held_by_killed_worker": held, "lease_s": LEASE_S}
    rep.notes["recovery_phases_s"] = await _recovery_phases(c, t_kill_host)
    rep.save()

    assert rep.done == rep.jobs
    # `held` was sampled just before the kill; a job can finish in between, so it is a hint,
    # not an exact count. What must hold: A was interrupted mid-job, and nothing A started
    # was left dangling (every unfinished attempt of A was closed by a takeover).
    assert rep.interrupted >= 1
    dangling = await c.q(
        "SELECT count(*) AS n FROM job_attempts WHERE worker_id = 'A' AND finished_at IS NULL"
    )
    assert dangling[0]["n"] == 0
    assert rep.recovery_s is not None
    assert rep.recovery_s < LEASE_S + 10, "takeover should take about one lease, not minutes"


async def _recovery_phases(c: Cluster, t_kill_host: datetime) -> dict[str, Any]:
    """Where recovery time goes, per interrupted job (all relative to the kill):

    detect   kill -> the survivor reclaims the message (XAUTOCLAIM; >= lease by design)
    wait     reclaimed -> claimed (waiting to run in isolation, or for a free slot)
    run      claimed -> done
    Host clock for log events, Postgres clock for attempt timestamps (each used consistently).
    """
    rows = await c.q(
        """SELECT nxt.job_id, nxt.started_at, nxt.finished_at FROM job_attempts cut
           JOIN job_attempts nxt ON nxt.job_id = cut.job_id AND nxt.epoch = cut.epoch
                                AND nxt.attempt = cut.attempt + 1
           WHERE cut.outcome = 'lost_lease' AND cut.worker_id = 'A'"""
    )
    reclaimed: dict[str, datetime] = {}
    for p in c.procs:
        for rec in p.records():
            if rec.get("event") == "isolating_suspect_job" and "job_id" in rec:
                t = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
                reclaimed.setdefault(rec["job_id"], t)
    detect, wait, run = [], [], []
    for r in rows:
        run.append((r["finished_at"] - r["started_at"]).total_seconds())
        t_rec = reclaimed.get(str(r["job_id"]))
        if t_rec is not None:
            detect.append((t_rec - t_kill_host).total_seconds())
    # wait: from the first reclaim to each claim, on the Postgres clock via the first claim
    starts = sorted(r["started_at"] for r in rows)
    if starts:
        wait = [(t - starts[0]).total_seconds() for t in starts]

    def stats(xs: list[float]) -> dict[str, float] | None:
        return {"min": round(min(xs), 2), "max": round(max(xs), 2)} if xs else None

    return {
        "jobs": len(rows),
        "detect": stats(detect),
        "claim_spread_after_first": stats(wait),
        "run": stats(run),
    }


async def _ge(v: Any, n: int) -> bool:
    return bool(await v >= n)


# ---- 2. restart Redis (with and without persistence) ---------------------------------------


async def _redis_restart(
    make_infra: Callable[..., Infra], make_cluster: Any, *, persistence: bool
) -> None:
    name = f"redis_restart_{'aof' if persistence else 'no_persistence'}"
    infra = make_infra(redis_persistence=persistence)
    c: Cluster = make_cluster(infra)
    await c.worker("A")
    await c.worker("B")
    await c.api()

    # Keep submitting while Redis restarts: the API must keep accepting (Postgres is up).
    submitting = asyncio.create_task(c.submit_many([io(100) for _ in range(400)], name, 8))  # noqa: RUF006 - awaited below
    await wait_until(lambda: _ge(_done_count(c), 50), 60, "50 jobs done before the fault")
    t_fault = await c.db_now()
    started = time.monotonic()
    infra.kill_restart_redis()
    await infra.wait_redis()
    redis_down_s = time.monotonic() - started
    t_back = await c.db_now()

    ids = await submitting
    await c.wait_final(ids, 90)
    await c.wait_stream_drained()
    rep = await c.check_invariant(name, ids)
    rep.recovery_s = await _first_done_after(c, t_back)
    rep.notes |= {
        "redis_persistence": persistence,
        "redis_down_s": round(redis_down_s, 2),
        "fault_at": str(t_fault),
    }
    rep.save()
    assert rep.done == rep.jobs


async def test_redis_kill_and_restart_with_aof(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    await _redis_restart(make_infra, make_cluster, persistence=True)


async def test_redis_kill_and_restart_losing_all_data(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    await _redis_restart(make_infra, make_cluster, persistence=False)


# ---- 3. Postgres unavailable (refused), frozen (hang), slow --------------------------------


async def _postgres_outage(
    make_infra: Callable[..., Infra], make_cluster: Any, *, mode: str, outage_s: float = 5.0
) -> None:
    name = f"postgres_{mode}"
    infra = make_infra()
    c: Cluster = make_cluster(infra)
    w = await c.worker("A")
    await c.worker("B")
    await c.api()

    submitting = asyncio.create_task(c.submit_many([io(100) for _ in range(300)], name, 8))  # noqa: RUF006 - awaited below
    await wait_until(lambda: _ge(_done_count(c), 50), 60, "50 jobs done before the fault")
    if mode == "stopped":
        infra.stop_postgres()
    else:
        infra.pause_postgres()
    await asyncio.sleep(outage_s)  # the outage itself is the fault: its length is the input
    if mode == "stopped":
        infra.start_postgres()
    else:
        infra.unpause_postgres()
    await infra.wait_postgres()
    t_back = await c.db_now()

    ids = await asyncio.wait_for(submitting, 120)
    await c.wait_final(ids, 120)
    await c.wait_stream_drained(60)
    rep = await c.check_invariant(name, ids)
    rep.recovery_s = await _first_done_after(c, t_back)
    m = await c.metrics(w)
    rep.notes |= {
        "outage_s": outage_s,
        "worker_A_commit_gave_up": m.get("qs_commit_gave_up_total", 0),
        "worker_A_db_unavailable": m.get("qs_db_unavailable_total", 0),
    }
    rep.save()
    assert rep.done == rep.jobs


async def test_postgres_stopped_for_5s(make_infra: Callable[..., Infra], make_cluster: Any) -> None:
    await _postgres_outage(make_infra, make_cluster, mode="stopped")


async def test_postgres_frozen_for_5s(make_infra: Callable[..., Infra], make_cluster: Any) -> None:
    await _postgres_outage(make_infra, make_cluster, mode="paused")


async def test_postgres_slow_100ms_each_way(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    infra = make_infra(toxiproxy=True)
    await infra.toxiproxy_latency(100, jitter_ms=20)
    c: Cluster = make_cluster(infra, pg_url=infra.proxied_pg_url)  # services go through the proxy
    await c.worker("A")
    await c.worker("B")
    await c.api()
    started = time.monotonic()
    ids = await c.submit_many([io(50) for _ in range(150)], "pg_slow", 16)
    await c.wait_final(ids, 180)
    elapsed = time.monotonic() - started
    await c.wait_stream_drained()
    rep = await c.check_invariant("postgres_slow_100ms", ids)
    rep.notes |= {"latency_ms_each_way": 100, "wall_s": round(elapsed, 2)}
    rep.save()
    assert rep.done == rep.jobs
    assert c.client_stats["503"] == 0, "slow is not overloaded: latency alone must not cause 503s"
    assert rep.extra_executions == 0, "slow is not dead: latency alone must not cause duplicates"


# ---- 4. poison messages ----------------------------------------------------------------------


async def _poison_run(
    make_infra: Callable[..., Infra],
    make_cluster: Any,
    bad: dict[str, Any],
    name: str,
    *,
    supervise: bool = False,
    **worker_env: str,
) -> tuple[Report, list[UUID], Cluster]:
    infra = make_infra()
    c: Cluster = make_cluster(infra)
    await c.worker("A", worker_env)
    await c.worker("B", worker_env)
    await c.api()
    restarts: Counter[str] = Counter()
    supervisor: asyncio.Task[None] | None = None
    if supervise:  # like a Deployment: a dead worker is replaced

        async def restart_dead() -> None:
            while True:
                for p in list(c.procs):
                    if p.name != "api" and not p.alive and p.name not in restarts.elements():
                        restarts[p.name] += 1
                        await c.worker(f"{p.name}-r{sum(restarts.values())}", worker_env)
                await asyncio.sleep(0.1)

        supervisor = asyncio.create_task(restart_dead())

    jobs = [io(50) for _ in range(60)]
    jobs[30] = bad
    ids = await c.submit_many(jobs, name, 8)
    try:
        await c.wait_final(ids, 120)
    finally:
        if supervisor is not None:
            supervisor.cancel()
    await c.wait_stream_drained(60)
    rep = await c.check_invariant(name, ids)
    rep.notes |= {"worker_restarts": sum(restarts.values())}
    rep.save()
    return rep, ids, c


async def _poison_row(c: Cluster, poison_id: UUID) -> Any:
    return (await c.q("SELECT state, attempts, dead_reason FROM jobs WHERE id = $1", poison_id))[0]


async def test_poison_permanent_error_dead_letters_once(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    rep, ids, c = await _poison_run(make_infra, make_cluster, fail("permanent"), "poison_permanent")
    row = await _poison_row(c, ids[30])
    assert (row["state"], row["attempts"], row["dead_reason"]) == ("dead", 1, "permanent")
    assert rep.done == rep.jobs - 1


async def test_poison_crashing_child_process(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    rep, ids, c = await _poison_run(
        make_infra, make_cluster, poison("child_exit"), "poison_child_exit"
    )
    row = await _poison_row(c, ids[30])
    assert (row["state"], row["dead_reason"]) == ("dead", "exhausted")
    assert row["attempts"] == 3  # QS_MAX_ATTEMPTS
    assert rep.done == rep.jobs - 1
    assert rep.notes["worker_restarts"] == 0  # the worker itself survived


async def test_poison_hanging_cpu_job_is_killed_by_the_timeout(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    rep, ids, c = await _poison_run(
        make_infra, make_cluster, poison("hang"), "poison_hang", QS_JOB_TIMEOUT_S="1"
    )
    row = await _poison_row(c, ids[30])
    assert (row["state"], row["dead_reason"]) == ("dead", "exhausted")
    assert rep.done == rep.jobs - 1


async def test_poison_that_kills_every_worker_is_dead_lettered_after_the_budget(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    rep, ids, c = await _poison_run(
        make_infra, make_cluster, poison("worker_exit"), "poison_worker_exit", supervise=True
    )
    row = await _poison_row(c, ids[30])
    # Collateral damage: innocent jobs whose crashes all happened on a worker that was
    # running the poison job at the time (i.e. they died with it, through no fault of their own).
    collateral = await c.q(
        """WITH poison_crashes AS (
               SELECT worker_id FROM job_attempts WHERE job_id = $1 AND outcome = 'lost_lease')
           SELECT a.job_id, count(*) AS crashes,
                  bool_and(a.worker_id IN (SELECT worker_id FROM poison_crashes)) AS all_with_poison
           FROM job_attempts a
           WHERE a.outcome = 'lost_lease' AND a.job_id <> $1 AND a.job_id = ANY($2::uuid[])
           GROUP BY a.job_id""",
        ids[30],
        ids,
    )
    innocents_dead = await c.q(
        "SELECT count(*) AS n FROM jobs WHERE id = ANY($1::uuid[]) AND id <> $2 AND state = 'dead'",
        ids,
        ids[30],
    )
    rep.notes |= {
        "innocent_jobs_crashed": len(collateral),
        "innocent_jobs_crashed_only_alongside_poison": sum(
            r["all_with_poison"] for r in collateral
        ),
        "innocent_jobs_dead_lettered": int(innocents_dead[0]["n"]),
        "max_crashes_charged_to_an_innocent_job": max(
            (r["crashes"] for r in collateral), default=0
        ),
    }
    rep.save()
    assert (row["state"], row["dead_reason"]) == ("dead", "crashed_max_attempts")
    assert rep.notes["innocent_jobs_dead_lettered"] == 0, "the poison took good jobs down with it"
    # The guarantee isolation gives: an innocent job pays for at most the one crash it
    # happened to share; after that it runs alone and cannot be next to the poison again.
    assert rep.notes["max_crashes_charged_to_an_innocent_job"] <= 1
    assert rep.done == rep.jobs - 1
    assert rep.notes["worker_restarts"] == 3  # one crash per allowed attempt, then it stops


# ---- 5. flood the API ------------------------------------------------------------------------


FLOOD_RATE_PER_S = int(os.environ.get("QS_FLOOD_RATE", "300"))  # ~4x the slow worker's drain rate
FLOOD_GENERATORS = 4  # separate processes: one Python client process cannot keep up
FLOOD_SECONDS = 8


async def test_flood_is_answered_with_429_not_errors_and_depth_stays_bounded(
    make_infra: Callable[..., Infra], make_cluster: Any
) -> None:
    """Open-loop flood: requests arrive at a fixed rate whether or not earlier ones finished.

    A closed-loop test (N clients, each waiting for its reply) slows its own arrival rate down
    as the server slows, so it measures concurrency / machine throughput rather than overload.
    Real traffic does not wait. Latency is measured from each request's *scheduled* send time,
    so a client that falls behind cannot hide server slowness (no coordinated omission).
    """
    max_depth = 100
    infra = make_infra()
    c: Cluster = make_cluster(infra)
    await c.worker("A", {"QS_MAX_IN_FLIGHT": "4"})  # deliberately slow consumer: ~80 jobs/s
    await c.api({"QS_MAX_DEPTH": str(max_depth), "QS_SAMPLE_INTERVAL_S": "0.1"})

    codes: Counter[int] = Counter()
    latencies: list[float] = []
    send_lateness: list[float] = []  # client health: did we send on schedule?
    by_code: dict[int, list[float]] = {}
    missing_retry_after = 0
    accepted: list[UUID] = []
    depths: list[int] = []
    stop = asyncio.Event()

    async def sample_depth() -> None:
        r = Redis.from_url(infra.redis_url, decode_responses=True)
        try:
            while not stop.is_set():
                groups = await r.xinfo_groups("jobs")
                for g in groups:
                    if g["name"] == "workers":
                        depths.append(int(g.get("lag") or 0) + int(g["pending"]))
                await asyncio.sleep(0.05)
        finally:
            await r.aclose()

    sampler = asyncio.create_task(sample_depth())
    gens = []
    for g in range(FLOOD_GENERATORS):
        out = c.log_dir / f"loadgen-{g}.json"
        gens.append(
            (
                out,
                await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "tests.failure.loadgen",
                    f"http://127.0.0.1:{c.api_port}",
                    str(FLOOD_RATE_PER_S / FLOOD_GENERATORS),
                    str(FLOOD_SECONDS),
                    f"flood{g}",
                    str(out),
                ),
            )
        )
    for _, proc in gens:
        assert await asyncio.wait_for(proc.wait(), FLOOD_SECONDS + 120) == 0
    offered_s = 0.0
    for out, _ in gens:
        data = json.loads(out.read_text())
        offered_s = max(offered_s, data["elapsed_s"])
        send_lateness.extend(data["send_lateness"])
        for status, latency, job_id, has_retry_after in data["results"]:
            code = status if isinstance(status, int) else 0  # 0 = transport error
            latencies.append(latency)
            by_code.setdefault(code, []).append(latency)
            codes[code] += 1
            if code == 202:
                accepted.append(UUID(job_id))
            elif code == 429 and not has_retry_after:
                missing_retry_after += 1
    stop.set()
    await sampler

    api_metrics = await c.metrics(next(p for p in c.procs if p.name == "api"))
    await c.wait_final(accepted, 120)
    rep = await c.check_invariant("api_flood", accepted)

    def pct(xs: list[float], q: float) -> float:
        xs = sorted(xs)
        return xs[min(len(xs) - 1, int(q * len(xs)))]

    rep.notes |= {
        "load": f"open loop, {FLOOD_RATE_PER_S} req/s for {FLOOD_SECONDS} s, {FLOOD_GENERATORS} procs",
        "requests": sum(codes.values()),
        "codes": dict(codes),
        "latency_ms": {
            q: round(pct(latencies, v) * 1000, 1)
            for q, v in (("p50", 0.50), ("p95", 0.95), ("p99", 0.99), ("max", 1.0))
        }
        | {"mean": round(statistics.fmean(latencies) * 1000, 1)},
        "p99_ms_by_code": {k: round(pct(v, 0.99) * 1000, 1) for k, v in by_code.items()},
        "client_send_lateness_p99_ms": round(pct(send_lateness, 0.99) * 1000, 1),
        "offered_rate_per_s": round(sum(codes.values()) / offered_s, 1),
        "max_depth_limit": max_depth,
        "max_observed_depth": max(depths) if depths else None,
        "api_loop_lag_mean_ms": round(
            1000
            * api_metrics.get("qs_event_loop_lag_seconds_sum", 0)
            / max(1.0, api_metrics.get("qs_event_loop_lag_seconds_count", 1)),
            2,
        ),
    }
    rep.save()

    # The measurement is only valid if the client really sent at the intended rate.
    # Measured on an M1 Pro (8 cores) shared with Docker: healthy runs 2-30 ms; one busy run
    # 149 ms with valid results. Beyond 200 ms the generator, not the API, is the bottleneck.
    assert pct(send_lateness, 0.99) < 0.2, "load generator fell behind: result not valid"
    assert codes[429] > 0, "the flood never triggered back-pressure"
    assert set(codes) <= {202, 429}, f"errors under load: {dict(codes)}"
    assert missing_retry_after == 0
    assert pct(latencies, 0.99) < FLOOD_P99_BOUND_S, "back-pressure should keep the API responsive"
    # Soft limit: depth can overshoot by what is accepted between two samples.
    assert max(depths) <= max_depth * 2, f"depth ran away: {max(depths)}"


# Valid runs after the open-loop rewrite: p99 30-645 ms at 300 req/s (9 runs, loaded laptop).
FLOOD_P99_BOUND_S = 1.0
