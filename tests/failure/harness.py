"""Failure-test harness: real containers, real worker/API processes, real faults.

Everything that is killed, restarted or paused here is a real OS process or a real container:
``kill -9`` is SIGKILL, a Redis restart is ``docker restart -t 0``, a Postgres outage is
``docker stop`` or ``docker pause``. Waiting is always "poll a condition with a deadline";
there are no fixed sleeps except the *duration of an injected outage*, which is the fault.
"""

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.redis import RedisContainer
from testcontainers.core.container import DockerContainer

FINAL = ("done", "dead")

# Fast-but-realistic settings: a 2 s lease makes crash recovery take seconds, not minutes.
FAST_ENV = {
    "QS_LEASE_S": "2",
    "QS_RECLAIM_INTERVAL_S": "0.5",
    "QS_DISPATCH_INTERVAL_S": "0.5",
    "QS_DISPATCH_GRACE_S": "2",
    "QS_BLOCK_MS": "200",
    "QS_BACKOFF_BASE_S": "0.1",
    "QS_BACKOFF_CAP_S": "0.5",
    "QS_JOB_TIMEOUT_S": "5",
    "QS_COMMIT_RETRY_S": "2",
    "QS_CPU_WORKERS": "1",
    "QS_MAX_IN_FLIGHT": "8",
    "QS_DRAIN_DEADLINE_S": "3",
    "QS_FAULT_INJECTION": "1",
    "QS_MAX_ATTEMPTS": "3",
    "QS_SAMPLE_INTERVAL_S": "0.2",
    "QS_LOG_LEVEL": "INFO",
}


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def wait_until(
    pred: Callable[[], Awaitable[bool]], timeout_s: float, what: str, every_s: float = 0.05
) -> float:
    start = time.monotonic()
    while True:
        try:
            if await pred():
                return time.monotonic() - start
        # RedisError is not an OSError: a restarting Redis raises redis ConnectionError.
        except (
            OSError,
            RedisError,
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            httpx.TransportError,
        ):
            pass  # the thing we are waiting for may itself be down right now
        if time.monotonic() - start > timeout_s:
            raise AssertionError(f"timed out after {timeout_s}s waiting for: {what}")
        await asyncio.sleep(every_s)


# ---- infrastructure ------------------------------------------------------------------------


class Infra:
    """Postgres + Redis (+ optional toxiproxy) on fixed host ports, so they survive restarts."""

    def __init__(self, *, redis_persistence: bool = True, toxiproxy: bool = False) -> None:
        self.pg_port, self.redis_port = free_port(), free_port()
        self.pg = PostgresContainer("postgres:17-alpine", driver=None).with_bind_ports(
            5432, self.pg_port
        )
        redis_cmd = (
            "redis-server --appendonly yes --appendfsync everysec"
            if redis_persistence
            else "redis-server --save '' --appendonly no"
        )
        self.redis = (
            RedisContainer("redis:8-alpine")
            .with_bind_ports(6379, self.redis_port)
            .with_command(redis_cmd)
        )
        self.toxi: DockerContainer | None = None
        self.toxi_api_port = self.toxi_pg_port = 0
        if toxiproxy:
            self.toxi_api_port, self.toxi_pg_port = free_port(), free_port()
            self.toxi = (
                DockerContainer("ghcr.io/shopify/toxiproxy:2.9.0")
                .with_bind_ports(8474, self.toxi_api_port)
                .with_bind_ports(15432, self.toxi_pg_port)
                .with_kwargs(extra_hosts={"host.docker.internal": "host-gateway"})
            )

    def start(self) -> Infra:
        self.pg.start()
        self.redis.start()
        if self.toxi is not None:
            self.toxi.start()
        return self

    def stop(self) -> None:
        for c in (self.toxi, self.redis, self.pg):
            if c is not None:
                with contextlib.suppress(Exception):  # best effort: a paused one may refuse
                    c.stop()

    @property
    def host(self) -> str:
        return str(self.pg.get_container_host_ip())

    @property
    def pg_url(self) -> str:
        return f"postgresql://{self.pg.username}:{self.pg.password}@{self.host}:{self.pg_port}/{self.pg.dbname}"

    @property
    def proxied_pg_url(self) -> str:
        return f"postgresql://{self.pg.username}:{self.pg.password}@{self.host}:{self.toxi_pg_port}/{self.pg.dbname}"

    @property
    def redis_url(self) -> str:
        return f"redis://{self.host}:{self.redis_port}/0"

    # -- faults --

    def kill_restart_redis(self) -> None:
        """docker restart -t 0: SIGKILL Redis, then start it again (AOF decides what survives)."""
        self.redis.get_wrapped_container().restart(timeout=0)

    def stop_postgres(self) -> None:
        self.pg.get_wrapped_container().stop(timeout=0)

    def start_postgres(self) -> None:
        self.pg.get_wrapped_container().start()

    def pause_postgres(self) -> None:
        """Freeze the server: TCP stays open, nothing answers. The nastiest kind of outage."""
        self.pg.get_wrapped_container().pause()

    def unpause_postgres(self) -> None:
        self.pg.get_wrapped_container().unpause()

    async def wait_redis(self, timeout_s: float = 30) -> None:
        r = Redis.from_url(self.redis_url)
        try:
            await wait_until(lambda: r.ping(), timeout_s, "redis answers PING")
        finally:
            await r.aclose()

    async def wait_postgres(self, timeout_s: float = 60) -> None:
        async def ok() -> bool:
            conn = await asyncpg.connect(self.pg_url, timeout=2)
            await conn.close()
            return True

        await wait_until(ok, timeout_s, "postgres accepts connections")

    async def toxiproxy_latency(self, latency_ms: int, jitter_ms: int = 0) -> None:
        assert self.toxi is not None
        base = f"http://{self.host}:{self.toxi_api_port}"
        async with httpx.AsyncClient(base_url=base) as c:
            await wait_until(lambda: _ok(c.get("/version")), 30, "toxiproxy API up")
            r = await c.post(
                "/proxies",
                json={
                    "name": "pg",
                    "listen": "0.0.0.0:15432",
                    "upstream": f"host.docker.internal:{self.pg_port}",
                },
            )
            assert r.status_code in (200, 201, 409), r.text
            for stream in ("downstream", "upstream"):
                r = await c.post(
                    "/proxies/pg/toxics",
                    json={
                        "name": f"latency_{stream}",
                        "type": "latency",
                        "stream": stream,
                        "attributes": {"latency": latency_ms, "jitter": jitter_ms},
                    },
                )
                assert r.status_code in (200, 201), r.text


async def _ok(resp: Awaitable[httpx.Response]) -> bool:
    return (await resp).status_code == 200


# ---- processes ------------------------------------------------------------------------------


class Proc:
    """A real `qs` process. Logs (JSON lines) go to a file so they survive SIGKILL."""

    def __init__(self, name: str, args: list[str], env: dict[str, str], log_dir: Path) -> None:
        self.name = name
        self.log_path = log_dir / f"{name}.log"
        self._log = self.log_path.open("w")
        self.popen = subprocess.Popen(
            [sys.executable, "-m", "queue_service.cli", *args],
            env=env,
            stdout=self._log,
            stderr=subprocess.STDOUT,
        )
        self.port = int(env.get("QS_OPS_PORT") or env.get("QS_API_PORT") or 0)

    @property
    def alive(self) -> bool:
        return self.popen.poll() is None

    def kill9(self) -> None:
        self.popen.send_signal(signal.SIGKILL)
        self.popen.wait(10)

    def terminate(self) -> None:
        if self.alive:
            self.popen.send_signal(signal.SIGTERM)

    async def wait_exit(self, timeout_s: float = 20) -> int:
        await wait_until(lambda: _done(self), timeout_s, f"{self.name} exits")
        return int(self.popen.returncode)

    def records(self) -> list[dict[str, Any]]:
        self._log.flush()
        out = []
        for line in self.log_path.read_text().splitlines():
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                out.append({"event": "non_json_line", "line": line})
        return out


async def _done(p: Proc) -> bool:
    return not p.alive


# ---- results --------------------------------------------------------------------------------


@dataclass
class Report:
    test: str
    jobs: int
    done: int
    dead: int
    executions: int  # attempts that ran (released attempts excluded)
    extra_executions: int  # executions beyond one per job: the cost of at-least-once
    interrupted: int  # attempts cut off by a crash or lost lease (outcome lost_lease)
    dead_reasons: dict[str, int]
    recovery_s: float | None = None
    notes: dict[str, Any] = field(default_factory=dict)

    def save(self) -> None:
        print("\nFAILURE-TEST RESULT " + json.dumps(asdict(self), default=str))
        out = os.environ.get("QS_RESULTS_DIR")
        if out:
            path = Path(out) / "failures"
            path.mkdir(parents=True, exist_ok=True)
            (path / f"{self.test}.json").write_text(json.dumps(asdict(self), indent=2, default=str))


# ---- cluster ---------------------------------------------------------------------------------


class Cluster:
    def __init__(self, infra: Infra, log_dir: Path, *, pg_url: str | None = None) -> None:
        self.infra = infra
        self.log_dir = log_dir
        self.pg_url = pg_url or infra.pg_url  # what the services use (maybe via toxiproxy)
        self.procs: list[Proc] = []
        self.client_stats: Counter[str] = Counter()
        self.api_port = 0

    def _env(self, **extra: str) -> dict[str, str]:
        return {
            **os.environ,
            **FAST_ENV,
            "QS_DATABASE_URL": self.pg_url,
            "QS_REDIS_URL": self.infra.redis_url,
            **extra,
        }

    async def worker(
        self, name: str, env: dict[str, str] | None = None, *, wait_ready: bool = True
    ) -> Proc:
        port = free_port()
        p = Proc(
            name,
            ["worker"],
            self._env(QS_WORKER_ID=name, QS_OPS_PORT=str(port), **(env or {})),
            self.log_dir,
        )
        self.procs.append(p)
        if wait_ready:
            await self._wait_http(p, "/readyz")
        return p

    async def api(self, env: dict[str, str] | None = None) -> Proc:
        self.api_port = free_port()
        p = Proc(
            "api", ["api"], self._env(QS_API_PORT=str(self.api_port), **(env or {})), self.log_dir
        )
        self.procs.append(p)
        await self._wait_http(p, "/readyz")
        return p

    async def _wait_http(self, p: Proc, path: str, timeout_s: float = 60) -> None:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{p.port}", timeout=2) as c:

            async def ok() -> bool:
                if not p.alive:
                    raise AssertionError(
                        f"{p.name} died during start-up:\n{p.log_path.read_text()[-3000:]}"
                    )
                return (await c.get(path)).status_code == 200

            await wait_until(ok, timeout_s, f"{p.name} {path}")

    async def metrics(self, p: Proc) -> dict[str, float]:
        async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{p.port}", timeout=2) as c:
            text = (await c.get("/metrics")).text
        out: dict[str, float] = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                key, value = line.rsplit(" ", 1)
                out[key] = float(value)
        return out

    def stop_all(self) -> None:
        for p in self.procs:
            if p.alive:
                p.popen.send_signal(signal.SIGKILL)
                p.popen.wait(10)

    # -- a well-behaved client: idempotency key, honours 429/503 Retry-After --

    async def submit(self, client: httpx.AsyncClient, job: dict[str, Any], key: str) -> UUID:
        while True:
            try:
                r = await client.post("/jobs", json=job, headers={"Idempotency-Key": key})
            except httpx.TransportError:
                self.client_stats["transport_error"] += 1
                await asyncio.sleep(0.2)
                continue
            if r.status_code == 202:
                self.client_stats["202"] += 1
                return UUID(r.json()["job_id"])
            if r.status_code in (429, 503):
                self.client_stats[str(r.status_code)] += 1
                await asyncio.sleep(min(float(r.headers.get("Retry-After", "1")), 0.5))
                continue
            raise AssertionError(f"unexpected {r.status_code}: {r.text}")

    async def submit_many(
        self, jobs: list[dict[str, Any]], prefix: str, concurrency: int = 16
    ) -> list[UUID]:
        sem = asyncio.Semaphore(concurrency)
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{self.api_port}", timeout=5
        ) as client:

            async def one(i: int, job: dict[str, Any]) -> UUID:
                async with sem:
                    return await self.submit(client, job, f"{prefix}-{i}")

            return list(await asyncio.gather(*(one(i, j) for i, j in enumerate(jobs))))

    # -- what is really stored --

    async def q(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        conn = await asyncpg.connect(self.infra.pg_url, timeout=5)
        try:
            return list(await conn.fetch(sql, *args))
        finally:
            await conn.close()

    async def db_now(self) -> datetime:
        rows = await self.q("SELECT now() AS t")
        return rows[0]["t"]  # type: ignore[no-any-return]

    async def count_final(self, ids: list[UUID]) -> int:
        rows = await self.q(
            "SELECT count(*) AS n FROM jobs WHERE id = ANY($1::uuid[]) AND state IN ('done','dead')",
            ids,
        )
        return int(rows[0]["n"])

    async def running_on(self, worker: str) -> int:
        rows = await self.q(
            "SELECT count(*) AS n FROM jobs WHERE state = 'running' AND lease_owner = $1", worker
        )
        return int(rows[0]["n"])

    async def wait_final(self, ids: list[UUID], timeout_s: float) -> float:
        async def all_final() -> bool:
            return await self.count_final(ids) == len(ids)

        try:
            return await wait_until(all_final, timeout_s, f"all {len(ids)} jobs final", 0.2)
        except AssertionError:
            stuck = await self.q(
                "SELECT state, count(*) AS n FROM jobs WHERE id = ANY($1::uuid[]) GROUP BY state",
                ids,
            )
            raise AssertionError(
                f"jobs never reached a final state: {[(r['state'], r['n']) for r in stuck]}\n"
                + self.log_tails()
            ) from None

    async def wait_stream_drained(self, timeout_s: float = 30) -> None:
        r = Redis.from_url(self.infra.redis_url, decode_responses=True)

        async def drained() -> bool:
            length = await r.xlen("jobs")
            groups = await r.xinfo_groups("jobs")
            return bool(length == 0 and sum(int(g["pending"]) for g in groups) == 0)

        try:
            await wait_until(drained, timeout_s, "stream drained (every delivery acked)", 0.2)
        finally:
            await r.aclose()

    def log_tails(self, n: int = 15) -> str:
        parts = []
        for p in self.procs:
            lines = p.log_path.read_text().splitlines()[-n:]
            parts.append(f"---- {p.name} (alive={p.alive}) ----\n" + "\n".join(lines))
        return "\n".join(parts)

    async def check_invariant(self, test: str, ids: list[UUID]) -> Report:
        """The core claim, checked against what Postgres actually holds."""
        assert len(set(ids)) == len(ids), "client got the same job id for different keys"
        rows = await self.q(
            "SELECT id, state, attempts, max_attempts, dead_reason FROM jobs WHERE id = ANY($1::uuid[])",
            ids,
        )
        found = {r["id"] for r in rows}
        assert found == set(ids), f"accepted jobs missing from the database: {set(ids) - found}"
        not_final = [r for r in rows if r["state"] not in FINAL]
        assert not not_final, f"jobs not in a final state: {not_final[:5]}"
        over = [r for r in rows if r["attempts"] > r["max_attempts"]]
        assert not over, f"attempt budget exceeded: {over[:5]}"

        done = {r["id"] for r in rows if r["state"] == "done"}
        results = await self.q(
            "SELECT job_id, count(*) AS n FROM results WHERE job_id = ANY($1::uuid[]) GROUP BY job_id",
            ids,
        )
        assert {r["job_id"] for r in results} == done, "done <=> has result is violated"
        assert all(r["n"] == 1 for r in results), "a job has more than one result"

        att = await self.q(
            """SELECT count(*) FILTER (WHERE outcome IS DISTINCT FROM 'released') AS execs,
                      count(*) FILTER (WHERE outcome = 'lost_lease') AS interrupted
               FROM job_attempts WHERE job_id = ANY($1::uuid[])""",
            ids,
        )
        dead_reasons = Counter(r["dead_reason"] for r in rows if r["state"] == "dead")
        execs = int(att[0]["execs"])
        return Report(
            test=test,
            jobs=len(ids),
            done=len(done),
            dead=len(ids) - len(done),
            executions=execs,
            extra_executions=execs - len(ids),
            interrupted=int(att[0]["interrupted"]),
            dead_reasons=dict(dead_reasons),
            notes={"client": dict(self.client_stats)},
        )


def io(ms: int) -> dict[str, Any]:
    return {"type": "io_sleep", "schema_version": 1, "payload": {"duration_ms": ms}}


def poison(mode: str) -> dict[str, Any]:
    return {"type": "poison", "schema_version": 1, "payload": {"mode": mode}}


def fail(kind: str, failures: int = 1) -> dict[str, Any]:
    return {"type": "fail", "schema_version": 1, "payload": {"kind": kind, "failures": failures}}
