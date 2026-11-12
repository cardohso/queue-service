# DESIGN

Status: **reviewed** (step 1 done, 2026-11-11). Nothing here is implemented yet. All owner
decisions are made; items marked **[VERIFY]** must be checked against current
library/Redis docs in step 2 before relying on them.

## 1. The claim

> Every accepted job reaches exactly one final state (`done` or `dead`), even when
> workers crash, the broker restarts or jobs fail; and under overload the service
> slows producers (429) instead of falling over.

Precise wording we can defend:

- **Accepted** = `POST /jobs` returned `202`. A `202` is sent only after the job row is
  committed in Postgres. Anything that failed before that point was never accepted
  (client got 5xx/429 and retries with the same idempotency key).
- **Delivery** is at-least-once. A handler can **run** more than once.
- **Effects in Postgres** happen once (fenced commit + unique constraint on `job_id`).
  Effects outside Postgres (HTTP calls, emails) are *not* covered; handlers receive
  `job_id` to use as their own idempotency key. This is "effectively-once", not
  exactly-once.
- **No ordering guarantee** between jobs.

## 2. Main change from the proposed design: Postgres is the source of truth

The proposal had Redis as the queue and Postgres only for results. That leaves two holes:

1. **Redis restart can lose accepted jobs.** With `appendfsync everysec` Redis can lose
   ~1 s of writes on a crash; with no persistence it loses everything. A `202` would
   then be a lie, and the "restart Redis" test would fail (or pass only because of a
   config flag).
2. **Idempotency keys stored in Redis vanish on the same restart**, so a client retry
   creates a second job.

**Proposal:** the `jobs` table in Postgres holds every job and its state. Redis Streams
is the *dispatch channel*: fast fan-out, consumer groups, pending-entry list (PEL),
`XAUTOCLAIM`. If Redis loses data, a **dispatcher** re-publishes from Postgres. The
invariant is then checkable with one SQL query.

> Interview version: "Two stores can't be updated atomically, so I picked one to be
> the truth and made the other rebuildable. Redis can forget anything and the system
> recovers, at the cost of duplicate deliveries, which the fenced, idempotent commit
> absorbs."

Alternatives considered:

| Option | Pro | Con |
|---|---|---|
| A. Redis = truth, `appendfsync always` | Simplest | Durability rests on one Redis flag; idempotency still fragile; DLQ/PEL lost if AOF is off |
| **B. Postgres = truth, Redis = dispatch (recommended)** | Survives total Redis loss; invariant is one query; keeps Streams/KEDA learning goals | More moving parts (dispatcher, leases) |
| C. Postgres only (`SELECT … FOR UPDATE SKIP LOCKED`) | Ack and commit in one transaction: simplest correct design | Drops Redis Streams, consumer groups and the KEDA Redis scaler, which are part of the CV goal |

**Decided (2026-11-11): B.** Postgres is the source of truth; Redis is a rebuildable
dispatch channel.

## 3. Architecture

```mermaid
flowchart LR
  C[Client] -->|POST /jobs + Idempotency-Key| API[FastAPI ingest]
  API -->|1. INSERT job, state=queued| PG[(Postgres: jobs, attempts, results)]
  API -->|2. XADD jobs stream| RS[(Redis Streams: jobs, dlq)]
  API -. reads lag sample .-> RS
  RS -->|XREADGROUP / XAUTOCLAIM| W[Worker: asyncio loop]
  W -->|claim / heartbeat / fenced commit| PG
  W -->|CPU jobs| PP[Process pool]
  W -->|XACK + XDEL| RS
  D[Dispatcher loop, runs in each worker] -->|due retries, lost dispatches| PG
  D -->|XADD| RS
  W -->|/metrics| P[Prometheus]
  API -->|/metrics| P
```

## 4. Job state machine

```
            claim                     fenced commit
 queued ─────────────▶ running ───────────────────────▶ done      (terminal)
   ▲  ▲                 │  │  │
   │  │ release         │  │  └── permanent error or attempts exhausted ──▶ dead (terminal)
   │  └─────────────────┘  │
   │   (graceful shutdown) │ transient error
   │                       ▼
   └──── dispatcher ── retry_wait (next_attempt_at = now + backoff)

 running with expired lease ── claim by another worker ──▶ running (attempt n+1)
 dead ── replay command ──▶ queued (logged as a new "epoch")
```

Tables (sketch):

- `jobs(id uuid pk, idempotency_key text unique null, type, schema_version, payload jsonb,
  state, attempts int, max_attempts int, lease_owner, lease_until, next_attempt_at,
  dispatched_at, epoch int, created_at, updated_at)`
- `job_attempts(job_id, epoch, attempt, worker_id, started_at, finished_at, outcome, error_class, error)`
- `results(job_id pk, epoch, result jsonb, committed_at)`

**Fencing.** `attempts` doubles as a fencing token. Every write after the claim says
`WHERE id=$1 AND state='running' AND attempts=$n AND lease_owner=$me`. A "zombie" worker
(paused by GC, network partition, laptop sleep) whose lease expired and was taken over
gets 0 rows and discards its result.

> Interview version: "A lease alone isn't enough, because the old holder doesn't know
> it lost the lease. Fencing makes the database reject its write, so at most one attempt
> can commit."

**Replay vs. "exactly one final state".** Replay moves `dead → queued` on purpose. The
invariant is stated per epoch: each `(job_id, epoch)` ends in exactly one final state;
replay is an explicit, audited new epoch.

## 5. Order of operations

### 5.1 Ingest (`POST /jobs`)

| # | Step | If the process dies right after it |
|---|---|---|
| 1 | Validate payload (pydantic, discriminated on `type` + `schema_version`) | Nothing written. Client gets error, retries. |
| 2 | `INSERT … ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`; if conflict, `SELECT` existing and return it | Not committed → not accepted. |
| 3 | `COMMIT` | **Accepted** but not dispatched, and the client may not have seen the 202. Dispatcher publishes it after `dispatch_grace` (e.g. 5 s). Client retry with the same key returns the same job. |
| 4 | `XADD jobs * job_id … epoch …`; `UPDATE dispatched_at` (best-effort) | Duplicate dispatch possible (dispatcher may publish again). Harmless: claim is conditional. |
| 5 | Return `202 {job_id}` | — |

Without an idempotency key, a client retry after a lost 202 creates a second job. That is
correct behaviour (we can't know) and is documented.

### 5.2 Worker, per message

| # | Step | If the worker dies right after it | Recovery |
|---|---|---|---|
| 1 | `XREADGROUP COUNT=free_slots` (message now in this consumer's PEL) | Message in PEL, job still `queued` | `XAUTOCLAIM` after `min_idle`, or dispatcher re-publishes |
| 2 | **Claim** in PG: `UPDATE … SET state='running', attempts=attempts+1, lease_until=now()+ttl, lease_owner=me WHERE id=$1 AND epoch=$e AND (state='queued' OR (state='running' AND lease_until<now())) RETURNING attempts`; insert `job_attempts` row. 0 rows → stale duplicate → go to step 6 | Job `running`, lease ticking | Lease expires → `XAUTOCLAIM` → re-claim as attempt n+1. **Crash counts as an attempt**, so a job that kills the worker is dead-lettered after `max_attempts` crashes instead of crash-looping forever. |
| 3 | Execute with timeout; heartbeat every `ttl/3` extends `lease_until` (fenced) and resets PEL idle (`XCLAIM … JUSTID` to self) | Work may be half done; nothing committed | Same as above. Handler runs again (at-least-once). |
| 4 | **Fenced commit**, one transaction: outcome → `done` + `INSERT results`, or `retry_wait` + `next_attempt_at`, or `dead` + reason. Attempt row finished. 0 rows → we were fenced out, discard | Outcome durable; message still in PEL | Redelivery hits step 2 with 0 rows (state no longer claimable) → ack. One duplicate *delivery*, zero duplicate effects. |
| 5 | If `dead`: `XADD dlq` (observability copy; may duplicate) | DLQ copy may be missing | DLQ truth is `state='dead'`; `dlq` stream is rebuildable |
| 6 | `XACK` + `XDEL` (or `XACKDEL` **[VERIFY]** Redis 8.2+) | — | — |

Rule: **never ack before the outcome is committed.** Every crash window therefore ends in
redelivery, never loss; redelivery is made safe by the conditional claim and fenced commit.

### 5.3 Dispatcher (a loop inside each worker; safe to run N copies)

*Revised in step 4.* Every `dispatch_interval` one statement picks candidates with
`FOR UPDATE SKIP LOCKED` and stamps `dispatched_at = now()` (a short "dispatch lease", so N
dispatchers don't publish the same job N times). Candidates:

- `retry_wait` with `next_attempt_at <= now()` → set `queued`, **always** publish.
- `queued` whose last dispatch is older than `dispatch_grace`.
- `running` whose lease expired more than `dispatch_grace` ago (the worker died *and* its PEL
  entry may be gone, e.g. Redis was wiped).

For the last two, publish **only if the recorded `stream_entry_id` no longer exists**. The
rule that makes this safe: an entry is deleted in the same `MULTI` as its `XACK`, so
*an entry that still exists has not been processed yet* and will be delivered or
reclaimed anyway.

> Interview version: "The first design re-published every queued job older than a few
> seconds. Under a 10k backlog that multiplies the stream every tick. Recording the entry ID
> and re-publishing only when the entry is provably gone keeps recovery after a Redis wipe
> without amplifying load."

Two details this rule depends on (both have tests that fail if broken):

- The consumer group is (re)created at ID `0`, not `$`. After a wipe the API can publish
  into a group-less stream; a group created at `$` would skip those entries, and the
  dispatcher would never re-publish them because they exist. They would be stuck forever.
- Ack is `XACK` + `XDEL` in one `MULTI`, so "exists" never means "acked but not deleted".

This loop covers: API crash between commit and `XADD`, Redis data loss, and delayed retries
(Redis Streams has no delayed delivery; sleeping in the worker would hold a slot and the PEL
entry).

**Known gap:** if Postgres is down when a worker *claims*, the entry stays in that worker's
PEL and is only picked up after `lease_s` (via `XAUTOCLAIM`). Recovery after a DB outage is
therefore up to one lease, not immediate. Measured in step 7.

## 6. Retries

- Error classes: `TransientError` (timeouts, connection errors, 5xx from dependencies,
  `BrokenProcessPool`) and `PermanentError` (validation, unknown type, handler raises
  permanent). Unknown exceptions: **transient**, so a bug doesn't silently dead-letter
  good jobs, but bounded by `max_attempts`. **Decided (2026-11-11): unknown → transient.**
The dead-letter record keeps the exception type, so a bug shows up clearly in the DLQ.
- Back-off: `delay = random.uniform(0, min(cap, base * 2**(attempt-1)))` (AWS "full
  jitter"). Computed with an injected `Clock` and `Random` so tests are exact.
- **Infrastructure errors don't burn attempts.** If Postgres is down, the worker can't
  claim (step 2) so attempts don't increase; it pauses fetching (circuit breaker:
  open → half-open probe) instead of pulling messages it can't process.
- Dead-letter record: reason, error class, full `job_attempts` history.
- `qs replay --job-id … | --all-dead --since …`: new epoch, attempts reset, logged.

> Interview version: "Full jitter spreads retries uniformly so a burst of failures
> doesn't come back as a synchronized burst. Infra outages pause consumption rather than
> consuming retry budget, otherwise a 2-minute DB blip would dead-letter healthy jobs."

## 7. Back-pressure

**Inside the worker.** `max_in_flight` = semaphore size. The fetch loop asks for exactly
`free_slots` messages and blocks when there are none. **No extra in-memory buffer:** a
message read into a buffer is already in our PEL, so buffering only adds messages that a
crash strands until `min_idle`. The broker *is* the buffer.

> Interview version: "Pull-based consumers get back-pressure for free if they only pull
> what they can start now. A local prefetch buffer improves throughput slightly but
> widens the crash blast radius, so I left it out."

**At the edge.** A background sampler reads consumer-group lag + pending
(`XINFO GROUPS`, Redis 7+) every 500 ms. If `lag > max_lag` → `429` with
`Retry-After = clamp(ceil(lag / recent_throughput), 1, 30)`. Using the same signal KEDA
scales on keeps admission and autoscaling consistent in phase 2. If Postgres is
unavailable or the DB pool can't give a connection within 200 ms → `503`. Admission is
approximate (sampled), so the limit is soft; the test asserts "bounded", not "exact".

If Redis is down but Postgres is up, the API still **accepts** (commit, `XADD` fails,
dispatcher catches up later) up to a backlog limit counted in Postgres.
**Decided (2026-11-11): keep accepting while Redis is down.** Acceptance depends only on
the durable store; a Redis outage delays work but doesn't reject producers.

**Load shedding by queue length, not timeout (revised in step 7).** The first version
answered 503 when a DB connection could not be acquired within 0.2 s. A failure test with
100 ms of injected latency each way showed it collapse: 11,864 503s for 150 accepted jobs,
587 s wall time. An experiment isolated why (16 clients, 10 s each):

| Pool config | Requests/s | Acquire timeouts | Server connections after |
|---|---|---|---|
| 0.2 s acquire timeout (old) | 0.4 | 626 | 0 |
| 2.0 s acquire timeout | 22.8 | 0 | 10 |
| pre-warmed to 10, 0.2 s timeout | 1.9 | 589 | 2 |

A new connection takes 0.84 s on that network, so it can never be opened within 0.2 s, and
cancelled acquires also tore down connections mid-setup, so even a pre-warmed pool shrank.
Now the API counts requests using or waiting for the DB and answers 503 **at once** beyond
`db_pool_max_size + db_max_waiting` (10 + 100), without touching the pool; the acquire timeout
(5 s) is only a safety net. Sizing rule: the last request in line must still get a connection
before the timeout: `pool * timeout / db_time_per_request` (10 * 5 / 0.25 s = 200).
The worker pool also has room for one heartbeat per in-flight job (`2 * max_in_flight + 4`);
failed heartbeats are now counted (`qs_heartbeat_failures_total`) instead of silent.
Result on the same test: 28.5 s, 0 503s, 0 duplicate executions.

> Interview version: "Shed load by queue length, not by timeout. A timeout that cancels work
> turns latency into errors, and here it even destroyed the connection pool."

## 8. Worker pool

- I/O jobs: asyncio tasks under the semaphore, `asyncio.timeout()`. Handlers must be
  cancellation-safe (no swallowing `CancelledError`).
- CPU jobs: process pool. Two traps in stdlib `ProcessPoolExecutor`:
  1. **Timeout doesn't stop the work.** Cancelling the awaiting future leaves the child
     process running; a stuck job keeps a pool slot forever.
  2. **One child crash breaks the whole pool** (`BrokenProcessPool`), failing every
     in-flight CPU job.
  Plan: use `pebble.ProcessPool` (per-task timeout kills the child) **[VERIFY]** 3.13/3.14
  support, or a small wrapper that recycles the pool on timeout/break. In-flight CPU jobs
  on a broken pool are classified transient. Pool sized to cores, separate semaphore from I/O.
- Event-loop lag: a task does `await asyncio.sleep(0.1)` and records the overshoot as a
  histogram. Test: run CPU jobs in-loop vs. in pool, assert p99 lag difference.

## 9. Crash recovery and the visibility timeout

`min_idle` (for `XAUTOCLAIM`) = `lease_ttl` (e.g. 30 s), heartbeat every 10 s.

| Shorter timeout | Longer timeout |
|---|---|
| Faster recovery after a crash | Slower recovery (up to `ttl` + scan interval) |
| More false reclaims of slow-but-alive jobs → duplicate execution | Fewer duplicates |
| Heartbeat must be frequent | Stuck jobs hold slots longer |

The heartbeat breaks the trade-off for *alive* workers: a live job's lease keeps
extending, so `ttl` only needs to cover "time to notice a dead worker", not "longest
job". Duplicates then come only from real crashes and pauses longer than `ttl`, and the
fence makes those harmless.

### 9.1 Isolating suspects (added in step 7)

**Found by a failure test.** "A crash counts as an attempt" stops a poison job crash-looping
forever, but a crash can't say *which* job caused it. Every job running on the dead worker
was charged an attempt, and `XAUTOCLAIM` then handed all of them, poison included, to the
next worker in one batch, which died the same way. Over 5 runs on the unfixed code, 0, 4, 6
and 7 innocent jobs were dead-lettered as `crashed_max_attempts`; every one had only ever
crashed next to the poison.

**Fix.** A claim that would be a takeover (the previous attempt's lease expired) is first
refused (`needs_isolation`, nothing changes). The worker then waits until it runs nothing
else, stops pulling new work, and runs that job **alone** (writer-preferring readers/writer
lock: normal jobs shared, suspects exclusive). A poison job then only ever kills itself, and
an innocent job pays for at most the one crash it happened to share. Five runs after the
fix: 0 innocent jobs dead-lettered, at most 1 crash charged to any innocent job, poison
dead-lettered after exactly 3 worker crashes.

**Cost.** Recovery after a crash is slower, because survivors re-run one at a time:
kill -9 recovery went from 7.0 s to 9.4 s for 8 interrupted jobs (one run each, 2 s lease).
It is only paid after a crash; normal throughput is unchanged.

> Interview version: "A crash can't tell you who caused it, so I don't guess: survivors are
> re-run in isolation, and the next crash has exactly one possible culprit."

## 10. Graceful shutdown (SIGTERM)

1. Stop fetching (readiness → 503 so the k8s Service stops routing; API only).
2. Wait for in-flight jobs up to `drain_deadline` (e.g. 25 s).
3. For jobs still running: cancel; **release** = fenced tx `state='queued',
   max_attempts=max_attempts+1` (shutdown isn't the job's fault), `XADD` new message, then
   `XACK` old one. *Revised in step 4:* the first draft decremented `attempts`, but
   `attempts` is the fencing token and must never go backwards (a reused token can let a
   stale write through). So the budget is refunded instead.
   Crash between `XADD` and `XACK` → duplicate delivery, harmless.
4. Close pools, exit 0. Phase 2: `terminationGracePeriodSeconds = drain_deadline + 5`.

A `kill -9` skips all of this; that path is covered by §9.

## 11. Observability (needed by phase 2)

*Revised in step 6.* One Prometheus registry per component. The names below are the phase-2
contract (alerts, dashboard, KEDA), so renaming one is a breaking change.

| Metric | Type | Labels | Exported by | Meaning |
|---|---|---|---|---|
| `qs_queue_depth` | gauge | `source` redis/postgres | API | Sampled depth used for admission (lag + pending) |
| `qs_jobs_in_flight` | gauge | `kind` io/cpu | worker | Jobs executing now |
| `qs_jobs_total` | counter | `outcome` done/retry/dead/released | worker | Attempt outcomes; `rate(done)` is throughput |
| `qs_job_duration_seconds` | histogram | `kind`, `outcome` | worker | One attempt, claim to decision |
| `qs_job_retries_total` | counter | `error_class` | worker | Retries by exception type |
| `qs_dead_letter_total` | counter | `reason` permanent/exhausted/crashed_max_attempts | worker | Alert on growth |
| `qs_duplicate_deliveries_total` | counter | | worker | Redeliveries absorbed by the claim |
| `qs_fenced_writes_total` | counter | | worker | Zombie commits rejected |
| `qs_reclaimed_total`, `qs_lost_lease_total` | counter | | worker | Crash recovery activity |
| `qs_dispatcher_republished_total` | counter | | worker | Retries due + lost dispatches |
| `qs_db_unavailable_total`, `qs_commit_gave_up_total` | counter | | worker | Postgres trouble |
| `qs_event_loop_lag_seconds` | histogram | | both | Lateness of a 100 ms sleep; proves CPU work is off the loop |
| `qs_api_jobs_accepted_total`, `qs_api_idempotent_replays_total` | counter | | API | |
| `qs_api_rejected_total` | counter | `code` 409/429/503 | API | |

**Health endpoints (API on the app port, worker on `QS_OPS_PORT`):**

- `/healthz` (liveness) fails only if the event loop has not turned for 5 s. It deliberately
  does **not** check Postgres or Redis: restarting a pod does not fix a dependency, and a
  liveness probe that fails on a dependency turns an outage into a restart storm.
- `/readyz` (readiness), API: Postgres reachable. Redis is *reported* (`redis=ok|degraded`)
  but does not gate readiness, because the API accepts while Redis is down (section 7).
- `/readyz`, worker: Postgres and Redis reachable and not draining.

> Interview version: "Liveness answers 'is this process stuck?', readiness answers 'can it do
> useful work right now?'. Mixing them is how a database blip becomes a restart loop."

**Logs:** one JSON object per line (structlog over stdlib). `job_id`, `epoch`, `attempt`,
`worker_id` are bound per message with contextvars, so every line a message produces carries
them, and concurrent jobs can't leak context into each other (each asyncio task has its own).
Infrastructure warnings include the exception: a bare `read_failed` once hid a config bug.

## 12. Testing strategy

Code is split behind ports (`Broker`, `Store`, `Clock`, `Executor`) so the same core runs
against fakes and real services.

1. **Unit:** back-off schedule with fake clock + seeded RNG; error classification;
   idempotent ingest; fence rejects stale attempt.
2. **Property-based (Hypothesis `RuleBasedStateMachine`):** rules = submit, deliver,
   handler succeed/transient/permanent, crash worker at step k, Redis wipe, advance
   clock, replay. Runs the *real* core against in-memory fakes. Invariant checked after
   every step and at the end (after advancing the clock past all retries): every accepted
   `(job, epoch)` is in exactly one terminal state; `results` has ≤ 1 row per job;
   `done ⇔ result exists`.
3. **Contract tests:** the same test suite against `FakeBroker` and real Redis, so
   fakes can't drift from real `XAUTOCLAIM`/PEL semantics (this is how property tests
   "pass for the wrong reason").
   *Findings from step 5 (mutation testing the property test):*
   - The model only proves something if it is deterministic. A wall-clock timeout, random
     UUIDs, an unseeded back-off RNG and a real-time heartbeat each made Hypothesis replays
     diverge (`FlakyStrategyDefinition`), and early "caught" mutants were really that crash.
     The model now uses event-based waits, sequential IDs, seeded RNGs, and no heartbeat
     (every lease runs out unless the job finishes: the harsher case).
   - Coverage is measured, not assumed: `--hypothesis-show-statistics` reports how often
     each fault path ran (fenced writes, crash loops, wipes with work in flight, replays).
   - **Defense in depth, observed:** acking *before* the commit, or after a failed claim or
     commit, still loses no job. The dispatcher sees a `running` job with an expired lease
     and no stream entry, and re-publishes it. Ack order therefore decides recovery *speed*
     (one lease vs. lease + grace), while the dispatcher is the second, independent
     guarantee of no loss.
4. **Integration:** testcontainers (Redis, Postgres) in CI.
5. **Failure tests** (each reports duplicates and recovery time, then checks the invariant
   with SQL):
   - `kill -9` worker mid-job: job type `block_until_released` emits a "started" event
     (Redis key / Postgres row); test waits on the event, kills, then releases.
     No sleeps.
   - Restart Redis, with AOF on **and** with persistence off (total loss).
   - Postgres slow (`toxiproxy` latency) and down (stop container).
   - Poison: permanent-error payload, and a payload that crashes the child process.
   - Flood: N concurrent clients; assert 429s appear, p99 of accepted stays bounded,
     no 5xx except 503 during injected outages, invariant holds.
6. **Benchmarks:** asyncio vs threads vs processes × I/O vs CPU jobs × worker count;
   ≥ 3 runs, report median and min–max; record CPU, RAM, OS, Python build. Optional
   extra: Python 3.14 free-threaded build (`3.14t`) to show the GIL effect disappearing
   for CPU-bound threads. **[VERIFY]** dependency wheels for 3.14t.

Determinism rules: fake clock for time logic; events instead of sleeps; fixed Hypothesis
seed in CI plus a nightly random-seed job; a flaky test is debugged, never retried away.

## 13. Known limits (to state in README)

- Single Redis node, single Postgres node; no HA.
- At-least-once delivery; once-only effects only inside Postgres.
- No ordering between jobs; no priorities.
- Admission control is sampled, so the lag limit is soft.
- Benchmarks are local, one machine.
- Postgres is the throughput ceiling (claim + commit = 2 transactions per job).
  Expected to be fine for a demo (thousands/s); measured in RESULTS.md, not assumed.

## 14. Stack

Python **3.14** (decided 2026-11-11; fall back to 3.13 only if step 2 finds a missing wheel
for `asyncpg`, `redis-py` or `pebble`); optional `3.14t` free-threaded benchmark;
FastAPI, pydantic v2, `redis-py` asyncio, `asyncpg`, `prometheus-client`, `structlog`;
uv, ruff, mypy (strict), pytest, pytest-asyncio, Hypothesis, testcontainers, toxiproxy;
Docker Compose; GitHub Actions; gitleaks for secret scan.
