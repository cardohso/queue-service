"""Open-loop load generator that runs *inside* the cluster for the chaos tests (deploy/chaos.py).

Why in the cluster: `kubectl port-forward` pins one API pod and dies with it, so a client on the
host would measure the tunnel, not the Service. This pod talks to http://api:8000 like any
other in-cluster client. Standard library only, so it runs in the app image unchanged.

Each job is offered once at its scheduled time (open loop) and retried with the SAME
Idempotency-Key until accepted: on 429/503 after Retry-After, on connection errors after a
short pause. A retry after a timeout cannot create a second job; it returns the first one.

Env: RATE (jobs/s), SECONDS, PREFIX, TARGET (default http://api:8000), GIVE_UP_S (default 180).
Prints `STARTED <unix time>` when the first job is due, and `RESULT <json>` at the end.
"""

import http.client
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor

TARGET = os.environ.get("TARGET", "http://api:8000")
RATE = float(os.environ["RATE"])
SECONDS = float(os.environ["SECONDS"])
PREFIX = os.environ["PREFIX"]
GIVE_UP_S = float(os.environ.get("GIVE_UP_S", "180"))


def job(i: int) -> dict[str, object]:
    """A fixed mix: retries (exercise the dispatcher), dead letters, CPU and I/O work."""
    m = i % 20
    if m == 0:
        return {
            "type": "fail",
            "schema_version": 1,
            "payload": {"kind": "transient", "failures": 2},
        }
    if m == 1:
        return {"type": "fail", "schema_version": 1, "payload": {"kind": "permanent"}}
    if m in (2, 3):
        return {"type": "cpu_hash", "schema_version": 1, "payload": {"iterations": 200_000}}
    return {
        "type": "io_sleep",
        "schema_version": 1,
        "payload": {"duration_ms": 300 + (i * 37) % 1200},
    }


lock = threading.Lock()
timeline: dict[int, Counter[str]] = defaultdict(Counter)  # second since start -> outcome counts
accepted: dict[int, str] = {}
tries_hist: Counter[int] = Counter()
gave_up: list[int] = []
rejected: dict[int, int] = {}
lateness: list[float] = []
start = 0.0


def tick(outcome: str) -> None:
    with lock:
        timeline[int(time.monotonic() - start)][outcome] += 1


def send(i: int, scheduled: float) -> None:
    with lock:
        lateness.append(time.monotonic() - scheduled)
    body = json.dumps(job(i)).encode()
    headers = {"content-type": "application/json", "Idempotency-Key": f"{PREFIX}-{i}"}
    tries = 0
    while True:
        tries += 1
        req = urllib.request.Request(f"{TARGET}/jobs", data=body, headers=headers, method="POST")
        retry_after: str | None = None
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                status, payload = r.status, r.read()
        except urllib.error.HTTPError as e:  # a response with a 4xx/5xx status
            status, payload, retry_after = e.code, e.read(), e.headers.get("retry-after")
        except urllib.error.URLError, OSError, http.client.HTTPException:
            status, payload = 0, b""  # refused, reset, timed out: no response at all
        if status == 202:
            tick("202")
            with lock:
                accepted[i] = json.loads(payload)["job_id"]
                tries_hist[tries] += 1
            return
        tick(str(status) if status else "conn_error")
        if status not in (0, 429, 503):
            with lock:
                rejected[i] = status  # a 4xx/500 for a valid job is a bug: never retried
            return
        if time.monotonic() - scheduled > GIVE_UP_S:
            with lock:
                gave_up.append(i)
            return
        wait = min(float(retry_after or 0.5), 2.0) if status else 0.5
        time.sleep(wait * random.uniform(0.5, 1.5))


def main() -> None:
    global start
    n = int(RATE * SECONDS)
    pool = ThreadPoolExecutor(max_workers=256)
    start = time.monotonic()
    print(f"STARTED {time.time():.3f}", flush=True)
    for i in range(n):
        scheduled = start + i / RATE
        delay = scheduled - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        pool.submit(send, i, scheduled)
    pool.shutdown(wait=True)
    result = {
        "prefix": PREFIX,
        "rate": RATE,
        "seconds": SECONDS,
        "offered": n,
        "accepted": len(accepted),
        "accepted_ids": sorted(accepted.values()),
        "gave_up": len(gave_up),
        "rejected": rejected,
        "tries_histogram": dict(sorted(tries_hist.items())),
        "max_send_lateness_s": round(max(lateness, default=0.0), 3),
        "elapsed_s": round(time.monotonic() - start, 1),
        "timeline": {s: dict(c) for s, c in sorted(timeline.items())},
    }
    print("RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
