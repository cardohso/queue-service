#!/usr/bin/env python3
"""Failure tests inside the kind cluster (phase 2, step 11). Standard library only.

    deploy/chaos.py kill|zero|rollout|drain|all  [--no-pdb]

Every scenario runs the same way: an open-loop load generator *inside* the cluster
(deploy/chaos/loadgen.py) submits a mix of I/O, CPU, retrying and dead-lettering jobs through
the api Service while the scenario breaks something. Then it waits for every job and checks the
claim against what Postgres holds:

    every accepted job is in exactly one final state (done or dead), none is lost (each job id
    the client got a 202 for exists), `done` <=> exactly one result row.

Scenarios:
  kill     pod deletions and crashes: force-delete and graceful delete of workers, SIGKILL of
           the worker process (container restarts), an API pod, the Redis pod (streams lost),
           the Postgres pod, and every worker at once
  zero     KEDA paused at 0 workers while load continues (jobs wait, retries are due with no
           dispatcher running), then resumed
  rollout  a new image rolled out to the API and the workers mid-load, then rolled back
  drain    every API and worker pod packed onto one node, then `kubectl drain` of that node:
           the PodDisruptionBudgets must keep at least one API pod serving
           (--no-pdb deletes the PDBs first: the mutation check, expected to FAIL)

Raw results: results/k8s/<scenario>.json. Exit 0 = every scenario passed.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CLUSTER = os.environ.get("KIND_CLUSTER", "qs")
CTX = f"kind-{CLUSTER}"
NS = "qs"
RATE = float(os.environ.get("CHAOS_RATE", "25"))
FINAL_TIMEOUT_S = float(os.environ.get("CHAOS_FINAL_TIMEOUT_S", "480"))

T0 = time.monotonic()
events: list[dict[str, Any]] = []


def log(msg: str) -> None:
    t = round(time.monotonic() - T0, 1)
    events.append({"t": t, "event": msg})
    print(f"  [{t:6.1f}s] {msg}", flush=True)


def run(
    cmd: list[str],
    *,
    input: str | None = None,
    check: bool = True,
    timeout: float = 300,
    merge_stderr: bool = False,
) -> str:
    p = subprocess.run(cmd, input=input, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode:
        raise RuntimeError(f"{' '.join(cmd[:6])}...: {p.stderr.strip() or p.stdout.strip()}")
    return p.stdout + p.stderr if merge_stderr else p.stdout


def kc(*args: str, **kw: Any) -> str:
    """kubectl in the qs namespace."""
    return run(["kubectl", "--context", CTX, "-n", NS, *args], **kw)


def kcc(*args: str, **kw: Any) -> str:
    """kubectl, cluster-scoped (nodes)."""
    return run(["kubectl", "--context", CTX, *args], **kw)


def psql(sql: str) -> list[str]:
    out = kc(
        "exec",
        "-i",
        "postgres-0",
        "--",
        "psql",
        "-U",
        "qs",
        "-d",
        "qs",
        "-At",
        "-v",
        "ON_ERROR_STOP=1",
        input=sql,
        timeout=60,
    )
    return out.strip().split("|")


def pods(app: str, *, terminating: bool = False) -> list[dict[str, Any]]:
    """Pods of an app; terminating ones (still draining, still running code) only on request."""
    items = json.loads(kc("get", "pods", "-l", f"app={app}", "-o", "json"))["items"]
    return [p for p in items if terminating or not p["metadata"].get("deletionTimestamp")]


def ready(pod: dict[str, Any]) -> bool:
    return any(
        c["type"] == "Ready" and c["status"] == "True" for c in pod["status"].get("conditions", [])
    )


def wait_for(pred: Any, what: str, timeout: float, every: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if pred():
                return
        except RuntimeError:
            pass  # e.g. Postgres restarting: keep polling
        time.sleep(every)
    raise RuntimeError(f"timed out after {timeout:.0f}s waiting for {what}")


def settle() -> None:
    """Start every scenario from a healthy system."""
    for d in ("api", "worker", "redis"):
        kc("rollout", "status", f"deployment/{d}", "--timeout=240s", timeout=260)
    kc("rollout", "status", "statefulset/postgres", "--timeout=240s", timeout=260)
    wait_for(lambda: psql("SELECT 1") == ["1"], "Postgres", 120)


# ---- load generator --------------------------------------------------------------------------


class Load:
    """The in-cluster load generator pod for one scenario."""

    def __init__(self, scenario: str, seconds: float) -> None:
        self.prefix = f"chaos-{scenario}-{int(time.time())}"
        self.name = f"loadgen-{scenario}"
        self.seconds = seconds
        self.image = kc(
            "get", "deploy", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}"
        )

    def start(self) -> None:
        cm = kc(
            "create",
            "configmap",
            "chaos-loadgen",
            f"--from-file=loadgen.py={ROOT / 'deploy/chaos/loadgen.py'}",
            "--dry-run=client",
            "-o",
            "yaml",
        )
        kc("apply", "-f", "-", input=cm)
        kc("delete", "pod", self.name, "--ignore-not-found", "--wait=true")
        env = {"RATE": str(RATE), "SECONDS": str(self.seconds), "PREFIX": self.prefix}
        pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": self.name, "namespace": NS, "labels": {"app": "loadgen"}},
            "spec": {
                "restartPolicy": "Never",
                # On the control plane: draining the app's nodes must not kill the client.
                "nodeSelector": {"node-role.kubernetes.io/control-plane": ""},
                "tolerations": [
                    {
                        "key": "node-role.kubernetes.io/control-plane",
                        "operator": "Exists",
                        "effect": "NoSchedule",
                    }
                ],
                "containers": [
                    {
                        "name": "loadgen",
                        "image": self.image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["python", "/lg/loadgen.py"],
                        "env": [{"name": k, "value": v} for k, v in env.items()],
                        "volumeMounts": [{"name": "lg", "mountPath": "/lg"}],
                        "resources": {"requests": {"cpu": "200m", "memory": "128Mi"}},
                    }
                ],
                "volumes": [{"name": "lg", "configMap": {"name": "chaos-loadgen"}}],
            },
        }
        kc("apply", "-f", "-", input=json.dumps(pod))
        wait_for(
            lambda: "STARTED" in kc("logs", self.name, check=False), "load generator", 120, 0.5
        )
        log(f"load started: {RATE:g} jobs/s for {self.seconds:g}s, prefix {self.prefix}")

    def result(self) -> dict[str, Any]:
        def finished() -> bool:
            phase = kc("get", "pod", self.name, "-o", "jsonpath={.status.phase}")
            return phase in ("Succeeded", "Failed")

        wait_for(finished, "load generator to finish", self.seconds + 240, 2)
        for line in kc("logs", self.name).splitlines():
            if line.startswith("RESULT "):
                log("load generator finished")
                return json.loads(line[len("RESULT ") :])
        raise RuntimeError("load generator printed no RESULT")

    def at(self, t: float) -> None:
        """Sleep until t seconds into the load."""
        while time.monotonic() < self.t_start + t:
            time.sleep(0.2)

    def __enter__(self) -> Load:
        self.start()
        self.t_start = time.monotonic()
        return self

    def __exit__(self, *exc: object) -> None:
        pass


# ---- the invariant ---------------------------------------------------------------------------


def verdict(load: Load, client: dict[str, Any]) -> dict[str, Any]:
    p = load.prefix
    where = f"j.idempotency_key LIKE '{p}-%'"

    def not_final() -> bool:
        return psql(
            f"SELECT count(*) FROM jobs j WHERE {where} AND state NOT IN ('done','dead')"
        ) == ["0"]

    t = time.monotonic()
    wait_for(not_final, "every job to reach a final state", FINAL_TIMEOUT_S, 2)
    log(f"every job final {time.monotonic() - t:.0f}s after the load ended")

    ids = ",".join(f"'{i}'" for i in client["accepted_ids"]) or "NULL"
    (accepted, done, dead, open_, dup, mism, execs, cut, released, workers, lost, wrong) = psql(f"""
        WITH j AS (SELECT * FROM jobs j WHERE {where}),
             a AS (SELECT a.* FROM job_attempts a JOIN j ON j.id = a.job_id)
        SELECT (SELECT count(*) FROM j),
               (SELECT count(*) FROM j WHERE state = 'done'),
               (SELECT count(*) FROM j WHERE state = 'dead'),
               (SELECT count(*) FROM j WHERE state NOT IN ('done','dead')),
               (SELECT count(*) FROM (SELECT r.job_id FROM results r JOIN j ON j.id = r.job_id
                                      GROUP BY 1 HAVING count(*) > 1) t),
               (SELECT count(*) FROM j LEFT JOIN results r ON r.job_id = j.id
                 WHERE (j.state = 'done') <> (r.job_id IS NOT NULL)),
               (SELECT count(*) FROM a WHERE outcome IS DISTINCT FROM 'released'),
               (SELECT count(*) FROM a WHERE outcome = 'lost_lease'),
               (SELECT count(*) FROM a WHERE outcome = 'released'),
               (SELECT count(DISTINCT worker_id) FROM a),
               (SELECT count(*) FROM unnest(ARRAY[{ids}]::uuid[]) u(id)
                 WHERE NOT EXISTS (SELECT 1 FROM jobs WHERE jobs.id = u.id)),
               (SELECT count(*) FROM j WHERE type = 'fail' AND payload->>'kind' = 'permanent'
                 AND state IS DISTINCT FROM 'dead');
    """)
    reasons = dict(
        row.split("=")
        for row in psql(f"""SELECT string_agg(dead_reason || '=' || n, '|') FROM
            (SELECT dead_reason, count(*) n FROM jobs j WHERE {where} AND state = 'dead'
             GROUP BY 1 ORDER BY 1) t""")
        if row
    )
    v: dict[str, Any] = {
        "offered": client["offered"],
        "accepted_by_client": client["accepted"],
        "accepted_in_postgres": int(accepted),
        "done": int(done),
        "dead": int(dead),
        "dead_reasons": {k: int(n) for k, n in reasons.items()},
        "not_final": int(open_),
        "lost": int(lost),
        "duplicate_results": int(dup),
        "done_result_mismatches": int(mism),
        "permanent_jobs_not_dead": int(wrong),
        "client_gave_up": client["gave_up"],
        "client_rejected": len(client["rejected"]),
        "executions": int(execs),
        "extra_executions": int(execs) - int(accepted),
        "attempts_cut_off": int(cut),
        "attempts_released": int(released),
        "distinct_workers": int(workers),
        "client_tries_histogram": client["tries_histogram"],
        "client_max_send_lateness_s": client["max_send_lateness_s"],
    }
    errs = Counter_sum(client["timeline"])
    v["client_responses"] = errs
    v["invariant_held"] = (
        v["accepted_in_postgres"] >= v["accepted_by_client"]
        and v["not_final"] == 0
        and v["lost"] == 0
        and v["duplicate_results"] == 0
        and v["done_result_mismatches"] == 0
        and v["permanent_jobs_not_dead"] == 0
    )
    return v


def Counter_sum(timeline: dict[str, dict[str, int]]) -> dict[str, int]:
    total: dict[str, int] = {}
    for counts in timeline.values():
        for k, n in counts.items():
            total[k] = total.get(k, 0) + n
    return dict(sorted(total.items()))


# ---- scenarios -------------------------------------------------------------------------------


def worker_node_pid(pod: dict[str, Any]) -> tuple[str, str]:
    """The node (a Docker container in kind) and host PID of the worker's main process."""
    node = pod["spec"]["nodeName"]
    cid = pod["status"]["containerStatuses"][0]["containerID"].split("://", 1)[1]
    info = json.loads(run(["docker", "exec", node, "crictl", "inspect", cid]))
    return node, str(info["info"]["pid"])


def scenario_kill() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    with Load("kill", 110) as load:

        def any_worker() -> dict[str, Any]:
            return random.choice(pods("worker"))

        load.at(10)
        w = any_worker()["metadata"]["name"]
        kc("delete", "pod", w, "--grace-period=0", "--force", "--wait=false")
        log(f"force-deleted worker pod {w} (no drain)")
        load.at(22)
        w = any_worker()["metadata"]["name"]
        kc("delete", "pod", w, "--wait=false")
        log(f"deleted worker pod {w} (graceful: SIGTERM, drain)")
        load.at(34)
        w_pod = any_worker()
        node, pid = worker_node_pid(w_pod)
        run(["docker", "exec", node, "kill", "-9", pid])
        log(f"SIGKILL of the worker process in {w_pod['metadata']['name']} (container restarts)")
        load.at(46)
        a = random.choice(pods("api"))["metadata"]["name"]
        kc("delete", "pod", a, "--wait=false")
        log(f"deleted API pod {a}")
        load.at(58)
        r = pods("redis")[0]["metadata"]["name"]
        kc("delete", "pod", r, "--grace-period=0", "--force", "--wait=false")
        log(f"force-deleted Redis pod {r}: every stream and consumer group is gone")
        load.at(72)
        kc("delete", "pod", "postgres-0", "--wait=false")
        log("deleted the Postgres pod (StatefulSet restarts it on the same volume)")
        load.at(92)
        names = [p["metadata"]["name"] for p in pods("worker")]
        kc("delete", "pod", *names, "--grace-period=0", "--force", "--wait=false")
        log(f"force-deleted every worker pod at once ({len(names)})")
        client = load.result()
    checks["disruptions"] = 7
    return {"load": client, **checks, "verdict": verdict(load, client)}


def scenario_zero() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    annotation = "autoscaling.keda.sh/paused-replicas"
    try:
        with Load("zero", 120) as load:
            load.at(15)
            kc("annotate", "scaledobject", "worker", f"{annotation}=0", "--overwrite")
            log("KEDA paused at 0 workers")
            wait_for(
                lambda: not pods("worker", terminating=True), "no worker process left", 90, 0.5
            )
            log("no worker pod left, not even a draining one")
            t_zero = time.monotonic()
            held = 35
            samples = []
            while time.monotonic() < t_zero + held:
                waiting, retry = psql(f"""SELECT count(*) FILTER (WHERE state = 'queued'),
                    count(*) FILTER (WHERE state = 'retry_wait' AND next_attempt_at <= now())
                    FROM jobs WHERE idempotency_key LIKE '{load.prefix}-%'""")
                samples.append((int(waiting), int(retry)))
                assert not pods("worker", terminating=True), (
                    "a worker came back while KEDA was paused"
                )
                time.sleep(3)
            checks["max_queued_at_zero"] = max(s[0] for s in samples)
            checks["max_retries_due_at_zero"] = max(s[1] for s in samples)
            log(
                f"held at 0 for {held}s: up to {checks['max_queued_at_zero']} jobs queued and "
                f"{checks['max_retries_due_at_zero']} retries due with no worker or dispatcher"
            )
            kc("annotate", "scaledobject", "worker", f"{annotation}-")
            log("KEDA resumed")
            t = time.monotonic()
            wait_for(lambda: any(ready(p) for p in pods("worker")), "a ready worker", 180, 0.5)
            checks["seconds_back_to_ready_worker"] = round(time.monotonic() - t, 1)
            log(f"a worker is ready {checks['seconds_back_to_ready_worker']}s after resuming")
            client = load.result()
    finally:
        kc("annotate", "scaledobject", "worker", f"{annotation}-", check=False)
    v = verdict(load, client)
    checks["zero_exercised_retries"] = checks["max_retries_due_at_zero"] > 0
    v["invariant_held"] = v["invariant_held"] and checks["zero_exercised_retries"]
    return {"load": client, **checks, "verdict": v}


def scenario_rollout() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    old = kc("get", "deploy", "worker", "-o", "jsonpath={.spec.template.spec.containers[0].image}")
    new = f"queue-service:rollout-{int(time.time())}"
    # Same code under a new tag: this tests the rollout mechanics (old and new pods side by side,
    # graceful replacement under load), not version skew between incompatible releases.
    run(["docker", "tag", old, new])
    run(["kind", "load", "docker-image", new, "--name", CLUSTER], timeout=600)
    with Load("rollout", 150) as load:
        load.at(15)
        kc("set", "image", "deployment/api", f"api={new}")
        kc("set", "image", "deployment/worker", f"worker={new}")
        log(f"rolling out {new} to the API and the workers")
        t = time.monotonic()
        kc("rollout", "status", "deployment/worker", "--timeout=300s", timeout=320)
        kc("rollout", "status", "deployment/api", "--timeout=300s", timeout=320)
        checks["rollout_s"] = round(time.monotonic() - t, 1)
        imgs = {p["spec"]["containers"][0]["image"] for p in pods("worker") + pods("api")}
        checks["all_pods_on_new_image"] = imgs == {new}
        log(f"rollout complete in {checks['rollout_s']}s; images now {sorted(imgs)}")
        load.at(load_end_minus(load, 55))
        kc("rollout", "undo", "deployment/worker")
        kc("rollout", "undo", "deployment/api")
        log("rolling back (kubectl rollout undo)")
        t = time.monotonic()
        kc("rollout", "status", "deployment/worker", "--timeout=300s", timeout=320)
        kc("rollout", "status", "deployment/api", "--timeout=300s", timeout=320)
        checks["rollback_s"] = round(time.monotonic() - t, 1)
        imgs = {p["spec"]["containers"][0]["image"] for p in pods("worker") + pods("api")}
        checks["all_pods_back_on_old_image"] = imgs == {old}
        log(f"rollback complete in {checks['rollback_s']}s")
        client = load.result()
    v = verdict(load, client)
    v["invariant_held"] = (
        v["invariant_held"]
        and checks["all_pods_on_new_image"]
        and checks["all_pods_back_on_old_image"]
    )
    return {"load": client, **checks, "verdict": v}


def load_end_minus(load: Load, s: float) -> float:
    return max(load.seconds - s, time.monotonic() - load.t_start)


def scenario_drain(no_pdb: bool) -> dict[str, Any]:
    checks: dict[str, Any] = {"pdb": not no_pdb}
    nodes = json.loads(kcc("get", "nodes", "-o", "json"))["items"]
    workers = [
        n["metadata"]["name"]
        for n in nodes
        if "node-role.kubernetes.io/control-plane" not in n["metadata"]["labels"]
    ]
    pg_node = json.loads(kc("get", "pod", "postgres-0", "-o", "json"))["spec"]["nodeName"]
    # Postgres here is one replica on node-local storage: draining its node is a database outage
    # by construction (production would use a replicated or managed Postgres), so drain the other.
    target = next(n for n in workers if n != pg_node)
    others = [n for n in workers if n != target]
    pdb_backup = ""
    try:
        # Pack every API and worker pod onto the target node, so that a drain without PDBs
        # would evict all of them at once: only then does the test show what the PDB prevents.
        for n in others:
            kcc("cordon", n)
        kc("rollout", "restart", "deployment/api", "deployment/worker")
        kc("rollout", "status", "deployment/api", "--timeout=240s", timeout=260)
        kc("rollout", "status", "deployment/worker", "--timeout=240s", timeout=260)
        for n in others:
            kcc("uncordon", n)
        on_target = {
            p["metadata"]["name"]: p["spec"]["nodeName"] for p in pods("api") + pods("worker")
        }
        assert set(on_target.values()) == {target}, on_target
        log(f"all {len(on_target)} API and worker pods are on {target}")
        if no_pdb:
            pdb_backup = kc("get", "pdb", "api", "worker", "-o", "yaml")
            kc("delete", "pdb", "api", "worker")
            log("MUTATION: PodDisruptionBudgets deleted")

        with Load("drain", 120) as load:
            load.at(20)
            stop = threading.Event()
            api_ready: list[int] = []

            def watch() -> None:
                while not stop.is_set():
                    api_ready.append(sum(ready(p) for p in pods("api")))
                    time.sleep(0.5)

            watcher = threading.Thread(target=watch, daemon=True)
            watcher.start()
            log(f"kubectl drain {target}")
            t = time.monotonic()
            out = kcc(
                "drain",
                target,
                "--ignore-daemonsets",
                "--delete-emptydir-data",
                "--timeout=240s",
                timeout=260,
                merge_stderr=True,  # refused evictions are reported on stderr
            )
            checks["drain_s"] = round(time.monotonic() - t, 1)
            checks["evictions_blocked_by_pdb"] = out.count("disruption budget")
            log(
                f"drain finished in {checks['drain_s']}s; "
                f"{checks['evictions_blocked_by_pdb']} evictions were refused by a PDB and retried"
            )
            time.sleep(10)
            stop.set()
            watcher.join()
            checks["min_ready_api_pods_during_drain"] = min(api_ready)
            log(f"fewest ready API pods in the drain: {checks['min_ready_api_pods_during_drain']}")
            client = load.result()
    finally:
        for n in workers:
            kcc("uncordon", n, check=False)
        if pdb_backup:
            kc("apply", "-f", "-", input=strip_server_fields(pdb_backup), check=False)
            log("PodDisruptionBudgets restored")
    v = verdict(load, client)
    v["invariant_held"] = v["invariant_held"] and checks["min_ready_api_pods_during_drain"] >= 1
    return {"load": client, **checks, "verdict": v}


def strip_server_fields(yaml_text: str) -> str:
    """Re-applying a fetched object: drop resourceVersion/uid so it can be created again."""
    drop = ("resourceVersion:", "uid:", "creationTimestamp:")
    return "\n".join(line for line in yaml_text.splitlines() if not line.strip().startswith(drop))


# ---- main ------------------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("scenario", choices=["kill", "zero", "rollout", "drain", "all"])
    ap.add_argument(
        "--no-pdb", action="store_true", help="drain only: delete the PDBs (mutation check)"
    )
    args = ap.parse_args()
    names = ["kill", "zero", "rollout", "drain"] if args.scenario == "all" else [args.scenario]
    out_dir = ROOT / "results" / "k8s"
    out_dir.mkdir(parents=True, exist_ok=True)
    failed = []
    for name in names:
        print(f"== {name}", flush=True)
        events.clear()
        settle()
        fn = {
            "kill": scenario_kill,
            "zero": scenario_zero,
            "rollout": scenario_rollout,
            "drain": lambda: scenario_drain(args.no_pdb),
        }[name]
        res = fn()
        res["events"] = list(events)
        res["load"].pop("accepted_ids", None)  # thousands of UUIDs: not useful in the record
        v = res["verdict"]
        suffix = "-no-pdb" if name == "drain" and args.no_pdb else ""
        (out_dir / f"{name}{suffix}.json").write_text(json.dumps(res, indent=2) + "\n")
        print(
            f"  accepted {v['accepted_in_postgres']} | done {v['done']} | dead {v['dead']} "
            f"{v['dead_reasons']} | not final {v['not_final']} | lost {v['lost']} | "
            f"duplicate results {v['duplicate_results']} | mismatches {v['done_result_mismatches']}"
        )
        print(
            f"  executions {v['executions']} (+{v['extra_executions']}) "
            f"| cut off {v['attempts_cut_off']} "
            f"| released {v['attempts_released']} | workers {v['distinct_workers']} | "
            f"client gave up {v['client_gave_up']} | responses {v['client_responses']}"
        )
        print(f"  {'PASS' if v['invariant_held'] else 'FAIL'}: {name}", flush=True)
        if not v["invariant_held"]:
            failed.append(name)
    print("FAILED: " + ", ".join(failed) if failed else "PASS: every scenario held the invariant")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
