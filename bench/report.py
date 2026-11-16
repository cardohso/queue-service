"""Generate docs/RESULTS.md from the raw JSON in results/. Every number in that file comes
from here; nothing is typed by hand except the explanatory prose around the tables.

Inputs  results/bench/executors.json   (python -m bench.executors)
        results/bench/e2e.json         (python -m bench.e2e)
        results/failures/run*/failures/*.json  (QS_RESULTS_DIR=results/failures/runN pytest -m failure)
Output  docs/RESULTS.md

Run: ``uv run python -m bench.report``
"""

import json
import statistics
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path
from typing import Any

R = Path("results")
OUT = Path("docs/RESULTS.md")


def load(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.exists() else None


def spread(xs: list[float], digits: int = 1) -> str:
    """median [min-max]"""
    if not xs:
        return "-"
    med, lo, hi = statistics.median(xs), min(xs), max(xs)
    f = f"{{:.{digits}f}}"
    return f"{f.format(med)} [{f.format(lo)}-{f.format(hi)}]"


def group(rows: list[dict[str, Any]], *keys: str) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    out: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        out[tuple(r[k] for k in keys)].append(r)
    return out


def machine_md(m: dict[str, Any], load: list[float] | None) -> str:
    cores = (
        f"{m.get('logical_cpus')} logical ({m['performance_cores']} performance + "
        f"{m['efficiency_cores']} efficiency)"
        if "performance_cores" in m
        else f"{m.get('logical_cpus')} logical"
    )
    gil = "enabled" if m["gil_enabled"] else "DISABLED (free-threaded build)"
    s = (
        f"- CPU: {m['cpu']}, {cores}\n- Memory: {m['memory_gb']} GB\n"
        f"- OS: {m['os']}" + (f" (macOS {m['macos']})" if "macos" in m else "") + "\n"
        f"- Python: {m['python_impl']} {m['python']}, GIL {gil}\n"
    )
    if load:
        s += (
            f"- Machine load average at start (1/5/15 min): {load[0]:.1f} / {load[1]:.1f} / "
            f"{load[2]:.1f} (a personal laptop, not an idle benchmark box)\n"
        )
    return s


# ---- executors -------------------------------------------------------------------------------


def executors_md(d: dict[str, Any]) -> str:
    p, rows = d["params"], d["results"]
    g = group(rows, "kind", "mode", "workers")
    workers = p["workers"]
    cpu_ms = statistics.median(r["p50_ms"] for r in g[("cpu", "processes", 1)])
    out = [
        "## 1. asyncio vs threads vs processes\n",
        f"Source: `uv run python -m bench.executors` -> `results/bench/executors.json`. "
        f"Each cell is the **median of {p['runs']} runs with [min-max]**; every run is a "
        f"{p['duration_s']:.0f} s window with W jobs in flight. I/O job = {p['io_wait_ms']:.0f} ms "
        f"wait; CPU job = {p['cpu_job_iterations']} sha256 iterations (calibration target "
        f"{p['cpu_job_target_ms']:.0f} ms; **measured: {cpu_ms:.1f} ms** per job on one core). "
        "asyncio runs the CPU job *inline on the event loop* (no offload), "
        "which is what is being measured.\n",
        machine_md(d["machine"], d.get("load_avg_at_start")),
    ]
    for kind, title in (("io", "I/O-bound jobs"), ("cpu", "CPU-bound jobs")):
        out.append(f"### {title}: throughput (jobs/s), by workers in flight\n")
        out.append("| mode | " + " | ".join(f"W={w}" for w in workers) + " |")
        out.append("|---|" + "---|" * len(workers))
        for mode in ("asyncio", "threads", "processes"):
            cells = [spread([r["throughput"] for r in g[(kind, mode, w)]], 1) for w in workers]
            out.append(f"| {mode} | " + " | ".join(cells) + " |")
        out.append("")
        out.append(f"### {title}: per-job latency p50 / p99 (ms), median of runs\n")
        out.append("| mode | " + " | ".join(f"W={w}" for w in workers) + " |")
        out.append("|---|" + "---|" * len(workers))
        for mode in ("asyncio", "threads", "processes"):
            cells = []
            for w in workers:
                rs = g[(kind, mode, w)]
                cells.append(
                    f"{statistics.median(r['p50_ms'] for r in rs):.1f} / "
                    f"{statistics.median(r['p99_ms'] for r in rs):.1f}"
                )
            out.append(f"| {mode} | " + " | ".join(cells) + " |")
        out.append("")

    # GIL effect: speedup relative to W=1 of the same mode
    wmax = max(workers)

    def med(kind: str, mode: str, w: int) -> float:
        return float(statistics.median(r["throughput"] for r in g[(kind, mode, w)]))

    out.append(f"### The GIL effect, in numbers (speedup of W={wmax} over W=1, same mode)\n")
    out.append("| mode | I/O-bound | CPU-bound |")
    out.append("|---|---|---|")
    for mode in ("asyncio", "threads", "processes"):
        out.append(
            f"| {mode} | {med('io', mode, wmax) / med('io', mode, 1):.2f}x "
            f"| {med('cpu', mode, wmax) / med('cpu', mode, 1):.2f}x |"
        )
    t_p99 = statistics.median(r["p99_ms"] for r in g[("cpu", "threads", wmax)])
    t_p99_1 = statistics.median(r["p99_ms"] for r in g[("cpu", "threads", 1)])
    lag = statistics.median(
        r["loop_lag_max_ms"]
        for r in g[("cpu", "asyncio", wmax)]
        if r["loop_lag_max_ms"] is not None
    )
    out.append(
        f"\nWith CPU-bound jobs, {wmax} threads do {med('cpu', 'threads', wmax) / med('cpu', 'threads', 1):.2f}x "
        f"the work of 1 thread (the GIL lets one thread run Python at a time), while their "
        f"per-job p99 latency grows from {t_p99_1:.1f} ms to {t_p99:.1f} ms as they take turns. "
        f"{wmax} processes do {med('cpu', 'processes', wmax) / med('cpu', 'processes', 1):.2f}x. "
        f"A CPU job run inline on the asyncio event loop blocks it: the median of the largest "
        f"event-loop stall seen in a run was {lag:.0f} ms at W={wmax}.\n"
    )
    return "\n".join(out)


# ---- end to end ------------------------------------------------------------------------------


def e2e_md(d: dict[str, Any]) -> str:
    p, rows = d["params"], d["results"]
    g = group(rows, "kind", "workers")
    out = [
        "## 2. The real system: capacity vs number of worker processes\n",
        "Source: `uv run python -m bench.e2e` -> `results/bench/e2e.json`. Real Postgres 17 and "
        f"Redis 8 in containers, real `qs worker` processes (each with up to "
        f"{p['max_in_flight_per_worker']} jobs in flight and {p['cpu_children_per_worker']} CPU "
        "child process), fenced commit and ack for every job. A backlog is pre-loaded so the "
        "API is not the bottleneck; throughput is measured from the first claim to the last "
        f"commit. Median of {p['runs']} runs with [min-max]. Latency is per attempt, claim to "
        "committed (service latency including real Postgres round trips), not end-to-end "
        "from submission (a pre-loaded backlog would only measure the backlog).\n",
        machine_md(d["machine"], d.get("load_avg_at_start")),
    ]
    for kind, title, n in (
        ("io", f"I/O jobs ({p['io_wait_ms']} ms wait)", p["jobs"]["io"]),
        ("cpu", f"CPU jobs (~{p.get('cpu_job_ms', 10)} ms of hashing)", p["jobs"]["cpu"]),
    ):
        out.append(f"### {title}, {n} jobs per run\n")
        out.append(
            "| workers | throughput (jobs/s) | attempt latency p50 (ms) | p95 (ms) "
            "| p99 (ms) | extra executions |"
        )
        out.append("|---|---|---|---|---|---|")
        for w in p["workers"]:
            rs = g[(kind, w)]
            out.append(
                f"| {w} | {spread([r['throughput'] for r in rs], 1)} "
                f"| {spread([r['p50_ms'] for r in rs], 1)} "
                f"| {spread([r['p95_ms'] for r in rs], 1)} "
                f"| {spread([r['p99_ms'] for r in rs], 1)} "
                f"| {spread([r['extra_executions'] for r in rs], 0)} |"
            )
        w1 = statistics.median(r["throughput"] for r in g[(kind, p["workers"][0])])
        wn = statistics.median(r["throughput"] for r in g[(kind, p["workers"][-1])])
        out.append(
            f"\nScaling: {p['workers'][-1]} workers deliver {wn / w1:.2f}x the throughput of "
            f"{p['workers'][0]} (ideal: {p['workers'][-1] / p['workers'][0]:.0f}x).\n"
        )
    return "\n".join(out)


# ---- failure tests ---------------------------------------------------------------------------


def junit_outcomes(run_dirs: list[Path]) -> dict[str, list[str | None]]:
    """test name -> per run: None if it passed, else the first line of its failure message."""
    out: dict[str, list[str | None]] = defaultdict(list)
    for rd in run_dirs:
        xml = rd.parent / "junit.xml"
        if not xml.exists():
            continue
        for tc in ET.parse(xml).getroot().iter("testcase"):
            bad = tc.find("failure")
            if bad is None:
                bad = tc.find("error")
            msg = None if bad is None else (bad.get("message") or "failed").splitlines()[0][:160]
            out[tc.get("name", "?")].append(msg)
    return out


def failures_md() -> str | None:
    runs = sorted((R / "failures").glob("run*/failures"))
    if not runs:
        return None
    per_test: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in runs:
        for f in sorted(r.glob("*.json")):
            d = json.loads(f.read_text())
            per_test[d["test"]].append(d)
    out = [
        "## 3. Failure tests: the invariant under real faults\n",
        f"Source: `QS_RESULTS_DIR=results/failures/runN uv run pytest -m failure` repeated "
        f"{len(runs)} times -> `results/failures/`. Each test starts real containers and real "
        "`qs` processes, injects the fault under load, then checks against Postgres that "
        "**every accepted job (every 202 the client received) is in exactly one final state, "
        "`done` has exactly one result row, and every delivery was acked**. Cells are "
        'median [min-max] over the runs. "Extra executions" = attempts beyond one per job '
        '(the cost of at-least-once delivery); "recovery" = fault cleared (or worker killed) '
        "until work completes again.\n",
        "| test | jobs | done / dead | lost jobs | jobs with >1 result "
        "| extra executions | recovery (s) |",
        "|---|---|---|---|---|---|---|",
    ]
    order = [
        "kill9_worker_mid_job", "redis_restart_aof", "redis_restart_no_persistence",
        "postgres_stopped", "postgres_paused", "postgres_slow_100ms",
        "poison_permanent", "poison_child_exit", "poison_hang", "poison_worker_exit", "api_flood",
    ]  # fmt: skip
    for name in order + sorted(set(per_test) - set(order)):
        rs = per_test.get(name)
        if not rs:
            continue
        done = spread([r["done"] for r in rs], 0)
        dead = spread([r["dead"] for r in rs], 0)
        rec = [r["recovery_s"] for r in rs if r["recovery_s"] is not None]
        out.append(
            f"| `{name}` | {rs[0]['jobs']} | {done} / {dead} | 0 | 0 "
            f"| {spread([r['extra_executions'] for r in rs], 0)} "
            f"| {spread(rec, 2) if rec else '-'} |"
        )
    out.append(
        '\n"Lost jobs" and "jobs with >1 result" are 0 because the test fails otherwise '
        "(they are assertions, checked on every run), not because they are counted here.\n"
    )
    flood = per_test.get("api_flood")
    if flood:
        n0 = flood[0]["notes"]
        p99 = [r["notes"]["latency_ms"]["p99"] for r in flood]
        c429 = [r["notes"]["codes"].get("429", 0) for r in flood]
        c202 = [r["notes"]["codes"].get("202", 0) for r in flood]
        out.append(
            f"**API flood** ({n0['load']}): accepted (202) {spread([float(x) for x in c202], 0)}, "
            f"rejected with 429 {spread([float(x) for x in c429], 0)}, no other status codes; "
            f"request p99 {spread(p99, 1)} ms; peak queue depth "
            f"{spread([float(r['notes']['max_observed_depth']) for r in flood], 0)} "
            f"against a limit of {n0['max_depth_limit']} (the limit is soft: admission uses a "
            "sampled depth).\n"
        )
    outcomes = junit_outcomes(runs)
    if outcomes:
        total = max(len(v) for v in outcomes.values())
        out.append(f"### Pass rate over {total} runs\n")
        out.append("| test | passed |")
        out.append("|---|---|")
        for name in sorted(outcomes):
            res = outcomes[name]
            out.append(f"| `{name}` | {sum(1 for m in res if m is None)} / {len(res)} |")
        bad = [(n, msg) for n, rs in sorted(outcomes.items()) for msg in rs if msg is not None]
        if bad:
            out.append("\n**Failures observed (kept, not retried away):**\n")
            for n, msg in bad:
                out.append(f"- `{n}`: {msg}")
        out.append("")
    return "\n".join(out)


def main() -> None:
    parts = [
        "# Results\n",
        "> Generated by `uv run python -m bench.report` from the raw JSON in `results/`. "
        "Do not edit by hand. Local, single-machine measurements: they show relative "
        "behaviour (what scales, what does not, what the faults cost), not absolute "
        "capacity of any deployment.\n",
    ]
    ex, e2e = load(R / "bench/executors.json"), load(R / "bench/e2e.json")
    if ex:
        parts.append(executors_md(ex))
    if e2e:
        parts.append(e2e_md(e2e))
    fm = failures_md()
    if fm:
        parts.append(fm)
    OUT.write_text("\n".join(parts) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
