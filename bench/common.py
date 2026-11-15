"""Shared helpers for the benchmark scripts."""

import json
import math
import platform
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Any

RESULTS_DIR = Path("results")


def _sh(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=10).stdout.strip()
    except OSError, subprocess.SubprocessError:
        return ""


def machine_info() -> dict[str, Any]:
    """Everything needed to interpret (and not over-generalise) a result."""
    gil = getattr(sys, "_is_gil_enabled", lambda: True)()
    info: dict[str, Any] = {
        "os": f"{platform.system()} {platform.release()}",
        "machine": platform.machine(),
        "python": platform.python_version(),
        "python_impl": platform.python_implementation(),
        "gil_enabled": bool(gil),
    }
    if platform.system() == "Darwin":
        info |= {
            "cpu": _sh("sysctl", "-n", "machdep.cpu.brand_string"),
            "logical_cpus": int(_sh("sysctl", "-n", "hw.ncpu") or 0),
            "performance_cores": int(_sh("sysctl", "-n", "hw.perflevel0.logicalcpu") or 0),
            "efficiency_cores": int(_sh("sysctl", "-n", "hw.perflevel1.logicalcpu") or 0),
            "memory_gb": round(int(_sh("sysctl", "-n", "hw.memsize") or 0) / 2**30),
            "macos": _sh("sw_vers", "-productVersion"),
        }
    else:
        import os

        info |= {
            "cpu": next(
                (
                    ln.split(":", 1)[1].strip()
                    for ln in Path("/proc/cpuinfo").read_text().splitlines()
                    if ln.startswith("model name")
                ),
                platform.processor(),
            ),
            "logical_cpus": os.cpu_count(),
            "memory_gb": round(int(Path("/proc/meminfo").read_text().split()[1]) / 2**20),
        }
    return info


def percentile(xs: list[float], q: float) -> float:
    """Nearest-rank percentile; q in [0, 1]."""
    s = sorted(xs)
    return s[min(len(s) - 1, max(0, math.ceil(q * len(s)) - 1))]


def summarize(xs: list[float]) -> dict[str, float]:
    return {
        "median": statistics.median(xs),
        "min": min(xs),
        "max": max(xs),
        "n": len(xs),
    }


def save(name: str, payload: dict[str, Any]) -> Path:
    out = RESULTS_DIR / "bench"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2))
    return path
