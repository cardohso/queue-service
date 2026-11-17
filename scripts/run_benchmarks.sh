#!/usr/bin/env bash
# Regenerate every number in docs/RESULTS.md.
#   scripts/run_benchmarks.sh            all stages (about 25 minutes)
#   scripts/run_benchmarks.sh bench      executor + end-to-end benchmarks only
#   scripts/run_benchmarks.sh e2e        end-to-end + CPU-slot benchmarks only (about 12 minutes)
#   scripts/run_benchmarks.sh failures   failure suite 3x only
#   scripts/run_benchmarks.sh report     regenerate docs/RESULTS.md from results/
# Needs Docker. Do not run other heavy work meanwhile: it is a benchmark, and the machine's
# load average at start is recorded in the results.
set -euo pipefail
cd "$(dirname "$0")/.."
stage="${1:-all}"

if [[ "$stage" == all || "$stage" == bench ]]; then
  rm -rf results/bench
  uv run python -m bench.executors
  PYTHONPATH=. uv run python -m bench.e2e
  PYTHONPATH=. uv run python -m bench.cpu_slots
fi

if [[ "$stage" == e2e ]]; then
  rm -f results/bench/e2e.json results/bench/cpu_slots.json
  PYTHONPATH=. uv run python -m bench.e2e
  PYTHONPATH=. uv run python -m bench.cpu_slots
fi

if [[ "$stage" == all || "$stage" == failures ]]; then
  rm -rf results/failures
  for i in 1 2 3; do
    # A failing test must not stop the benchmark: its pass/fail is part of the result
    # (junit.xml), and the report states how many of the 3 runs each test passed.
    QS_RESULTS_DIR="results/failures/run$i" uv run pytest -m failure -s -p no:logging -q \
      --junitxml="results/failures/run$i/junit.xml" || true
  done
fi

uv run python -m bench.report
