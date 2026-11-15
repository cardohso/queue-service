#!/usr/bin/env bash
# Regenerate every number in docs/RESULTS.md: executor benchmark, end-to-end benchmark,
# the failure suite three times (for run-to-run spread), then the report.
# Takes about 25 minutes. Needs Docker. Do not run other heavy work meanwhile: it is a
# benchmark. Results land in results/ and docs/RESULTS.md.
set -euo pipefail
cd "$(dirname "$0")/.."

rm -rf results
uv run python -m bench.executors
PYTHONPATH=. uv run python -m bench.e2e
for i in 1 2 3; do
  QS_RESULTS_DIR="results/failures/run$i" uv run pytest -m failure -s -p no:logging -q
done
uv run python -m bench.report
