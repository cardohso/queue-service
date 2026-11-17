#!/usr/bin/env bash
# KEDA check: a backlog of slow jobs must scale the workers up, and the invariant must still
# hold afterwards (scaling must not lose or duplicate jobs). Exit 0 = both held.
set -euo pipefail
cd "$(dirname "$0")/.."
CTX="kind-${KIND_CLUSTER:-qs}"
k() { kubectl --context "$CTX" -n qs "$@"; }
MIN_PEAK="${EXPECT_PEAK_REPLICAS:-3}"

peak=0; : > /tmp/qs_peak
( while true; do k get deploy worker -o jsonpath='{.spec.replicas}' >> /tmp/qs_peak; echo >> /tmp/qs_peak; sleep 2; done ) &
SAMPLER=$!
trap 'kill $SAMPLER 2>/dev/null || true' EXIT

# 3 s jobs: one worker does ~10 jobs/s, clients submit ~100/s, so a backlog must build.
SMOKE_JOBS="${SCALE_JOBS:-800}" SMOKE_MS=3000 ./deploy/smoke.sh | tail -3
peak="$(sort -n /tmp/qs_peak | tail -1)"
echo "peak desired worker replicas during the run: $peak (expected >= $MIN_PEAK)"
[[ "$peak" -ge "$MIN_PEAK" ]] || { echo "FAIL: workers did not scale up"; exit 1; }
echo "PASS: KEDA scaled workers up under backlog and the invariant held"
