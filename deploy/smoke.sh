#!/usr/bin/env bash
# Smoke test for the cluster deployment: submit jobs through the API, wait, and check the
# invariant against what Postgres holds (every accepted job in exactly one final state,
# `done` <=> exactly one result row). Exit 0 = held.
#   deploy/smoke.sh                 300 jobs (DEMO_JOBS=n to change)
set -euo pipefail
CTX="kind-${KIND_CLUSTER:-qs}"
N="${SMOKE_JOBS:-300}"
MS="${SMOKE_MS:-100}"   # job duration in ms
RUN="$(date +%s)"
k() { kubectl --context "$CTX" -n qs "$@"; }
psql() { k exec postgres-0 -- psql -U qs -d qs -At -c "$1"; }

PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
k port-forward svc/api "$PORT:8000" >/dev/null 2>&1 &
PF=$!
trap 'kill $PF 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$PORT/readyz" >/dev/null && break; sleep 0.2; done

echo "== submitting $N jobs through the Service"
seq 1 "$N" | xargs -P 8 -I{} curl -sS --fail --retry 10 -o /dev/null \
  -X POST "http://127.0.0.1:$PORT/jobs" -H 'content-type: application/json' \
  -H "Idempotency-Key: smoke-$RUN-{}" \
  -d "{\"type\":\"io_sleep\",\"schema_version\":1,\"payload\":{\"duration_ms\":$MS}}"

echo "== waiting for every job to reach a final state"
for _ in $(seq 1 240); do
  final="$(psql "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'smoke-$RUN-%' AND state IN ('done','dead')")"
  [[ "$final" == "$N" ]] && break
  sleep 0.5
done

where="j.idempotency_key LIKE 'smoke-$RUN-%'"
accepted="$(psql "SELECT count(*) FROM jobs j WHERE $where")"
done_n="$(psql "SELECT count(*) FROM jobs j WHERE $where AND state = 'done'")"
dead_n="$(psql "SELECT count(*) FROM jobs j WHERE $where AND state = 'dead'")"
dups="$(psql "SELECT count(*) FROM (SELECT r.job_id FROM results r JOIN jobs j ON j.id = r.job_id WHERE $where GROUP BY 1 HAVING count(*) > 1) t")"
mism="$(psql "SELECT count(*) FROM jobs j LEFT JOIN results r ON r.job_id = j.id WHERE $where AND ((j.state = 'done') <> (r.job_id IS NOT NULL))")"
execs="$(psql "SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE $where AND a.outcome IS DISTINCT FROM 'released'")"
workers="$(psql "SELECT count(DISTINCT a.worker_id) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE $where")"

printf '  accepted %s | done %s | dead %s | duplicate results %s | done<->result mismatches %s | executions %s | distinct workers %s\n' \
  "$accepted" "$done_n" "$dead_n" "$dups" "$mism" "$execs" "$workers"
if [[ "$accepted" == "$N" && $((done_n + dead_n)) == "$N" && "$dups" == 0 && "$mism" == 0 ]]; then
  echo "PASS: every accepted job reached exactly one final state"
else
  echo "FAIL: invariant violated"; exit 1
fi
