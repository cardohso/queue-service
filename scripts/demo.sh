#!/usr/bin/env bash
# One command, only Docker + curl + bash needed:
#
#   ./scripts/demo.sh
#
# Starts the whole system (Postgres, Redis, API, 2 workers), submits jobs, kill -9s a worker
# in the middle of them, waits, then checks the claim against what Postgres actually holds:
#
#   every accepted job ends in exactly one final state (done or dead), nothing is lost,
#   and "done" means exactly one result row.
#
# Exit code 0 = the invariant held. Afterwards: `docker compose down -v` to clean up.
set -euo pipefail
cd "$(dirname "$0")/.."

N="${DEMO_JOBS:-300}"
RUN="$(date +%s)"
API="http://localhost:8000"
psql() { docker compose exec -T postgres psql -U qs -d qs -At -c "$1"; }

echo "== 1/5  starting Postgres, Redis, the API and 2 workers (first run builds the image)"
docker compose up -d --build --wait

echo "== 2/5  submitting $N jobs (idempotency keys; 200 ms each) from 8 parallel clients"
seq 1 "$N" | xargs -P 8 -I{} curl -sS --fail --retry 10 -o /dev/null \
  -X POST "$API/jobs" -H 'content-type: application/json' \
  -H "Idempotency-Key: demo-$RUN-{}" \
  -d '{"type":"io_sleep","schema_version":1,"payload":{"duration_ms":200}}' &
SUBMITTER=$!

echo "== 3/5  waiting until a worker is mid-job, then kill -9"
victim=""
for _ in $(seq 1 100); do
  victim="$(psql "SELECT lease_owner FROM jobs WHERE state = 'running' GROUP BY 1 ORDER BY count(*) DESC LIMIT 1")"
  [[ -n "$victim" ]] && break
  sleep 0.2
done
[[ -n "$victim" ]] || { echo "no job ever started running"; exit 1; }
container="${victim%-*}"   # worker id is "<container hostname>-<random>"
held="$(psql "SELECT count(*) FROM jobs WHERE state = 'running' AND lease_owner = '$victim'")"
killed_at="$(psql "SELECT now()")"
docker kill --signal SIGKILL "$container" >/dev/null
echo "   killed worker $container while it held $held running job(s)"
wait "$SUBMITTER"

echo "== 4/5  waiting for every job to reach a final state (the survivor takes over after the lease)"
for _ in $(seq 1 240); do
  final="$(psql "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'demo-$RUN-%' AND state IN ('done','dead')")"
  [[ "$final" == "$N" ]] && break
  sleep 0.5
done

echo "== 5/5  checking the invariant against Postgres"
accepted="$(psql "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'demo-$RUN-%'")"
done_n="$(psql "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'demo-$RUN-%' AND state = 'done'")"
dead_n="$(psql "SELECT count(*) FROM jobs WHERE idempotency_key LIKE 'demo-$RUN-%' AND state = 'dead'")"
not_final=$((accepted - done_n - dead_n))
dup_results="$(psql "SELECT count(*) FROM (SELECT r.job_id FROM results r JOIN jobs j ON j.id = r.job_id WHERE j.idempotency_key LIKE 'demo-$RUN-%' GROUP BY 1 HAVING count(*) > 1) t")"
mismatch="$(psql "SELECT count(*) FROM jobs j LEFT JOIN results r ON r.job_id = j.id WHERE j.idempotency_key LIKE 'demo-$RUN-%' AND ((j.state = 'done') <> (r.job_id IS NOT NULL))")"
execs="$(psql "SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key LIKE 'demo-$RUN-%' AND a.outcome IS DISTINCT FROM 'released'")"
cut="$(psql "SELECT count(*) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key LIKE 'demo-$RUN-%' AND a.outcome = 'lost_lease'")"
recovery="$(psql "SELECT round(extract(epoch FROM max(a.finished_at) - '$killed_at'::timestamptz)::numeric, 1) FROM job_attempts a JOIN jobs j ON j.id = a.job_id WHERE j.idempotency_key LIKE 'demo-$RUN-%'")"

printf '\n  accepted jobs (HTTP 202)        %s\n' "$accepted"
printf '  done / dead                     %s / %s\n' "$done_n" "$dead_n"
printf '  not in a final state            %s\n' "$not_final"
printf '  jobs with more than 1 result    %s\n' "$dup_results"
printf '  done <-> result row mismatches  %s\n' "$mismatch"
printf '  executions started              %s  (%s beyond one per job: the cost of at-least-once)\n' "$execs" "$((execs - accepted))"
printf '  attempts cut off by the kill    %s\n' "$cut"
printf '  time from kill to last finish   %s s\n\n' "$recovery"

if [[ "$accepted" == "$N" && "$not_final" == 0 && "$dup_results" == 0 && "$mismatch" == 0 ]]; then
  echo "PASS: every accepted job reached exactly one final state, none lost, none run twice to completion."
  echo "      Clean up with: docker compose down -v"
else
  echo "FAIL: the invariant was violated. Logs: docker compose logs worker api"
  exit 1
fi
