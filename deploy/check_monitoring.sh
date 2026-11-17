#!/usr/bin/env bash
# Monitoring check: under a workload that produces every kind of event, Prometheus must scrape
# the pods, every dashboard panel must return data, and the alerts must really fire.
set -euo pipefail
cd "$(dirname "$0")/.."
CTX="kind-${KIND_CLUSTER:-qs}"
k() { kubectl --context "$CTX" -n qs "$@"; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }
AP="$(free_port)"; PP="$(free_port)"
k port-forward svc/api "$AP:8000" >/dev/null 2>&1 & P1=$!
k port-forward svc/prometheus "$PP:9090" >/dev/null 2>&1 & P2=$!
trap 'kill $P1 $P2 2>/dev/null || true' EXIT
for _ in $(seq 1 50); do curl -sf "http://127.0.0.1:$AP/readyz" >/dev/null && break; sleep 0.2; done
post() { curl -s -o /dev/null -w "%{http_code}" -X POST "http://127.0.0.1:$AP/jobs" -H 'content-type: application/json' -H "Idempotency-Key: $1" -d "$2"; }
RUN="$(date +%s)"
# 5 s jobs: the backlog (and so the alert's 30 s "for" window) outlasts the autoscaler's ramp-up.
IO='{"type":"io_sleep","schema_version":1,"payload":{"duration_ms":5000}}'

echo "== workload: backlog, CPU jobs, retries, dead letters, an idempotent replay, a 409"
seq 1 1500 | xargs -P 8 -I{} curl -s -o /dev/null --retry 10 -X POST "http://127.0.0.1:$AP/jobs" \
  -H 'content-type: application/json' -H "Idempotency-Key: mon-$RUN-io-{}" -d "$IO" &
LOAD=$!
for i in $(seq 1 20); do post "mon-$RUN-cpu-$i" '{"type":"cpu_hash","schema_version":1,"payload":{"iterations":2000000}}' >/dev/null; done
for i in $(seq 1 5);  do post "mon-$RUN-dead-$i" '{"type":"fail","schema_version":1,"payload":{"kind":"permanent","failures":1}}' >/dev/null; done
for i in $(seq 1 10); do post "mon-$RUN-retry-$i" '{"type":"fail","schema_version":1,"payload":{"kind":"transient","failures":2}}' >/dev/null; done
post "mon-$RUN-replay" "$IO" >/dev/null; post "mon-$RUN-replay" "$IO" >/dev/null
[[ "$(post "mon-$RUN-replay" '{"type":"io_sleep","schema_version":1,"payload":{"duration_ms":1}}')" == 409 ]] || { echo "FAIL: expected 409"; exit 1; }

echo "== polling alerts for 100 s (they need their 'for:' window)"
: > /tmp/qs_alerts
for _ in $(seq 1 20); do
  curl -s "http://127.0.0.1:$PP/api/v1/alerts" | python3 -c "
import sys,json
for x in json.load(sys.stdin)['data']['alerts']: print(x['labels'].get('alertname'), x['state'])" >> /tmp/qs_alerts
  sleep 5
done
sort /tmp/qs_alerts | uniq -c | sed 's/^/   /'
wait "$LOAD" 2>/dev/null || true

echo "== targets and dashboard"
curl -sG "http://127.0.0.1:$PP/api/v1/query" --data-urlencode 'query=count(up{app=~"api|worker"} == 1)' \
  | python3 -c "import sys,json; n=int(float(json.load(sys.stdin)['data']['result'][0]['value'][1])); print('   scraped pods up:', n); sys.exit(0 if n >= 2 else 1)"
python3 deploy/check_dashboard.py "$PP"

grep -q "DeadLettersGrowing firing" /tmp/qs_alerts || { echo "FAIL: DeadLettersGrowing never fired"; exit 1; }
grep -q "QueueBacklogHigh firing" /tmp/qs_alerts || { echo "FAIL: QueueBacklogHigh never fired"; exit 1; }
echo "PASS: pods scraped, every panel has data, DeadLettersGrowing and QueueBacklogHigh fired"
