"""Run every query of the committed Grafana dashboard against Prometheus: each panel must return
data. A panel that is empty under a representative workload is a typo or a missing metric.
Usage: check_dashboard.py PROMETHEUS_PORT"""

import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

port = sys.argv[1]
dash = json.loads(Path("deploy/k8s/monitoring/dashboards/queue-service.json").read_text())
empty = []
for panel in dash["panels"]:
    counts = []
    for t in panel["targets"]:
        query = urllib.parse.urlencode({"query": t["expr"]})
        url = f"http://127.0.0.1:{port}/api/v1/query?{query}"
        res = json.load(urllib.request.urlopen(url))
        counts.append(len(res["data"]["result"]) if res["status"] == "success" else -1)
    if not all(c > 0 for c in counts):
        empty.append((panel["title"], counts))
print(f"{len(dash['panels'])} panels checked")
for title, counts in empty:
    print(f"  NO DATA: {title} {counts}")
sys.exit(1 if empty else 0)
