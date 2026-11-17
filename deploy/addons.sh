#!/usr/bin/env bash
# KEDA (autoscaling), Prometheus and Grafana on top of `deploy/up.sh`. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."
CTX="kind-${KIND_CLUSTER:-qs}"
KEDA_CHART_VERSION="2.21.1"   # tested against Kubernetes v1.34-v1.36; the kind config pins v1.36

helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update kedacore >/dev/null
helm --kube-context "$CTX" upgrade --install keda kedacore/keda --version "$KEDA_CHART_VERSION" \
  --namespace keda --create-namespace --wait --timeout 5m

kubectl --context "$CTX" apply -k deploy/k8s/keda
kubectl --context "$CTX" apply -k deploy/k8s/monitoring
kubectl --context "$CTX" -n qs rollout status deployment/prometheus --timeout=240s
kubectl --context "$CTX" -n qs rollout status deployment/grafana --timeout=240s
echo "addons up. Grafana: kubectl --context $CTX -n qs port-forward svc/grafana 3000:3000"
