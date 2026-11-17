#!/usr/bin/env bash
# Build the image, load it into the local kind cluster and deploy everything.
#   deploy/up.sh            cluster + app (base manifests)
# Needs Docker, kind and kubectl. The database password is generated here and kept only in a
# cluster Secret: nothing secret is committed.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER="${KIND_CLUSTER:-qs}"
CTX="kind-$CLUSTER"

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/kind-config.yaml --wait 180s
fi

# Tag by image ID: the same tag would not make Kubernetes roll out a rebuilt image.
docker build -t queue-service:dev .
IMAGE="queue-service:$(docker image inspect queue-service:dev --format '{{.Id}}' | sed 's/sha256://' | cut -c1-12)"
docker tag queue-service:dev "$IMAGE"
kind load docker-image "$IMAGE" --name "$CLUSTER"

kubectl --context "$CTX" apply -f deploy/k8s/base/namespace.yaml
if ! kubectl --context "$CTX" -n qs get secret qs-db >/dev/null 2>&1; then
  kubectl --context "$CTX" -n qs create secret generic qs-db \
    --from-literal=password="$(openssl rand -hex 16)"
fi
kubectl --context "$CTX" apply -k deploy/k8s/base
# Roll out the image just built (a no-op when it did not change).
kubectl --context "$CTX" -n qs set image deployment/api api="$IMAGE"
kubectl --context "$CTX" -n qs set image deployment/worker worker="$IMAGE"

kubectl --context "$CTX" -n qs rollout status statefulset/postgres --timeout=180s
kubectl --context "$CTX" -n qs rollout status deployment/redis --timeout=120s
kubectl --context "$CTX" -n qs rollout status deployment/api --timeout=180s
kubectl --context "$CTX" -n qs rollout status deployment/worker --timeout=180s
echo "up. try: kubectl --context $CTX -n qs port-forward svc/api 8000:8000"
