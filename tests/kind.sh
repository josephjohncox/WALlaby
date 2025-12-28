#!/usr/bin/env bash
set -euo pipefail

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is required (https://kind.sigs.k8s.io/docs/user/quick-start/)" >&2
  exit 1
fi

KIND_CLUSTER="${KIND_CLUSTER:-wallaby-test}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
KIND_KEEP="${KIND_KEEP:-0}"

created=0
if ! kind get clusters | grep -qx "$KIND_CLUSTER"; then
  if [[ -n "$KIND_NODE_IMAGE" ]]; then
    kind create cluster --name "$KIND_CLUSTER" --image "$KIND_NODE_IMAGE"
  else
    kind create cluster --name "$KIND_CLUSTER"
  fi
  created=1
fi

kubeconfig="$(mktemp -t wallaby-kind-kubeconfig.XXXXXX)"
kind get kubeconfig --name "$KIND_CLUSTER" > "$kubeconfig"

cleanup() {
  rm -f "$kubeconfig"
  if [[ "$created" == "1" && "$KIND_KEEP" != "1" ]]; then
    kind delete cluster --name "$KIND_CLUSTER"
  fi
}
trap cleanup EXIT

export WALLABY_TEST_K8S_KUBECONFIG="$kubeconfig"
export WALLABY_TEST_K8S_NAMESPACE="${WALLABY_TEST_K8S_NAMESPACE:-default}"

go test ./tests/integration -run KubernetesDispatcherIntegration -v
