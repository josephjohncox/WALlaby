#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
chart="$root/charts/wallaby"
output_dir=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-helm.XXXXXX")
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

helm lint "$chart"
helm template wallaby-ci "$chart" >"$output_dir/default.yaml"
helm template wallaby-ci "$chart" \
	-f "$chart/values-prod.yaml" >"$output_dir/production.yaml"
helm template wallaby-ci "$chart" \
	--set kubernetesDispatch.enabled=true >"$output_dir/kubernetes-dispatch.yaml"
helm template wallaby-ci "$chart" \
	-f "$chart/values.example.yaml" \
	--set kubernetesDispatch.enabled=true \
	--set workers.enabled=true >"$output_dir/dispatch-workers.yaml"

render="$output_dir/dispatch-workers.yaml"
if ! grep -q '^      serviceAccountName: wallaby-ci$' "$render"; then
	echo "dispatcher pod does not use the default dispatcher service account" >&2
	exit 1
fi
if ! grep -q '^      serviceAccountName: wallaby-ci-worker$' "$render"; then
	echo "worker pod does not use the distinct default worker service account" >&2
	exit 1
fi
if ! grep -q '^      automountServiceAccountToken: false$' "$render"; then
	echo "worker pods must disable service-account token automount by default" >&2
	exit 1
fi
if ! grep -q "^  name: 'wallaby-ci'$" "$render" ||
	! grep -q "^  name: 'wallaby-ci-worker'$" "$render"; then
	echo "dispatcher and worker service accounts must remain distinct" >&2
	exit 1
fi

printf '%s\n' 'Helm lint and default, production, Kubernetes-dispatch, and worker renders passed.'
