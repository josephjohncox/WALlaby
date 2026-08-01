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
helm template wallaby-ci "$chart" \
	--set observability.metrics.enabled=true \
	--set observability.metrics.otel.endpoint=metrics.example:4317 \
	--set observability.traces.enabled=true \
	--set observability.traces.otel.endpoint=traces.example:4318 \
	>"$output_dir/telemetry.yaml"

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

default_render="$output_dir/default.yaml"
for probe in startupProbe readinessProbe livenessProbe; do
	if ! grep -q "^          ${probe}:$" "$default_render"; then
		echo "default deployment is missing ${probe}" >&2
		exit 1
	fi
done
if ! grep -q 'service: wallaby.readiness' "$default_render"; then
	echo "Helm test and deployment must use the readiness health service" >&2
	exit 1
fi
if ! grep -q 'grpc-health-probe@sha256:' "$default_render"; then
	echo "Helm readiness test must use an immutable gRPC health probe image" >&2
	exit 1
fi

telemetry_render="$output_dir/telemetry.yaml"
if ! grep -q 'name: OTEL_EXPORTER_OTLP_METRICS_ENDPOINT' "$telemetry_render" ||
	! grep -q 'value: "metrics.example:4317"' "$telemetry_render"; then
	echo "metrics OTLP endpoint was not rendered independently" >&2
	exit 1
fi
if ! grep -q 'name: OTEL_EXPORTER_OTLP_TRACES_ENDPOINT' "$telemetry_render" ||
	! grep -q 'value: "traces.example:4318"' "$telemetry_render"; then
	echo "traces OTLP endpoint was not rendered independently" >&2
	exit 1
fi
if grep -q 'name: OTEL_EXPORTER_OTLP_ENDPOINT' "$telemetry_render"; then
	echo "signal-specific OTLP endpoints must not collapse to the generic endpoint" >&2
	exit 1
fi

printf '%s\n' 'Helm lint and deployment, health, telemetry, dispatch, and worker renders passed.'
