#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
chart="$root/charts/wallaby"
output_dir=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-helm.XXXXXX")
trap 'rm -rf "$output_dir"' EXIT HUP INT TERM

require_env_value() {
	file=$1
	name=$2
	value=$3
	if ! grep -A1 -m1 "name: $name" "$file" | grep -q "value: \"$value\""; then
		echo "$file lacks authoritative $name=$value" >&2
		exit 1
	fi
}

expect_snowflake_value_failure() {
	name=$1
	if helm template wallaby-ci "$chart" \
		--set snowflake.enabled=true \
		--set snowflake.account=account \
		--set snowflake.user=user \
		--set snowflake.host=account.snowflakecomputing.com \
		--set snowflake.privateKeyFile=/run/secrets/wallaby/snowflake-key.pem \
		--set snowflake.privateKeySecretName=wallaby-snowflake \
		--set snowflake.privateKeySecretKey=private-key.pem \
		--set "$name=" >"$output_dir/missing-${name##*.}.yaml" 2>&1; then
		echo "empty $name unexpectedly rendered with snowflake.enabled=true" >&2
		exit 1
	fi
}

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
if ! grep -q '^      automountServiceAccountToken: false$' "$default_render"; then
	echo "server pod must disable its Kubernetes token when dispatch is disabled" >&2
	exit 1
fi
if ! grep -q '^      automountServiceAccountToken: true$' "$output_dir/kubernetes-dispatch.yaml"; then
	echo "server dispatcher pod must mount its Kubernetes token when dispatch is enabled" >&2
	exit 1
fi
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

# Render every worker controller kind with the global policy disabled. The
# policy ConfigMap and exact worker key reference must still exist.
for kind in deployment job cronjob; do
	set -- --set workers.enabled=true --set 'workers.items[0].name=policy-check' --set "workers.items[0].kind=$kind"
	if [ "$kind" = cronjob ]; then
		set -- "$@" --set-string 'workers.items[0].schedule=*/5 * * * *'
	fi
	helm template wallaby-ci "$chart" "$@" >"$output_dir/worker-$kind.yaml"
	worker_render="$output_dir/worker-$kind.yaml"
	require_env_value "$worker_render" WALLABY_WORKER_SNOWFLAKE_ENABLED false
	require_env_value "$worker_render" WALLABY_WORKER_SNOWFLAKE_ACCOUNT ""
	require_env_value "$worker_render" WALLABY_WORKER_SNOWFLAKE_USER ""
	require_env_value "$worker_render" WALLABY_WORKER_SNOWFLAKE_HOST ""
	require_env_value "$worker_render" WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE /run/secrets/wallaby/snowflake-key.pem
	grep -q 'name: WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED' "$worker_render" || {
		echo "$kind worker lacks the mandatory Streaming policy gate" >&2; exit 1;
	}
	grep -q 'fsGroup: 65532' "$output_dir/worker-$kind.yaml" || {
		echo "$kind worker lacks the private-key group context" >&2; exit 1;
	}
	grep -q 'automountServiceAccountToken: false' "$output_dir/worker-$kind.yaml" || {
		echo "$kind worker unexpectedly mounts a service-account token" >&2; exit 1;
	}
	grep -q 'checksum/snowflake-streaming-policy:' "$output_dir/worker-$kind.yaml" || {
		echo "$kind worker lacks policy ConfigMap rollout checksum" >&2; exit 1;
	}
done

grep -q 'snowflake-streaming-rest-enabled: "false"' "$output_dir/default.yaml" || {
	echo 'default policy ConfigMap must render an exact false value' >&2; exit 1;
}
grep -q 'checksum/snowflake-streaming-policy:' "$output_dir/default.yaml" || {
	echo 'server deployment lacks policy ConfigMap rollout checksum' >&2; exit 1;
}

for kind in deployment job cronjob; do
	set -- --set snowflake.enabled=true \
		--set snowflake.streamingRest.enabled=true \
		--set snowflake.account=account \
		--set snowflake.user=user \
		--set snowflake.host=account.snowflakecomputing.com \
		--set snowflake.privateKeyFile=/run/secrets/wallaby/snowflake-key.pem \
		--set snowflake.privateKeySecretName=wallaby-snowflake \
		--set snowflake.privateKeySecretKey=private-key.pem \
		--set workers.enabled=true \
		--set 'workers.items[0].name=enabled' \
		--set "workers.items[0].kind=$kind"
	if [ "$kind" = cronjob ]; then
		set -- "$@" --set-string 'workers.items[0].schedule=*/5 * * * *'
	fi
	enabled_render="$output_dir/snowflake-enabled-$kind.yaml"
	helm template wallaby-ci "$chart" "$@" >"$enabled_render"
	for required in 'snowflake-streaming-rest-enabled: "true"' 'defaultMode: 288' 'mode: 288' 'fsGroup: 65532' 'mountPath: "/run/secrets/wallaby/snowflake-key.pem"'; do
		grep -q "$required" "$enabled_render" || {
			echo "enabled $kind Snowflake render lacks $required" >&2; exit 1;
		}
	done
	require_env_value "$enabled_render" WALLABY_WORKER_SNOWFLAKE_ENABLED true
	require_env_value "$enabled_render" WALLABY_WORKER_SNOWFLAKE_ACCOUNT account
	require_env_value "$enabled_render" WALLABY_WORKER_SNOWFLAKE_USER user
	require_env_value "$enabled_render" WALLABY_WORKER_SNOWFLAKE_HOST account.snowflakecomputing.com
	require_env_value "$enabled_render" WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE /run/secrets/wallaby/snowflake-key.pem
done

for name in snowflake.account snowflake.user snowflake.host snowflake.privateKeyFile snowflake.privateKeySecretName snowflake.privateKeySecretKey; do
	expect_snowflake_value_failure "$name"
done

if helm template wallaby-ci "$chart" --set snowflake.streamingRest.enabled=true >"$output_dir/invalid-streaming.yaml" 2>&1; then
	echo 'Streaming REST enabled without base Snowflake policy unexpectedly rendered' >&2; exit 1
fi
if helm template wallaby-ci "$chart" --set snowflake.streamingREST.enabled=true >"$output_dir/removed-alias.yaml" 2>&1; then
	echo 'removed snowflake.streamingREST alias unexpectedly rendered' >&2; exit 1
fi
if helm template wallaby-ci "$chart" --set snowflake.streamingRest.policyConfigMapKey= >"$output_dir/missing-policy-key.yaml" 2>&1; then
	echo 'empty Streaming policy ConfigMap key unexpectedly rendered' >&2; exit 1
fi
for name in \
	WALLABY_WORKER_SNOWFLAKE_ENABLED WALLABY_WORKER_SNOWFLAKE_ACCOUNT WALLABY_WORKER_SNOWFLAKE_USER WALLABY_WORKER_SNOWFLAKE_HOST WALLABY_WORKER_SNOWFLAKE_PRIVATE_KEY_FILE WALLABY_WORKER_SNOWFLAKE_STREAMING_REST_ENABLED \
	WALLABY_SNOWFLAKE_ENABLED WALLABY_SNOWFLAKE_ACCOUNT WALLABY_SNOWFLAKE_USER WALLABY_SNOWFLAKE_HOST WALLABY_SNOWFLAKE_PRIVATE_KEY_FILE WALLABY_SNOWFLAKE_STREAMING_REST_ENABLED; do
	if helm template wallaby-ci "$chart" --set workers.enabled=true --set 'workers.items[0].name=bad' --set "workers.items[0].env[0].name=$name" --set 'workers.items[0].env[0].value=true' >"$output_dir/worker-alias.yaml" 2>&1; then
		echo "worker policy environment override $name unexpectedly rendered" >&2; exit 1
	fi
done

printf '%s\n' 'Helm lint and deployment, health, telemetry, dispatch, policy, and all worker-kind renders passed.'
