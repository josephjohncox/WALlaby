#!/bin/sh
set -eu

GO=${GO:-go}
GO_TEST_VERBOSE=${GO_TEST_VERBOSE:-1}
GO_TEST_VERBOSE_FLAG=${GO_TEST_VERBOSE_FLAG:-}
GO_TEST_COVERPKG=${GO_TEST_COVERPKG:-}
GO_TEST_COVERPROFILE=${GO_TEST_COVERPROFILE:-}
if [ -z "$GO_TEST_VERBOSE_FLAG" ] && [ "$GO_TEST_VERBOSE" = "1" ]; then
	GO_TEST_VERBOSE_FLAG=-v
fi
IT_KIND=${IT_KIND:-${WALLABY_TEST_K8S_KIND:-1}}
IT_KEEP=${IT_KEEP:-0}
IT_KIND_CLUSTER=${IT_KIND_CLUSTER:-${KIND_CLUSTER:-wallaby-test}}
IT_KIND_NODE_IMAGE=${IT_KIND_NODE_IMAGE:-${KIND_NODE_IMAGE:-kindest/node:v1.35.0}}
IT_SERVICE_READY_TIMEOUT_SECONDS=${IT_SERVICE_READY_TIMEOUT_SECONDS:-240}
IT_SERVICES=${IT_SERVICES:-all}
IT_RUN_FILTER=${IT_RUN_FILTER:-}
IT_SKIP_FILTER=${IT_SKIP_FILTER:-}
IT_REQUIRED_TESTS=${IT_REQUIRED_TESTS:-}
IT_COUNT=${IT_COUNT:-}
IT_PACKAGE_PARALLELISM=${IT_PACKAGE_PARALLELISM:-1}
IT_EXPECTED_HARNESS_PARTICIPANTS=${IT_EXPECTED_HARNESS_PARTICIPANTS:-$IT_PACKAGE_PARALLELISM}
INTEGRATION_PACKAGE=${INTEGRATION_PACKAGE:-./tests/...}

set -- "$GO" test -p "$IT_PACKAGE_PARALLELISM"
if [ -n "$GO_TEST_VERBOSE_FLAG" ]; then
	set -- "$@" "$GO_TEST_VERBOSE_FLAG"
fi
set -- "$@" "$INTEGRATION_PACKAGE" \
	-it-kind="$IT_KIND" \
	-it-keep="$IT_KEEP" \
	-it-k8s-kind-cluster="$IT_KIND_CLUSTER" \
	-it-k8s-kind-node-image="$IT_KIND_NODE_IMAGE" \
	-it-services="$IT_SERVICES" \
	-it-expected-harness-participants="$IT_EXPECTED_HARNESS_PARTICIPANTS"
if [ -n "$IT_RUN_FILTER" ]; then
	set -- "$@" -run "$IT_RUN_FILTER"
fi
if [ -n "$IT_SKIP_FILTER" ]; then
	set -- "$@" -skip "$IT_SKIP_FILTER"
fi
if [ -n "$IT_COUNT" ]; then
	set -- "$@" -count="$IT_COUNT"
fi
if [ -n "$GO_TEST_COVERPKG" ]; then
	set -- "$@" -coverpkg="$GO_TEST_COVERPKG"
fi
if [ -n "$GO_TEST_COVERPROFILE" ]; then
	set -- "$@" -coverprofile="$GO_TEST_COVERPROFILE"
fi

export IT_VERBOSE="$GO_TEST_VERBOSE"
export WALLABY_IT_VERBOSE="$GO_TEST_VERBOSE"
export WALLABY_IT_SERVICE_READY_TIMEOUT_SECONDS="$IT_SERVICE_READY_TIMEOUT_SECONDS"

if [ -z "$IT_REQUIRED_TESTS" ]; then
	exec "$@"
fi

results=$(mktemp "${TMPDIR:-/tmp}/wallaby-go-test.XXXXXX.json")
trap 'rm -f "$results"' EXIT HUP INT TERM
set +e
"$@" -json >"$results"
status=$?
set -e
cat "$results"
if [ "$status" -ne 0 ]; then
	exit "$status"
fi
expected_runs=${IT_COUNT:-1}
"$GO" run ./scripts/verify-go-test-json.go \
	-results "$results" \
	-required "$IT_REQUIRED_TESTS" \
	-expected-runs "$expected_runs"
