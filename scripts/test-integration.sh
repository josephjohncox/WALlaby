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
# Empty means the harness derives a unique per-invocation cluster name; only an
# explicit name opts into a named (possibly developer-owned) cluster.
IT_KIND_CLUSTER=${IT_KIND_CLUSTER:-${KIND_CLUSTER:-}}
IT_KIND_NODE_IMAGE=${IT_KIND_NODE_IMAGE:-${KIND_NODE_IMAGE:-kindest/node:v1.35.0}}
IT_SERVICE_READY_TIMEOUT_SECONDS=${IT_SERVICE_READY_TIMEOUT_SECONDS:-240}
IT_SERVICES=${IT_SERVICES:-all}
IT_COMPOSE_IGGY=${IT_COMPOSE_IGGY:-false}
IT_RESULTS_FILE=${IT_RESULTS_FILE:-}
IT_RUN_FILTER=${IT_RUN_FILTER:-}
IT_SKIP_FILTER=${IT_SKIP_FILTER:-}
IT_REQUIRED_TESTS=${IT_REQUIRED_TESTS:-}
IT_COUNT=${IT_COUNT:-}
IT_PACKAGE_PARALLELISM=${IT_PACKAGE_PARALLELISM:-1}
IT_EXPECTED_HARNESS_PARTICIPANTS=${IT_EXPECTED_HARNESS_PARTICIPANTS:-}
INTEGRATION_PACKAGE=${INTEGRATION_PACKAGE:-./tests/...}
# Empty keeps go test's own default (10m); a value is passed as -timeout and the
# harness bounds its owner's peer wait by it.
GO_TEST_TIMEOUT=${GO_TEST_TIMEOUT:-}

fail_config() {
	echo "test-integration: $1" >&2
	exit 2
}

require_positive_integer() {
	case $2 in
	'' | *[!0-9]*) fail_config "$1 must be a positive integer, got \"$2\"" ;;
	esac
	if [ "$2" -lt 1 ]; then
		fail_config "$1 must be a positive integer, got \"$2\""
	fi
}

# harness_packages_for prints how many selected test packages run the live
# harness (their TestMain calls RunIntegrationHarness): ./tests and
# ./tests/integration each have one. Unknown selectors print 0.
harness_packages_for() {
	selector=${1#./}
	selector=${selector%/}
	case $selector in
	tests | tests/integration | tests/integration/...) echo 1 ;;
	tests/...) echo 2 ;;
	*) echo 0 ;;
	esac
}

require_positive_integer IT_PACKAGE_PARALLELISM "$IT_PACKAGE_PARALLELISM"
known_harness_packages=$(harness_packages_for "$INTEGRATION_PACKAGE")
if [ -n "$IT_EXPECTED_HARNESS_PARTICIPANTS" ]; then
	require_positive_integer IT_EXPECTED_HARNESS_PARTICIPANTS "$IT_EXPECTED_HARNESS_PARTICIPANTS"
	if [ "$IT_EXPECTED_HARNESS_PARTICIPANTS" -gt "$IT_PACKAGE_PARALLELISM" ]; then
		fail_config "IT_EXPECTED_HARNESS_PARTICIPANTS=$IT_EXPECTED_HARNESS_PARTICIPANTS exceeds IT_PACKAGE_PARALLELISM=$IT_PACKAGE_PARALLELISM; packages cannot run concurrently enough to share one harness"
	fi
	if [ "$known_harness_packages" -gt 0 ] && [ "$IT_EXPECTED_HARNESS_PARTICIPANTS" -gt "$known_harness_packages" ]; then
		fail_config "IT_EXPECTED_HARNESS_PARTICIPANTS=$IT_EXPECTED_HARNESS_PARTICIPANTS exceeds the $known_harness_packages harness package(s) selected by $INTEGRATION_PACKAGE"
	fi
elif [ "$known_harness_packages" -gt 0 ] && [ "$known_harness_packages" -lt "$IT_PACKAGE_PARALLELISM" ]; then
	IT_EXPECTED_HARNESS_PARTICIPANTS=$known_harness_packages
elif [ "$known_harness_packages" -gt 0 ]; then
	IT_EXPECTED_HARNESS_PARTICIPANTS=$IT_PACKAGE_PARALLELISM
elif [ "$IT_PACKAGE_PARALLELISM" -gt 1 ]; then
	fail_config "cannot infer the harness package count for INTEGRATION_PACKAGE=$INTEGRATION_PACKAGE with IT_PACKAGE_PARALLELISM=$IT_PACKAGE_PARALLELISM; set IT_EXPECTED_HARNESS_PARTICIPANTS explicitly"
else
	IT_EXPECTED_HARNESS_PARTICIPANTS=1
fi

set -- "$GO" test -p "$IT_PACKAGE_PARALLELISM"
if [ -n "$GO_TEST_VERBOSE_FLAG" ]; then
	set -- "$@" "$GO_TEST_VERBOSE_FLAG"
fi
set -- "$@" "$INTEGRATION_PACKAGE" \
	-it-kind="$IT_KIND" \
	-it-compose-iggy="$IT_COMPOSE_IGGY" \
	-it-keep="$IT_KEEP" \
	-it-k8s-kind-node-image="$IT_KIND_NODE_IMAGE" \
	-it-services="$IT_SERVICES" \
	-it-expected-harness-participants="$IT_EXPECTED_HARNESS_PARTICIPANTS"
if [ -n "$IT_KIND_CLUSTER" ]; then
	set -- "$@" -it-k8s-kind-cluster="$IT_KIND_CLUSTER"
fi
if [ -n "$GO_TEST_TIMEOUT" ]; then
	set -- "$@" -timeout="$GO_TEST_TIMEOUT"
fi
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

# Every test package shares one unpredictable private directory created by this
# invocation. The harness keeps its state, lock and generated kubeconfigs there;
# an inherited WALLABY_IT_STATE_DIR is never trusted, and this script removes
# only the directory it created.
state_dir=
child=
child_status=0
launching=
pending_signal=

cleanup() {
	if [ -n "$state_dir" ]; then
		rm -rf -- "$state_dir"
		state_dir=
	fi
}

forward_signal() {
	if [ -n "$child" ]; then
		kill -s "$1" "$child" 2>/dev/null || true
	elif [ -n "$launching" ]; then
		# The child is being started; deliver the signal once its pid is known.
		pending_signal=$1
	else
		exit "$2"
	fi
}

create_state_dir() {
	created=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-it.XXXXXXXXXX") || return 1
	if [ -L "$created" ] || [ ! -d "$created" ] || ! chmod 700 "$created"; then
		echo "refusing unsafe integration state directory" >&2
		rm -rf -- "$created"
		return 1
	fi
	resolved=$(cd "$created" && pwd -P) || resolved=
	case $resolved in
	/*) ;;
	*)
		echo "cannot resolve integration state directory" >&2
		rm -rf -- "$created"
		return 1
		;;
	esac
	state_dir=$resolved
}

# run_go_test runs its arguments in the background so signals sent to this
# script reach the Go test process while the directory trap stays armed. An
# asynchronous command gets /dev/null as stdin before explicit redirections in
# some shells, so the real stdin is passed through a saved descriptor.
run_go_test() {
	launching=1
	pending_signal=
	# A failed redirection of a special builtin can exit the shell, so probe in
	# a subshell and fall back to /dev/null when stdin is closed.
	if (exec 3<&0) 2>/dev/null; then
		exec 3<&0
	else
		exec 3</dev/null
	fi
	"$@" <&3 3<&- &
	child=$!
	exec 3<&-
	launching=
	if [ -n "$pending_signal" ]; then
		kill -s "$pending_signal" "$child" 2>/dev/null || true
		pending_signal=
	fi
	while :; do
		if wait "$child"; then
			child_status=0
		else
			child_status=$?
		fi
		if ! kill -0 "$child" 2>/dev/null; then
			break
		fi
	done
	child=
}

trap cleanup EXIT
trap 'forward_signal INT 130' INT
trap 'forward_signal TERM 143' TERM
trap 'forward_signal HUP 129' HUP

unset WALLABY_IT_STATE_DIR
create_state_dir || exit 1
export WALLABY_IT_STATE_DIR="$state_dir"

if [ -z "$IT_REQUIRED_TESTS" ]; then
	run_go_test "$@"
	exit "$child_status"
fi

if [ -n "$IT_RESULTS_FILE" ]; then
	umask 077
	results=$IT_RESULTS_FILE
else
	results=$state_dir/results.json
fi
run_go_test "$@" -json >"$results"
status=$child_status
cat "$results"
if [ "$status" -ne 0 ]; then
	exit "$status"
fi
expected_runs=${IT_COUNT:-1}
"$GO" run ./scripts/verify-go-test-json.go \
	-results "$results" \
	-required "$IT_REQUIRED_TESTS" \
	-expected-runs "$expected_runs"
