#!/bin/sh
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-proto-breaking-selftest.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM

fake_buf=$tmpdir/fake-buf
cat >"$fake_buf" <<'FAKE'
#!/bin/sh
case ${FAKE_BUF_CASE:-} in
	exact)
		cat <<'OUTPUT'
proto/wallaby/v1/ddl.proto:91:7:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto/wallaby/v1/ddl.proto:4:99:Previously present message "MarkDDLAppliedResponse" was deleted from file.
proto/wallaby/v1/ddl.proto:812:3:Previously present RPC "MarkDDLApplied" on service "DDLService" was deleted.
OUTPUT
		exit 100
		;;
	extra)
		cat <<'OUTPUT'
proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedResponse" was deleted from file.
proto/wallaby/v1/ddl.proto:54:1:Previously present RPC "MarkDDLApplied" on service "DDLService" was deleted.
proto/wallaby/v1/flow.proto:8:2:Previously present message "UnexpectedRemoval" was deleted from file.
OUTPUT
		exit 100
		;;
	missing)
		cat <<'OUTPUT'
proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto/wallaby/v1/ddl.proto:54:1:Previously present RPC "MarkDDLApplied" on service "DDLService" was deleted.
OUTPUT
		exit 100
		;;
	malformed)
		echo 'proto/wallaby/v1/ddl.proto:1:1:Previously present thing "Unknown" disappeared.'
		exit 100
		;;
	concatenated-allowed-extra)
		printf '%s%s\n' \
			'proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.' \
			'proto/wallaby/v1/flow.proto:2:2:Previously present message "UnexpectedRemoval" was deleted from file.'
		exit 100
		;;
	concatenated-extra-allowed)
		printf '%s%s\n' \
			'proto/wallaby/v1/flow.proto:2:2:Previously present message "UnexpectedRemoval" was deleted from file.' \
			'proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	double-dot-identifier)
		echo 'proto/wallaby/v1/ddl.proto:1:1:Previously present message "wallaby..MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	double-dot-service)
		echo 'proto/wallaby/v1/ddl.proto:1:1:Previously present RPC "MarkDDLApplied" on service "wallaby..DDLService" was deleted.'
		exit 100
		;;
	ansi)
		printf '\033[31m%s\033[0m\n' 'proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	duplicate)
		cat <<'OUTPUT'
proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto/wallaby/v1/ddl.proto:2:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto/wallaby/v1/ddl.proto:3:1:Previously present message "MarkDDLAppliedResponse" was deleted from file.
proto/wallaby/v1/ddl.proto:4:1:Previously present RPC "MarkDDLApplied" on service "DDLService" was deleted.
OUTPUT
		exit 100
		;;
	prefix-injection)
		echo 'NOTICE proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	suffix-injection)
		echo 'proto/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file. trailing chatter'
		exit 100
		;;
	colon-path)
		echo 'proto:injected/wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	empty-path)
		echo ':1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	double-slash-path)
		echo 'proto//wallaby/v1/ddl.proto:1:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	zero-location)
		echo 'proto/wallaby/v1/ddl.proto:0:1:Previously present message "MarkDDLAppliedRequest" was deleted from file.'
		exit 100
		;;
	valid-path-edge)
		cat <<'OUTPUT'
proto-dir/wallaby_v1/ddl-file.proto:91:7:Previously present message "MarkDDLAppliedRequest" was deleted from file.
proto-dir/wallaby_v1/ddl-file.proto:4:99:Previously present message "MarkDDLAppliedResponse" was deleted from file.
proto-dir/wallaby_v1/ddl-file.proto:812:3:Previously present RPC "MarkDDLApplied" on service "DDLService" was deleted.
OUTPUT
		exit 100
		;;
	failure)
		echo 'unable to read comparison image' >&2
		exit 2
		;;
	zero)
		exit 0
		;;
	*)
		echo "unknown fake case: ${FAKE_BUF_CASE:-}" >&2
		exit 2
		;;
esac
FAKE
chmod +x "$fake_buf"

allowlist=$tmpdir/allowlist
cp "$script_dir/proto-breaking.allowlist" "$allowlist"
empty_allowlist=$tmpdir/empty-allowlist
: >"$empty_allowlist"

run_case() {
	name=$1
	scenario=$2
	list=$3
	want_status=$4
	want_message=$5
	log=$tmpdir/$name.log
	if FAKE_BUF_CASE=$scenario BUF_BREAKING_AGAINST=.git#ref=fake PROTO_BREAKING_ALLOWLIST=$list \
		"$script_dir/proto-breaking.sh" "$fake_buf" >"$log" 2>&1; then
		status=0
	else
		status=$?
	fi
	if [ "$status" -ne "$want_status" ]; then
		echo "$name: status=$status want=$want_status" >&2
		cat "$log" >&2
		exit 1
	fi
	if [ -n "$want_message" ] && ! grep -Fq "$want_message" "$log"; then
		echo "$name: missing diagnostic: $want_message" >&2
		cat "$log" >&2
		exit 1
	fi
	printf 'PASS %s\n' "$name"
}

run_case exact-match exact "$allowlist" 0 ''
run_case extra-break extra "$allowlist" 1 'unexpected break(s):'
run_case missing-break missing "$allowlist" 1 'missing allowed break(s):'
run_case malformed-output malformed "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case concatenated-allowed-extra concatenated-allowed-extra "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case concatenated-extra-allowed concatenated-extra-allowed "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case double-dot-identifier double-dot-identifier "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case double-dot-service double-dot-service "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case ansi-control-byte ansi "$allowlist" 1 'ANSI or control byte'
run_case duplicate-diagnostic duplicate "$allowlist" 1 'duplicate buf breaking diagnostic identity'
run_case prefix-injection prefix-injection "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case suffix-injection suffix-injection "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case colon-path colon-path "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case empty-path empty-path "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case double-slash-path double-slash-path "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case zero-location zero-location "$allowlist" 1 'malformed or unsupported buf breaking diagnostic'
run_case valid-path-edge valid-path-edge "$allowlist" 0 ''
run_case tool-failure failure "$allowlist" 1 'buf breaking execution failed with status 2'
run_case zero-break-obsolete-allowlist zero "$allowlist" 1 'allowlist is nonempty and obsolete'
run_case zero-break-empty-allowlist zero "$empty_allowlist" 0 ''
