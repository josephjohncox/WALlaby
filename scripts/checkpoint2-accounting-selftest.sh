#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${root}"

results=
selected_profile=
while [[ $# -gt 0 ]]; do
	case "$1" in
	--results)
		results=$2
		shift 2
		;;
	--profile)
		selected_profile=$2
		shift 2
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
done

recipe_body() {
	local recipe=$1
	awk -v recipe="${recipe}:" '
    $0==recipe { active=1; next }
    active && /^[A-Za-z0-9_-]+:/ { exit }
    active { print }
  ' justfile
}

profile_evidence=$(go run ./scripts/managed-profile-evidence.go)
if [[ -z "${profile_evidence}" ]]; then
	echo "managed profile evidence selftest found no promoted gates" >&2
	exit 1
fi

listed=$(go test -list '^Test' \
	./tests ./tests/integration ./internal/runner ./internal/replication \
	./connectors/destinations/postgres ./connectors/destinations/clickhouse \
	./connectors/sources/postgres ./internal/telemetry ./internal/controlplane)

postgres_recipe=$(recipe_body test-checkpoint2-postgres-profile)
clickhouse_recipe=$(recipe_body test-clickhouse-managed-profile)
snowflake_recipe=$(recipe_body test-snowflake-managed-profile)
snowflake_required=$(printf '%s\n' "${snowflake_recipe}" | sed -n "s/.*required[^']*'[,]*\(Test[A-Za-z0-9_]*\)'.*/\1/p")
[[ -n "${snowflake_required}" ]] || {
	echo 'Snowflake managed profile gate has no required tests' >&2
	exit 1
}
snowflake_duplicates=$(printf '%s\n' "${snowflake_required}" | sort | uniq -d)
[[ -z "${snowflake_duplicates}" ]] || {
	echo "Snowflake managed profile gate duplicates required tests: ${snowflake_duplicates}" >&2
	exit 1
}
while IFS= read -r test_name; do
	[[ -n "${test_name}" ]] || continue
	count=$(printf '%s\n' "${listed}" | grep -xc -- "${test_name}" || true)
	if [[ "${count}" -ne 1 ]]; then
		echo "Snowflake managed profile evidence ${test_name} listed ${count} times; want exactly once" >&2
		exit 1
	fi
done <<<"${snowflake_required}"

while IFS='|' read -r profile test_name; do
	[[ -n "${profile}" && -n "${test_name}" ]] || continue
	count=$(printf '%s\n' "${listed}" | grep -xc -- "${test_name}" || true)
	if [[ "${count}" -ne 1 ]]; then
		echo "promoted profile ${profile} evidence ${test_name} listed ${count} times; want exactly once" >&2
		exit 1
	fi
	case "${profile}" in
	postgresql-to-postgresql-v1) recipe=${postgres_recipe} ;;
	postgresql-to-clickhouse-append-v1) recipe=${clickhouse_recipe} ;;
	*)
		echo "promoted profile ${profile} has no strict justfile gate" >&2
		exit 1
		;;
	esac
	recipe_count=$(printf '%s\n' "${recipe}" | grep -o -- "${test_name}" | wc -l | tr -d ' ')
	if [[ "${recipe_count}" -ne 1 ]]; then
		echo "promoted profile ${profile} evidence ${test_name} occurs ${recipe_count} times in its justfile gate; want exactly once" >&2
		exit 1
	fi
done <<<"${profile_evidence}"

if [[ -n "${results}" || -n "${selected_profile}" ]]; then
	[[ -n "${results}" && -n "${selected_profile}" ]] || {
		echo '--results and --profile must be supplied together' >&2
		exit 2
	}
	required=$(printf '%s\n' "${profile_evidence}" | awk -F'|' -v profile="${selected_profile}" '$1==profile { values=(values ? values "," : "") $2 } END { print values }')
	[[ -n "${required}" ]] || {
		echo "no promoted evidence for ${selected_profile}" >&2
		exit 1
	}
	go run ./scripts/verify-go-test-json.go -results "${results}" -required "${required}"
fi

echo "promoted profile evidence is non-vacuous, resolves exactly once, and is bound to strict no-skip gates"
