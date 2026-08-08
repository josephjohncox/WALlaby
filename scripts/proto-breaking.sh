#!/bin/sh
set -eu

buf=${1:-buf}
script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
allowlist=${PROTO_BREAKING_ALLOWLIST:-$script_dir/proto-breaking.allowlist}

against=${BUF_BREAKING_AGAINST:-}
if [ -z "$against" ]; then
	ref=${BUF_BREAKING_REF:-}
	if [ -z "$ref" ]; then
		if [ -n "${GITHUB_BASE_SHA:-}" ]; then
			ref=$GITHUB_BASE_SHA
		elif git show-ref --verify --quiet refs/remotes/origin/main; then
			ref=refs/remotes/origin/main
		elif git show-ref --verify --quiet refs/heads/main; then
			ref=refs/heads/main
		elif git show-ref --verify --quiet refs/remotes/origin/HEAD; then
			ref=refs/remotes/origin/HEAD
		else
			ref=$(git rev-parse --verify HEAD~1 2>/dev/null || git rev-parse --verify HEAD)
		fi
	fi
	against=.git#ref=$ref
fi

if [ ! -f "$allowlist" ]; then
	echo "proto breaking allowlist does not exist: $allowlist" >&2
	exit 1
fi

tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-proto-breaking.XXXXXX")
trap 'rm -rf "$tmpdir"' EXIT HUP INT TERM
raw=$tmpdir/raw
actual=$tmpdir/actual
actual_sorted=$tmpdir/actual.sorted
allowed=$tmpdir/allowed
allowed_sorted=$tmpdir/allowed.sorted

: >"$actual"
: >"$allowed"

path='[A-Za-z0-9_][A-Za-z0-9_.-]*(/[A-Za-z0-9_][A-Za-z0-9_.-]*)*\.proto'
identifier='[A-Za-z_][A-Za-z0-9_]*'
qualified_identifier="${identifier}(\\.${identifier})*"
allowlist_grammar="^${path}: (message ${qualified_identifier} deleted|rpc ${qualified_identifier}\.${identifier} deleted|enum ${identifier}(\.(0|[1-9][0-9]*))? deleted|field ${qualified_identifier}\.${identifier}#(0|[1-9][0-9]*) deleted)$"
while IFS= read -r entry || [ -n "$entry" ]; do
	if ! printf '%s\n' "$entry" | LC_ALL=C grep -Eq "$allowlist_grammar"; then
		echo "malformed proto breaking allowlist entry: $entry" >&2
		exit 1
	fi
	printf '%s\n' "$entry" >>"$allowed"
done <"$allowlist"

LC_ALL=C sort "$allowed" >"$allowed_sorted"
if [ -n "$(LC_ALL=C uniq -d "$allowed_sorted")" ]; then
	echo "duplicate proto breaking allowlist entry" >&2
	exit 1
fi

echo "buf breaking --against '$against'"
set +e
"$buf" breaking --against "$against" >"$raw" 2>&1
status=$?
set -e

if LC_ALL=C od -An -tu1 "$raw" | awk '
	{
		for (i = 1; i <= NF; i++) {
			if (($i < 32 && $i != 10) || $i == 127) {
				found = 1
			}
		}
	}
	END { exit found ? 0 : 1 }
'; then
	echo "buf breaking output contains an ANSI or control byte" >&2
	exit 1
fi

cat "$raw"

case "$status" in
0 | 100) ;;
*)
	echo "buf breaking execution failed with status $status" >&2
	exit 1
	;;
esac

location='[1-9][0-9]*:[1-9][0-9]*'
message_grammar="^${path}:${location}:Previously present message \"${qualified_identifier}\" was deleted from file\\.$"
rpc_grammar="^${path}:${location}:Previously present RPC \"${identifier}\" on service \"${qualified_identifier}\" was deleted\\.$"
enum_value_grammar="^${path}:${location}:Previously present enum value \"(0|[1-9][0-9]*)\" on enum \"${identifier}\" was deleted\\.$"
enum_grammar="^${path}:${location}:Previously present enum \"${identifier}\" was deleted from file\\.$"
field_grammar="^${path}:${location}:Previously present field \"(0|[1-9][0-9]*)\" with name \"${identifier}\" on message \"${qualified_identifier}\" was deleted\\.$"

while IFS= read -r diagnostic || [ -n "$diagnostic" ]; do
	proto_file=${diagnostic%%:*}
	if printf '%s\n' "$diagnostic" | LC_ALL=C grep -Eq "$message_grammar"; then
		identity=$(printf '%s\n' "$diagnostic" | awk -F'"' -v path="$proto_file" '{ print path ": message " $2 " deleted" }')
	elif printf '%s\n' "$diagnostic" | LC_ALL=C grep -Eq "$rpc_grammar"; then
		identity=$(printf '%s\n' "$diagnostic" | awk -F'"' -v path="$proto_file" '{ print path ": rpc " $4 "." $2 " deleted" }')
	elif printf '%s\n' "$diagnostic" | LC_ALL=C grep -Eq "$enum_value_grammar"; then
		identity=$(printf '%s\n' "$diagnostic" | awk -F'"' -v path="$proto_file" '{ print path ": enum " $4 "." $2 " deleted" }')
	elif printf '%s\n' "$diagnostic" | LC_ALL=C grep -Eq "$enum_grammar"; then
		identity=$(printf '%s\n' "$diagnostic" | awk -F'"' -v path="$proto_file" '{ print path ": enum " $2 " deleted" }')
	elif printf '%s\n' "$diagnostic" | LC_ALL=C grep -Eq "$field_grammar"; then
		identity=$(printf '%s\n' "$diagnostic" | awk -F'"' -v path="$proto_file" '{ print path ": field " $6 "." $4 "#" $2 " deleted" }')
	else
		echo "malformed or unsupported buf breaking diagnostic: $diagnostic" >&2
		exit 1
	fi
	printf '%s\n' "$identity" >>"$actual"
done <"$raw"

LC_ALL=C sort "$actual" >"$actual_sorted"
if [ -n "$(LC_ALL=C uniq -d "$actual_sorted")" ]; then
	echo "duplicate buf breaking diagnostic identity" >&2
	exit 1
fi

if [ "$status" -eq 0 ]; then
	if [ -s "$actual_sorted" ]; then
		echo "buf returned success while reporting breaking diagnostics" >&2
		exit 1
	fi
	if [ -s "$allowed_sorted" ]; then
		echo "buf reports no breaking changes but the proto breaking allowlist is nonempty and obsolete" >&2
		exit 1
	fi
	exit 0
fi

if [ ! -s "$actual_sorted" ]; then
	echo "buf returned breaking-change status without diagnostics" >&2
	exit 1
fi

if ! cmp -s "$allowed_sorted" "$actual_sorted"; then
	echo "buf breaking changes do not exactly match the checked-in allowlist" >&2
	missing=$(LC_ALL=C comm -23 "$allowed_sorted" "$actual_sorted")
	unexpected=$(LC_ALL=C comm -13 "$allowed_sorted" "$actual_sorted")
	if [ -n "$missing" ]; then
		printf 'missing allowed break(s):\n%s\n' "$missing" >&2
	fi
	if [ -n "$unexpected" ]; then
		printf 'unexpected break(s):\n%s\n' "$unexpected" >&2
	fi
	exit 1
fi
