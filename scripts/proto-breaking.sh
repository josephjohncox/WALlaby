#!/bin/sh
set -eu

buf=${1:-buf}
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

echo "buf breaking --against '$against'"
exec "$buf" breaking --against "$against"
