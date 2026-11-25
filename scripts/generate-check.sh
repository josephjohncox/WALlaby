#!/bin/sh
set -eu

if ! git diff --exit-code HEAD -- gen/go; then
	echo "generated artifacts are stale; run mise run generate and include the results" >&2
	exit 1
fi

if [ "${CI:-}" = "true" ]; then
	untracked=$(git ls-files --others --exclude-standard -- gen/go)
	if [ -n "$untracked" ]; then
		echo "generated protobuf stubs are untracked in CI; run mise run generate and commit them" >&2
		git status --short -- gen/go >&2
		exit 1
	fi
fi
