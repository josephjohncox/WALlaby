#!/bin/sh
set -eu

if ! git diff --exit-code HEAD -- gen/go; then
	echo "generated protobuf stubs are stale; run just generate and include the results" >&2
	exit 1
fi

# Permit a local uncommitted first generation, but require generated Go to be
# tracked in CI so clean-clone builds cannot pass with omitted artifacts.
untracked=$(git ls-files --others --exclude-standard -- gen/go)
if [ "${CI:-}" = "true" ] && [ -n "$untracked" ]; then
	echo "generated protobuf stubs are untracked in CI; run just generate and commit them" >&2
	git status --short -- gen/go >&2
	exit 1
fi
