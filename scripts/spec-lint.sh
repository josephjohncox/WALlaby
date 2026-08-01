#!/bin/sh
set -eu

GO=${GO:-go}
GOBIN=${GOBIN:?GOBIN is required}
SPEC_LINT_VERBOSE_MODE=${SPEC_LINT_VERBOSE_MODE:-checks}
SPEC_LINT_VERBOSE=${SPEC_LINT_VERBOSE:-}

mkdir -p "$GOBIN"
"$GO" build -o "$GOBIN/wallaby-speccheck" ./cmd/wallaby-speccheck
set -- "$GO" vet -vettool="$GOBIN/wallaby-speccheck" -specaction.verbose-mode="$SPEC_LINT_VERBOSE_MODE"
if [ -n "$SPEC_LINT_VERBOSE" ]; then
	set -- "$@" -specaction.verbose="$SPEC_LINT_VERBOSE"
fi
exec "$@" ./...
