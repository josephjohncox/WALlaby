#!/bin/sh
set -eu

GO=${GO:-go}
GO_TEST_VERBOSE=${GO_TEST_VERBOSE:-1}
GO_TEST_VERBOSE_FLAG=${GO_TEST_VERBOSE_FLAG:-}
if [ -z "$GO_TEST_VERBOSE_FLAG" ] && [ "$GO_TEST_VERBOSE" = "1" ]; then
	GO_TEST_VERBOSE_FLAG=-v
fi

if [ -n "$GO_TEST_VERBOSE_FLAG" ]; then
	exec "$GO" test "$GO_TEST_VERBOSE_FLAG" ./...
fi
exec "$GO" test ./...
