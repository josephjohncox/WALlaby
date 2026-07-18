#!/bin/sh
set -eu

GO=${GO:-go}
RAPID_PACKAGES=${RAPID_PACKAGES:-}
RAPID_CHECKS=${RAPID_CHECKS:-100}

if [ -z "$RAPID_PACKAGES" ]; then
	echo "RAPID_PACKAGES must not be empty" >&2
	exit 2
fi

# Package lists are intentionally whitespace-delimited recipe inputs.
# shellcheck disable=SC2086
"$GO" test $RAPID_PACKAGES -args -rapid.checks="$RAPID_CHECKS"

package_list=$(mktemp "${TMPDIR:-/tmp}/wallaby-packages.XXXXXX")
filtered_packages=$(mktemp "${TMPDIR:-/tmp}/wallaby-non-rapid.XXXXXX")
trap 'rm -f "$package_list" "$filtered_packages"' EXIT HUP INT TERM

# Keep producer and filter exit statuses separate: a failed go list must never
# look like an empty package set, while grep status 1 simply means no matches.
"$GO" list ./... >"$package_list"
skip_regex='^github.com/josephjohncox/wallaby/(pkg/stream|pkg/wire|internal/ddl|internal/registry|internal/workflow|connectors/sources/postgres)(/.*)?$'
if grep -Ev "$skip_regex" "$package_list" >"$filtered_packages"; then
	:
else
	status=$?
	if [ "$status" -ne 1 ]; then
		exit "$status"
	fi
fi
non_rapid_packages=$(cat "$filtered_packages")
if [ -n "$non_rapid_packages" ]; then
	# shellcheck disable=SC2086
	"$GO" test $non_rapid_packages
fi
