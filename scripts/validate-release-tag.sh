#!/bin/sh
set -eu

tag=${1:-${GITHUB_REF_NAME:-}}
if [ -z "$tag" ]; then
	echo "release tag is required" >&2
	exit 2
fi

number='(0|[1-9][0-9]*)'
prerelease='(0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)'
build='[0-9A-Za-z-]+'
pattern="^v${number}\\.${number}\\.${number}(-${prerelease}(\\.${prerelease})*)?(\\+${build}(\\.${build})*)?$"

if ! printf '%s\n' "$tag" | grep -Eq "$pattern"; then
	echo "invalid release tag $tag: expected strict SemVer with a v prefix" >&2
	exit 1
fi
