#!/bin/sh
set -eu

script_dir=$(cd "$(dirname "$0")" && pwd)
validator="$script_dir/validate-release-tag.sh"

for tag in \
	v0.0.0 \
	v1.2.3 \
	v1.2.3-alpha.1 \
	v1.2.3-0 \
	v1.2.3+build.5 \
	v1.2.3-rc.1+build.5
do
	"$validator" "$tag"
done

for tag in \
	1.2.3 \
	v01.2.3 \
	v1.02.3 \
	v1.2.03 \
	v1.2 \
	v1.2.3-01 \
	v1.2.3-.. \
	v1.2.3-alpha..1 \
	v1.2.3+..
do
	if "$validator" "$tag" >/dev/null 2>&1; then
		echo "validator accepted invalid tag: $tag" >&2
		exit 1
	fi
done

echo "release tag validation self-test passed"
