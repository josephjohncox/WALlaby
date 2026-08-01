#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
GO=${GO:-go}
temporary=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-spec-verify.XXXXXX")
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

cd "$root"
cp specs/coverage*.json "$temporary/"

"$GO" run ./cmd/wallaby-spec-sync --spec-dir specs --manifest-dir "$temporary"
"$GO" run ./cmd/wallaby-spec-manifest --out "$temporary/coverage.json" --dir specs

for actual in specs/coverage*.json; do
	name=$(basename "$actual")
	if ! diff -u "$actual" "$temporary/$name"; then
		echo "spec manifest differs from a fresh generation: $actual" >&2
		echo "run just spec-sync and just spec-manifest, then include every changed manifest" >&2
		exit 1
	fi
done

if [ "${CI:-}" = "true" ]; then
	for actual in specs/coverage*.json; do
		if ! git ls-files --error-unmatch -- "$actual" >/dev/null 2>&1; then
			echo "spec manifest is not tracked in CI: $actual" >&2
			exit 1
		fi
	done
fi

printf '%s\n' 'Spec coverage manifests match a fresh, nonmutating generation.'
