#!/bin/sh
set -eu

root=${DOCS_VERIFY_ROOT:-$(cd -- "$(dirname "$0")/.." && pwd)}
generator=${DOCS_GENERATOR:-$root/scripts/docs-generate.sh}
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-docs-verify.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

"$generator" "$temporary_root"

expected="$temporary_root/docs/reference/generated"
actual="$root/docs/reference/generated"
if ! diff -ru "$expected" "$actual"; then
	echo "generated documentation differs from a fresh generation; run make docs-generate and include all modified, removed, and new outputs" >&2
	exit 1
fi

# A matching uncommitted source/output change is valid locally. CI additionally
# requires every expected output to exist in the repository index.
if [ "${CI:-}" = "true" ]; then
	list="$temporary_root/expected-files"
	(
		cd "$expected"
		find . -type f -print | LC_ALL=C sort
	) >"$list"
	while IFS= read -r relative; do
		relative=${relative#./}
		if ! git -C "$root" ls-files --error-unmatch -- "docs/reference/generated/$relative" >/dev/null 2>&1; then
			echo "generated documentation is not tracked in CI: docs/reference/generated/$relative" >&2
			echo "run make docs-generate and commit every generated output" >&2
			exit 1
		fi
	done <"$list"
fi

printf '%s\n' 'Generated documentation matches a fresh, nonmutating generation.'
