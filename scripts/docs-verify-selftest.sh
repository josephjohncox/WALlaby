#!/bin/sh
set -eu

repository_root=$(cd -- "$(dirname "$0")/.." && pwd)
verifier="$repository_root/scripts/docs-verify.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/wallaby-docs-selftest.XXXXXX")
trap 'rm -rf "$fixture"' EXIT HUP INT TERM

mkdir -p "$fixture/source" "$fixture/scripts"
cat >"$fixture/scripts/generate.sh" <<EOF
#!/bin/sh
set -eu
output=\${1:?output root is required}
rm -rf "\$output/docs/reference/generated"
mkdir -p "\$output/docs/reference/generated"
cp -R "$fixture/source/." "\$output/docs/reference/generated/"
EOF
chmod +x "$fixture/scripts/generate.sh"

verify() {
	DOCS_VERIFY_ROOT="$fixture" \
		DOCS_GENERATOR="$fixture/scripts/generate.sh" \
		CI=false \
		"$verifier" >/dev/null
}

expect_failure() {
	name=$1
	if verify >/dev/null 2>&1; then
		echo "docs verification self-test unexpectedly passed: $name" >&2
		exit 1
	fi
}

reset_fixture() {
	rm -rf "$fixture/source" "$fixture/docs"
	mkdir -p "$fixture/source/go" "$fixture/docs/reference/generated/go"
	printf '%s\n' 'generated grpc docs' >"$fixture/source/grpc.md"
	printf '%s\n' 'generated Go docs' >"$fixture/source/go/package.md"
	cp -R "$fixture/source/." "$fixture/docs/reference/generated/"
}

reset_fixture
verify

reset_fixture
printf '%s\n' 'local edit' >>"$fixture/docs/reference/generated/grpc.md"
expect_failure modified

reset_fixture
rm "$fixture/docs/reference/generated/go/package.md"
expect_failure deleted

reset_fixture
printf '%s\n' 'obsolete output' >"$fixture/docs/reference/generated/stale.md"
expect_failure stale-extra

reset_fixture
git -C "$fixture" init -q
git -C "$fixture" add docs/reference/generated/grpc.md docs/reference/generated/go/package.md
printf '%s\n' 'new generated output' >"$fixture/source/new.md"
cp "$fixture/source/new.md" "$fixture/docs/reference/generated/new.md"
if [ -z "$(git -C "$fixture" ls-files --others --exclude-standard -- docs/reference/generated/new.md)" ]; then
	echo 'self-test setup did not create an untracked generated output' >&2
	exit 1
fi
verify

printf '%s\n' 'Documentation verification self-tests passed: fresh, modified, deleted, stale-extra, and matching uncommitted generation.'
