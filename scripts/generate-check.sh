#!/bin/sh
set -eu

if ! git diff --exit-code HEAD -- \
	gen/go \
	third_party/hamba-avro-shim/avro_shim.go \
	third_party/hamba-avro-shim/ocf/ocf_shim.go; then
	echo "generated artifacts are stale; run just generate and include the results" >&2
	exit 1
fi

# Permit a local uncommitted first generation, but require all generators and
# outputs to be tracked in CI so clean-clone builds cannot omit them.
required='third_party/hamba-avro-shim/cmd/shimgen/main.go
third_party/hamba-avro-shim/cmd/shimgen/main_test.go
third_party/hamba-avro-shim/avro_shim.go
third_party/hamba-avro-shim/ocf/ocf_shim.go'
if [ "${CI:-}" = "true" ]; then
	for path in $required; do
		if ! git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
			echo "required generated artifact or generator is untracked: $path" >&2
			exit 1
		fi
	done
	untracked=$(git ls-files --others --exclude-standard -- gen/go)
	if [ -n "$untracked" ]; then
		echo "generated protobuf stubs are untracked in CI; run just generate and commit them" >&2
		git status --short -- gen/go >&2
		exit 1
	fi
fi
