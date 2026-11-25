#!/bin/sh
# Regenerate the goose translation of internal/reconcilepolicy and diff it
# against the files checked in under verification/goose. Upstream trees stay
# in the repository .cache and are not vendored.
set -eu

root=$(cd "$(dirname "$0")/../../.." && pwd)
vg="$root/verification/goose"
cache="$root/.cache/perennial"
pin=e9988b5cde5907d9a66177ad7912c4d3f3729651
pkg=github.com/josephjohncox/wallaby/internal/reconcilepolicy

if [ ! -d "$cache/.git" ]; then
	mkdir -p "$root/.cache"
	git clone --filter=blob:none https://github.com/mit-pdos/perennial.git "$cache"
fi
git -C "$cache" fetch --depth 1 origin "$pin"
git -C "$cache" checkout --detach "$pin"
test "$(git -C "$cache" rev-parse HEAD)" = "$pin"
git -C "$cache" diff --exit-code HEAD

mkdir -p "$root/.cache/goose-bin"
(
	cd "$cache"
	GOFLAGS='-mod=readonly' go build -o "$root/.cache/goose-bin/goose" ./goose/cmd/goose
	GOFLAGS='-mod=readonly' go build -o "$root/.cache/goose-bin/proofgen" ./goose/cmd/proofgen
)

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/Code" "$tmp/GeneratedProof"
# The config lives next to the checked-in translation. Copy the directory's toml.
config_src="$vg/WallabyGoose/Code/github_com/josephjohncox/wallaby/internal/reconcilepolicy.toml"
mkdir -p "$tmp/Code/github_com/josephjohncox/wallaby/internal"
cp "$config_src" "$tmp/Code/github_com/josephjohncox/wallaby/internal/reconcilepolicy.toml"

"$root/.cache/goose-bin/goose" \
	-out "$tmp/Code" -configdir "$tmp/Code" -dir "$root" \
	-lean-root github.com/josephjohncox/wallaby=WallabyGoose \
	"$pkg"
"$root/.cache/goose-bin/proofgen" \
	-out "$tmp/GeneratedProof" -configdir "$tmp/Code" -dir "$root" \
	-lean-root github.com/josephjohncox/wallaby=WallabyGoose \
	"$pkg"

code_rel=github_com/josephjohncox/wallaby/internal/reconcilepolicy.lean
diff -u "$vg/WallabyGoose/Code/$code_rel" "$tmp/Code/$code_rel"
diff -u "$vg/WallabyGoose/GeneratedProof/$code_rel" "$tmp/GeneratedProof/$code_rel"
echo "drift-check: ok"
