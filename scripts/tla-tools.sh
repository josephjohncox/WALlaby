#!/bin/sh
set -eu

url=${1:?TLA tools URL is required}
tools_dir=${2:?TLA tools directory is required}
jar=${3:?TLA tools jar path is required}
gobin=${4:?Go bin path is required}
checksum_file=${5:?TLA tools SHA-256 file is required}

expected=$(awk 'NF { print $1; exit }' "$checksum_file")
case "$expected" in
	????????????????????????????????????????????????????????????????) ;;
	*)
		echo "invalid TLA tools SHA-256 in $checksum_file" >&2
		exit 1
		;;
esac

mkdir -p "$tools_dir" "$gobin"
temporary="${jar}.tmp"
trap 'rm -f "$temporary"' EXIT HUP INT TERM
if command -v curl >/dev/null 2>&1; then
	curl -fsSL "$url" -o "$temporary"
elif command -v wget >/dev/null 2>&1; then
	wget -qO "$temporary" "$url"
else
	echo "curl or wget is required to download tla2tools.jar" >&2
	exit 1
fi

if command -v sha256sum >/dev/null 2>&1; then
	actual=$(sha256sum "$temporary" | awk '{print $1}')
elif command -v shasum >/dev/null 2>&1; then
	actual=$(shasum -a 256 "$temporary" | awk '{print $1}')
else
	echo "sha256sum or shasum is required to verify tla2tools.jar" >&2
	exit 1
fi
if [ "$actual" != "$expected" ]; then
	echo "TLA tools SHA-256 mismatch: expected $expected, got $actual" >&2
	exit 1
fi

chmod 0644 "$temporary"
mv "$temporary" "$jar"
trap - EXIT HUP INT TERM
printf '%s\n' '#!/bin/sh' "exec java -cp \"$jar\" tlc2.TLC \"\$@\"" >"$gobin/tlc2.TLC"
printf '%s\n' '#!/bin/sh' "exec java -cp \"$jar\" pcal.trans \"\$@\"" >"$gobin/pcal"
chmod +x "$gobin/tlc2.TLC" "$gobin/pcal"
