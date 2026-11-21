#!/bin/sh
# Check the conditional Goose/Perennial WP theorem for production Decide.
# Uses the local lean-toolchain (Lean 4.34.1) and does not change elan's default.
set -eu

vg=$(cd "$(dirname "$0")/.." && pwd)
root=$(cd "$vg/../.." && pwd)
art="$vg/artifacts"
mkdir -p "$art"
rm -f "$art/evidence.json"
cd "$vg"

echo "== drift =="
sh "$vg/scripts/regenerate.sh" >"$art/drift.txt" 2>&1 || {
	cat "$art/drift.txt"
	exit 1
}
cat "$art/drift.txt"

echo "== lean =="
elan show >>"$art/versions.txt"
ELAN_TOOLCHAIN=leanprover/lean4:v4.34.1 lean --version | tee -a "$art/versions.txt"
go version | tee -a "$art/versions.txt"
git -C "$root/.cache/perennial" rev-parse HEAD | tee -a "$art/versions.txt"
shasum -a 256 "$root/internal/reconcilepolicy/decision.go" >"$art/decision.sha256"
actual=$(cut -d " " -f 1 "$art/decision.sha256")
expected=$(tr -d '\n\r' <"$vg/SOURCE.sha256")
[ "$actual" = "$expected" ] || {
	echo "frozen production source drifted" >&2
	exit 1
}

echo "== build =="
# shellcheck disable=SC2064
start=$(date +%s)
set +e
lake build WallabyGoose >"$art/lake-build.txt" 2>&1
build_rc=$?
set -e
end=$(date +%s)
echo "lake build exit $build_rc wall_s $((end - start))" | tee -a "$art/lake-build.txt"
if [ "$build_rc" -ne 0 ]; then
	tail -80 "$art/lake-build.txt"
	exit "$build_rc"
fi

echo "== axioms =="
set +e
lake env lean "$vg/scripts/PrintAxioms.lean" >"$art/axioms.txt" 2>&1
ax_rc=$?
set -e
echo "print axioms exit $ax_rc"
if [ "$ax_rc" -ne 0 ]; then
	cat "$art/axioms.txt"
	exit "$ax_rc"
fi
if grep -E 'sorryAx|sorry' "$art/axioms.txt"; then
	echo "subject proof depends on sorry" >&2
	exit 1
fi

echo "== negative control =="
cp "$vg/negative/FalseAdopt.lean.in" "$art/FalseAdopt.lean"
set +e
lake env lean artifacts/FalseAdopt.lean >"$art/negative.txt" 2>&1
neg_rc=$?
set -e
if [ "$neg_rc" -ne 1 ]; then
	echo "negative control must exit 1 for the false property, got $neg_rc" >&2
	cat "$art/negative.txt" >&2
	exit 1
fi
python "$vg/scripts/audit.py"
