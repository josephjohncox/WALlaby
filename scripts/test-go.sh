#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
cd "$root"
exec go run ./tools/testcomplete
