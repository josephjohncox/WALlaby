#!/bin/sh
set -eu

root=$(cd -- "$(dirname "$0")/.." && pwd)
GO=${GO:-go}

cd "$root"
exec "$GO" run ./tools/testcomplete --go "$GO"
