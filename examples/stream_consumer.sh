#!/usr/bin/env bash
set -euo pipefail

ADMIN_BIN=${WALLABY_ADMIN:-./bin/wallaby-admin}
STREAM=${1:-orders}
GROUP=${2:-search}
ENDPOINT=${WALLABY_ENDPOINT:-localhost:8080}
VISIBILITY=${VISIBILITY:-30}
MAX=${MAX:-10}
SLEEP=${SLEEP:-2}

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required for this example." >&2
  exit 1
fi

while true; do
  payload=$($ADMIN_BIN stream pull --json \
    -endpoint "$ENDPOINT" \
    -stream "$STREAM" \
    -group "$GROUP" \
    -max "$MAX" \
    -visibility "$VISIBILITY")

  count=$(echo "$payload" | jq '.count')
  if [ "$count" -eq 0 ]; then
    sleep "$SLEEP"
    continue
  fi

  echo "$payload" | jq -r '.messages[] | "id=\(.id) table=\(.table) lsn=\(.lsn)"'

  ids=$(echo "$payload" | jq -r '.messages[].id' | paste -sd, -)
  if [ -n "$ids" ]; then
    $ADMIN_BIN stream ack -endpoint "$ENDPOINT" -stream "$STREAM" -group "$GROUP" -ids "$ids"
  fi

done
