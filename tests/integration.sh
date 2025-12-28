#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/docker-compose.yml"

RESET_VOLUMES="${RESET_VOLUMES:-1}"
PG_PORT="${TEST_PG_PORT:-5433}"
CLICKHOUSE_PORT="${TEST_CLICKHOUSE_PORT:-9001}"

if [[ "$RESET_VOLUMES" == "1" ]]; then
  docker compose -f "$COMPOSE_FILE" down -v
fi
docker compose -f "$COMPOSE_FILE" up -d

wait_for_port() {
  local host="$1"
  local port="$2"
  local name="$3"
  local attempts="${4:-60}"
  local delay="${5:-2}"

  for ((i=1; i<=attempts; i++)); do
    if command -v nc >/dev/null 2>&1; then
      nc -z "$host" "$port" >/dev/null 2>&1 && return 0
    else
      timeout 1 bash -c ":</dev/tcp/$host/$port" >/dev/null 2>&1 && return 0
    fi
    echo "Waiting for $name on $host:$port ($i/$attempts)..."
    sleep "$delay"
  done
  echo "Timed out waiting for $name" >&2
  return 1
}

wait_for_port "localhost" "$PG_PORT" "postgres"
wait_for_port "localhost" "$CLICKHOUSE_PORT" "clickhouse"

export TEST_PG_DSN="${TEST_PG_DSN:-postgres://postgres:postgres@localhost:${PG_PORT}/wallaby?sslmode=disable}"
export WALLABY_TEST_DBOS_DSN="${WALLABY_TEST_DBOS_DSN:-$TEST_PG_DSN}"
export WALLABY_TEST_CLICKHOUSE_DSN="${WALLABY_TEST_CLICKHOUSE_DSN:-clickhouse://default:@localhost:${CLICKHOUSE_PORT}/default}"
export WALLABY_TEST_CLICKHOUSE_DB="${WALLABY_TEST_CLICKHOUSE_DB:-default}"

go test ./tests/... -v

if [[ "${KEEP_CONTAINERS:-0}" != "1" ]]; then
  docker compose -f "$COMPOSE_FILE" down -v
fi
