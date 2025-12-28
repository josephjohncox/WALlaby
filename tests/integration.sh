#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/docker-compose.yml"

RESET_VOLUMES="${RESET_VOLUMES:-1}"
PG_PORT="${TEST_PG_PORT:-5433}"
CLICKHOUSE_PORT="${TEST_CLICKHOUSE_PORT:-9001}"
CLICKHOUSE_HTTP_PORT="${TEST_CLICKHOUSE_HTTP_PORT:-8124}"
CLICKHOUSE_USER="${TEST_CLICKHOUSE_USER:-wallaby}"
CLICKHOUSE_PASSWORD="${TEST_CLICKHOUSE_PASSWORD:-wallaby}"
FAKESNOW_PORT="${TEST_FAKESNOW_PORT:-8000}"

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
      if nc -z "$host" "$port" >/dev/null 2>&1; then
        echo "Port $port is open"
        return 0
      fi
    else
      if timeout 1 bash -c ":</dev/tcp/$host/$port" >/dev/null 2>&1; then
        echo "Port $port is open"
        return 0
      fi
    fi
    echo "Waiting for $name on $host:$port ($i/$attempts)..."
    sleep "$delay"
  done
  echo "Timed out waiting for $name" >&2
  return 1
}

wait_for_port "localhost" "$PG_PORT" "postgres"
wait_for_port "localhost" "$CLICKHOUSE_PORT" "clickhouse"
wait_for_clickhouse() {
  local host="$1"
  local port="$2"
  local attempts="${3:-60}"
  local delay="${4:-2}"

  for ((i=1; i<=attempts; i++)); do
    if command -v curl >/dev/null 2>&1; then
      if curl -fsS -u "${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}" "http://${host}:${port}/ping" >/dev/null 2>&1; then
        echo "Clickhouse HTTP ping is open (auth)"
        return 0
      fi
      if curl -fsS "http://${host}:${port}/ping" >/dev/null 2>&1; then
        echo "Clickhouse HTTP ping is open (no auth)"
        CLICKHOUSE_USER="default"
        CLICKHOUSE_PASSWORD=""
        return 0
      fi
    elif command -v wget >/dev/null 2>&1; then
      if wget -qO- --user="${CLICKHOUSE_USER}" --password="${CLICKHOUSE_PASSWORD}" "http://${host}:${port}/ping" >/dev/null 2>&1; then
        echo "Clickhouse HTTP ping is open (auth)"
        return 0
      fi
      if wget -qO- "http://${host}:${port}/ping" >/dev/null 2>&1; then
        echo "Clickhouse HTTP ping is open (no auth)"
        CLICKHOUSE_USER="default"
        CLICKHOUSE_PASSWORD=""
        return 0
      fi
    fi
    echo "Waiting for clickhouse HTTP ping on $host:$port ($i/$attempts)..."
    sleep "$delay"
  done
  echo "Timed out waiting for clickhouse HTTP ping" >&2
  return 1
}

wait_for_clickhouse "localhost" "$CLICKHOUSE_HTTP_PORT"
wait_for_port "localhost" "$FAKESNOW_PORT" "fakesnow"

export TEST_PG_DSN="${TEST_PG_DSN:-postgres://postgres:postgres@localhost:${PG_PORT}/wallaby?sslmode=disable}"
export WALLABY_TEST_DBOS_DSN="${WALLABY_TEST_DBOS_DSN:-$TEST_PG_DSN}"
export WALLABY_TEST_CLICKHOUSE_DSN="${WALLABY_TEST_CLICKHOUSE_DSN:-clickhouse://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@localhost:${CLICKHOUSE_PORT}/default}"
export WALLABY_TEST_CLICKHOUSE_DB="${WALLABY_TEST_CLICKHOUSE_DB:-default}"
export WALLABY_TEST_FAKESNOW_HOST="${WALLABY_TEST_FAKESNOW_HOST:-localhost}"
export WALLABY_TEST_FAKESNOW_PORT="${WALLABY_TEST_FAKESNOW_PORT:-${FAKESNOW_PORT}}"
export WALLABY_TEST_FORCE_FAKESNOW="${WALLABY_TEST_FORCE_FAKESNOW:-1}"
export WALLABY_TEST_CLI_LOG="${WALLABY_TEST_CLI_LOG:-1}"

# Avoid invoking external credential helpers during integration tests.
if [[ -n "${WALLABY_TEST_K8S_KUBECONFIG:-}" ]]; then
  export KUBECONFIG="${WALLABY_TEST_K8S_KUBECONFIG}"
else
  export KUBECONFIG="/dev/null"
fi
export AWS_PROFILE=""
export AWS_DEFAULT_PROFILE=""
export AWS_CONFIG_FILE="/dev/null"
export AWS_SHARED_CREDENTIALS_FILE="/dev/null"
export AWS_EC2_METADATA_DISABLED="true"
export AWS_SDK_LOAD_CONFIG="0"
unset WALLABY_TEST_SNOWFLAKE_DSN WALLABY_TEST_SNOWFLAKE_SCHEMA

GO_TEST_TIMEOUT="${GO_TEST_TIMEOUT:-8m}"
echo "Running go test ./tests/... -v (timeout=${GO_TEST_TIMEOUT})"
packages=$(go list ./tests/...)
for pkg in $packages; do
  echo "Running go test ${pkg}"
  go test "$pkg" -v -count=1 -timeout="$GO_TEST_TIMEOUT"
done

if [[ "${KEEP_CONTAINERS:-0}" != "1" ]]; then
  docker compose -f "$COMPOSE_FILE" down -v
fi
