#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

PROFILES_RAW="${PROFILES:-small medium large}"
SCENARIOS_RAW="${SCENARIOS:-base}"
TARGETS="${TARGETS:-all}"
OUTPUT_ROOT="${OUTPUT_ROOT:-$ROOT_DIR/bench/results}"
RUN_ID="${RUN_ID:-$(date -u +"%Y%m%dT%H%M%SZ")}"
OUT_DIR="$OUTPUT_ROOT/run_$RUN_ID"
ENABLE_PROFILES="${ENABLE_PROFILES:-0}"
PROFILE_FORMAT="${PROFILE_FORMAT:-flamegraph}"
PROFILE_RENDER="${PROFILE_RENDER:-1}"
PROFILE_TOP="${PROFILE_TOP:-1}"

PROFILES_RAW="${PROFILES_RAW//,/ }"
SCENARIOS_RAW="${SCENARIOS_RAW//,/ }"

mkdir -p "$OUT_DIR"

RESET_VOLUMES="${RESET_VOLUMES:-1}"

echo "Starting benchmark dependencies (docker compose)"
if [[ "$RESET_VOLUMES" == "1" ]]; then
  docker compose -f "$ROOT_DIR/bench/docker-compose.yml" down -v
fi
docker compose -f "$ROOT_DIR/bench/docker-compose.yml" up -d
docker compose -f "$ROOT_DIR/bench/docker-compose.yml" ps

port_open() {
  local host="$1"
  local port="$2"

  if command -v nc >/dev/null 2>&1; then
    nc -z "$host" "$port" >/dev/null 2>&1
    return $?
  fi

  if command -v bash >/dev/null 2>&1; then
    timeout 1 bash -c ":</dev/tcp/$host/$port" >/dev/null 2>&1
    return $?
  fi

  return 1
}

container_id() {
  docker compose -f "$ROOT_DIR/bench/docker-compose.yml" ps -q "$1"
}

container_status() {
  local cid="$1"
  docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || true
}

container_health() {
  local cid="$1"
  docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid" 2>/dev/null || true
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local name="$3"
  local attempts="${4:-60}"
  local delay="${5:-2}"

  for ((i=1; i<=attempts; i++)); do
    if port_open "$host" "$port"; then
      echo "Ready: $name on $host:$port"
      return 0
    fi
    echo "Waiting for $name on $host:$port ($i/$attempts)..."
    sleep "$delay"
  done

  echo "Timed out waiting for $name on $host:$port" >&2
  return 1
}

wait_for_postgres() {
  local attempts="${1:-60}"
  local delay="${2:-2}"

  for ((i=1; i<=attempts; i++)); do
    local cid
    cid="$(container_id postgres)"
    if [[ -z "$cid" ]]; then
      echo "Waiting for postgres container ID ($i/$attempts)..."
      sleep "$delay"
      continue
    fi

    local status health
    status="$(container_status "$cid")"
    health="$(container_health "$cid")"

    if [[ "$status" == "exited" || "$status" == "dead" ]]; then
      echo "Postgres container stopped (status=$status). See logs: docker compose -f bench/docker-compose.yml logs postgres" >&2
      return 1
    fi

    if [[ "$health" == "healthy" ]]; then
      if port_open "localhost" 5432; then
        echo "Ready: postgres (healthy)"
        return 0
      fi
    fi

    if [[ -z "$health" && "$status" == "running" ]]; then
      if port_open "localhost" 5432; then
        echo "Ready: postgres (running)"
        return 0
      fi
    fi

    echo "Waiting for postgres (status=$status health=${health:-n/a}) ($i/$attempts)..."
    sleep "$delay"
  done

  echo "Timed out waiting for postgres" >&2
  return 1
}

wait_for_postgres
wait_for_port "localhost" 9000 "clickhouse"
wait_for_port "localhost" 9092 "redpanda"

echo "Writing benchmark results to $OUT_DIR"

resolve_targets() {
  local raw="$1"
  if [[ "$raw" == "all" || -z "$raw" ]]; then
    echo "kafka postgres clickhouse"
    return 0
  fi
  echo "${raw//,/ }"
}

build_bench_bin() {
  local bin_path="$1"
  if [[ -f "$bin_path" ]]; then
    return 0
  fi
  (cd "$ROOT_DIR" && go build -o "$bin_path" ./cmd/wallaby-bench)
}

pprof_supports_flamegraph() {
  if ! command -v go >/dev/null 2>&1; then
    return 1
  fi
  go tool pprof -help 2>/dev/null | grep -q "flamegraph"
}

pprof_command() {
  if command -v go >/dev/null 2>&1 && go tool pprof -help >/dev/null 2>&1; then
    echo "go tool pprof"
    return 0
  fi
  if command -v pprof >/dev/null 2>&1; then
    echo "pprof"
    return 0
  fi
  return 1
}

render_profile() {
  local bin_path="$1"
  local cpu_path="$2"
  local out_base="$3"

  local pprof_cmd
  pprof_cmd="$(pprof_command || true)"
  if [[ -z "$pprof_cmd" ]]; then
    echo "pprof not available; skipping profile rendering"
    return 0
  fi

  render_flamegraph() {
    if $pprof_cmd -flamegraph "$bin_path" "$cpu_path" > "${out_base}.svg"; then
      return 0
    fi
    return 1
  }

  render_callgraph() {
    $pprof_cmd -svg "$bin_path" "$cpu_path" > "${out_base}.svg"
  }

  case "$PROFILE_FORMAT" in
    flamegraph)
      if ! render_flamegraph; then
        render_callgraph
      fi
      ;;
    svg)
      render_callgraph
      ;;
    both)
      render_callgraph
      if ! $pprof_cmd -flamegraph "$bin_path" "$cpu_path" > "${out_base}.flame.svg"; then
        echo "pprof does not support -flamegraph; install github.com/google/pprof for flamegraphs"
      fi
      ;;
    none)
      ;;
    *)
      echo "Unknown PROFILE_FORMAT=$PROFILE_FORMAT (use flamegraph|svg|both|none)"
      render_callgraph
      ;;
  esac

  if [[ "$PROFILE_TOP" == "1" ]]; then
    $pprof_cmd -top "$bin_path" "$cpu_path" > "${out_base}.top.txt"
  fi
}

write_profile_readme() {
  local out_dir="$1"
  cat <<'NOTE' > "$out_dir/PROFILE_README.txt"
Profile artifacts:
- cpu_*.pprof (CPU)
- heap_*.pprof (heap)
- trace_*.out (Go execution trace)
- cpu_*.top.txt (top CPU functions)

Render flamegraphs:
  go tool pprof -flamegraph wallaby-bench.bin cpu_*.pprof > cpu_*.svg
  # or install pprof binary: go install github.com/google/pprof@latest
  pprof -flamegraph wallaby-bench.bin cpu_*.pprof > cpu_*.svg

View trace:
  go tool trace trace_*.out
NOTE
}

for profile in $PROFILES_RAW; do
  for scenario in $SCENARIOS_RAW; do
    if [[ "$ENABLE_PROFILES" == "1" ]]; then
      for target in $(resolve_targets "$TARGETS"); do
        echo "Running profile=$profile scenario=$scenario target=$target (profiling)"
        cpu_path="$OUT_DIR/cpu_${profile}_${scenario}_${target}.pprof"
        mem_path="$OUT_DIR/heap_${profile}_${scenario}_${target}.pprof"
        trace_path="$OUT_DIR/trace_${profile}_${scenario}_${target}.out"
        (cd "$ROOT_DIR" && go run ./cmd/wallaby-bench -profile "$profile" -targets "$target" -scenario "$scenario" -output-dir "$OUT_DIR" -cpu-profile "$cpu_path" -mem-profile "$mem_path" -trace "$trace_path")
        if [[ "$PROFILE_RENDER" == "1" && "$PROFILE_FORMAT" != "none" ]]; then
          bench_bin="$OUT_DIR/wallaby-bench.bin"
          build_bench_bin "$bench_bin"
          render_profile "$bench_bin" "$cpu_path" "$OUT_DIR/cpu_${profile}_${scenario}_${target}"
        fi
      done
      write_profile_readme "$OUT_DIR"
    else
      echo "Running profile=$profile scenario=$scenario targets=$TARGETS"
      (cd "$ROOT_DIR" && go run ./cmd/wallaby-bench -profile "$profile" -targets "$TARGETS" -scenario "$scenario" -output-dir "$OUT_DIR")
    fi
  done
done

echo "Summary (table):"
(cd "$ROOT_DIR" && go run ./cmd/wallaby-bench-summary -dir "$OUT_DIR" -format table | tee "$OUT_DIR/summary.txt")
(cd "$ROOT_DIR" && go run ./cmd/wallaby-bench-summary -dir "$OUT_DIR" -format markdown > "$OUT_DIR/summary.md")

echo "Summary written to $OUT_DIR/summary.txt and $OUT_DIR/summary.md"
