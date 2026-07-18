# Replicate your first table

This tutorial runs WALlaby locally and replicates one PostgreSQL table into another PostgreSQL database. You will start the control plane, create a flow, run one worker, write a row, and verify the destination.

The tutorial uses one Docker container with three databases:

- `wallaby` stores flow state and checkpoints.
- `source` contains the table that WALlaby reads.
- `destination` contains the table that WALlaby writes.

Nothing in this tutorial uses DBOS or Kubernetes. Those runtimes come after the local data path works.

## Prerequisites

Install:

- Go matching the version in `go.mod`
- Docker with Compose v2
- Buf
- `jq`

Run every command from the repository root.

## 1. Start PostgreSQL

```bash
docker compose -f examples/quickstart/docker-compose.yml up -d --wait

cleanup_quickstart() {
  kill "${WALLABY_WORKER_PID:-}" 2>/dev/null || true
  kill "${WALLABY_SERVER_PID:-}" 2>/dev/null || true
  docker compose -f examples/quickstart/docker-compose.yml down -v
}
trap cleanup_quickstart EXIT
```

The Compose file enables logical replication and creates the three databases and both `public.orders` tables. Verify PostgreSQL before continuing:

```bash
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
  psql -U wallaby -d source -Atc "SHOW wal_level"
```

Expected output:

```text
logical
```

## 2. Build WALlaby

```bash
make proto
mkdir -p bin
go build -o bin/wallaby ./cmd/wallaby
go build -o bin/wallaby-admin ./cmd/wallaby-admin
go build -o bin/wallaby-worker ./cmd/wallaby-worker
```

## 3. Start the control plane

```bash
export WALLABY_ENV=dev
export WALLABY_WORKFLOW_STORE=postgres
export WALLABY_POSTGRES_DSN='postgres://wallaby:wallaby@localhost:55432/wallaby?sslmode=disable'
export WALLABY_GRPC_LISTEN='127.0.0.1:8080'
export WALLABY_GRPC_REFLECTION=true

./bin/wallaby > /tmp/wallaby-server.log 2>&1 &
export WALLABY_SERVER_PID=$!
```

Wait until the command endpoint responds:

```bash
attempt=0
ready=false
while [ "$attempt" -lt 30 ]; do
  if ./bin/wallaby-admin flow list >/dev/null 2>&1; then
    ready=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$ready" != true ]; then
  tail -50 /tmp/wallaby-server.log
  exit 1
fi
```

The loop stops after 30 seconds and prints the server log when startup fails.

## 4. Validate and create the flow

The checked-in flow definition scopes the publication to `public.orders`. It also asks WALlaby to create the publication and replication slot when they do not exist.

```bash
./bin/wallaby-admin flow validate \
  --file examples/quickstart/postgres-to-postgres.json
```

Expected output includes `valid`.

Create and start the flow, then capture its generated ID:

```bash
export FLOW_ID=$(
  ./bin/wallaby-admin flow create \
    --file examples/quickstart/postgres-to-postgres.json \
    --start \
    --json | jq -r '.id'
)

test -n "$FLOW_ID" && test "$FLOW_ID" != "null"
echo "$FLOW_ID"
```

Verify the durable lifecycle state:

```bash
./bin/wallaby-admin flow wait \
  --flow-id "$FLOW_ID" \
  --state running \
  --timeout 30s
```

## 5. Run the worker

The local runtime does not launch workers for you. Start one process for this flow:

```bash
./bin/wallaby-worker --flow-id "$FLOW_ID" \
  > /tmp/wallaby-worker.log 2>&1 &
export WALLABY_WORKER_PID=$!
```

The worker registers against the flow's current lifecycle generation. A worker from an older generation cannot continue after pause, resume, or stop.

## 6. Write and verify a row

Insert one row into the source:

```bash
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
  psql -U wallaby -d source -v ON_ERROR_STOP=1 -c \
  "INSERT INTO public.orders (id, customer, total_cents) VALUES (1, 'Ada', 2599)"
```

Wait for the destination row:

```bash
attempt=0
replicated=false
while [ "$attempt" -lt 60 ]; do
  count=$(docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
    psql -U wallaby -d destination -Atc \
    "SELECT count(*) FROM public.orders WHERE id = 1")
  if [ "$count" = "1" ]; then
    replicated=true
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
if [ "$replicated" != true ]; then
  tail -80 /tmp/wallaby-worker.log
  exit 1
fi
```

Read the replicated values:

```bash
docker compose -f examples/quickstart/docker-compose.yml exec -T postgres \
  psql -U wallaby -d destination -c \
  "SELECT id, customer, total_cents FROM public.orders WHERE id = 1"
```

Expected row:

```text
 id | customer | total_cents
----+----------+-------------
  1 | Ada      |        2599
```

## 7. Stop and clean up

Stop is terminal. It fences new work, waits for the worker to become quiescent, and then reports `stopped`.

```bash
./bin/wallaby-admin flow stop --flow-id "$FLOW_ID"
./bin/wallaby-admin flow wait \
  --flow-id "$FLOW_ID" \
  --state stopped \
  --timeout 30s
```

Remove the source slot and source-state row, then delete the stopped flow:

```bash
./bin/wallaby-admin flow cleanup --flow-id "$FLOW_ID"
./bin/wallaby-admin flow delete --flow-id "$FLOW_ID"
```

Stop the local processes and remove the databases:

```bash
cleanup_quickstart
trap - EXIT
```

You now have the complete core path: PostgreSQL logical replication → worker → destination write → durable checkpoint → source acknowledgement.

Next:

- [Understand the core model](../concepts/core-model.md)
- [Manage flow lifecycle](../guides/flows.md)
- [Choose a runtime](../deployment/index.md)
- [Configure PostgreSQL connectors](../connectors/postgres.md)
