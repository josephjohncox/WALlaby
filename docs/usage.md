# Usage

This guide covers how to run DuctStream and operate flows using the gRPC API, worker mode, and DBOS scheduling.

## Prerequisites
- PostgreSQL with logical replication enabled.
- A replication slot + publication for the source database.
- Destinations (Kafka, S3) reachable from the DuctStream process.

## API Server
Start the gRPC API server:

```bash
export DUCTSTREAM_POSTGRES_DSN="postgres://user:pass@localhost:5432/ductstream?sslmode=disable"
export DUCTSTREAM_GRPC_LISTEN=":8080"
export DUCTSTREAM_WIRE_FORMAT="arrow"
export DUCTSTREAM_WIRE_ENFORCE="true"
./bin/ductstream
```

### Create a Flow (gRPC)
Use `grpcurl` with local proto files:

```bash
examples/grpc/create_flow.sh
```

Flow definitions can also be copied from `examples/flows/*.json`.

## Worker Mode (Per-Flow Process)
Run a single flow in its own process. This is recommended for Kubernetes or when you want isolated scaling per flow.

```bash
./bin/ductstream-worker -flow-id "<flow-id>"
```

Add `-max-empty-reads 1` to stop after one empty poll. This is useful for periodic/scheduled runs.

## DBOS Scheduling
Enable DBOS and (optionally) a schedule to run flows as durable jobs:

```bash
export DUCTSTREAM_DBOS_ENABLED="true"
export DUCTSTREAM_DBOS_APP="ductstream"
export DUCTSTREAM_DBOS_QUEUE="ductstream"
export DUCTSTREAM_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
```

If `DUCTSTREAM_DBOS_SCHEDULE` is set, DBOS enqueues one run for each flow in `running` state.

## Wire Formats
DuctStream supports multiple wire formats. Set the default at the service level or per-flow:

```bash
export DUCTSTREAM_WIRE_FORMAT="arrow"   # arrow | parquet | avro | proto | json
export DUCTSTREAM_WIRE_ENFORCE="true"
```

Per-flow overrides are supported via `flow.wire_format` or connector `options.format`.

## DDL Governance
Enable gating to require approval before applying DDL-derived schema changes:

```bash
export DUCTSTREAM_DDL_GATE="true"
export DUCTSTREAM_DDL_AUTO_APPROVE="false"
export DUCTSTREAM_DDL_AUTO_APPLY="false"
```

Use the admin CLI to list and approve DDL events:

```bash
./bin/ductstream-admin ddl list -status pending
./bin/ductstream-admin ddl approve -id 1
./bin/ductstream-admin ddl apply -id 1
```

## Checkpointing
Checkpoints are stored in Postgres and are used to resume streams from the last confirmed LSN. If a checkpoint exists, the runner sets `start_lsn` on the source before opening the replication stream.
