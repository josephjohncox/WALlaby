# Examples

These examples are intended to stay current with the API surface and connector options. If you change gRPC messages, connector option keys, or flow lifecycle behavior, update the files in this folder in the same PR.

## Quick Start (API Server)
Set the minimum environment variables and launch the gRPC API server:

```bash
export DUCTSTREAM_POSTGRES_DSN="postgres://user:pass@localhost:5432/ductstream?sslmode=disable"
export DUCTSTREAM_GRPC_LISTEN=":8080"
export DUCTSTREAM_WIRE_FORMAT="arrow"
export DUCTSTREAM_WIRE_ENFORCE="true"
./bin/ductstream
```

## Create a Flow (gRPC)
Use `grpcurl` with local proto files (reflection is not enabled yet). See `examples/grpc/create_flow.sh` for a runnable command, or copy a flow spec from `examples/flows/`.

## Run a Flow Worker (Standalone)
Run a single flow in its own process (useful for Kubernetes deployments or per-flow scaling):

```bash
./bin/ductstream-worker -flow-id "<flow-id>" -max-empty-reads 1
```

`-max-empty-reads 1` tells the worker to stop when no changes are available, which is useful for periodic scheduling (DBOS or cron). Omit it for continuous streaming.

## DBOS Scheduling (Durable Runs)
Enable DBOS and optional scheduling to run flow batches durably:

```bash
export DUCTSTREAM_DBOS_ENABLED="true"
export DUCTSTREAM_DBOS_APP="ductstream"
export DUCTSTREAM_DBOS_QUEUE="ductstream"
export DUCTSTREAM_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
```

## DDL Gating + Approval
Enable DDL gating to require explicit approval before continuing:

```bash
export DUCTSTREAM_DDL_GATE="true"
export DUCTSTREAM_DDL_AUTO_APPROVE="false"
export DUCTSTREAM_DDL_AUTO_APPLY="false"
```

Use the DDLService to list and approve/reject DDL events (see `examples/grpc/ddl_approve.sh`).
Or use the CLI admin tool:

```bash
./bin/ductstream-admin ddl list -status pending
./bin/ductstream-admin ddl approve -id 1
./bin/ductstream-admin ddl apply -id 1
```

## Terraform Provider
See `examples/terraform/flow.tf` for a minimal provider + flow resource definition.

## Example Flow Specs
- `examples/flows/postgres_to_kafka.json`
- `examples/flows/postgres_to_s3_parquet.json`
