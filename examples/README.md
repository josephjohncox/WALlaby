# Examples

These examples are intended to stay current with the API surface and connector options. If you change gRPC messages, connector option keys, or flow lifecycle behavior, update the files in this folder in the same PR.

## Quick Start (API Server)
Set the minimum environment variables and launch the gRPC API server:

```bash
export WALLABY_POSTGRES_DSN="postgres://user:pass@localhost:5432/wallaby?sslmode=disable"
export WALLABY_GRPC_LISTEN=":8080"
export WALLABY_WIRE_FORMAT="arrow"
export WALLABY_WIRE_ENFORCE="true"
./bin/wallaby
```

## Create a Flow (gRPC)
Use `grpcurl` with local proto files (reflection is not enabled yet). See `examples/grpc/create_flow.sh` for a runnable command, or copy a flow spec from `examples/flows/`.

## Run a Flow Worker (Standalone)
Run a single flow in its own process (useful for Kubernetes deployments or per-flow scaling):

```bash
./bin/wallaby-worker -flow-id "<flow-id>" -max-empty-reads 1
```

`-max-empty-reads 1` tells the worker to stop when no changes are available, which is useful for periodic scheduling (DBOS or cron). Omit it for continuous streaming.
For backfill runs that land in staging tables, add `-resolve-staging` to apply staged data before the worker exits.

## DBOS Scheduling (Durable Runs)
Enable DBOS and optional scheduling to run flow batches durably:

```bash
export WALLABY_DBOS_ENABLED="true"
export WALLABY_DBOS_APP="wallaby"
export WALLABY_DBOS_QUEUE="wallaby"
export WALLABY_DBOS_SCHEDULE="*/10 * * * * *" # every 10 seconds
```

## DDL Gating + Approval
Enable DDL gating to require explicit approval before continuing:

```bash
export WALLABY_DDL_GATE="true"
export WALLABY_DDL_AUTO_APPROVE="false"
export WALLABY_DDL_AUTO_APPLY="false"
```

Use the DDLService to list and approve/reject DDL events (see `examples/grpc/ddl_approve.sh`).
Or use the CLI admin tool:

```bash
./bin/wallaby-admin ddl list -status pending
./bin/wallaby-admin ddl approve -id 1
./bin/wallaby-admin ddl apply -id 1
```

## Terraform Provider
See `examples/terraform/flow.tf` for a minimal provider + flow resource definition.

## Example Flow Specs
- `examples/flows/postgres_to_kafka.json`
- `examples/flows/postgres_to_s3_parquet.json`
- `examples/flows/postgres_to_http.json`
- `examples/flows/postgres_to_pgstream.json`
- `examples/flows/postgres_to_snowflake.json`
- `examples/flows/postgres_to_snowpipe.json`
- `examples/flows/postgres_to_duckdb.json`
- `examples/flows/postgres_to_clickhouse.json`
- `examples/flows/postgres_to_bufstream.json`

## Snowpipe Auto-Ingest (Upload Only)
Use the Snowpipe destination with external stage notifications. Set `auto_ingest=true` to skip COPY and only upload files:

```json
{
  "name": "snowpipe-out",
  "type": "snowpipe",
  "options": {
    "dsn": "user:pass@account/db/schema?role=SYSADMIN",
    "stage": "@my_external_stage",
    "format": "parquet",
    "auto_ingest": "true",
    "copy_on_write": "false"
  }
}
```

## Stream Consumer Example
- `examples/stream_consumer.sh` — minimal pull/ack loop using `wallaby-admin` + `jq`.
- `examples/stream_consumer.go` — minimal Go client (no external tools).
