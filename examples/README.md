# Examples

These examples are intended to stay current with the API surface and connector options. Every flow has complete destination-scoped table mappings and an explicit write policy. `TestShippedFlowExamplesStrictLoadValidateAndUseCurrentMappings` strictly loads and validates every JSON/YAML flow example and rejects removed logical endpoint options. If you change gRPC messages, connector option keys, mappings, or lifecycle behavior, update the files in this folder in the same PR.

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
Use `grpcurl` with local proto files. If you enable reflection (`WALLABY_GRPC_REFLECTION=true`), you can omit `-proto`. See `examples/grpc/create_flow.sh` for a runnable command, or copy a flow spec from `examples/flows/`.

## Run a Flow Worker (Standalone)
Run a single flow in its own process (useful for Kubernetes deployments or per-flow scaling):

```bash
./bin/wallaby-worker --flow-id "<flow-id>" --max-empty-reads 1
```

`--max-empty-reads 1` tells the worker to stop when no changes are available, which is useful for periodic scheduling (DBOS or cron). Omit it for continuous streaming.
For backfill runs that land in staging tables, add `--resolve-staging` to apply staged data before the worker exits.

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

Use the DDLService to list and approve/reject DDL events (see `examples/grpc/ddl_approve.sh`), or inspect and approve them with the supported CLI commands:

```bash
./bin/wallaby-admin ddl list --status pending
./bin/wallaby-admin ddl show --id 1
./bin/wallaby-admin ddl approve --id 1
```

Approval records the control-plane decision. The running flow's data plane applies approved DDL when automatic DDL execution is enabled; there is no separate administrative apply subcommand.

## Terraform Provider
See `examples/terraform/flow.tf` for a minimal provider + flow resource definition.

## Example Flow Specs
- `examples/flows/postgres_to_kafka.json`
- `examples/flows/postgres_to_kafka_http_primary.json`
- `examples/flows/postgres_to_s3_parquet.json`
- `examples/flows/postgres_to_http.json`
- `examples/flows/postgres_to_http_toast_full.json`
- `examples/flows/postgres_to_grpc.json`
- `examples/flows/postgres_to_pgstream.json`
- `examples/flows/postgres_to_snowflake.json`
- `examples/flows/postgres_to_snowpipe.json`
- `examples/flows/postgres_to_duckdb.json`
- `examples/flows/postgres_to_ducklake.json`
- `examples/flows/postgres_to_clickhouse.json`
- `examples/flows/postgres_to_bufstream.json`

## Snowpipe Auto-Ingest (Upload Only)
Use the Snowpipe destination with real external-stage notifications. Set `auto_ingest=true` to skip COPY and only upload files. Upload failures are returned unchanged, and target tables change only through configured COPY or external pipe ingestion:

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
- `examples/workflows/` — CLI/DBOS/Kubernetes workflow configs.
