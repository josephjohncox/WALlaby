# Examples

These examples are intended to stay current with the API surface and connector options. Every flow has complete destination-scoped table mappings and an explicit write policy. `TestShippedFlowExamplesStrictLoadValidateAndUseCurrentMappings` exhaustively manifests every JSON/YAML file in `examples/flows` and also covers `examples/quickstart/postgres-to-postgres.json`; each is strictly loaded and validated through the production admin loader and protobuf conversion path with current mapping/destination checks. The gRPC example gate extracts the quoted JSON heredoc from `examples/grpc/create_flow.sh`, strictly decodes it as `CreateFlowRequest`, and runs production protobuf conversion and flow validation without executing the shell or contacting services. Connector and table-mapping tests also load the typed examples below through the production option parsers, type-mapping loader, mapping validator, and compiled projector. If you change gRPC messages, connector option keys, mappings, or lifecycle behavior, update the files in this folder in the same PR.

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
- `examples/flows/postgres_to_http_typed.yaml` — record-JSON HTTP delivery with typed retry/timing options, CSV-quoted headers, root-relative type mappings, and a flow-relative table-mapping import.
- `examples/flows/postgres_to_grpc.json`
- `examples/flows/postgres_to_grpc_typed.json` — record-JSON gRPC delivery to an explicit insecure localhost development endpoint with typed retry/timing options, CSV-quoted metadata, inline type mappings, and component-local Go templates.
- `examples/flows/postgres_to_pgstream.json`
- `examples/flows/postgres_to_snowflake.json`
- `examples/flows/postgres_to_snowpipe.json`
- `examples/flows/postgres_to_duckdb.json`
- `examples/flows/postgres_to_ducklake.json`
- `examples/flows/postgres_to_clickhouse.json`
- `examples/flows/postgres_to_redpanda.json`

Supporting executable configuration:

- `examples/mappings/http_typed.yaml` — version 2 HTTP destination projection using append writes and component-local schema, table, and column Go templates.
- `examples/type-mappings/web.yaml` — canonical PostgreSQL-to-web/wire type overrides used by the typed HTTP flow.

The shipped-example tests mutate in-memory copies to prove unknown flow and protobuf fields, malformed typed options and type maps, mapping version 1, wrong mapping destinations, legacy or foreign template fields, and other invalid contracts are rejected without checking malformed examples into the repository.

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
