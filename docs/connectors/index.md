# Connectors

A connector is an adapter at the data-path seam. It translates WALlaby batches to or from one external system. Connectors do not own flow lifecycle, dispatch, or checkpoint ordering. The generated [support matrix](../reference/generated/connector-support.md) is authoritative for support level and default delivery guarantees.

## Start with PostgreSQL

The supported source is PostgreSQL logical replication. The clearest first destination is another PostgreSQL table. A separate `pgstream` destination provides pull/ack queue semantics.

Read [PostgreSQL connectors](postgres.md) before using any of those three roles.

## Choose a destination by contract

| Need | Destination type | Contract to verify |
| --- | --- | --- |
| Replicate ordinary tables | `postgres` | Compatible target schema and stable keys |
| Pull and acknowledge messages | `pgstream` | Consumer group, visibility timeout, and acknowledgement handling |
| Publish ordered records | `kafka` or `bufstream` | Partition key, registry, and idempotent producer settings |
| Write objects | `s3` or `parquet` | Object naming, partitioning, and replay behavior |
| Call an application endpoint | `http` or `grpc` | Idempotency key and retry behavior |
| Load an analytical store | `snowflake`, `snowpipe`, `clickhouse`, `duckdb`, or `ducklake` | Type mapping, DDL behavior, and mutation support |

WALlaby includes these adapters, but destination adapters remain experimental until restart, replay, schema-evolution, and integration contracts all pass. Startup validation rejects lossy acknowledgement paths, unsafe primary acknowledgement, and automatic DDL execution through destinations that do not execute DDL.

## Flow shape

Every endpoint has a stable name, a type, and string-valued options:

```json
{
  "name": "orders-postgres",
  "type": "postgres",
  "options": {
    "dsn": "postgres://user:password@destination:5432/app?sslmode=require",
    "schema": "public"
  }
}
```

Stable destination names are required for primary acknowledgement because pending outbox rows address destinations by name.

## Extended adapter notes

[Extended connector notes](../connectors.md) cover Kafka, Snowflake, Snowpipe, DuckLake, HTTP, and S3. Those notes supplement, rather than replace, adapter tests and the generated Go contracts.

New connector and wire-format work is intentionally secondary to contract coverage for existing adapters.
