# Connectors

A connector is an adapter at the data-path seam. It translates WALlaby batches to or from one external system. Connectors do not own flow lifecycle, dispatch, or checkpoint ordering. The generated [support matrix](../reference/generated/connector-support.md) is authoritative for support level and default delivery guarantees.

## Start with PostgreSQL

The implemented source is PostgreSQL logical replication. Its generic adapter remains experimental; maintained status belongs only to an explicitly promoted profile. The clearest first destination is another PostgreSQL table. A separate `pgstream` destination provides pull/ack queue semantics.

Read [PostgreSQL connectors](postgres.md) before using any of those three roles.

## Choose a destination by contract

| Need | Destination type | Contract to verify |
| --- | --- | --- |
| Replicate ordinary tables | `postgres` | Compatible target schema and stable keys |
| Pull and acknowledge messages | `pgstream` | Consumer group, visibility timeout, and acknowledgement handling |
| Publish ordered records | `kafka` or `redpanda` | Partition key, registry, and idempotent producer settings |
| Write objects | `s3` or `parquet` | Object naming, partitioning, and replay behavior |
| Call an application endpoint | `http` or `grpc` | Idempotency key and retry behavior |
| Load an analytical store | [`snowflake`](snowflake.md), `snowpipe`, [`clickhouse`](clickhouse.md), `duckdb`, or `ducklake` | Type mapping, DDL behavior, and mutation support |

Redpanda uses the Kafka API. Redpanda supports [Iceberg topics](https://docs.redpanda.com/streaming/current/manage/iceberg/about-iceberg-topics/) with an enterprise license. Configure Iceberg in Redpanda, not WALlaby.

WALlaby includes these adapters, but destination adapters remain experimental until their restart, replay, schema-evolution, and integration contracts pass. Maintained status applies only to rows marked maintained in the support matrix, including `postgresql-to-postgresql-v1` and `postgresql-to-clickhouse-append-v1`; it does not promote every mode of the underlying adapter. The implemented Snowflake SQL, staged COPY append, and Streaming append contracts are modeled protocol profiles, not supported-profile claims. SQL and staged COPY lack a reviewed Snowflake service version/deployment cell with complete same-SHA live evidence. Streaming also lacks a linked reviewed append transport and fails closed before external I/O. Startup validation rejects lossy acknowledgement paths, unsafe primary acknowledgement, and automatic DDL execution through destinations that do not execute DDL.

## Flow shape

Every endpoint has a stable name and exactly one typed configuration branch. Native booleans, integers, lists, maps, durations, and enums are preserved:

```json
{
  "name": "orders-postgres",
  "postgres_destination": {
    "connection": {
      "dsn": "postgres://user:password@destination:5432/app?sslmode=require"
    },
    "synchronous_commit": "on"
  }
}
```

`custom` is the only branch with a free-form option map. Its `connector_type` must be registered for the endpoint role in both API and worker processes.

Stable destination names are required for primary acknowledgement because pending outbox rows address destinations by name. Logical table and column targets and write behavior belong only to the mandatory mapping for this destination name.

## Extended adapter notes

The [Snowpipe guide](snowpipe.md) documents its append-only staged-delivery and failure contract. [Extended connector notes](../connectors.md) cover Kafka, Snowflake, DuckLake, HTTP, and S3. Those notes supplement, rather than replace, adapter tests and the generated Go contracts.

New connector and wire-format work is intentionally secondary to contract coverage for existing adapters.
