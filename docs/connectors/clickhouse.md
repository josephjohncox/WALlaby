# ClickHouse destination

WALlaby has two ClickHouse contracts. Do not treat them as equivalent.

| Contract | Support | Intended use |
| --- | --- | --- |
| `postgresql-to-clickhouse-append-v1` | maintained | Durable, append-only PostgreSQL CDC on the exact admitted profile below |
| Generic `clickhouse` modes | experimental | Mutation-based target tables, staging, and other legacy configurations |

Maintained status applies only to the named profile. Selecting `type: clickhouse` does not promote a generic ClickHouse configuration.

## Maintained profile boundary

`postgresql-to-clickhouse-append-v1` admits only this deployment:

- PostgreSQL 16 as the source;
- ClickHouse 25.12.1.649 on both replicas;
- one self-managed ClickHouse destination backed by ClickHouse Keeper 25.12.1.649;
- exactly two healthy ClickHouse replicas with stable, explicitly admitted names;
- native protocol connections with verified TLS;
- `ack=all`;
- one destination sink;
- at-least-once delivery;
- immutable append-only changelog and completion-receipt tables.

ClickHouse Cloud, every other ClickHouse patch, and every other PostgreSQL major are outside the maintained matrix until their exact versions and deployment semantics pass the same gates. The connector fails admission instead of extrapolating support.

## Delivery model

The destination stores a CDC event log, not a materialized copy of each PostgreSQL table.

For each source transaction, WALlaby:

1. validates the complete transaction and its immutable delivery intent;
2. appends transaction fragments in source order;
3. assigns stable record hashes, query IDs, and insert deduplication tokens;
4. inserts a completion receipt only after every fragment succeeds;
5. advances the PostgreSQL durable-delivery authority only after the receipt reconciles.

A matching receipt is the commit fact. If ClickHouse commits and the response is lost, WALlaby reads the receipt with `FINAL` and adopts the prior commit. If the receipt is absent, WALlaby retries the same logical batch. Stable replacing keys converge after ClickHouse's finite deduplication window expires, so correctness does not depend on that window retaining an old token.

Updates, key-changing updates, deletes, and tombstones remain immutable events. Each row records the operation, key, before image, after image, schema identity, source position, transaction identity, fragment ordinal, and record ordinal. PostgreSQL values are encoded in JSON envelopes to preserve values that do not map safely to a fixed ClickHouse column type.

Raw DDL execution is not part of this profile. Structured DDL barriers are appended to the changelog so schema order is recoverable without executing PostgreSQL SQL against ClickHouse.

## Required ClickHouse objects

Create a database, a changelog table, a receipt table, and a `FINAL` view before starting the flow. Admission compares the live definitions with the contract. The tables must:

- use `ReplicatedReplacingMergeTree` with exact Keeper paths and one of the two admitted replica names;
- have no partition key and no TTL;
- use the exact columns and ordering keys shown below;
- set explicit deduplication and part-pressure limits;
- report writable, non-expired replicas in `system.replicas`.

Replace the database, table, Keeper path, and replica values with stable deployment-specific names.

```sql
CREATE TABLE wallaby.wallaby_cdc_log
(
    flow_id String,
    flow_incarnation_id String,
    source_lineage_id String,
    destination_revision_id String,
    logical_batch_id String,
    content_hash FixedString(64),
    source_position String,
    transaction_id UInt64,
    begin_lsn String,
    commit_lsn String,
    end_lsn String,
    fragment_ordinal UInt64,
    record_ordinal UInt64,
    source_namespace String,
    source_table String,
    schema_version Int64,
    schema_fingerprint FixedString(64),
    schema_json String,
    operation LowCardinality(String),
    tombstone UInt8,
    key_json String,
    before_json String,
    after_json String,
    payload String,
    ddl_plan String,
    event_time DateTime64(9, 'UTC'),
    record_hash FixedString(64),
    wallaby_version UInt64
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/01/wallaby/wallaby_cdc_log',
    'clickhouse-1',
    wallaby_version
)
ORDER BY (destination_revision_id, logical_batch_id, fragment_ordinal, record_ordinal)
SETTINGS
    replicated_deduplication_window = 1000,
    replicated_deduplication_window_seconds = 3600,
    parts_to_delay_insert = 100,
    parts_to_throw_insert = 200,
    max_parts_in_total = 1000;

CREATE TABLE wallaby.wallaby_delivery_receipts
(
    flow_id String,
    flow_incarnation_id String,
    source_lineage_id String,
    destination_revision_id String,
    logical_batch_id String,
    content_hash FixedString(64),
    source_position String,
    transaction_id UInt64,
    fragment_count UInt64,
    record_count UInt64,
    query_ids Array(String),
    committed_at DateTime64(9, 'UTC'),
    wallaby_version UInt64,
    external_id String
)
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/01/wallaby/wallaby_delivery_receipts',
    'clickhouse-1',
    wallaby_version
)
ORDER BY (destination_revision_id, logical_batch_id)
SETTINGS
    replicated_deduplication_window = 1000,
    replicated_deduplication_window_seconds = 3600,
    parts_to_delay_insert = 100,
    parts_to_throw_insert = 200,
    max_parts_in_total = 1000;

CREATE VIEW wallaby.wallaby_cdc_log_final AS
SELECT * FROM wallaby.wallaby_cdc_log FINAL;
```

Create the same tables on the second server with replica name `clickhouse-2`; keep the Keeper paths identical. Do not change these definitions in place. Provision new named objects and change `destination_revision_id` when the destination contract changes.

## Destination configuration

```json
{
  "name": "analytics-clickhouse",
  "type": "clickhouse",
  "options": {
    "dsn": "clickhouse://wallaby:secret@clickhouse:9440/wallaby?secure=true",
    "managed_profile": "postgresql-to-clickhouse-append-v1",
    "destination_revision_id": "clickhouse-production-v1",
    "write_mode": "managed_append",
    "batch_mode": "target",
    "batch_resolution": "none",
    "meta_table_enabled": "false",
    "managed_deployment": "self-managed-keeper",
    "managed_database": "wallaby",
    "managed_changelog_table": "wallaby_cdc_log",
    "managed_receipts_table": "wallaby_delivery_receipts",
    "managed_final_view": "wallaby_cdc_log_final",
    "managed_keeper_path_prefix": "/clickhouse/tables/01",
    "managed_keeper_address": "keeper:9181",
    "managed_replica_dsn": "clickhouse://wallaby:secret@clickhouse-2:9440/wallaby?secure=true",
    "managed_replica_names": "clickhouse-1,clickhouse-2",
    "insert_quorum": "1",
    "async_insert": "false",
    "wait_for_async_insert": "true",
    "managed_max_active_parts": "180",
    "managed_max_transaction_rows": "100000",
    "managed_max_transaction_bytes": "134217728",
    "managed_max_transaction_fragments": "128",
    "managed_max_rows_per_batch": "10000",
    "managed_max_batch_bytes": "16777216",
    "tls_ca_file": "/etc/wallaby/tls/clickhouse-ca.pem"
  }
}
```

The flow must also use `ack=all` and exactly one sink. Managed admission rejects staging, metadata mutations, asynchronous inserts, a different insert quorum, or generic batch delivery.

### TLS

Set `secure=true` in the native DSN and provide a trusted CA. WALlaby enforces certificate verification and TLS 1.2 or newer.

```json
{
  "dsn": "clickhouse://wallaby:secret@clickhouse.example:9440/wallaby?secure=true",
  "tls_ca_file": "/etc/wallaby/tls/clickhouse-ca.pem",
  "tls_server_name": "clickhouse.example"
}
```

`managed_replica_dsn` is a second verified native TLS endpoint. The order of `managed_replica_names` is significant: the first name must be reported by `dsn`, and the second by `managed_replica_dsn`. If the second certificate needs a different hostname override, set `managed_replica_tls_server_name`.

For mutual TLS, set both `tls_cert_file` and `tls_key_file`. `skip_verify` is rejected.

## Resource and failure behavior

Admission checks transaction-wide fragment, row, and encoded-byte bounds before delivery. Planning also bounds every insert by rows and bytes. Before any write, WALlaby enforces `active_parts + planned_inserts <= managed_max_active_parts`. ClickHouse's own `parts_to_delay_insert`, `parts_to_throw_insert`, and `max_parts_in_total` settings remain the final server-side guardrails.

Keeper loss makes replicated tables read-only. WALlaby does not acknowledge during that interval. After Keeper returns and both managed replicas report writable, reconciliation resumes from the completion receipt.

The integration gates cover:

- the exact PostgreSQL 16 / ClickHouse 25.12.1.649 / Keeper 25.12.1.649 version pair;
- exact table, view, Keeper, two-replica topology, and replica-health admission;
- commit-before-response ambiguity;
- replay after deduplication-window eviction;
- fragment ordering and concurrent transactions;
- schema barriers and PostgreSQL value envelopes;
- forced ClickHouse and Keeper process replacement;
- active-part and planned-part backpressure;
- verified native TLS;
- bounded 1k, 10k, and 100k transaction query counts;
- PostgreSQL-authoritative receipt, checkpoint, ACK, and lease-takeover recovery;
- bounded telemetry attributes and metrics.

Run the profile gate with `just test-clickhouse-managed-profile`. The generated [connector support matrix](../reference/generated/connector-support.md) lists the executable gate names.

## ClickHouse semantics used by the profile

The contract follows ClickHouse's documented behavior:

- [insert deduplication](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries) is finite and token-based;
- [replicated table engines](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication) coordinate through Keeper;
- [asynchronous insert deduplication](https://clickhouse.com/docs/operations/settings/settings#async-insert-deduplicate) has separate behavior, so this profile disables asynchronous inserts;
- [MergeTree part settings](https://clickhouse.com/docs/operations/settings/merge-tree-settings) bound part pressure;
- [native protocol TLS](https://clickhouse.com/docs/guides/sre/configuring-ssl) requires a secure native port and verified certificates.
