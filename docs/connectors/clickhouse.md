# ClickHouse destination

WALlaby has two ClickHouse contracts. Do not treat them as equivalent.

| Contract | Support | Intended use |
| --- | --- | --- |
| `postgresql-to-clickhouse-append-v1` | maintained | Durable, append-only PostgreSQL CDC on the exact admitted profile below |
| Generic `clickhouse` modes | experimental | Mutation-based target tables, staging, and other legacy configurations |

Maintained status applies only to the `clickhouse_postgres_append` typed branch. Selecting the generic `clickhouse` branch does not promote a generic ClickHouse configuration.

## Maintained profile boundary

`postgresql-to-clickhouse-append-v1` admits only this deployment:

- PostgreSQL 16 as the source;
- ClickHouse 25.12.1.649 on both replicas;
- one self-managed ClickHouse destination backed by ClickHouse Keeper 25.12.1.649;
- exactly two healthy ClickHouse replicas with stable, explicitly admitted names;
- native protocol connections with verified TLS;
- `config.ack_policy=all`;
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

Create the same tables on the second server with replica name `clickhouse-2`; keep the Keeper paths identical. Do not change these definitions in place. Provision new named objects and change `destination_revision_id` when the destination contract changes. Managed `UpdateFlow` and `ReconfigureFlow` are both rejected, including name and parallelism changes. Stop the old flow, create/validate/start a replacement with a new flow ID and revision, cut over, and delete the old flow only when safe. Every Terraform update fails; Terraform does not perform this lifecycle.

## Destination configuration

```json
{
  "name": "analytics-clickhouse",
  "clickhouse_postgres_append": {
    "dsn": "clickhouse://wallaby:secret@clickhouse:9440/wallaby?secure=true",
    "destination_revision_id": "clickhouse-production-v1",
    "database": "wallaby",
    "changelog_table": "wallaby_cdc_log",
    "receipts_table": "wallaby_delivery_receipts",
    "final_view": "wallaby_cdc_log_final",
    "keeper_path_prefix": "/clickhouse/tables/01",
    "keeper_address": "keeper:9181",
    "replica_dsn": "clickhouse://wallaby:secret@clickhouse-2:9440/wallaby?secure=true",
    "replica_names": ["clickhouse-1", "clickhouse-2"],
    "insert_quorum": 2,
    "max_active_parts": 180,
    "max_transaction_rows": 100000,
    "max_transaction_bytes": 134217728,
    "max_transaction_fragments": 128,
    "max_rows_per_batch": 10000,
    "max_batch_bytes": 16777216,
    "tls": {
      "ca_file": "/etc/wallaby/tls/clickhouse-ca.pem"
    }
  }
}
```

The flow must also use `config.ack_policy=all` and exactly one sink. Managed admission requires `insert_quorum=2`, so every fragment and its completion receipt reach both admitted replicas before source acknowledgement. It rejects staging, metadata mutations, asynchronous inserts, a different insert quorum, or generic batch delivery.

### TLS

Set `secure=true` in the native DSN and provide a trusted CA. WALlaby enforces certificate verification and TLS 1.2 or newer.

```json
{
  "dsn": "clickhouse://wallaby:secret@clickhouse.example:9440/wallaby?secure=true",
  "tls": {
    "ca_file": "/etc/wallaby/tls/clickhouse-ca.pem",
    "server_name": "clickhouse.example"
  }
}
```

`replica_dsn` is a second verified native TLS endpoint. The order of `replica_names` is significant: the first name must be reported by `dsn`, and the second by `replica_dsn`. If the second certificate needs a different hostname override, set `tls.replica_server_name`.

For mutual TLS, set both `tls.certificate_file` and `tls.private_key_file`. Certificate verification cannot be disabled.

## Resource and failure behavior

Admission checks transaction-wide fragment, row, and encoded-byte bounds before delivery. Planning also bounds every insert by rows and bytes. PostgreSQL first takes the destination-revision budget lock; only while that lock is held does WALlaby require fresh reads from both endpoints, zero replication/Keeper queue work, and the maximum changelog-plus-receipt active-part count. It then atomically reserves every planned changelog insert plus the receipt insert and enforces `server_active_parts + charged_parts + planned_parts <= max_active_parts`. A missing endpoint or non-quiescent replication state rejects new admission. Concurrent coordinators therefore cannot reuse a stale observation or consume the same remaining capacity.

A deterministic reservation is bound to the destination revision, source lineage, immutable logical batch, source position, content hash, complete ordered part-plan hash, and stable insert query IDs. Before each fragment or receipt insert, PostgreSQL holds the same budget lock across destination reconciliation, any irreversible insert, and the progress commit. Retries compare each exact fragment on both endpoints before deciding to skip or insert, so convergence does not depend on ClickHouse retaining an old deduplication token. Endpoint failover preserves an already admitted reservation, but it does not permit a new reservation without both endpoints.

Receipt finalization changes the reservation to `completed_pending_observation`; it does not immediately subtract the charge. A later fresh, locked, two-endpoint quiescent observation proves that the durable parts are included in the server count before releasing the charge. Absent-batch reclaim is a versioned two-phase takeover: only a demonstrably superseding producer fence can commit `reclaim_pending`, which blocks stale pre-write guards, and a second locked phase releases the exact reclaim epoch only after both endpoints again prove quiescence and absence. Released exact identities may be re-reserved only after another fresh absence observation and an audited reservation-epoch increment. ClickHouse's own `parts_to_delay_insert`, `parts_to_throw_insert`, and `max_parts_in_total` settings remain the final server-side guardrails.

Metrics expose gauges `wallaby.clickhouse.managed.parts.server_active`, `wallaby.clickhouse.managed.parts.reserved`, and `wallaby.clickhouse.managed.parts.capacity`, plus `wallaby.clickhouse.managed.parts.rejected` with a bounded rejection reason. Revision and logical-batch identities remain trace-only and are never metric labels.

Keeper loss makes replicated tables read-only. WALlaby does not acknowledge during that interval. After Keeper returns and both managed replicas report writable, reconciliation resumes from the completion receipt.

If the primary client endpoint has a transport failure while both ClickHouse replicas remain healthy, WALlaby retries the immutable fragment or receipt through the typed `replica_dsn` with the same deduplication token; `insert_quorum=2` still requires both server-side replicas. Reconciliation reads both admitted endpoints so a matching receipt or immutable conflict on either replica dominates an absent peer.

After primary process and storage loss, startup may admit the intact second replica in **recovery-only** mode when its TLS identity, table/view definitions, Keeper path, registered two-replica identity, and local health still satisfy the profile. Recovery-only mode can adopt an already replicated completion receipt but rejects every new transaction with a recoverable indeterminate result. Restore a healthy two-replica topology and reopen the destination before writes resume. WALlaby does not lower quorum or acknowledge a one-replica write.

The integration gates cover:

- the exact PostgreSQL 16 / ClickHouse 25.12.1.649 / Keeper 25.12.1.649 version pair;
- exact table, view, Keeper, two-replica topology, and replica-health admission;
- commit-before-response ambiguity;
- replay after deduplication-window eviction;
- fragment ordering and concurrent transactions;
- schema barriers and PostgreSQL value envelopes;
- forced ClickHouse and Keeper process replacement;
- primary client-endpoint write/reconciliation failover while quorum two remains healthy;
- survivor-only receipt recovery after destructive primary storage loss, with new writes fenced until the primary is rebuilt;
- barrier-driven concurrent active-part reservations and crash recovery after reservation, fragment, and receipt progress;
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
