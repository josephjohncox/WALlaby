# Iceberg changelog consumer

Status: **supported configuration preview; experimental service evidence**.

The PostgreSQL-to-canonical-Parquet-to-Iceberg configuration surface is versioned and tested. Promotion to maintained support remains blocked on same-commit AWS S3 Tables and commercial Snowflake readback evidence. See [Query WALlaby Iceberg tables from Snowflake](../guides/s3-tables-snowflake.md) for the documented S3 Tables configuration preview.

The Iceberg endpoint consumes `canonical_cdc_parquet_v1` publications after PostgreSQL commits them. It is not a direct destination. PostgreSQL owns delivery attempts, receipts, consumer checkpoints, quotas, and garbage-collection roots. The catalog owns Iceberg metadata. Ordinary versioned S3 retains canonical recovery objects.

## Data model

Each source table maps to an ordinary Iceberg table. Wallaby appends source inserts, updates, deletes, and loads as changelog rows. It does not apply updates or deletes to prior Iceberg rows.

Wallaby writes DDL barriers to a separate append-only control table before it writes data from the same publication. Data snapshots contain these summary properties:

- `wallaby.flow-id`
- `wallaby.logical-batch-id`
- `wallaby.manifest-sha256`
- `wallaby.projection-id`
- `wallaby.schema-fingerprint`
- `wallaby.commit-id`
- `wallaby.publication-id`
- `wallaby.projection-group-id`

A retry adopts a snapshot only when every identity property matches. A reused batch or commit ID with different content fails closed.

## Field identity and schema rules

The Iceberg catalog owns table field IDs. A real Apache Iceberg REST catalog replaces caller-supplied IDs with fresh sequential table field IDs on create and assigns new IDs to columns added by evolution. Wallaby never requires a catalog to preserve its hash-derived canonical field IDs.

Wallaby always rewrites `canonical_cdc_parquet_v1` objects. It first verifies the exact S3 `VersionId`, SHA-256, length, canonical schema fingerprint, row count, logical batch ID, source relation, operation, and record ordinals. It then loads, creates, or evolves the target table and builds an authoritative canonical-field-to-Iceberg-field mapping from the schema the catalog returns. The mapping is keyed on stable source identity (PostgreSQL relation and column identity), which survives supported renames; it validates names, types, requiredness, and nested paths, and fails closed on collisions. The rewritten data files carry the catalog-assigned `PARQUET:field_id` values and the target table's partition specification, so downstream readers resolve every column by the catalog's own field IDs.

Stable identity is persisted in each Iceberg field's doc as immutable commit metadata. A bounded SHA-256 mapping digest is recorded as audit evidence in the snapshot summary. A retry rebuilds and validates the authoritative mapping from the catalog instead of inferring renames by name; the audit digest does not independently authorize snapshot adoption.

Wallaby creates missing tables as unpartitioned Iceberg v2 tables and evolves existing tables additively; a new canonical column is added as a nullable Iceberg column and a supported rename is applied by stable identity. Existing partitioned tables are accepted only when every partition source field still resolves in the current table schema. Incompatible type changes and ambiguous renames fail closed. Schema evolution and data append are separate catalog commits. Recovery reloads the catalog-owned schema and reconciles the append by exact snapshot summary; consumers may briefly observe the evolved schema before the new data snapshot.

## Flow configuration

Use a managed PostgreSQL CDC source with `bootstrap=never` and disabled source-resource mutation options. Initial snapshot publication through the canonical artifact log is not implemented.

```yaml
source:
  type: postgres
  options:
    managed: "true"
    bootstrap: "never"
    create_slot: "false"
    ensure_state: "false"
    ensure_publication: "false"
    sync_publication: "false"
    source_system_identifier: system-1
    source_lineage_id: lineage-1
    publication_revision: revision-1

destinations:
  - name: lake
    type: iceberg
    options:
      destination_revision_id: iceberg-append-v1
      catalog_profile: rest
      namespace: analytics
      table_prefix: cdc_

config:
  ack_policy: materialized
  materialization:
    projection_id: canonical_cdc_parquet_v1
```

The flow endpoint selects only catalog profile, target mapping, and immutable destination revision identity. Deployment configuration owns URI, warehouse, REST prefix, region, S3 Tables bucket ARN, S3 FileIO endpoint/region, and behavior controls. Persisting any deployment-owned, unknown, or secret option is rejected before storage. Supply OAuth tokens, OAuth credentials, client certificates, CA data, and AWS credentials only through deployment configuration or environment variables.

## REST client

The production REST client supports:

- HTTPS by default, custom CA roots, mutual TLS, and server-name verification;
- static bearer tokens and same-origin OAuth client credentials over HTTPS;
- AWS Signature Version 4 with a configurable region and signing name;
- bounded dial, TLS handshake, response-header, and request timeouts; and
- optimistic commit retries after conclusive HTTP 409 conflicts.

HTTP requires `allow_http=true` and is intended only for local REST emulation.

Data and metadata objects are read and written through the catalog-reported object store. For S3-compatible storage such as MinIO in local emulation, set deployment `iceberg.s3_endpoint` and `iceberg.s3_region`; access and secret keys are secrets and come only from deployment configuration or the AWS environment. Authenticated REST requests are transport-bound to one deployment catalog origin; OAuth client credentials must use that same HTTPS origin. Catalog defaults, overrides, and table-level FileIO configuration are rejected if they change deployment URI/warehouse/prefix, S3 region, or the deployment-bound/standard AWS S3 endpoint, so a catalog response cannot redirect ambient AWS credentials.

## S3 Tables profile

Set deployment `iceberg.profile=s3tables`; the flow may repeat `catalog_profile=s3tables`. The client derives and signs `https://glue.<region>.amazonaws.com/iceberg` with signing name `glue`, and production admission rejects any other S3 Tables catalog endpoint. The warehouse uses the AWS form `<account-id>:s3tablescatalog/<table-bucket-name>`. Deployment must also set `iceberg.expected_aws_role_arn`; startup calls STS and rejects an active caller that is not that writer role before catalog recovery.

Wallaby calls the current S3 Tables APIs to inspect the table, snapshot-management configuration, compaction configuration, and maintenance job status. If configured, it writes snapshot-management and compaction settings before admission. Snapshot retention must cover the configured reconciliation horizon. A failed maintenance job blocks delivery.

S3 Tables compaction and snapshot expiration may replace or delete managed-table files. Wallaby never records those files as canonical artifact roots. Canonical objects remain in ordinary S3 under independent retention.

## Operational caveats

- **Orphaned managed-table files.** A crash between a data-file write and the catalog commit, and every optimistic-conflict retry, can leave Iceberg-managed Parquet in the table data directory. These files are never Wallaby garbage-collection roots and never become visible rows (only summary-matched snapshots are adopted), so there is no correctness or duplication impact. The REST + object-store profile has no built-in reclamation, so storage grows until catalog-side maintenance removes unreferenced files; Amazon S3 Tables can reclaim them through its separate table-bucket unreferenced-file-removal policy. Keep that AWS policy enabled and verify it independently; the snapshot-management and compaction settings WALlaby configures do not perform orphan-file removal.
- **Retryable catalog outages consume backlog before they stop source reads.** A classified transport timeout, temporary REST server failure, expired authorization, or exhausted optimistic-conflict retry leaves the publication undelivered and is retried in sequence. While PostgreSQL-authoritative artifact count, bytes, and age remain below their configured high-water marks, WALlaby may continue reading and rooting canonical source transactions. At any high-water mark it stops new source reads and retries the consumer until backlog falls. Outcomes `retry_deferred_below_watermark` and `retry_blocked_at_watermark` make this distinction observable.
- **Indeterminate delivery halts the consumer.** A conflicting or ambiguous snapshot summary yields an indeterminate reconciliation. This is fail-safe (no data loss), but the consumer halts on that publication, its delivery stays undelivered, retention keeps its canonical bytes, and sustained retention can apply publisher backpressure. The consumer emits the `wallaby.artifact.consumer.outcomes` metric with `outcome=indeterminate`; alert on a persistent indeterminate outcome and resolve the ambiguity operationally.
- **Startup availability coupling.** The catalog committer is constructed when the flow runtime starts. An unreachable catalog at worker start fails runtime construction, which also blocks the PostgreSQL-authoritative canonical publication for that flow. Ensure the catalog is reachable before starting affected workers.
- **Cross-table visibility is not atomic.** One PostgreSQL publication can create several Iceberg snapshots sequentially. Snowflake or another catalog reader can temporarily observe only part of a multi-table source transaction.
- **Consumer reconfiguration requires a new incarnation.** Adding or removing an Iceberg destination on a live incarnation fails closed against the consumer fingerprint pin. This is the correct safety posture: a removed consumer with pending deliveries would otherwise pin retention indefinitely. Reconfigure the consumer set through a new flow incarnation.

## Unsupported behavior

This profile does not provide:

- a current-state table;
- upserts, merges, equality deletes, or position deletes;
- exactly-once delivery;
- initial snapshot/backfill publication;
- arbitrary target DDL: only additive nullable columns and renames tracked by stable source identity are applied; incompatible type changes, drops, and ambiguous renames fail closed;
- direct reuse of canonical S3 objects;
- Wallaby workflow authority in an Iceberg catalog or S3 table bucket; or
- maintained support without the named live-service gates.

The integration harness provisions a real Apache Iceberg REST catalog backed by MinIO; the mandatory checkpoint gate runs `just test-checkpoint5-iceberg-integration`, where `TestIcebergRESTLiveAppendProjection` and `TestIcebergRESTLiveSchemaEvolutionRename` must run rather than skip. The rename gate proves that stable identity carried in each field's doc survives a real catalog's fresh field-ID reassignment, so an identity-tracked rename evolves the column in place instead of degrading to an add-column. Run the append test against operator-supplied services with `just test-iceberg-rest`. The AWS S3 Tables and Snowflake catalog-linked readback gates remain credential-gated; run S3 Tables with `just test-s3tables-live` and follow the same-commit observer procedure in the [Snowflake guide](../guides/s3-tables-snowflake.md). Neither an unrun gate nor a skipped credential cell is promotion evidence.
