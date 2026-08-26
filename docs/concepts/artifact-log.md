# Canonical artifact log

`internal/artifactlog` is the PostgreSQL-authoritative implementation behind the experimental `ack_policy=materialized` contract. `canonical_cdc_parquet_v1` remains byte-for-byte frozen. Current mapped Iceberg flows explicitly use `canonical_cdc_parquet_v2`, bound to the destination mapping fingerprint, source lineage, and already-mapped relation identity. V2 publication IDs are deterministic over the durable projection domain, so a pre-publication crash reproduces the same ID.

`materialized` means that immutable canonical objects and one generation-fenced PostgreSQL publication/checkpoint transaction are durable. It does **not** mean that downstream tables have committed the batch. A configured Iceberg endpoint registers a restartable asynchronous consumer and delivery queue; other materialized destinations retain canonical-publication-only behavior. Source acknowledgement never waits for an Iceberg commit. The complete contract remains experimental.

## Ordering

For each committed source transaction, the worker:

1. restores quota and backlog state before opening the CDC source;
2. checks byte, batch-count, and age high-water marks before every read;
3. validates the complete `SourceTransaction` and every contained batch;
4. preserves ordered schema fragments and records DDL as PostgreSQL barriers rather than Parquet changelog rows;
5. assigns the deterministic `LogicalBatchID`, schema fingerprint, record ordinals, partition, and shard IDs;
6. projects the source transaction exactly once and encodes bounded `canonical_cdc_parquet_v2` objects before reservation, upload, or catalog delivery;
7. commits exact upload intents and quota reservations in PostgreSQL;
8. performs conditional single-part S3 PUTs and reconciles exact `VersionId`, SHA-256, projection metadata, and length;
9. commits the publication root, barrier references, delivery rows, quota conversion, monotonic checkpoint, and source ACK intent in one fenced PostgreSQL transaction; and
10. sends source feedback only after that transaction commits.

A crash after S3 upload but before PostgreSQL publication leaves an orphan, never an acknowledged position. A crash after the PostgreSQL commit but before source feedback reuses the existing publication and ACK intent. Before feedback, a data-free startup checkpoint is also assigned a deterministic object-free publication so restored and bootstrap cuts follow the same publication-authority rule. Generation is attempt metadata; it never changes the logical batch or artifact identity.

## Canonical identity and layout

`LogicalBatchID` is derived from source lineage, source position, and the type-sensitive logical transaction hash. `ArtifactID` includes the projection, schema fingerprint, source identity, namespace, table, partition specification/value, shard, and logical batch identity. Neither includes the worker generation.

Version 1 is explicitly unpartitioned (`partition=unpartitioned`) but retains the partition dimension in object identity and layout. Objects target approximately 32 MiB and fail closed above the 64 MiB single-object limit. That limit is not a worker-memory bound: planning retains the source transaction and final encoded shards, and recursive split attempts can temporarily hold additional Arrow/Parquet buffers. No RSS promotion gate exists yet, so deployment memory must be measured against representative incompressible transactions. Paths are split by source, namespace, table, schema, partition, and shard. Every data row carries its source position, transaction-wide record ordinal, logical batch ID, and sorted unchanged-column markers in the canonical envelope. DDL records are ordered PostgreSQL barriers and are never encoded as ordinary data rows. The experimental catalog seam receives rooted object metadata and barriers together, with record ordinals for deterministic merge order; barrier-only publications remain consumable.

## Authority, recovery, and retention

PostgreSQL stores upload intents, exact object evidence, publication sequence and roots, delivery rows and receipts, quota reservations and usage, checkpoints, ACK intents/receipts, GC epochs, and GC claims. S3 stores immutable object versions only. There is no mutable `latest` object and S3 listing never establishes progress.

Recovery recomputes quota from PostgreSQL before source or bootstrap reads, resumes reserved/uploaded/verified intents, reopens deterministic artifacts that an orphan sweep conclusively deleted, and reconciles ambiguous PUTs against exact versions. Prepared PUT attempts remain PostgreSQL roots across takeover until replay either adopts exact-version evidence or supersedes the stale attempt; GC never turns one not-found listing into permission to forget a possibly in-flight PUT. A wrong checksum, length, projection, or reused identity fails closed. A restored checkpoint containing an artifact publication ID is not feedback proof by itself: the worker revalidates its publication, ACK intent, active roots, source-ACK retention root, and every exact S3 version before opening the source.

Garbage collection is epoch-based mark/sweep:

- unpublished reserved/uploaded/verified objects become orphan candidates after the configured grace period;
- a reserved intent with a prepared PUT and no version evidence remains quota-charged until source replay reconciles it; one absence observation cannot rule out an old-fence PUT that is still in flight after takeover;
- rooted objects become retention candidates only after an observed source ACK receipt, every delivery receipt, the retention period, and a newer authoritative checkpoint;
- the mark transaction removes the active root and records the fenced claim before S3 deletion;
- delivery pruning preserves the source ACK receipt while any artifact publication retains an unreleased source-ACK root;
- finalization revalidates the claim and safety predicates before releasing quota and the source-ACK retention root; and
- publication locks reject an object carrying any GC claim, so a paused publisher cannot root an object after the sweeper marks it.

The production worker registers the request-oriented `ChangelogCommitter` seam only for an Iceberg endpoint. It persists a deterministic commit ID before catalog I/O, rewrites canonical v1 objects, uses optimistic Iceberg commits, reconciles exact snapshot summaries after ambiguous responses, and advances a PostgreSQL consumer checkpoint with the immutable receipt. S3 Tables implements the same seam through the AWS Glue Iceberg REST endpoint and current S3 Tables maintenance APIs.

PostgreSQL metadata retention is separately bounded. A durable prune claim is created only after every object root and ACK-retention root is released, deliveries and canonical receipts are terminal, source and consumer checkpoints have moved to successors, no GC or delivery claim is live, exact attempt/receipt/publication identities agree, and the metadata horizon has elapsed. Each sweep enforces independent publication and deleted-row caps and revalidates the run fence and every authority predicate while holding row locks. Current checkpoint publications, pending deliveries, active roots/claims, and unreceipted prepared or indeterminate catalog attempts remain retained. A deferred claim receives a future retry time so it cannot permanently occupy every bounded scan slot.

Partial sweeps may remove released roots, barriers, terminal object rows, and unreferenced canonical schemas. Catalog deliveries, attempts, and receipts remain intact while their publication survives; they are removed only with the publication and prune claim in one final transaction whose entire row count fits the remaining sweep budget. After that commit, historical DDL barriers, object-version evidence, commit IDs, snapshot receipts, and immutable-conflict evidence are permanently discarded and unavailable for forensic replay. Current checkpoint recovery and replay remain supported; arbitrarily old replay from pruned PostgreSQL metadata is intentionally unsupported.

## Worker configuration

A materialized flow must set:

```yaml
config:
  ack_policy: materialized
  materialization:
    projection_id: canonical_cdc_parquet_v2
```

The worker deployment supplies ordinary-S3 and operational settings under `artifacts.*` or the matching `WALLABY_ARTIFACT_*` / `WALLABY_WORKER_ARTIFACT_*` environment variables: bucket, region, endpoint, credentials, path style, retained-byte limit, backlog batch/byte/age limits, poll interval, orphan grace, object retention, metadata retention, metadata publication/row sweep limits, and GC interval. `metadata_retention` defaults to 168 hours, `metadata_max_publications` to 100, and `metadata_max_rows` to 1000. The row limit must be at least 3; a final publication bundle larger than the remaining budget is deferred intact to a later sweep. Credentials are deployment secrets and are never persisted in flow configuration or publication metadata.
