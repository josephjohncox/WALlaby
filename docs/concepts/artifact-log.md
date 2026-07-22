# Canonical artifact log

`internal/artifactlog` is the PostgreSQL-authoritative implementation behind the experimental `ack_policy=materialized` contract. The first and only admitted projection is `canonical_cdc_parquet_v1` for managed PostgreSQL CDC.

`materialized` means that immutable canonical objects and one generation-fenced PostgreSQL publication/checkpoint transaction are durable. It does **not** mean that downstream tables have committed the batch. The materializing worker does not open the configured destination on the CDC path after bootstrap and registers no production catalog consumer, so it also creates no destination delivery queue entries. The package-level queue and catalog seam remain available to explicit experimental callers. The public contract is canonical publication only and remains experimental.

## Ordering

For each committed source transaction, the worker:

1. restores quota and backlog state before opening the CDC source;
2. checks byte, batch-count, and age high-water marks before every read;
3. validates the complete `SourceTransaction` and every contained batch;
4. preserves ordered schema fragments and records DDL as PostgreSQL barriers rather than Parquet changelog rows;
5. assigns the deterministic `LogicalBatchID`, schema fingerprint, record ordinals, partition, and shard IDs;
6. encodes bounded `canonical_cdc_parquet_v1` objects before destination transforms or retries;
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

The delivery queue and `Catalog` seam remain package-level experiments and are not registered by the production worker. No production Iceberg REST or S3 Tables consumer is claimed by this checkpoint. S3 bytes are quota-bounded, but PostgreSQL artifact/publication/barrier/attempt history is not yet pruned; only GC claims and released-byte accounting are bounded today. Operators must monitor control-database growth, and this is another reason the profile is not maintained.

## Worker configuration

A materialized flow must set:

```yaml
config:
  ack_policy: materialized
  materialization:
    projection_id: canonical_cdc_parquet_v1
```

The worker deployment supplies ordinary-S3 and operational settings under `artifacts.*` or the matching `WALLABY_ARTIFACT_*` / `WALLABY_WORKER_ARTIFACT_*` environment variables: bucket, region, endpoint, credentials, path style, retained-byte limit, backlog batch/byte/age limits, poll interval, orphan grace, retention, and GC interval. Credentials are deployment secrets and are never persisted in flow configuration or publication metadata.
