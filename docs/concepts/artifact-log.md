# Canonical artifact log

`internal/artifactlog` is an experimental PostgreSQL-authoritative publication primitive. It is not wired into managed execution.

A committed source transaction is encoded with a frozen Arrow 18/Parquet v2 projection. PostgreSQL stores source-lineage and transaction identity, stable field IDs, logical and encoded hashes, exact byte length, bucket, immutable key, `VersionId`, S3 checksum, quota reservation, publication root, and consumer backlog.

Publication follows this order:

1. Validate that the checkpoint equals the transaction-end LSN.
2. Enforce transaction-wide record, fragment, nesting, input-byte, and encoded-byte limits.
3. Encode locally and reserve exact bytes in PostgreSQL.
4. Upload to an immutable versioned key.
5. Verify the exact version's S3 checksum, projection metadata, and length.
6. Commit roots, quota conversion, consumer deliveries, a monotonic checkpoint, and the ACK intent together.

A replay must match the persisted source transaction, artifact graph, checkpoint, and ACK intent. The publisher revalidates every rooted exact version before returning an ACK grant.

S3 never owns progress. No mutable `latest` object exists. Version listing can discover evidence after an ambiguous upload; PostgreSQL decides whether to adopt it.

A PostgreSQL claim serializes each catalog delivery. The collector can delete old uploaded or verified objects that were never rooted. It cannot yet release a reserved object that lacks exact-version evidence, and it never deletes a rooted publication. The hard retained-byte limit is fail-stop containment, not a retention strategy.
