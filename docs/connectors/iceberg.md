# Iceberg connector status

Status: **experimental seam only**.

The artifact log exposes an append-only `Catalog` interface. A consumer claims rooted publications in PostgreSQL sequence order, persists an attempt before catalog I/O, and records the resulting snapshot evidence in a PostgreSQL receipt.

A catalog implementation must reconcile an ambiguous commit by exact publication ID and content hash. Missing evidence remains indeterminate.

S3 Tables must implement this Iceberg catalog interface. It must not expose managed-table files as Wallaby GC roots.

No REST catalog client, S3 Tables adapter, current-state table, equality delete, merge, or compaction implementation is included in this slice.
