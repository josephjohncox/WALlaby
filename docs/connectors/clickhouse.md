# ClickHouse connector status

Status: **experimental**.

The existing connector uses asynchronous mutations and does not provide the managed reconciliation contract. Managed admission rejects ClickHouse rather than treating mutation completion or a separate metadata mutation as a durable receipt.

Maintained support requires real-service evidence for ambiguous commits, deduplication-window expiry, partial batches, key-changing updates and tombstones, replicated Keeper behavior, DDL recovery, type round trips, process restarts, TLS/auth, bounded query counts, and telemetry correlation. Existing mutation and staging tests do not satisfy those gates.
