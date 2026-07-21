# Managed delivery semantics

Managed delivery is at-least-once with reconciliation. It is not exactly-once.

For the named PostgreSQL profile, one committed source transaction is the
logical batch. Its identity includes source lineage, XID, begin/commit/end LSNs,
ordered fragment ordinals, schemas, records, and the transaction-end
checkpoint. Worker generation identifies an attempt; it does not create a new
logical delivery.

For each destination revision, `internal/delivery.Coordinator` executes this
protocol:

1. Validate the complete source transaction and destination admission contract.
2. Persist the immutable logical batch manifest and numbered attempt under the
   current `RunFence` before target I/O.
3. Reconcile any unfinished attempt before replay.
4. Apply ordered target side effects and a deterministic logical-batch marker in
   one destination transaction.
5. Persist returned evidence and the terminal attempt state under the current
   fence.
6. Commit the receipt, authoritative checkpoint, and source ACK intent in one
   control-PostgreSQL transaction.
7. Validate the ACK grant, send monotonic source feedback without holding a
   control transaction open, and observe the exact slot
   `confirmed_flush_lsn`.
8. Revalidate the current fence before recording the flush receipt.

A target commit followed by a transport failure leaves an unfinished attempt.
A later owner queries the target marker before revalidating mutable target
schema. Matching lineage, logical batch, position, and content hash permits
receipt adoption; missing evidence permits a bounded retry. Reconciliation
errors and indeterminate responses persist their own bounded exponential
backoff. Transient indeterminate results leave the public flow `running` for a
later owner; exhausting the 16-attempt reconciliation budget fails closed for
operator recovery.

Normal and streamed pgoutput transactions preserve multi-table, multi-schema,
DDL, and control fragments in source order. The PostgreSQL target does not
coalesce repeated table fragments or move DML across structured DDL barriers.
Only the transaction-end LSN can advance the checkpoint.

Empty source transactions use the same fenced checkpoint and ACK-intent
transaction but need no destination receipt. Startup verifies any restored ACK
intent before source feedback. A crash after source flush but before the flush
receipt leaves a representable recovery state: the next owner re-sends the same
authorized checkpoint and records the observation. A new slot's consistent
point is persisted before destination startup, so a crash before the first
source transaction is recoverable.

The checkpoint carries delivered relation-schema baselines. Bootstrap seeds
them from the frozen snapshot manifest, and each finalized source transaction
updates them atomically with its checkpoint. A restarted decoder therefore
diffs its first pgoutput `Relation` message against delivered state rather than
an empty process cache.

The maintained `postgresql-to-postgresql-v1` profile admits one sink and
`ack_policy=all`. Generic runners retain both `all` and `primary` semantics:
`all` waits for every required destination, while `primary` atomically
checkpoints the primary and persists secondary outbox work. Those generic modes
remain experimental and do not inherit the named profile's support status.

Terminal delivery state is pruned only after observed source flush evidence.
PostgreSQL stores the retention root, and the current authoritative checkpoint
is never eligible for deletion. A long-running worker repeats bounded pruning
sweeps instead of pruning only at startup; saturated sweeps renew the producer
lease between batches and stop after a fixed batch budget. The PostgreSQL target retains the
current marker per flow incarnation and destination revision; older markers
are removed only when a later transaction commits, after the sequential
coordinator has already finalized the older control receipt.
