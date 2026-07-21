# Managed delivery semantics

Managed delivery is at-least-once with reconciliation. It is not exactly-once.

For each PostgreSQL destination revision, `internal/delivery.Coordinator` executes this protocol:

1. Persist an immutable manifest and append-only attempt under the current `RunFence`.
2. Reconcile any unfinished attempt before replay.
3. Apply the external side effect outside the authority transaction.
4. Persist returned evidence under the current fence.
5. Adopt matching evidence as an immutable receipt.
6. Commit the receipt, authoritative checkpoint, and source ACK intent in one PostgreSQL transaction.
7. Give the authorized position to the source adapter and record that the adapter accepted it for feedback. `observed_flush_lsn` remains null unless the adapter can prove an externally observed flush position.

A transport failure after target commit leaves an unfinished attempt. An indeterminate result does not move the public flow to `failed`; the producer exits, the flow stays `running`, and a later producer reconciles the target marker. PostgreSQL adopts the marker only when source lineage, position, and content hash match. Missing or conflicting evidence fails closed.

Before any restored feedback is scheduled, the runner verifies that the current authoritative checkpoint and matching ACK intent exist in PostgreSQL. A new slot's consistent point is persisted with an ACK intent before destination startup, so a crash before the first source transaction remains recoverable.

Empty source transactions use the same fenced checkpoint and ACK-intent transaction, but require no destination receipt. Reusing a `destination_revision_id` with a changed name or configuration fingerprint is a delivery conflict.

The initial runtime profile admits one PostgreSQL target and one table/schema fragment per source transaction. Multi-table atomic target transactions remain deferred.
