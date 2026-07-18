# Delivery and checkpoints

The runner reads a batch, writes it to the destinations required by the acknowledgement policy, persists the durable checkpoint, and then acknowledges the source.

## Default policy

With `ack_policy=all`, every configured destination must accept the batch before WALlaby persists its checkpoint. A destination error leaves the source position unacknowledged so the batch can be retried.

## Primary policy

With `ack_policy=primary`, the named primary destination controls source progress. After the primary succeeds, WALlaby atomically persists the checkpoint and one durable outbox entry per secondary before acknowledging the source. It drains restored outbox entries before reading new source data and deletes each entry only after that destination accepts the batch.

Primary acknowledgement requires a SQLite or PostgreSQL checkpoint store that implements the atomic outbox; WALlaby fails closed without one. Secondary destination identities must remain configured until their entries are drained. Every destination write in primary mode must be idempotent: a crash can replay the primary after its write but before atomic persistence, or replay a secondary after its write but before durable outbox deletion.

## Durable ordering

When a checkpoint store is configured, WALlaby persists the delivered position before it acknowledges the source. If persistence fails, the source is not acknowledged. If source acknowledgement fails after persistence, restart restores the durable position and repeats the source acknowledgement.

Checkpoint stores reject position regressions. CDC positions use native PostgreSQL `pg_lsn` ordering. Backfill progress is not ordered as an LSN: it is tracked by snapshot task metadata, partition identity, cursors, completion markers, and control positions.

## Restore

At startup the runner reads the checkpoint once. A missing checkpoint means the flow is new. Any other read error stops startup. For CDC, a restored LSN becomes the source `start_lsn` unless the flow configuration already supplies one. Backfill resumes from its persistent snapshot task state and checkpoint metadata, including partition cursors and completed-task markers; it does not use PostgreSQL LSN ordering to decide snapshot progress.

After opening the source and destinations, the runner first drains any restored primary-ack outbox work and then acknowledges the restored checkpoint. These operations are intentionally idempotent.

## DDL execution receipts

Automatic DDL execution requires a durable registry receipt store. Before calling a destination, the runner validates any existing immutable destination manifest and checks for a receipt at `(flow, position, destination)`. The first preparation fixes the manifest before any downstream side effect. After the destination accepts both the DDL and its batch, WALlaby stores that destination's receipt. The registry changes the event to `applied` in the same transaction that records the final receipt. Checkpoint persistence follows receipt completion, so a crash before the checkpoint replays the batch but skips already-receipted DDL. A batch that spans source positions carries a position on each record; ambiguous duplicate DDL positions fail before destination execution.

Administrators may approve or reject DDL before execution preparation, but cannot change status once the immutable manifest exists, assert `applied`, or change a receipt-backed applied event. The deprecated `MarkDDLApplied` RPC fails with `FailedPrecondition`, and the CLI no longer exposes a manual apply command. Legacy applied rows without receipts fail closed during replay instead of causing unverified DDL re-execution.

A receipt cannot atomically cover an external database commit. A process failure after downstream DDL commits but before WALlaby stores the receipt can still replay that DDL. Destination-specific schema reconciliation remains required to close that final window; the support matrix therefore does not claim blanket exactly-once DDL execution.

## Verification

Runtime JSONL traces record delivery, checkpoint, acknowledgement, restore, and failure actions. `wallaby-trace-validate` partitions checks by flow ID and validates PostgreSQL LSN order. The TLA+ models separate delivery, persistence, source acknowledgement, persistence failure, crash, and restart.
