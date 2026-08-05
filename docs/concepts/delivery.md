# Delivery and checkpoints

The runner reads a batch, writes it to the destinations required by the acknowledgement policy, persists the durable checkpoint, and then acknowledges the source.

## Default policy

With `ack_policy=all`, every configured destination must accept the batch before WALlaby persists its checkpoint. A destination error leaves the source position unacknowledged so the batch can be retried.

A single declared, non-lossy destination may use this policy without replay safety; that is an explicit at-least-once mode, so a crash after downstream commit but before checkpoint persistence can duplicate delivery. Multi-destination `all` fan-out is rejected unless every destination is replay-safe and idempotent, because partial success otherwise amplifies replay across sinks. `ack_policy=primary` also requires replay-safe, idempotent destinations.

## Primary policy

With `ack_policy=primary`, the named primary destination controls source progress. After the primary succeeds, WALlaby atomically persists the checkpoint and one durable outbox entry per secondary before acknowledging the source. It drains restored outbox entries before reading new source data and deletes each entry only after that destination accepts the batch.

Primary acknowledgement requires a SQLite or PostgreSQL checkpoint store that implements the atomic outbox; WALlaby fails closed without one. Secondary destination identities must remain configured until their entries are drained. Every destination write in primary mode must be idempotent: a crash can replay the primary after its write but before atomic persistence, or replay a secondary after its write but before durable outbox deletion.

## Durable ordering

When a checkpoint store is configured, WALlaby persists the delivered position before it acknowledges the source. If persistence fails, the source is not acknowledged. If source acknowledgement fails after persistence, restart restores the durable position and repeats the source acknowledgement.

Checkpoint stores reject position regressions. CDC positions use native PostgreSQL `pg_lsn` ordering. Backfill progress is not ordered as an LSN: it is tracked by snapshot task metadata, partition identity, cursors, completion markers, and control positions.

A positionless, recordless polling heartbeat is ignored. An empty batch with an observed WAL position is not a heartbeat: it represents filtered or metadata-only WAL and is persisted and acknowledged. PostgreSQL emits this progress even when `emit_empty_batches` is disabled; that option controls only positionless polling heartbeats.

## Restore

At startup the runner reads the checkpoint once. A missing checkpoint means the flow is new. Any other read error stops startup. For CDC, a restored LSN becomes the source `start_lsn` unless the flow configuration already supplies one. Backfill resumes from its persistent snapshot task state and checkpoint metadata, including partition cursors and completed-task markers; it does not use PostgreSQL LSN ordering to decide snapshot progress.

After opening the source and destinations, the runner first drains any restored primary-ack outbox work and then acknowledges the restored checkpoint. These operations are intentionally idempotent.

## Managed delivery identity

Every managed delivery, including snapshot bootstrap batches, carries the exact current `logical_batch_id` derived as SHA-256 over immutable source lineage, source position, and logical content with NUL separators. PostgreSQL attempts, receipts, and manifests use that identity directly; runtime code never substitutes a position-derived identity or adopts a receipt through an older position-only lookup.

The current delivery migration recomputes that canonical identity with PostgreSQL's built-in SHA-256 function and rejects NULL, empty, malformed, case-variant, arbitrary, `legacy:`-prefixed, or ambiguous values. It performs no inference or backfill and requires no `pgcrypto` extension. Recreate incompatible delivery state under a new flow incarnation instead of attempting an in-place rolling upgrade.

Managed PostgreSQL target receipt tables are created at the exact current schema and subsequently verified without runtime ALTER or adoption behavior. Reconciliation locks and checks both durable unique identities—the logical batch and source position—and rejects any different immutable tuple or content. Exact receipts are adopted. Receipts are immutable recovery evidence and are not pruned merely because a later batch commits; deletion requires a separate PostgreSQL-authoritative lifecycle proof.

## DDL execution receipts

Automatic DDL execution requires a durable registry receipt store and a destination reconciler. A session-scoped advisory lock serializes each flow/destination DDL stream across manifest preparation, downstream side effects, batch writes, and receipt persistence. A dedicated, bounded lock pool keeps those long-lived lock sessions from starving the registry transaction pool. Before calling a destination, the runner fixes the immutable destination manifest and commits an execution attempt. A new attempt may execute DDL. A complete attempt skips it. An incomplete attempt means a prior process crashed after preparation, so the next lock owner asks the destination to classify the schema as applied, not applied, or indeterminate. Applied work receives the missing receipt without replay; absent work may execute; indeterminate state fails closed. After the destination accepts both the DDL and its batch, WALlaby stores the receipt. The registry changes the event to `applied` in the same transaction that records the final receipt. Checkpoint persistence follows receipt completion. A batch that spans source positions carries a position on each record; ambiguous duplicate DDL positions fail before destination execution.

Administrators may approve or reject DDL before execution preparation, but cannot change status once the immutable manifest exists, assert `applied`, or change a receipt-backed applied event. The API exposes no administrative applied transition, and the CLI exposes no manual apply command. Existing applied rows without receipts fail closed during replay instead of causing unverified DDL re-execution.

PostgreSQL reconciles structured `DDLPlan` changes through catalog inspection, including table and column create/drop, type/nullability changes, renames, and generated-column state. Raw SQL, malformed plans, unsupported operations, and conflicting partial schemas remain indeterminate and fail closed. Destinations without a reconciler are rejected when automatic DDL is enabled. WALlaby does not claim blanket exactly-once DDL execution: each destination needs equivalent schema inspection and conflict detection before it can safely enable this path.

## Verification

Runtime JSONL traces record delivery, checkpoint, acknowledgement, restore, and failure actions. `wallaby-trace-validate` partitions checks by flow ID and validates PostgreSQL LSN order. The TLA+ models separate delivery, persistence, source acknowledgement, persistence failure, crash, and restart.
