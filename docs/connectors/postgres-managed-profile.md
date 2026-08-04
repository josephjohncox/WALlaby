# PostgreSQL managed profile

## Support status

`postgresql-to-postgresql-v1` is the maintained managed profile. The generic
PostgreSQL source and destination modes remain **experimental**. Maintained
status applies only when startup admits the exact profile and the generated
real-service gates pass for the reviewed revision.

The profile provides at-least-once delivery with durable reconciliation. It
does not claim exactly-once delivery.

## Exact admission contract

The source and destination must both set:

```json
{
  "managed_profile": "postgresql-to-postgresql-v1"
}
```

On the source, the named profile implies managed execution; the older
`managed=true` switch is accepted but is not required. A profile declaration
can never fall through to the generic runner.

The admitted configuration is deliberately narrow:

- PostgreSQL 14, 15, 16, or 17 at both ends, with matching source and
  destination majors; mixed-major pairs remain unpromoted;
- one PostgreSQL source and exactly one PostgreSQL destination revision;
- `ack_policy=all`;
- `bootstrap=required` and PostgreSQL-backed snapshot state;
- `streaming_transactions=true`;
- target write and batch modes;
- explicit `synchronous_commit=on` or `remote_apply`;
- a stable source system identifier, source lineage, publication revision, and
  destination revision;
- source primary/replica identity columns matched by a valid, non-partial,
  non-deferrable target primary or unique constraint usable by `ON CONFLICT`;
  and
- compatible target columns, types, nullability, generated-column status, and
  required defaults before snapshot publication or CDC side effects.

Admission rejects arbitrary `start_lsn`, legacy backfill, file/disabled
snapshot authority, generic staging, drop-slot failure mode, raw DDL capture,
automatic raw SQL DDL, unsupported PostgreSQL versions, multiple sinks, and
`ack_policy=primary`. Those modes remain experimental even when the underlying
connectors are usable.

Managed `UpdateFlow` and `ReconfigureFlow` are both rejected, including name and
parallelism changes. For any change, stop the old flow, create and validate a
replacement with a new flow ID, publication revision, and destination revision,
start and verify it, cut over, then delete and clean up the old flow only when safe.
Every Terraform update fails; Terraform does not perform this lifecycle.

## Transaction and recovery protocol

The source uses pgoutput protocol v2 and buffers both normal and streamed
transactions through the PostgreSQL commit record. One `SourceTransaction`
retains contiguous fragment ordinals across tables, schemas, structured DDL,
and control barriers. Only `TransactionEndLSN` can become a checkpoint.

For each transaction, the delivery coordinator:

1. validates the full transaction and target schema contract;
2. persists the immutable logical batch manifest and numbered destination
   attempt under the current `RunFence`;
3. applies every fragment in source order and writes a deterministic Wallaby
   logical-batch marker in the same target transaction;
4. reconciles an ambiguous target commit from that marker after restart;
5. records evidence and a terminal attempt state;
6. commits the destination receipt, authoritative checkpoint, and source ACK
   intent in one fenced control-PostgreSQL transaction;
7. validates the ACK grant, sends monotonic standby feedback without holding a
   control transaction open, and observes the exact slot
   `confirmed_flush_lsn`; and
8. revalidates the fence before recording the source-flush receipt.

A crash after the target commit but before the control receipt reuses the target
marker. A missing marker permits a numbered retry after persisted exponential
backoff. New side-effect attempts and reconciliation attempts each have a
persisted 16-attempt limit. Transient indeterminate evidence leaves the public
flow `running` for a later owner; exhausted reconciliation fails the flow for
operator recovery rather than restarting without bound.

The target protocol preserves operation order. It does not coalesce a
multi-table transaction, reorder repeated table fragments, or move DML across a
structured DDL barrier. Contiguous records for one target are applied as a
batch, so a large source transaction does not create a temporary table per
record. Metadata schema changes use the active target transaction and therefore
work with an admitted one-connection pool. Relation-diff DDL plans resolve the
configured destination schema and table before executing in the same target
transaction as dependent DML. Raw SQL DDL and unsupported controls fail before
checkpoint advancement.

## Bootstrap and source feedback

The profile creates or exactly adopts the publication under the current fence,
creates a logical slot with `EXPORT_SNAPSHOT`, keeps the exporter session open,
and imports the snapshot in bounded worker transactions. The destination
publishes all snapshot stages atomically before CDC opens at the consistent
point.

Source feedback is not inferred from an in-process call. The coordinator first
validates the ACK grant and fence, the replication connection sends the
authorized LSN, and the source catalog reports the exact
`confirmed_flush_lsn`. It then revalidates the fence before storing the receipt.
No control transaction or takeover lock spans source network I/O. A crash or
takeover after the source flush but before its receipt is repaired by re-sending
the same authoritative checkpoint; the slot flush is monotonic. A stale
acquisition is rejected before feedback or at receipt recording.

The authoritative checkpoint also carries the last delivered schema for each
source relation. Bootstrap handoff seeds those baselines from the frozen
snapshot manifest. On restart, the first pgoutput `Relation` message is diffed
against that delivered baseline, so an `ALTER TABLE` committed while the worker
was down still produces ordered structured DDL before dependent DML.

## Retention and observability

Terminal attempts, evidence, receipts, manifests, and old ACK records become
eligible for bounded pruning only after observed source flush evidence exists.
The current authoritative checkpoint is stored as a PostgreSQL retention root
and is never pruned. Workers prune at startup and periodically while streaming.
Each invocation is capped at eight 1,000-row accounting batches, renews the
producer lease between saturated batches, and leaves any remainder for the next
sweep. The default retention window is seven days and the default
sweep interval is one minute; set `delivery_retention` or
`delivery_prune_interval` to positive Go durations. The target keeps only the
latest deterministic marker per flow incarnation and destination revision:
committing a later source transaction proves the previous transaction already
has an authoritative control receipt, so its target marker is no longer a
reconciliation root.

Logical-batch identity is an additive rolling migration. Control-domain
migration files commit under one coordinator transaction, and the new columns
remain nullable for authority-v2 checkpoint-1 writers. Current workers adopt a
matching legacy manifest or target marker to the new logical identity before
continuing; partial unique indexes enforce new identities without rejecting an
old writer that omits the column.

Managed delivery emits bounded outcome metrics for attempt preparation,
receipt commit/reuse, indeterminate results, and apply failures. Runner traces
include `deliver`, `checkpoint`, `source_flush`, and `ack` in that order; the
trace validator rejects ACKs without flush evidence on managed traces.

## Executable promotion matrix

The generated [connector support matrix](../reference/generated/connector-support.md)
contains the exact PostgreSQL versions, executable test, and whether each gate
requires a real service. Schema evolution, DDL reconciliation,
snapshot-to-CDC handoff, process kill, pool exhaustion, restart,
retry/retention, and upgrade migrations run against PostgreSQL; the bounded
metric-label contract is deterministic SDK evidence. CI runs
`just test-checkpoint2-postgres-profile` against every admitted PostgreSQL
major twice and rejects missing or skipped named tests. Removing or disabling a
gate makes the maintained profile declaration invalid.
