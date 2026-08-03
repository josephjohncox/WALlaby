# Durable core implementation report

## Status

This branch implements the maintained `postgresql-to-postgresql-v1` managed profile plus experimental generic connector and artifact-log primitives. The named profile is at-least-once with reconciliation; it does not claim exactly-once delivery.

The maintained profile fails closed outside its exact admission contract. Generic PostgreSQL modes, raw automatic DDL, generic staging, ClickHouse mutations, and the incomplete Iceberg path remain experimental.

## Implemented slices

### Authority and fencing

`internal/authority`, `internal/workflow`, and `internal/checkpoint` add immutable flow incarnations, generation-qualified producer acquisitions, lease epochs, work claims, and fenced checkpoints. Authoritative mutations validate the complete `RunFence` inside the same PostgreSQL transaction as the mutation.

Public lifecycle states remain unchanged: `created`, `running`, `paused`, `stopping`, `stopped`, and `failed`.

### PostgreSQL source transaction boundary

`internal/replication` handles pgoutput protocol v1 transactions and protocol v2 streamed transaction segments. It buffers every segment through commit and emits complete, ordered multi-table and multi-schema fragments with the transaction-end LSN, XID, ordinal, and final-fragment marker. Emitted records are released from the decoder buffer as ownership transfers to the source transaction assembler, and transaction byte limits include relation, type, and DDL metadata. DDL/control records remain ordered barriers. Received WAL is never used as a durable ACK position.

Authoritative checkpoints carry delivered relation-schema baselines. Bootstrap seeds them from its frozen manifest, and each finalized transaction advances them with the checkpoint. A replacement process can therefore detect schema changes from its first pgoutput `Relation` message after downtime.

The managed source rejects an existing slot without a PostgreSQL-authoritative checkpoint. It also rejects a checkpoint behind `confirmed_flush_lsn`, before retained `restart_lsn`, or beyond the server WAL end.

### Managed destination delivery

`internal/delivery` and `connectors/destinations/postgres` implement:

1. immutable destination revision registration with a configuration fingerprint;
2. durable manifest and append-only attempt preparation before external I/O;
3. source-order application of every table/schema fragment, destination-mapped structured DDL plan, metadata row, and deterministic logical-batch marker in one target transaction, with contiguous records batched instead of creating one temporary table per record;
4. external-commit reconciliation from that marker;
5. evidence adoption and terminal retry state under the current fence; and
6. one PostgreSQL transaction for the receipt, authoritative checkpoint, and source ACK intent.

The managed target requires explicit `synchronous_commit=on` or `remote_apply`. Omission, `off`, `local`, and `remote_write` are rejected.

The named profile validates the ACK grant before source feedback and revalidates the fence before committing the observed `confirmed_flush_lsn` as the ACK receipt. No control transaction or takeover lock spans source I/O. A crash after slot flush but before the receipt is repaired by reissuing the same authoritative checkpoint. A stale acquisition is rejected before feedback or at receipt recording.

Attempts use persisted numbering, bounded exponential backoff, and a 16-attempt ceiling. Terminal manifests, attempts, evidence, receipts, and old ACK rows are pruned only after observed flush evidence; the current checkpoint remains a PostgreSQL retention root. Long-running workers repeat fixed-budget sweeps and renew the producer lease between saturated batches. The target keeps the current reconciliation marker per flow incarnation and destination revision rather than accumulating markers indefinitely.

### Slot-anchored bootstrap

`internal/bootstrap` creates a bootstrap-generation-qualified logical slot with `EXPORT_SNAPSHOT` and retains the exporter connection while bounded tasks import that snapshot in read-only repeatable-read transactions. Task cursor/receipt updates are atomic. A replacement process cannot import the lost exporter's snapshot: exporter loss uses a retryable `abandoning` cleanup phase, starts a new exported snapshot generation and physical slot, and restarts every task from zero.

The production worker and in-process DBOS path now run these primitives for `bootstrap=auto|required`. A pre-slot relation barrier prevents DDL from crossing planning; the publication is created or exactly adopted before slot creation so it is visible at the decoding consistent point. Bounded table tasks import the slot snapshot, write generation-qualified PostgreSQL staging tables, and publish every table in one destination transaction. Exporter loss abandons all cursors and restarts from zero with a new physical slot. `bootstrap=never` remains an explicit pre-provisioned mode.

### Canonical artifact log

`internal/artifactlog` implements a bounded Arrow 18/Parquet v2 projection with microsecond timestamps, field IDs derived from source lineage plus PostgreSQL relation/column identity, separate logical and encoded hashes, quota reservation before upload, exact S3 `VersionId` and checksum verification, immutable publication roots, monotonic checkpoints, and ACK-intent coupling.

The package also provides claimed append-only catalog consumption and conservative deletion of old uploaded/verified objects that were never rooted. It is not wired into a worker. Reserved objects with no exact version evidence and published-artifact retention remain fail-closed remainders. No Iceberg REST client exists.

## PostgreSQL migrations

`internal/controlstore` owns the worker's shared control pool. All changed migration domains delegate to one checksum-verifying coordinator, import legacy workflow/checkpoint/registry history, and apply SQL plus history in one transaction under `wallaby_control_migrations`. Runtime pools set the `wallaby.authority_protocol=v2` session capability; workflow, checkpoint, registry, delivery, bootstrap, and artifact-log mutation tables have exact enabled v2 trigger coverage after the quiesced cutover. A separately ledgered controlplane repair promotes historical registry-only 006/007 histories, and central startup verifies required tables, columns, constraints, indexes, and triggers before workers start.

- `internal/workflow/migrations/006_authority_fences.sql`
  - flow incarnations, execution acquisitions, producer leases, and work claims;
  - incarnation provenance on lifecycle events and executions.
- `internal/checkpoint/migrations/003_authority_fencing.sql`
  - incarnation-scoped authoritative checkpoints and retained outbox completion.
- `internal/delivery/migrations/001_attempts_receipts.sql`
  - destination revisions, delivery manifests, attempts, evidence, receipts, ACK intents, and ACK receipts.
- `internal/bootstrap/migrations/001_bootstraps.sql` and `002_managed_bootstrap.sql`
  - bootstrap sessions, fenced multi-table tasks and delivery receipts, source-resource ownership/operations, and publication receipts.
- `internal/registry/migrations/006_run_fencing.sql`
  - complete-or-legacy DDL/catalog provenance, takeover-safe attempts, and schema-publication operations.
- `internal/delivery/migrations/002_authority_protocol.sql`, `004_logical_batches_retry_retention.sql`, and `006_rolling_logical_batch_compatibility.sql`
  - positive delivery/ACK provenance, stale-client protocol gates, logical batch identity, bounded retry state, retention roots, indexed logical attempts, and nullable additive identity columns so authority-v2 checkpoint-1 workers remain writable during a rolling upgrade.
- `internal/artifactlog/migrations/001_artifacts.sql`
  - streams, quotas, objects, upload attempts, GC claims, publications, and publication objects.
- `internal/artifactlog/migrations/002_consumers.sql` and `003_authority_protocol_v2.sql`
  - artifact delivery queues, attempts, and receipts; authority-v2 triggers cover canonical schemas, streams, objects, upload attempts, publications, publication objects, deliveries, quota accounts/reservations, GC claims, and delivery attempts/receipts.

## Runtime admission

The worker constructs workflow, checkpoint, authority, delivery, and registry repositories over one bounded control pool. `internal/runner` acquires a producer fence before opening connectors and renews it with the execution heartbeat. Managed executions no longer use the compatibility `flow_executions` finish API; lifecycle quiescence reads current producer leases directly.

The maintained profile admission requires:

- `managed_profile=postgresql-to-postgresql-v1` on both endpoints;
- matching PostgreSQL majors from 14 through 17 at both ends; mixed-major pairs remain unpromoted;
- a transactional source with observed flush evidence, `bootstrap=required`, and `streaming_transactions=true`;
- explicit source system, lineage, and publication revision identities;
- exactly one PostgreSQL destination revision and `ack_policy=all`;
- compatible target columns and a valid, non-partial, non-deferrable target primary/unique constraint over source identity columns;
- target write and batch modes plus explicit durable `synchronous_commit`; and
- no arbitrary `start_lsn`, legacy backfill, file/disabled snapshot authority, drop-slot failure mode, generic staging, raw DDL capture, or raw automatic DDL.

Generic managed modes remain experimental even when they pass their narrower startup checks.

## Executable evidence

The acceptance workflow requires the following gates; a gate is not evidence of a pass unless its command completed successfully in the reviewed revision:

- `just fmt-check` and `just lint`;
- `go test -count=1 ./...`;
- `just test-rapid` and `just test-durable-race`;
- `just test-durable-pr`;
- `just test-durable-integration` — requires every named live PostgreSQL/MinIO worker, bootstrap, and fencing test to run without skips;
- `just test-checkpoint2-postgres-profile` — CI runs the exact managed admission and evidence suite twice against each same-major PostgreSQL 14, 15, 16, and 17 profile;
- `just test-durable-dbos-integration` — requires the named in-process DBOS bootstrap test to run without a skip;
- `just check-tla` and `just spec-verify`;
- `just generate-check`; and
- `just docs-check`.

The process recovery test starts the built worker with `bootstrap=required`, proves an existing source row is atomically published before CDC, sends SIGKILL, expires the abandoned lease, starts a replacement process, reopens the generated logical slot at the authoritative checkpoint, and delivers a subsequent transaction. This test also covers the replay-stable PostgreSQL commit timestamp used by managed records.

## Deferred work

The following requested outcomes remain open and are not represented as maintained support:

- external schema-registry publication intents/receipts beyond the admitted PostgreSQL relation-diff DDL plans;
- a fenced administrative resource-revision workflow; legacy managed slot/publication mutation RPCs currently fail closed;
- reconciliation and cleanup for reserved artifact objects that lack exact-version evidence, plus published-artifact retention GC;
- an Iceberg REST catalog implementation and live catalog recovery tests;
- the append-only ClickHouse managed changelog connector;
- a 100-cycle process-kill chaos profile and long-running soak gate; and
- maintained profiles for any connector combination other than `postgresql-to-postgresql-v1`.

Those deferred connectors and modes remain experimental.
