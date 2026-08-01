# Durable core implementation report

## Status

This branch implements an experimental PostgreSQL-to-PostgreSQL managed CDC slice plus bootstrap and artifact-log primitives. It does not establish maintained support or exactly-once delivery.

The implementation fails closed outside that profile. In particular, it does not admit multi-fragment source transactions, raw automatic DDL, generic staging, ClickHouse mutations, or an Iceberg REST catalog.

## Implemented slices

### Authority and fencing

`internal/authority`, `internal/workflow`, and `internal/checkpoint` add immutable flow incarnations, generation-qualified producer acquisitions, lease epochs, work claims, and fenced checkpoints. Authoritative mutations validate the complete `RunFence` inside the same PostgreSQL transaction as the mutation.

Public lifecycle states remain unchanged: `created`, `running`, `paused`, `stopping`, `stopped`, and `failed`.

### PostgreSQL source transaction boundary

`internal/replication` buffers pgoutput changes from `BEGIN` through `COMMIT`. Every emitted fragment carries the transaction-end LSN, XID, ordinal, and final-fragment marker. Received WAL is never used as a durable ACK position.

The managed source rejects an existing slot without a PostgreSQL-authoritative checkpoint. It also rejects a checkpoint behind `confirmed_flush_lsn`, before retained `restart_lsn`, or beyond the server WAL end.

### Managed destination delivery

`internal/delivery` and `connectors/destinations/postgres` implement:

1. immutable destination revision registration with a configuration fingerprint;
2. durable manifest and append-only attempt preparation before external I/O;
3. target DML, metadata, and a deterministic delivery marker in one target transaction;
4. external-commit reconciliation from that marker;
5. evidence adoption under the current fence; and
6. one PostgreSQL transaction for the receipt, authoritative checkpoint, and source ACK intent.

The managed target requires explicit `synchronous_commit=on` or `remote_apply`. Omission, `off`, `local`, and `remote_write` are rejected.

An ACK receipt means the source adapter accepted an authorized position for feedback. `observed_flush_lsn` remains null unless an adapter can provide externally observed evidence. A restart reissues the authoritative checkpoint and repairs this receipt.

### Slot-anchored bootstrap

`internal/bootstrap` creates a bootstrap-generation-qualified logical slot with `EXPORT_SNAPSHOT`, retains the exporter connection, and lets replacement workers import the same snapshot in read-only repeatable-read transactions. Task cursor/receipt updates are atomic. Publication requires at least one completed task receipt, and handoff locks the persisted slot, manifest, source identity, and consistent LSN. Exporter loss uses a retryable `abandoning` cleanup phase and a new physical generation on restart.

These primitives are not wired into `wallaby-worker`. Managed admission accepts only `bootstrap=never`; `auto` and `required` fail closed.

### Canonical artifact log

`internal/artifactlog` implements a bounded Arrow 18/Parquet v2 projection with microsecond timestamps, field IDs derived from source lineage plus PostgreSQL relation/column identity, separate logical and encoded hashes, quota reservation before upload, exact S3 `VersionId` and checksum verification, immutable publication roots, monotonic checkpoints, and ACK-intent coupling.

The package also provides claimed append-only catalog consumption and conservative deletion of old uploaded/verified objects that were never rooted. It is not wired into a worker. Reserved objects with no exact version evidence and published-artifact retention remain fail-closed remainders. No Iceberg REST client exists.

## PostgreSQL migrations

`internal/controlstore` owns the worker's shared control pool. All changed migration domains delegate to one checksum-verifying coordinator, import legacy workflow/checkpoint/registry history, and apply SQL plus history in one transaction under `wallaby_control_migrations`. The pool also sets the `wallaby.authority_protocol=v1` session capability; workflow, checkpoint, and registry triggers reject pre-authority clients after the quiesced cutover.

- `internal/workflow/migrations/006_authority_fences.sql`
  - flow incarnations, execution acquisitions, producer leases, and work claims;
  - incarnation provenance on lifecycle events and executions.
- `internal/checkpoint/migrations/003_authority_fencing.sql`
  - incarnation-scoped authoritative checkpoints and retained outbox completion.
- `internal/delivery/migrations/001_attempts_receipts.sql`
  - destination revisions, delivery manifests, attempts, evidence, receipts, ACK intents, and ACK receipts.
- `internal/bootstrap/migrations/001_bootstraps.sql`
  - bootstrap sessions, snapshot tasks, and publication receipts.
- `internal/artifactlog/migrations/001_artifacts.sql`
  - streams, quotas, objects, upload attempts, GC claims, publications, and publication objects.
- `internal/artifactlog/migrations/002_consumers.sql`
  - artifact delivery queues, attempts, and receipts.

## Runtime admission

The worker constructs workflow, checkpoint, authority, delivery, and registry repositories over one bounded control pool. `internal/runner` acquires a producer fence before opening connectors and renews it with the execution heartbeat. Managed executions no longer use the compatibility `flow_executions` finish API; lifecycle quiescence reads current producer leases directly.

Managed admission currently requires:

- PostgreSQL transactional source with `managed=true`, `bootstrap=never`, `ensure_publication=false`, and `ensure_state=false`;
- explicit source system, lineage, and publication revision identities;
- one PostgreSQL destination revision;
- `ack_policy=all`;
- target write and batch modes;
- explicit durable `synchronous_commit`;
- no arbitrary `start_lsn`, legacy backfill, managed bootstrap, source publication/state mutation, file snapshot authority, drop-slot failure mode, generic staging, or raw automatic DDL; and
- one table/schema fragment per source transaction.

## Executable evidence

The following gates passed on this branch:

- `go test -count=1 ./...`;
- `just lint` — zero golangci-lint issues;
- `go test -race` across the changed replication, source, target, runner, stream, artifact, and bootstrap packages;
- `just test-durable-pr`;
- `just test-durable-integration` — 17 required live PostgreSQL/MinIO tests ran with no skips, including a built `wallaby-worker` process kill and replacement;
- `just check-tla`; the managed-durability model explored 32,028 distinct states with all configured invariants satisfied;
- `just spec-verify`;
- `just generate-check`; and
- `just docs-check`.

The process recovery test sends SIGKILL to the built worker, expires its abandoned lease, starts a replacement process, reopens the existing logical slot at the authoritative checkpoint, and delivers a subsequent transaction. This test exposed and fixed a replay-unstable record timestamp: managed records now use PostgreSQL's commit timestamp rather than the transport observation time.

## Deferred work

The following requested outcomes remain open and are not represented as maintained support:

- DBOS and every administrative/source-resource mutation using the new fence;
- multi-table PostgreSQL transactions delivered in one target transaction;
- structured DDL and schema-registry mutations under the managed fence;
- reconciliation and cleanup for reserved artifact objects that lack exact-version evidence, plus published-artifact retention GC;
- an Iceberg REST catalog implementation and live catalog recovery tests;
- the append-only ClickHouse managed changelog connector;
- a 100-cycle process-kill chaos profile and long-running soak gate; and
- promotion contracts that require every maintained profile in real-service CI.

Until those items have executable evidence, the relevant connectors and modes remain experimental.
