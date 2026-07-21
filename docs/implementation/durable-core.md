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
- `internal/delivery/migrations/002_authority_protocol.sql`
  - positive delivery/ACK provenance and stale-client protocol gates.
- `internal/artifactlog/migrations/001_artifacts.sql`
  - streams, quotas, objects, upload attempts, GC claims, publications, and publication objects.
- `internal/artifactlog/migrations/002_consumers.sql` and `003_authority_protocol_v2.sql`
  - artifact delivery queues, attempts, and receipts; authority-v2 triggers cover canonical schemas, streams, objects, upload attempts, publications, publication objects, deliveries, quota accounts/reservations, GC claims, and delivery attempts/receipts.

## Runtime admission

The worker constructs workflow, checkpoint, authority, delivery, and registry repositories over one bounded control pool. `internal/runner` acquires a producer fence before opening connectors and renews it with the execution heartbeat. Managed executions no longer use the compatibility `flow_executions` finish API; lifecycle quiescence reads current producer leases directly.

Managed admission currently requires:

- PostgreSQL transactional source with `managed=true` and `bootstrap=auto|required|never`; `never` additionally requires publication creation to be disabled;
- explicit source system, lineage, and publication revision identities;
- one PostgreSQL destination revision;
- `ack_policy=all`;
- target write and batch modes;
- explicit durable `synchronous_commit`;
- no arbitrary `start_lsn`, legacy backfill, file/disabled snapshot authority, drop-slot failure mode, generic staging, DDL-capture resource creation, or raw automatic DDL; and
- one table/schema fragment per source transaction.

## Executable evidence

The acceptance workflow requires the following gates; a gate is not evidence of a pass unless its command completed successfully in the reviewed revision:

- `just fmt-check` and `just lint`;
- `go test -count=1 ./...`;
- `just test-rapid` and `just test-durable-race`;
- `just test-durable-pr`;
- `just test-durable-integration` — requires every named live PostgreSQL/MinIO worker, bootstrap, and fencing test to run without skips;
- `just test-durable-dbos-integration` — requires the named in-process DBOS bootstrap test to run without a skip;
- `just check-tla` and `just spec-verify`;
- `just generate-check`; and
- `just docs-check`.

The process recovery test starts the built worker with `bootstrap=required`, proves an existing source row is atomically published before CDC, sends SIGKILL, expires the abandoned lease, starts a replacement process, reopens the generated logical slot at the authoritative checkpoint, and delivers a subsequent transaction. This test also covers the replay-stable PostgreSQL commit timestamp used by managed records.

## Deferred work

The following requested outcomes remain open and are not represented as maintained support:

- managed multi-fragment CDC transactions delivered in one target transaction;
- external schema-registry publication intents/receipts and structured automatic DDL application (source DDL/catalog rows and DDL attempts/receipts are fenced, but automatic application remains unadmitted);
- a fenced administrative resource-revision workflow; legacy managed slot/publication mutation RPCs currently fail closed;
- reconciliation and cleanup for reserved artifact objects that lack exact-version evidence, plus published-artifact retention GC;
- an Iceberg REST catalog implementation and live catalog recovery tests;
- the append-only ClickHouse managed changelog connector;
- a 100-cycle process-kill chaos profile and long-running soak gate; and
- promotion contracts that require every maintained profile in real-service CI.

Until those items have executable evidence, the relevant connectors and modes remain experimental.
