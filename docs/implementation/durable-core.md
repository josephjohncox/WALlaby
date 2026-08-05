# Durable core implementation report

## Status

This branch implements maintained PostgreSQL and ClickHouse managed profiles, the experimental constrained `postgresql-to-snowflake-sql-v1` profile, generic experimental connectors, and experimental `ack_policy=materialized` artifact publication. Every named profile remains at-least-once and requires `ack_policy=all`; none claims exactly-once delivery.

Maintained profiles fail closed outside their exact admission contracts. The Snowflake SQL profile also fails closed, but it has no reviewed Snowflake service version or deployment cell. Unit tests, PostgreSQL-only runs, mocks, and fakesnow cannot promote it. Generic PostgreSQL, ClickHouse, Snowflake, and Snowpipe modes remain experimental.

## Implemented slices

### Authority and fencing

`internal/authority`, `internal/workflow`, and `internal/checkpoint` add immutable flow incarnations, generation-qualified producer acquisitions, lease epochs, work claims, and fenced checkpoints. Authoritative mutations validate the complete `RunFence` inside the same PostgreSQL transaction as the mutation.

Public lifecycle states remain unchanged: `created`, `running`, `paused`, `stopping`, `stopped`, and `failed`.

### PostgreSQL source transaction boundary

`internal/replication` handles pgoutput protocol v1 transactions and protocol v2 streamed transaction segments. It buffers every segment through commit and emits complete, ordered multi-table and multi-schema fragments with the transaction-end LSN, XID, ordinal, and final-fragment marker. Emitted records are released from the decoder buffer as ownership transfers to the source transaction assembler, and transaction byte limits include relation, type, and DDL metadata. DDL/control records remain ordered barriers. Received WAL is never used as a durable ACK position.

Authoritative checkpoints carry delivered relation-schema baselines. Bootstrap seeds them from its frozen manifest, and each finalized transaction advances them with the checkpoint. A replacement process can therefore detect schema changes from its first pgoutput `Relation` message after downtime.

The managed source rejects an existing slot without a PostgreSQL-authoritative checkpoint. It also rejects a checkpoint behind `confirmed_flush_lsn`, before retained `restart_lsn`, or beyond the server WAL end.

### Managed destination delivery

`internal/delivery` and the PostgreSQL, ClickHouse, and Snowflake managed drivers implement:

1. immutable destination revision registration with a configuration fingerprint;
2. durable manifest and append-only attempt preparation before external I/O;
3. source-order application of every table/schema fragment, destination-mapped structured DDL plan, metadata row, and deterministic logical-batch marker in one target transaction, with contiguous records batched instead of creating one temporary table per record;
4. external-commit reconciliation from that marker;
5. evidence adoption and terminal retry state under the current fence; and
6. one PostgreSQL transaction for the receipt, authoritative checkpoint, and source ACK intent.

The PostgreSQL target requires explicit `synchronous_commit=on` or `remote_apply`. The Snowflake SQL profile requires hybrid target and receipt tables owned by a role distinct from the execution role, exact object creation identities, narrow direct grants, disabled secondary roles, no owner-role inheritance in execution sessions, no task visible to that role, an exact schema contract, and a configured service-version pin. Snowflake role visibility cannot yet prove the absence of hidden automation or global writer paths, so those remain live promotion blockers. The adapter inserts the receipt first and then applies ordered DML in one pinned Snowflake transaction. Any ambiguous commit discards that physical session before receipt reconciliation. SQL v1 rejects raw and structured DDL.

The named profile validates the ACK grant before source feedback and revalidates the fence before committing the observed `confirmed_flush_lsn` as the ACK receipt. No control transaction or takeover lock spans source I/O. A crash after slot flush but before the receipt is repaired by reissuing the same authoritative checkpoint. A stale acquisition is rejected before feedback or at receipt recording.

Attempts use persisted numbering, bounded exponential backoff, and a 16-attempt ceiling. Terminal manifests, attempts, evidence, receipts, and old ACK rows are pruned only after observed flush evidence; the current checkpoint remains a PostgreSQL retention root. Long-running workers repeat fixed-budget sweeps and renew the producer lease between saturated batches. The target keeps the current reconciliation marker per flow incarnation and destination revision rather than accumulating markers indefinitely.

### Slot-anchored bootstrap

`internal/bootstrap` creates a bootstrap-generation-qualified logical slot with `EXPORT_SNAPSHOT` and retains the exporter connection while bounded tasks import that snapshot in read-only repeatable-read transactions. Task cursor/receipt updates are atomic. A replacement process cannot import the lost exporter's snapshot: exporter loss uses a retryable `abandoning` cleanup phase, starts a new exported snapshot generation and physical slot, and restarts every task from zero.

The production worker and in-process DBOS path now run these primitives for `bootstrap=auto|required`. A pre-slot relation barrier prevents DDL from crossing planning; the publication is created or exactly adopted before slot creation so it is visible at the decoding consistent point. Bounded table tasks import the slot snapshot, write generation-qualified PostgreSQL staging tables, and publish every table in one destination transaction. Exporter loss abandons all cursors and restarts from zero with a new physical slot. `bootstrap=never` is normally pre-provisioned. The experimental Snowflake SQL profile is the narrow exception: `slot=managed` proves the source relation and bound Snowflake objects are empty, then creates and roots a flow-incarnation-specific slot under the run fence without taking a snapshot.

### Canonical artifact log

`internal/artifactlog` keeps the bounded `canonical_cdc_parquet_v1` Arrow 18/Parquet v2 projection byte-for-byte frozen, including its golden hashes and object keys. Mapped Iceberg materialization uses explicit `canonical_cdc_parquet_v2`: the immutable destination projector runs exactly once, and projection version, mapping fingerprint, and mapped relation identity bind canonical schemas, artifact identities, PostgreSQL publication rows, recovery, and consumer requests. Objects target approximately 32 MiB and fail closed above 64 MiB. DDL is rooted as an ordered PostgreSQL barrier rather than encoded as a changelog row.

The experimental `ack_policy=materialized` worker path restores PostgreSQL quota/backlog state before source reads, encodes before destination transforms or retries, records durable upload intents, reconciles conditional S3 PUTs by exact `VersionId`/SHA-256/length/projection, and commits publication roots, quota conversion, checkpoint, and ACK intent in one revalidated generation-fenced transaction. Source feedback occurs only after commit. The production worker registers no catalog consumer and does not create destination delivery rows or open synchronous destination connectors for CDC; the public behavior is canonical publication only.

Epoch-based mark/sweep handles uploaded/verified unpublished orphans and rooted retention. A reserved intent with a prepared PUT but no exact-version evidence remains quota-charged until replay because an old-fence request may still complete after takeover. Rooted objects require an observed source ACK receipt, any explicitly registered package-level deliveries to be complete, elapsed retention, and a newer checkpoint. Publication rechecks GC claims under its final fence. The package still provides only a catalog abstraction, not a production Iceberg REST or S3 Tables client.

## PostgreSQL migrations

`internal/controlstore` owns the worker's shared control pool. Every shared control-plane migration domain delegates to one ordered, checksum-verifying coordinator and records SQL plus history atomically under the sole authoritative ledger, `public.wallaby_control_migrations`. Old workflow/checkpoint/registry/pgstream/schema-registry ledgers and any conflicting `wallaby*_migrations` relation fail startup explicitly; they are never discovered, imported, copied, or dual-written. Runtime pools set the `wallaby.authority_protocol=v2` session capability; workflow, checkpoint, registry, delivery, bootstrap, and artifact-log mutation tables have exact enabled v2 trigger coverage after the quiesced cutover. Central startup migrates pgstream and the PostgreSQL schema registry after the core domains and before opening components. Their constructors only verify the authoritative checksummed history and required table shape; package users must call the explicit current `ApplyMigrations` API before `NewStore` or a PostgreSQL `NewRegistry`.

- `internal/workflow/migrations/006_authority_fences.sql`
  - flow incarnations, execution acquisitions, producer leases, and work claims;
  - incarnation provenance on lifecycle events and executions.
- `internal/checkpoint/migrations/003_authority_fencing.sql`
  - incarnation-scoped authoritative checkpoints and retained outbox completion.
- `internal/delivery/migrations/001_attempts_receipts.sql`
  - destination revisions, delivery manifests, attempts, evidence, receipts, ACK intents, and ACK receipts.
- `internal/bootstrap/migrations/001_bootstraps.sql` through `007_snapshot_destination_contract.sql`
  - bootstrap sessions, fenced multi-table tasks and delivery receipts, source-resource ownership/operations, and publication receipts. Every frozen task persists separate immutable source-query and mapped-destination contracts: source namespace/table/schema/PK drive snapshot queries and cursors, while destination schema, resolved write policy, projection fingerprint, and contract version exclusively govern delivery. The manifest hash binds both contracts; recovery recomputes it and legacy tasks without the destination contract fail migration explicitly.
- `internal/registry/migrations/006_run_fencing.sql`
  - complete-or-legacy DDL/catalog provenance, takeover-safe attempts, and schema-publication operations.
- `internal/delivery/migrations/002_authority_protocol.sql`, `004_logical_batches_retry_retention.sql`, and `006_rolling_logical_batch_compatibility.sql`
  - positive delivery/ACK provenance, stale-client protocol gates, logical batch identity, bounded retry state, retention roots, indexed logical attempts, and nullable additive identity columns so authority-v2 checkpoint-1 workers remain writable during a rolling upgrade.
- `internal/artifactlog/migrations/001_artifacts.sql`
  - streams, quotas, objects, upload attempts, GC claims, publications, and publication objects.
- `internal/artifactlog/migrations/002_consumers.sql`, `003_authority_protocol_v2.sql`, and `004_materialized_publication.sql`
  - artifact delivery queues, attempts, and receipts; deterministic logical/shard identity, publication sequencing, ordered barriers, upload-attempt state, GC epochs/claim kinds, and rooted-retention marks; authority-v2 triggers cover every mutable artifact table.
- `internal/artifactlog/migrations/007_current_catalog_attempt_identity.sql`
  - fail-closed canonical catalog commit and logical-batch identities, one durable attempt per publication, exact receipt linkage, and no legacy attempt adoption or inferred backfill. Startup verifies the exact attempt, receipt, and consumer-checkpoint column sets, types, nullability, defaults, identity/generated state, and authority triggers. A single manifest covers all 22 current PK, FK, unique, sequence/projection, and canonical-identity constraints plus all 12 explicit and backing indexes introduced by artifact migrations 002, 005, 006, and 007, including exact definitions, ordered keys, sort options, btree method, predicate/expression form, readiness, and validity. Missing, extra-key, nullable, defaulted, weakened, or otherwise altered state fails restart without migration replay or runtime repair; incompatible consumer state must be recreated.
- `pkg/pgstream/migrations/*.sql` and `pkg/schemaregistry/migrations/*.sql`
  - embedded package schemas registered as the ordered `pgstream` and `schema_registry` domains in the authoritative control ledger; constructors never create an independent ledger or auto-migrate.

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

The experimental Snowflake SQL profile requires PostgreSQL 16, `bootstrap=never`, `slot=managed`, `create_slot=true`, `toast_fetch=off`, one source relation, two pre-provisioned hybrid tables, distinct owner and execution roles, exact grants and creation identities, no schema tasks, enforced identity constraints, an immutable schema contract and hash, and a live `CURRENT_VERSION()` equal to its configured pin. Its complete type cell and transaction bounds are documented in the Snowflake connector reference. Generic managed modes remain experimental even when they pass narrower startup checks.

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
- an Iceberg REST catalog implementation and live catalog recovery tests;
- the append-only ClickHouse managed changelog connector;
- a 100-cycle process-kill chaos profile and long-running soak gate; and
- maintained Snowflake or Snowpipe profiles; `postgresql-to-snowflake-sql-v1` has no reviewed service version or deployment cell and still lacks same-SHA proof for every required network fault, detached transaction takeover, full worker `SIGKILL`, account edition/type, bounded-load, telemetry, redaction, and cleanup gate.

Those deferred connectors and modes remain experimental.
