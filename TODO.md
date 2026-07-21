# WALlaby backlog and context notes

## Production-readiness contracts

- [x] **P0 — Destination capability contract**
  - [x] Declare transactional-batch, idempotent, replay-safe, DDL-executing, and lossy behavior for every destination.
  - [x] Validate acknowledgement and DDL policies before opening connectors or starting execution.
  - [x] Add table-driven capability and invalid-flow contract tests.
- [x] **P0 — DDL execution receipt/outbox**
  - [x] Persist replay-safe destination DDL execution receipts and immutable destination manifests.
  - [x] Couple receipt persistence, registry transition, and checkpoint advancement through one recoverable protocol.
  - [x] Reject administrative `applied` transitions without an execution receipt.
  - [x] Add receipt-before-checkpoint crash recovery, replay, and integration tests.
  - [x] Preserve per-record source positions and validate immutable manifests before destination execution.
  - [x] Persist attempts before side effects and reconcile PostgreSQL structured plans after an external-commit-before-receipt crash.
  - [x] Fail closed for raw SQL, conflicting schemas, and automatic-DDL destinations without a reconciler.
- [ ] **P1 — Remaining destination DDL reconcilers**
  - [ ] Add catalog reconciliation to each remaining destination before enabling its automatic-DDL path.
- [x] **P1 — Connector support matrix**
  - [x] Classify every connector as maintained, experimental, deprecated, or placeholder.
  - [x] Require restart, replay, schema-evolution, and integration contracts before maintained status.
  - [x] Generate user-facing support documentation from the executable matrix.
- [x] **P1 — CI execution completeness**
  - [x] Enumerate expected Go tests deterministically.
  - [x] Fail CI when an expected test or package is omitted from machine-readable test results.
  - [x] Keep flaky-test quarantine explicit; never infer success from automatic reruns.
- [x] **P1 — Property and formal verification expansion**
  - [x] Add randomized DDL crash-boundary, schema-diff, lifecycle-generation, and composite-cursor properties.
  - [x] Model DDL execution locks/receipts, generation-fenced lifecycle, and snapshot-to-stream handoff in TLA+.
  - [x] Enforce nonzero TLC action coverage for the new models.
- [x] **P1 — Bounded snapshot replay**
  - [x] Use primary-key tie-breakers in versioned backfill cursors.
  - [x] Preserve legacy cursor compatibility and `NULLS LAST` recovery.
  - [x] Cover duplicate partition values, null partitions, binary keys, and malformed cursors.
- [x] **P1 — Health contracts**
  - [x] Add startup, readiness, and liveness endpoints and Kubernetes probes.
  - [x] Replace the TCP-only Helm test with a readiness assertion.
  - [x] Support independent OTLP metrics and traces endpoints.
  - [x] Add configuration, server, chart, and deployment tests.

- [x] Config precedence contract confirmed and documented: `--config`/`WALLABY*_CONFIG` uses `config file > env > defaults`.
- [x] `WALLABY_DBOS_MAX_RETRIES` parsing is now fail-fast on malformed values (no silent disable).
  - Return structured validation errors for malformed env/config values (including numeric/range checks and string enums).
- [x] Worker command mutates persisted flow source options in place.
  - Apply CLI overrides via copy-on-write into local flow copies.
- [x] `just check` remains usable in environments where TLC cannot open JMX/RMI sockets.
  - Added the `SKIP_TLA_CHECKS` recipe input for sandboxed checks.
- [x] WAL replication frame handling can panic on malformed/empty payloads (`msg.Data[0]`).
  - Add explicit length checks and robust error accounting/metrics for malformed protocol frames.
- [x] App shutdown teardown is path-dependent and can skip closes.
  - Normalize lifecycle cleanup through a single shutdown path for all runtime modes and error paths.
- [x] Kubernetes job-name derivation can still collide under truncation stress.
  - Use deterministic, collision-resistant suffixing from full flow identity and guard name building rules.
- [x] Worker `--mode` accepts unknown values without validation.
  - Enforce strict enum validation (e.g. `cdc`, `backfill`) with clear error.
- [x] API start semantics differ between immediate-start and run-once paths when dispatcher is missing.
  - Decide and apply one consistent precondition contract; align return codes/messages.

- Backlog follow-up work to implement in one module:
  - [x] Define and export a config precedence contract (file > env > defaults).
  - [x] Add startup validation for numeric/time/string-enum settings using validator tags and startup contracts.
  - [x] Add explicit protocol-frame metrics/counters for parse errors.
  - [x] Complete immutable copy-on-write for flow option overrides across worker and orchestrated paths.
- [x] Normalize resource ownership and teardown boundaries around app shutdown.
  - [x] Standardize `RunFlowOnce` contract and messaging across server and dispatcher modes.

## Durable canonical artifacts, Parquet, Iceberg, and S3 Tables

- [ ] **P1 — Build a PostgreSQL-authoritative S3 artifact log.**
  - Feasibility boundary:
    - PostgreSQL remains authoritative for lifecycle generations, leases/fence
      tokens, checkpoints, delivery attempts, outbox publication, DDL state,
      snapshot progress, manifest publication, quotas, and garbage-collection
      roots.
    - Ordinary S3 stores immutable payload objects, schemas, and manifests. Do
      not implement S3-only workflow/checkpoint/outbox authority or a mutable
      `latest.json` publication head.
    - Intercept only after `connector.ValidateBatch`, ordered data/control
      separation, and DDL/schema barriers. Materialize once per validated source
      batch, outside the per-destination transform and retry loops; do not
      intercept destination-specific wire bytes.
    - The first projection is `canonical_cdc_parquet_v1`: PostgreSQL CDC only,
      CDC-only ingestion, append-only changelog, one compatible
      table/schema/partition shard per artifact, approximately 32 MiB target and
      64 MiB hard object cap, conditional single-part PUT, and explicit
      checksums.
    - `ack_policy=materialized` means the canonical artifact and its fenced PostgreSQL publication are durable; it does not mean every downstream table committed the batch.
  - Required invariants:
    - Every acknowledged source position maps to exactly one published `LogicalBatchID`, independent of worker generation.
    - `ArtifactID` is deterministic from projection ID, schema fingerprint, source/table, partition, shard, and logical batch identity; generation identifies an attempt, never a new logical delivery.
    - Every published manifest references present checksum-valid immutable objects. A matching object is reusable; the same identity with different logical content fails closed.
    - A fenced PostgreSQL transaction publishes the manifest, delivery rows, outbox state, quota accounting, and checkpoint together. A stale worker cannot publish or advance any of them.
    - Object upload before PostgreSQL commit may leave an orphan but cannot
      acknowledge the source. Record durable upload intents before transfer.
      Garbage collection uses epoch-based mark/sweep with transactional claims;
      publication must revalidate the claim in its fenced transaction so a
      paused publisher cannot publish an object after GC deletes it.
    - DDL/control records remain ordered barriers and are never encoded as ordinary changelog rows. Schema fingerprint changes split artifacts.
    - Backlog byte, batch-count, and age high-water marks stop checkpoint advancement before source acknowledgement; restart restores accounting before new reads.
    - Encode-once means once per identical projection ID and shard, not one universal encoding for all destinations.
  - Milestones:
    - [ ] Define `LogicalBatchID`, `ArtifactID`, canonical schema fingerprinting, deterministic record ordinals, projection compatibility, and property tests under `internal/artifactlog`.
    - [ ] Implement a pure planner that splits validated batches by ordered DDL barrier, table, schema fingerprint, partition, shard, and size; unsupported records fail before upload.
    - [ ] Implement deterministic `canonical_cdc_parquet_v1` encoding with per-record source position and operation envelope; prove repeated encoding and mixed-table/schema failure properties.
    - [ ] Upload immutable objects and manifests with conditional creation, SHA-256 verification, durable upload intents, and package-local plus real-S3 ambiguous-response, conflict, concurrent-writer, and partial-upload tests.
    - [ ] In one generation-fenced PostgreSQL transaction publish manifests, downstream delivery rows, outbox state, quota accounting, and checkpoint; acknowledge the source only after commit.
    - [ ] Crash-test upload, manifest, PostgreSQL commit, checkpoint, and source-ACK boundaries; inject delayed stale workers and verify lifecycle/spec invariants.
    - [ ] Add artifact-reference delivery queues, conservative orphan GC, and quota recovery; in-memory queues must not duplicate batch payloads.
    - [ ] Add an append-only ordinary-Iceberg changelog consumer using
      `Commit`/`Reconcile`, stable WALlaby batch IDs in snapshot summaries, and
      catalog-commit crash recovery. Reuse immutable files only when the
      canonical schema carries stable Iceberg field IDs and compatible partition
      semantics; otherwise rewrite them for the table projection.
    - [ ] Add S3 Tables only as an Iceberg REST backend. Keep canonical recovery
      objects in ordinary S3 under independent retention; do not rely on S3
      Tables maintenance to pin WALlaby recovery data or assume ordinary-S3
      canonical files can be reused directly inside a managed table bucket.
    - [ ] Defer current-state tables, deletes/merges, compaction, retained-manifest GC, multi-source/vector-frontier support, and universal destination routing until append-only changelog recovery and soak tests pass.

### Implemented durable-core vertical slice (current branch)

- [x] Buffer PostgreSQL pgoutput changes through `COMMIT`, checkpoint at `TransactionEndLSN`, and remove received-WAL feedback fallback.
- [x] Validate existing logical-slot plugin/database/health, reject an authorized checkpoint behind `confirmed_flush_lsn`, and require an authoritative checkpoint for managed existing-slot startup.
- [x] Add immutable flow incarnations, producer acquisitions, lease epochs, work claims, and stale-owner integration tests.
- [x] Add fenced PostgreSQL checkpoints/outbox state isolated by flow incarnation.
- [x] Add durable destination manifests, immutable configuration-qualified destination revisions, append-only attempts/evidence, immutable receipts, checkpoint/ACK-intent coupling, and PostgreSQL target-marker reconciliation.
- [x] Add the managed PostgreSQL worker path and a live PostgreSQL-to-PostgreSQL transaction-end/feedback test.
- [x] Wire slot-exported bootstrap into `wallaby-worker` and in-process DBOS. `bootstrap=auto|required` now uses a pre-slot DDL barrier, publication create/adopt journal, imported snapshot tasks, receipt-backed exclusive cursors, atomic PostgreSQL target publication, exact-LSN handoff, and whole-generation restart after exporter loss.
- [ ] Wire canonical artifacts into managed execution. The package now validates source transaction identity, exact-version checksums, monotonic publication, quota conversion, and consumer claims, but no worker constructs a publisher.
- [x] Downgrade PostgreSQL CDC and legacy backfill source capability claims to experimental pending the complete promotion matrix.
- [x] Add the `ManagedDurability` TLA+ model and required PR, live-integration, and scheduled nightly recipes/jobs.
- [ ] Finish every remaining managed mutation seam. DBOS, source DDL/catalog rows, DDL attempts/receipts, bootstrap staging, checkpoints, delivery, and owned slot/publication operations now carry the fence; legacy managed administrative resource mutation fails closed. External schema-registry publication, DDL-capture resource creation, generic staging, and automatic structured DDL application remain unadmitted.
- [x] Add an authority-protocol session gate to workflow, checkpoint, and registry mutations so a pre-authority binary is rejected after the quiesced migration.
- [ ] Support multi-table PostgreSQL source transactions in one target transaction; the initial managed runner fails closed when a transaction has multiple table/schema fragments.
- [ ] Reconcile and release old `reserved` artifact objects that have no exact `VersionId`. The conservative collector deletes only unrooted `uploaded`/`verified` exact versions.
- [ ] Add published-artifact retention GC and long-running publisher/GC race tests. The hard retained-byte ceiling is fail-stop containment, not retention.
- [ ] Implement and live-test an Iceberg REST catalog adapter. The current code supplies only the PostgreSQL queue and catalog abstraction.
- [ ] Implement the append-only ClickHouse managed changelog profile. Managed admission currently rejects the mutation connector.
- [x] Share one bounded worker control pool and one checksum-verifying migration coordinator; import and dual-record legacy workflow/checkpoint/registry history.
- [ ] Export observable gauges for active leases, bootstrap progress, artifact backlog count/bytes/age, retained bytes, and GC lag. The current durable telemetry records transition/outcome counters only.

## Direct S3 follow-up

- [x] Preserve explicit single-destination at-least-once operation under
  `ack_policy=all`; reject replay-unsafe multi-destination fan-out and every
  replay-unsafe `ack_policy=primary` configuration at startup.
- [ ] Direct S3 writes remain experimental and at-least-once. Add a replay-safe multipart protocol before admitting objects above the explicit 5 GiB conditional single-PUT limit.
  Conditional object creation, full partition-set identity markers, and stored
  checksum verification make an exact batch retry converge, but a restart may
  regroup the same records under different terminal checkpoints. Do not declare
  `IdempotentReplay` or `ReplaySafe` until stable per-record/logical-batch
  identity survives rebatching.
- [ ] Treat the existing Arrow/Parquet codecs as destination encodings, not
  `canonical_cdc_parquet_v1`. The canonical artifact envelope must include
  per-record source position, deterministic ordinal, operation, and ordered
  control/DDL separation.
- [ ] Establish performance acceptance separately from local seam benchmarks.
  Bound retained benchmark state; record command, Go version, repository
  identity, environment, custom request/call metrics, and baseline/candidate
  labels. Run batch-size, width, table-interleave, partition-count, concurrency,
  slow-sink, recovery, and sustained-soak matrices before making throughput or
  latency claims.
- [ ] Add focused `-race` coverage and benchmark smoke checks to pull-request CI;
  keep long real-service throughput and soak jobs scheduled or manually gated.
