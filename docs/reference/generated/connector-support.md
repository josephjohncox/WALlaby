<!-- Generated from executable connector capability declarations. Do not edit. -->

# Connector support matrix

`maintained` requires restart, replay, schema-evolution, and integration contract evidence. `experimental` adapters are usable but have not passed every maintained gate. `placeholder` endpoints have no runtime adapter.

## Sources

| Connector | Mode | Status | Restart | Replay | Schema evolution | Integration |
| --- | --- | --- | --- | --- | --- | --- |
| `postgres` | cdc | experimental | no | no | yes | no |
| `postgres` | backfill | experimental | no | no | yes | no |

## Destinations

| Connector | Status | Runtime | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Reconciles DDL | Lossy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `postgres` | experimental | yes | yes | yes | yes | yes | yes | no |
| `pgstream` | experimental | yes | no | no | no | no | no | no |
| `kafka` | experimental | yes | no | no | no | no | no | no |
| `bufstream` | experimental | yes | no | no | no | no | no | no |
| `s3` | experimental | yes | no | no | no | no | no | no |
| `http` | experimental | yes | no | no | no | no | no | no |
| `grpc` | experimental | yes | no | no | no | no | no | no |
| `snowflake` | experimental | yes | no | no | no | yes | no | no |
| `snowpipe` | experimental | yes | no | no | no | yes | no | no |
| `clickhouse` | experimental | yes | no | no | no | yes | no | no |
| `duckdb` | experimental | yes | yes | no | no | yes | no | no |
| `ducklake` | experimental | yes | yes | no | no | yes | no | no |
| `proto` | placeholder | no | no | no | no | no | no | no |
| `parquet` | placeholder | no | no | no | no | no | no | no |

## Managed profiles

| Profile | Status | Source | Destination | PostgreSQL | ClickHouse | Snowflake version | Deployment | Pairing | Ack | Sinks | Delivery |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `postgresql-to-postgresql-v1` | maintained | `postgres` | `postgres` | 14, 15, 16, 17 | — | — | — | same major | all | one | at-least-once |
| `postgresql-to-clickhouse-append-v1` | maintained | `postgres` | `clickhouse` | 16 | 25.12.1.649 | — | self-managed-keeper | mixed majors | all | one | at-least-once |
| `postgresql-to-snowflake-sql-v1` | experimental | `postgres` | `snowflake` | 16 | — | configured-exact-version-unreviewed (reviewed versions: none) | commercial-aws-snowflake-hybrid-table [reviewed cells: none] | configured runtime pin; unreviewed | all | one | at-least-once |
| `postgresql-to-snowflake-staged-append-v1` | experimental | `postgres` | `snowflake` | 16 | — | configured-exact-version-unreviewed (reviewed versions: none) | commercial-aws-snowflake-internal-stage-copy [reviewed cells: none] | configured runtime pin; unreviewed | all | one | at-least-once |
| `postgresql-to-snowflake-streaming-rest-append-v1` | experimental | `postgres` | `snowflake` | 16 | — | configured-exact-version-unreviewed (reviewed versions: none) | commercial-aws-snowpipe-streaming-highperf-rest [reviewed cells: none] | configured runtime pin; unreviewed | all | one | at-least-once |

### `postgresql-to-postgresql-v1` evidence gates

| Admission/evidence gate | Real service | Required test |
| --- | --- | --- |
| postgres versions | yes | `TestPostgresManagedProfileVersionContract` |
| streamed transactions | yes | `TestPostgresManagedStreamedSubtransactionAbort` |
| target admission | yes | `TestPostgresManagedProfileTargetAdmission` |
| schema evolution | yes | `TestPostgresManagedProfileSourceSchemaEvolutionAfterRestart` |
| DDL reconciliation | yes | `TestPostgresManagedProfileDDLCommitReconciliation` |
| snapshot to CDC | yes | `TestManagedBootstrapWorkerWiringConcurrentBoundary` |
| process kill | yes | `TestWallabyWorkerProcessKillRecovery` |
| pool exhaustion | yes | `TestPostgresManagedProfilePoolExhaustion` |
| restart | yes | `TestPostgresManagedOverlappingTakeoverAdoptsConcurrentCommit` |
| retry and retention | yes | `TestPostgresManagedDeliveryRetryAndRetention` |
| metrics | no | `TestPostgresManagedProfileMetrics` |
| upgrade migrations | yes | `TestPostgresManagedProfileUpgradeMigrations` |

### `postgresql-to-clickhouse-append-v1` evidence gates

| Admission/evidence gate | Real service | Required test |
| --- | --- | --- |
| clickhouse versions | yes | `TestClickHouseManagedProfileVersionMatrix` |
| target admission | yes | `TestClickHouseManagedProfileAdmission` |
| ambiguous response | yes | `TestClickHouseManagedProfileCommitBeforeReceipt` |
| deduplication window | yes | `TestClickHouseManagedProfileDedupWindowEviction` |
| ordered fragments | yes | `TestClickHouseManagedProfileOrderingAndConcurrency` |
| key changes and tombstones | yes | `TestClickHouseManagedProfileKeyChangesAndTombstones` |
| schema and types | yes | `TestClickHouseManagedProfileSchemaEvolutionAndTypes` |
| PostgreSQL recovery | yes | `TestPostgresToClickHouseManagedProfileRecoveryContract` |
| bounded load | yes | `TestClickHouseManagedProfileBoundedLoad` |
| process kill | yes | `TestClickHouseManagedProfileProcessKillRecovery` |
| keeper recovery | yes | `TestClickHouseManagedProfileKeeperFailureRecovery` |
| backpressure | yes | `TestClickHouseManagedProfileBackpressure` |
| TLS | yes | `TestClickHouseManagedProfileTLS` |
| telemetry | no | `TestClickHouseManagedProfileTelemetry` |

### `postgresql-to-snowflake-sql-v1` evidence gates

| Admission/evidence gate | Real service | Required test |
| --- | --- | --- |
| runtime deployment | yes | `TestSnowflakeManagedProfileReviewedDeploymentCell` |
| source catalog and clean cut | yes | `TestPostgresToSnowflakeManagedProfileRecoveryContract` |
| target direct grants objects and constraints | yes | `TestSnowflakeManagedProfileLiveAdmission` |
| role hierarchy and alternate writers | yes | `TestSnowflakeManagedProfileRoleIsolation` |
| task visibility and automation isolation | yes | `TestSnowflakeManagedProfileTaskIsolation` |
| rollback cardinality ordering and types | yes | `TestSnowflakeManagedProfileOrderedFragmentsAndTypes` |
| confirmed commit reconciliation | yes | `TestSnowflakeManagedProfileAmbiguousCommit` |
| commit transport loss and detached takeover | yes | `TestSnowflakeManagedProfileCommitTransportLossAndDetachedTakeover` |
| DDL rejection and replacement | yes | `TestSnowflakeManagedProfileSchemaReconciliation` |
| adapter process kill | yes | `TestSnowflakeManagedProfileProcessKillRecovery` |
| full worker SIGKILL | yes | `TestSnowflakeManagedProfileWorkerSIGKILLRecovery` |
| network fault matrix | yes | `TestSnowflakeManagedProfileNetworkFaultMatrix` |
| cancellation and pool safety | yes | `TestSnowflakeManagedProfileCancellationAndPoolSafety` |
| bounded load and backpressure | yes | `TestSnowflakeManagedProfileBoundedLoadAndBackpressure` |
| PostgreSQL receipt checkpoint and feedback recovery | yes | `TestPostgresToSnowflakeManagedProfileRecoveryContract` |
| TLS and JWT | yes | `TestSnowflakeManagedProfileLiveAdmission` |
| secret redaction | yes | `TestSnowflakeManagedProfileSecretRedaction` |
| cleanup | yes | `TestSnowflakeManagedProfileCleanup` |
| telemetry | no | `TestSnowflakeManagedProfileTelemetry` |

### `postgresql-to-snowflake-staged-append-v1` evidence gates

| Admission/evidence gate | Real service | Required test |
| --- | --- | --- |
| runtime deployment | yes | `TestSnowflakeStagedManagedProfileReviewedDeploymentCell` |
| source catalog and clean cut | yes | `TestPostgresToSnowflakeStagedManagedProfileRecoveryContract` |
| target stage grants objects and file format | yes | `TestSnowflakeStagedManagedProfileLiveAdmission` |
| role hierarchy and alternate writers | yes | `TestSnowflakeStagedManagedProfileRoleIsolation` |
| pipe visibility and auto-ingest isolation | yes | `TestSnowflakeStagedManagedProfilePipeIsolation` |
| deterministic stage identity and wrong-byte collision | yes | `TestSnowflakeStagedManagedProfileStageIdentityCollision` |
| PUT uncertainty reconciliation | yes | `TestSnowflakeStagedManagedProfilePutUncertainty` |
| fail-closed COPY and partial-load rejection | yes | `TestSnowflakeStagedManagedProfileFailClosedCopy` |
| load history verification and receipt adoption | yes | `TestSnowflakeStagedManagedProfileLoadHistoryAdoption` |
| auto-ingest verified completion | yes | `TestSnowflakeStagedManagedProfileAutoIngestCompletion` |
| copy transport loss and detached takeover | yes | `TestSnowflakeStagedManagedProfileCopyTransportLossAndDetachedTakeover` |
| DDL rejection and replacement | yes | `TestSnowflakeStagedManagedProfileSchemaReconciliation` |
| adapter process kill | yes | `TestSnowflakeStagedManagedProfileProcessKillRecovery` |
| full worker SIGKILL | yes | `TestSnowflakeStagedManagedProfileWorkerSIGKILLRecovery` |
| network fault matrix | yes | `TestSnowflakeStagedManagedProfileNetworkFaultMatrix` |
| cancellation and pool safety | yes | `TestSnowflakeStagedManagedProfileCancellationAndPoolSafety` |
| bounded load and backpressure | yes | `TestSnowflakeStagedManagedProfileBoundedLoadAndBackpressure` |
| cleanup release receipts and retention roots | yes | `TestSnowflakeStagedManagedProfileCleanup` |
| PostgreSQL receipt checkpoint and feedback recovery | yes | `TestPostgresToSnowflakeStagedManagedProfileRecoveryContract` |
| TLS and JWT | yes | `TestSnowflakeStagedManagedProfileLiveAdmission` |
| secret redaction | yes | `TestSnowflakeStagedManagedProfileSecretRedaction` |
| telemetry | no | `TestSnowflakeStagedManagedProfileTelemetry` |

### `postgresql-to-snowflake-streaming-rest-append-v1` evidence gates

| Admission/evidence gate | Real service | Required test |
| --- | --- | --- |
| reviewed high-performance append transport | yes | `TestSnowflakeStreamingManagedProfileReviewedTransport` |
| runtime deployment | yes | `TestSnowflakeStreamingManagedProfileReviewedDeploymentCell` |
| source catalog and clean cut | yes | `TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract` |
| target channel grants objects and pipe | yes | `TestSnowflakeStreamingManagedProfileLiveAdmission` |
| role hierarchy and alternate writers | yes | `TestSnowflakeStreamingManagedProfileRoleIsolation` |
| channel and pipe revision evidence | yes | `TestSnowflakeStreamingManagedProfileChannelRevisionEvidence` |
| deterministic row identity and SQL-observed completeness | yes | `TestSnowflakeStreamingManagedProfileDeterministicRowObservation` |
| reopen after uncommitted rows and append proven-missing | yes | `TestSnowflakeStreamingManagedProfileReopenAppendsProvenMissing` |
| terminal token with rejected rows fails closed | yes | `TestSnowflakeStreamingManagedProfileRejectedRowsFailClosed` |
| complete-unreceipted recovery and receipt adoption | yes | `TestSnowflakeStreamingManagedProfileCompleteUnreceiptedRecovery` |
| receipt conflicts and channel invalidation | yes | `TestSnowflakeStreamingManagedProfileReceiptConflictAndChannelInvalidation` |
| schema evolution and TOAST unchanged fields | yes | `TestSnowflakeStreamingManagedProfileSchemaEvolutionAndToast` |
| auth expiry refresh | yes | `TestSnowflakeStreamingManagedProfileAuthExpiryRefresh` |
| throttling and backpressure | yes | `TestSnowflakeStreamingManagedProfileThrottlingBackpressure` |
| oversize rejection | yes | `TestSnowflakeStreamingManagedProfileOversizeRejection` |
| adapter process kill | yes | `TestSnowflakeStreamingManagedProfileProcessKillRecovery` |
| full worker SIGKILL | yes | `TestSnowflakeStreamingManagedProfileWorkerSIGKILLRecovery` |
| cancellation and pool safety | yes | `TestSnowflakeStreamingManagedProfileCancellationAndPoolSafety` |
| cleanup release receipts and channel state | yes | `TestSnowflakeStreamingManagedProfileCleanup` |
| PostgreSQL receipt checkpoint and feedback recovery | yes | `TestPostgresToSnowflakeStreamingManagedProfileRecoveryContract` |
| TLS and JWT | yes | `TestSnowflakeStreamingManagedProfileLiveAdmission` |
| secret redaction | yes | `TestSnowflakeStreamingManagedProfileSecretRedaction` |
| telemetry | no | `TestSnowflakeStreamingManagedProfileTelemetry` |

These are declared defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution. Generic PostgreSQL, ClickHouse, Snowflake, and Snowpipe modes remain experimental. Maintained status applies only to rows explicitly marked maintained; the named Snowflake SQL profile has no reviewed service version or deployment cell and remains experimental until every unskipped real-service recovery gate passes on one reviewed SHA.
