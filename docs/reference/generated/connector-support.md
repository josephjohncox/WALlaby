<!-- Generated from executable connector capability declarations. Do not edit. -->

# Connector support matrix

`maintained` requires restart, replay, schema-evolution, and integration contract evidence. `experimental` adapters are usable but have not passed every maintained gate. `placeholder` endpoints have no runtime adapter.

## Sources

| Connector | Mode | Status | Restart | Replay | Schema evolution | Integration |
| --- | --- | --- | --- | --- | --- | --- |
| `postgres` | cdc | experimental | no | no | yes | no |
| `postgres` | backfill | experimental | no | no | yes | no |

## Destinations

| Connector | Status | Runtime | Append mapping | Explicit-key upsert | Watermark guard | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Reconciles DDL | Lossy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `postgres` | experimental | yes | yes | yes | yes | yes | no | no | yes | yes | no |
| `pgstream` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `kafka` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `bufstream` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `s3` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `http` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `grpc` | experimental | yes | yes | no | no | no | no | no | no | no | no |
| `snowflake` | experimental | yes | yes | no | no | no | no | no | yes | no | no |
| `snowpipe` | experimental | yes | yes | no | no | no | no | no | yes | no | no |
| `clickhouse` | experimental | yes | yes | no | no | no | no | no | yes | no | no |
| `duckdb` | experimental | yes | yes | no | no | yes | no | no | yes | no | no |
| `ducklake` | experimental | yes | yes | no | no | yes | no | no | yes | no | no |
| `iceberg` | experimental | yes | yes | no | no | no | yes | yes | no | no | no |
| `proto` | placeholder | no | no | no | no | no | no | no | no | no | no |
| `parquet` | placeholder | no | no | no | no | no | no | no | no | no | no |

Snowpipe is append-only staged delivery: PUT, optional COPY, and metadata-receipt errors are returned unchanged; target tables change only through configured COPY or external pipe ingestion.

## Configuration-controlled capability profiles

| Connector | Profile | Append | Explicit-key upsert | Watermark guard | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Lossy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `kafka` | `base` | yes | no | no | no | no | no | no | no |
| `kafka` | `transactional-only` | yes | no | no | yes | no | no | no | no |
| `kafka` | `lossy-only` | yes | no | no | no | no | no | no | yes |
| `kafka` | `transactional+lossy` | yes | no | no | yes | no | no | no | yes |
| `bufstream` | `base` | yes | no | no | no | no | no | no | no |
| `bufstream` | `transactional-only` | yes | no | no | yes | no | no | no | no |
| `bufstream` | `lossy-only` | yes | no | no | no | no | no | no | yes |
| `bufstream` | `transactional+lossy` | yes | no | no | yes | no | no | no | yes |
| `snowflake` | `base` | yes | no | no | no | no | no | yes | no |
| `snowflake` | `postgresql-to-snowflake-sql-v1` | no | yes | no | yes | yes | yes | no | no |
| `clickhouse` | `base` | yes | no | no | no | no | no | yes | no |
| `clickhouse` | `postgresql-to-clickhouse-append-v1` | yes | no | no | no | no | no | no | no |

## Managed profiles

| Profile | Status | Source | Destination | PostgreSQL | ClickHouse | Snowflake version | Deployment | Pairing | Ack | Sinks | Delivery | Table mappings |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `postgresql-to-postgresql-v1` | maintained | `postgres` | `postgres` | 14, 15, 16, 17 | — | — | — | same major | all | one | at-least-once | append; explicit-key upsert; watermark guard |
| `postgresql-to-clickhouse-append-v1` | maintained | `postgres` | `clickhouse` | 16 | 25.12.1.649 | — | self-managed-keeper | mixed majors | all | one | at-least-once | append only |
| `postgresql-to-snowflake-sql-v1` | experimental | `postgres` | `snowflake` | 16 | — | configured-exact-version-unreviewed (reviewed versions: none) | commercial-aws-snowflake-hybrid-table [reviewed cells: none] | configured runtime pin; unreviewed | all | one | at-least-once | one exact relation; explicit-key upsert; complete source PK; future tables excluded; no watermark |

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

These are declared defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution. Generic PostgreSQL, ClickHouse, Snowflake, and Snowpipe modes remain experimental. Maintained status applies only to rows explicitly marked maintained; the named Snowflake SQL profile has no reviewed service version or deployment cell and remains experimental until every unskipped real-service recovery gate passes on one reviewed SHA.
