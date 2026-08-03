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

| Profile | Status | Source | Destination | PostgreSQL | Pairing | Ack | Sinks | Delivery |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `postgresql-to-postgresql-v1` | maintained | `postgres` | `postgres` | 14, 15, 16, 17 | same major | all | one | at-least-once |

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

These are guaranteed defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution. Generic PostgreSQL modes remain experimental; maintained status applies only to the exact named managed profile above.
