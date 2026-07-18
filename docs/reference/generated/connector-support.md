<!-- Generated from executable connector capability declarations. Do not edit. -->

# Connector support matrix

`maintained` requires restart, replay, schema-evolution, and integration contract evidence. `experimental` adapters are usable but have not passed every maintained gate. `placeholder` endpoints have no runtime adapter.

## Sources

| Connector | Mode | Status | Restart | Replay | Schema evolution | Integration |
| --- | --- | --- | --- | --- | --- | --- |
| `postgres` | cdc | maintained | yes | yes | yes | yes |
| `postgres` | backfill | maintained | yes | yes | yes | yes |

## Destinations

| Connector | Status | Runtime | Transactional batch | Idempotent replay | Replay safe | Executes DDL | Lossy |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `postgres` | experimental | yes | yes | yes | yes | yes | no |
| `pgstream` | experimental | yes | no | no | no | no | no |
| `kafka` | experimental | yes | no | no | no | no | no |
| `bufstream` | experimental | yes | no | no | no | no | no |
| `s3` | experimental | yes | no | no | no | no | no |
| `http` | experimental | yes | no | no | no | no | no |
| `grpc` | experimental | yes | no | no | no | no | no |
| `snowflake` | experimental | yes | no | no | no | yes | no |
| `snowpipe` | experimental | yes | no | no | no | yes | no |
| `clickhouse` | experimental | yes | no | no | no | yes | no |
| `duckdb` | experimental | yes | yes | no | no | yes | no |
| `ducklake` | experimental | yes | yes | no | no | yes | no |
| `proto` | placeholder | no | no | no | no | no | no |
| `parquet` | placeholder | no | no | no | no | no | no |

These are guaranteed defaults. Options can reduce guarantees; startup validation resolves configured capabilities before execution.
