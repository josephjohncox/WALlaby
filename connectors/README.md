# Connectors

Source and destination implementations live here. The initial focus is Postgres logical replication as a source, with multiple destinations.

## Sources
- `sources/postgres`: Postgres logical replication (pgoutput).

## Destinations
- `destinations/snowflake`
- `destinations/s3`
- `destinations/kafka`
- `destinations/grpc`
- `destinations/proto`
- `destinations/snowpipe`
- `destinations/parquet`
- `destinations/duckdb`
- `destinations/bufstream`
- `destinations/clickhouse`
