# DuctStream

DuctStream is a Go-first CDC adapter for PostgreSQL logical replication. It is API-driven (gRPC + Terraform-friendly setup), supports multiple sources and destinations, and uses a durable workflow engine backed by Postgres. The focus is on performance, reliability, schema evolution, and observability.

## Goals
- High-throughput logical replication with checkpointing and recovery.
- Multi-source, multi-destination routing with lifecycle management.
- Efficient wire formats (Arrow, Parquet) and full DDL support.
- Best-in-class Go libraries with minimal custom reinvention.

## Status
Early scaffolding. Interfaces are being defined before full implementations.

## Examples
See `examples/README.md` for gRPC, Terraform, and worker usage examples.

Flow specs you can copy:
- `examples/flows/postgres_to_snowflake.json`
- `examples/flows/postgres_to_snowpipe.json`
- `examples/flows/postgres_to_duckdb.json`
- `examples/flows/postgres_to_clickhouse.json`
- `examples/flows/postgres_to_bufstream.json`

Snowpipe auto-ingest (upload-only) snippet:

```json
{
  "name": "snowpipe-out",
  "type": "snowpipe",
  "options": {
    "dsn": "user:pass@account/db/schema?role=SYSADMIN",
    "stage": "@my_external_stage",
    "format": "parquet",
    "auto_ingest": "true",
    "copy_on_write": "false"
  }
}
```

## Documentation
- `docs/usage.md` — operational usage and configuration.
- `docs/tutorials.md` — step-by-step tutorials.
- `docs/architecture.md` — system architecture overview.
- `docs/streams.md` — stream consumer operations and recovery playbooks.

## Helm (OCI via GHCR)
Once a tagged release is published, install via OCI Helm chart:

```bash
helm install ductstream oci://ghcr.io/josephjohncox/ductstream/charts/ductstream --version <tag>
```

An example values file is available at `charts/ductstream/values.example.yaml`.
For production-style values (ConfigMap + Secret pattern), see `charts/ductstream/values-prod.yaml`.

## License
PolyForm Noncommercial 1.0.0. Commercial use requires a separate license.
