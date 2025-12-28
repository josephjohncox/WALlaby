# WALlaby

![Wallaby logo. Kicking a WAL block](./wallaby-small.png)


WALlaby is a Go-first CDC adapter for PostgreSQL logical replication. It is API-driven (gRPC + Terraform-friendly setup), supports multiple sources and destinations, and uses a durable workflow engine backed by Postgres. The focus is on performance, reliability, schema evolution, and observability.

## Goals
- High-throughput logical replication with checkpointing and recovery.
- Multi-source, multi-destination routing with lifecycle management.
- Efficient wire formats (Arrow, Parquet) and full DDL support.
- Best-in-class Go libraries with minimal custom reinvention.

## Why WALlaby (vs Airbyte/Fivetran/Debezium/PeerDB/Sequin)
- Low‑latency Postgres CDC with fan‑out built in, so one source can serve many destinations reliably.
- Schema fidelity via binary wire formats and DDL capture/gating, so downstream schemas stay aligned.
- API‑driven control plane for flows and lifecycle (start/stop/resume, publication sync, backfill/replay) that’s easy to automate.
- Simple deployment: a small Go service or per‑flow workers (DBOS/K8s) with explicit checkpoints and predictable recovery.
- Designed for streaming and event‑driven pipelines without the operational weight of larger platforms.

Quick contrasts:
- Airbyte/Fivetran: broad connector catalogs and managed ELT; WALlaby prioritizes streaming CDC and direct control.
- Debezium: JVM + Kafka‑centric; WALlaby is a Go service with API‑driven lifecycle and multi‑sink fan‑out.
- PeerDB: strong warehouse replication; WALlaby is API‑first with desired‑state configuration and multi‑destination CDC.
- Sequin: API/webhook‑first; WALlaby includes that plus durable flows, DDL gating, and multiple sink types.

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
helm install wallaby oci://ghcr.io/josephjohncox/wallaby/charts/wallaby --version <tag>
```

An example values file is available at `charts/wallaby/values.example.yaml`.
For production-style values (ConfigMap + Secret pattern), see `charts/wallaby/values-prod.yaml`.

## License
PolyForm Noncommercial 1.0.0. Commercial use requires a separate license.
