# DuctStream

DuctStream is a Go-first CDC adapter for PostgreSQL logical replication. It is API-driven (gRPC + Terraform-friendly setup), supports multiple sources and destinations, and uses a durable workflow engine backed by Postgres. The focus is on performance, reliability, schema evolution, and observability.

## Goals
- High-throughput logical replication with checkpointing and recovery.
- Multi-source, multi-destination routing with lifecycle management.
- Efficient wire formats (Arrow, Parquet) and full DDL support.
- Best-in-class Go libraries with minimal custom reinvention.

## Status
Early scaffolding. Interfaces are being defined before full implementations.

## License
PolyForm Noncommercial 1.0.0. Commercial use requires a separate license.
