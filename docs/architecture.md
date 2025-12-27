# Architecture

DuctStream is organized as a small set of composable subsystems designed for high‑throughput CDC and flexible deployment topologies.

## High-Level Flow

1. **Source** reads logical replication events from Postgres.
2. **Decoder** converts tuples into structured records with typed values and schema metadata.
3. **Runner** batches records and writes to one or more destinations.
4. **Checkpoint Store** persists LSNs for recovery and replay.
5. **Lifecycle Engine** manages flow state (created, running, paused, failed).
6. **Registry + DDL** store schema versions and DDL events for governance.
7. **Orchestrator (DBOS)** optionally schedules durable, periodic runs.

## Components

### Sources
- `connectors/sources/postgres` uses `pgoutput` logical replication.
- Records include `before`, `after`, and `unchanged` fields to handle TOAST and updates.

### Destinations
- Kafka and S3 destinations are implemented with wire‑format encoding.
- Each destination advertises capabilities and supported wire formats.

### Wire Formats
- Supported formats: Arrow, Parquet, Avro, Proto, JSON.
- The runner can enforce a single format across the source and all destinations.

### Workflow + Orchestration
- The Postgres-backed workflow engine stores flow metadata and lifecycle transitions.
- DBOS (optional) runs scheduled workflows and queues per‑flow executions.
- Worker mode runs a single flow in a dedicated process without DBOS.

### Schema Registry + DDL
- The registry stores schema snapshots and DDL events.
- DDL events can be gated for approval and marked applied.
- A catalog scanner can diff `pg_catalog` for schema drift or generated column changes.

## Deployment Modes

### API Server + Workers
- Run the API server (`ductstream`) for control plane.
- Run per‑flow workers (`ductstream-worker`) for data plane.

### DBOS Scheduling
- The API server can also run DBOS to schedule flow runs.
- Each scheduled run processes a batch and exits when no data is available.

## Data Model

- **Flow**: source + destinations + lifecycle state + wire format.
- **Record**: operation + timestamps + before/after data.
- **Checkpoint**: LSN + timestamp.
- **DDLEvent**: DDL text, schema diff plan, status.

## Extensibility

- Add connectors by implementing `connector.Source` or `connector.Destination`.
- Add wire formats by implementing `wire.Codec`.
- Add orchestration by implementing `workflow.Engine` and `workflow.FlowDispatcher`.
