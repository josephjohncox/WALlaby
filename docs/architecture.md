# Architecture

WALlaby is organized as a small set of composable subsystems designed for high‑throughput CDC and flexible deployment topologies.

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
- Kafka, S3, HTTP webhooks, and Postgres streams are implemented with wire‑format encoding.
- Each destination advertises capabilities and supported wire formats.

### Wire Formats
- Supported formats: Arrow, Parquet, Avro, Proto, JSON.
- The runner can enforce a single format across the source and all destinations.

### Workflow + Orchestration
- The Postgres-backed workflow engine stores flow metadata and lifecycle transitions.
- DBOS (optional) runs scheduled workflows and queues per‑flow executions.
- Worker mode runs a single flow in a dedicated process without DBOS.
- Fan‑out is explicit: each destination is written independently, and consumers can scale separately.

### Fan‑out vs Multiple Replication Slots
Fan‑out keeps a single logical replication slot per flow and multiplexes downstream writes. Compared to multiple slots:
- **Lower source load:** WAL is decoded once, not N times, reducing CPU and I/O on the primary.
- **Lower WAL retention risk:** one slot means one acknowledged LSN; fewer slots reduces “stuck slot” risk and WAL bloat.
- **Consistent ordering + DDL gating:** a single read stream preserves ordering across destinations and shares DDL approval gates.
- **Centralized lifecycle:** one place to pause/resume/fail and coordinate backfill/stream handoff.

Multiple slots are still valid when destinations must be fully isolated (independent retention, separate lag policies, or separate ownership), but they multiply decoding cost and operational overhead.

### Schema Registry + DDL
- The registry stores schema snapshots and DDL events.
- DDL events can be gated for approval and marked applied.
- A catalog scanner can diff `pg_catalog` for schema drift or generated column changes.
- DDL gating is per‑flow; blocked gates emit log lines and an OpenTelemetry event (`ddl.gated`) for alerting.

## Deployment Modes

### API Server + Workers
- Run the API server (`wallaby`) for control plane.
- Run per‑flow workers (`wallaby-worker`) for data plane.

### DBOS Scheduling
- The API server can also run DBOS to schedule flow runs.
- Each scheduled run processes a batch and exits when no data is available.

## Data Model

- **Flow**: source + destinations + lifecycle state + wire format.
- **Record**: operation + timestamps + before/after data.
- **Checkpoint**: LSN + timestamp.
- **DDLEvent**: DDL text, schema diff plan, status.
- **StreamMessage**: stream event with visibility timeout and consumer group state.

## Extensibility

- Add connectors by implementing `connector.Source` or `connector.Destination`.
- Add wire formats by implementing `wire.Codec`.
- Add orchestration by implementing `workflow.Engine` and `workflow.FlowDispatcher`.

## Delivery Semantics
The runner acknowledges a source checkpoint after **all** destinations succeed by default. When `ack_policy=primary`, it acknowledges after the primary destination succeeds and queues secondary destinations for replay. This trades off per‑destination durability for source progress and lower WAL retention.
