# Observability

WALlaby supports OpenTelemetry metrics export via OTLP gRPC.

## Metrics

### Stream Runner

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.records.processed` | Counter | `flow_id` | Implemented |
| `wallaby.batches.processed` | Counter | `flow_id` | Implemented |
| `wallaby.batch.latency` | Histogram | `flow_id` | Implemented |
| `wallaby.destination.write.latency` | Histogram | `flow_id` | Implemented |
| `wallaby.batch.records` | Histogram | `flow_id` | Implemented |
| `wallaby.errors.total` | Counter | `error_type` | Implemented |
| `wallaby.checkpoints.commits` | Counter | `flow_id` | Implemented |

**Error Types:**
- `source_read` - Error reading from source
- `source_ack` - Error acknowledging source
- `destination_write` - Error writing to destination
- `checkpoint_persist` - Error persisting checkpoint

**Histogram Buckets:**
- Latency (ms): `1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000`
- Count: `1, 10, 50, 100, 500, 1000, 5000, 10000`

### gRPC API Server

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.grpc.requests.total` | Counter | `method`, `status` | Planned |
| `wallaby.grpc.request.latency` | Histogram | `method` | Planned |
| `wallaby.grpc.errors.total` | Counter | `method`, `code` | Planned |

### Workflow Engine

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.flows.active` | Gauge | - | Planned |
| `wallaby.flow.state.transitions` | Counter | `from_state`, `to_state` | Planned |
| `wallaby.flow.create.total` | Counter | - | Planned |

### Checkpoint Store

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.checkpoint.get.latency` | Histogram | `backend` | Planned |
| `wallaby.checkpoint.put.latency` | Histogram | `backend` | Planned |

### Orchestrators

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.dbos.jobs.scheduled` | Counter | - | Planned |
| `wallaby.dbos.jobs.completed` | Counter | `status` | Planned |
| `wallaby.k8s.jobs.launched` | Counter | - | Planned |
| `wallaby.k8s.jobs.failed` | Counter | `reason` | Planned |

### Source Connectors

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.source.replication.lag` | Gauge | `slot` | Planned |
| `wallaby.source.read.latency` | Histogram | - | Planned |

### Destination Connectors

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.destination.write.total` | Counter | `type` | Planned |
| `wallaby.destination.ddl.applied` | Counter | `type` | Planned |

## Tracing

Basic OpenTelemetry tracing is available via spans:

| Span | Location | Description |
|------|----------|-------------|
| `flow.run` | `internal/runner/flow_runner.go` | Parent span for flow execution |
| `stream.batch` | `pkg/stream/runner.go` | Span for each batch processed |
