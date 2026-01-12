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
| `wallaby.grpc.requests.total` | Counter | `method`, `status` | Implemented |
| `wallaby.grpc.request.latency` | Histogram | `method` | Implemented |
| `wallaby.grpc.errors.total` | Counter | `method`, `code` | Implemented |

### Workflow Engine

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.flows.active` | UpDownCounter | - | Implemented |
| `wallaby.flow.state.transitions` | Counter | `from_state`, `to_state` | Implemented |
| `wallaby.flow.create.total` | Counter | - | Implemented |

### Checkpoint Store

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.checkpoint.get.latency` | Histogram | `backend` | Implemented |
| `wallaby.checkpoint.put.latency` | Histogram | `backend` | Implemented |

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
| `wallaby.source.replication.lag` | Gauge | `slot` | Implemented |
| `wallaby.source.read.latency` | Histogram | - | Implemented |

### Destination Connectors

| Metric | Type | Labels | Status |
|--------|------|--------|--------|
| `wallaby.destination.write.total` | Counter | `type` | Implemented |
| `wallaby.destination.ddl.applied` | Counter | `type` | Implemented |

## Tracing

OpenTelemetry tracing is available via spans:

| Span | Location | Description |
|------|----------|-------------|
| `stream.batch` | `pkg/stream/runner.go` | Parent span for each batch processed |
| `source.read` | `pkg/stream/runner.go` | Span wrapping source Read() call |
| `source.wait` | `connectors/sources/postgres/source.go` | Time waiting for first change |
| `source.process` | `connectors/sources/postgres/source.go` | Time processing changes in batch |
| `destination.write` | `pkg/stream/runner.go` | Span wrapping destination Write() calls |

### Trace Hierarchy

```
stream.batch (root)
├── source.read
│   ├── source.wait      ← waiting for data
│   └── source.process   ← processing changes
└── destination.write
```

### Span Attributes

**stream.batch:**
- `flow.id` - Flow identifier
- `source.type` - Source connector type
- `batch.records` - Number of records in batch
- `batch.schema` - Schema name
- `batch.latency_ms` - Total batch latency
- `destination.write_latency_ms` - Write latency

**source.read:**
- `records_count` - Number of records read
- `wait_ms` - Time waiting for first change
- `process_ms` - Time processing changes
- `checkpoint.lsn` - Checkpoint LSN

**source.wait:**
- `timeout` - Whether wait ended due to timeout

**source.process:**
- `records` - Number of records processed

**destination.write:**
- `destinations.count` - Number of destinations
- `latency_ms` - Write latency
